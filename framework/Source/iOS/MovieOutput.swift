import AVFoundation

public protocol AudioEncodingTarget {
    func activateAudioTrack() throws
    func processAudioBuffer(_ sampleBuffer:CMSampleBuffer, shouldInvalidateSampleWhenDone:Bool)
    // Note: This is not used for synchronized encoding.
    func readyForNextAudioBuffer() -> Bool
}

public protocol MovieOutputDelegate: class {
    func movieOutputDidStartWriting(_ movieOutput: MovieOutput, at time: CMTime)
    func movieOutputWriterError(_ movieOutput: MovieOutput, error: Error)
}

public extension MovieOutputDelegate {
    func movieOutputDidStartWriting(_ movieOutput: MovieOutput, at time: CMTime) {}
    func movieOutputWriterError(_ movieOutput: MovieOutput, error: Error) {}
}

public enum MovieOutputError: Error, CustomStringConvertible {
    case startWritingError(assetWriterError: Error?)
    case pixelBufferPoolNilError
    case activeAudioTrackError
    
    public var errorDescription: String {
        switch self {
        case .startWritingError(let assetWriterError):
            return "Could not start asset writer: \(String(describing: assetWriterError))"
        case .pixelBufferPoolNilError:
            return "Asset writer pixel buffer pool was nil. Make sure that your output file doesn't already exist."
        case .activeAudioTrackError:
            return "cannot active audio track when assetWriter status is not 0"
        }
    }
    
    public var description: String {
        return "<\(type(of: self)): errorDescription = \(self.errorDescription)>"
    }
}

public enum MovieOutputState: String {
    case unknown
    case idle
    case caching
    case writing
    case finished
    case canceled
}

public class MovieOutput: ImageConsumer, AudioEncodingTarget {
    private static let assetWriterQueue = DispatchQueue(label: "com.GPUImage2.MovieOutput.assetWriterQueue", qos: .userInitiated)
    public let sources = SourceContainer()
    public let maximumInputs:UInt = 1
    
    public weak var delegate: MovieOutputDelegate?
    
    private let assetWriter:AVAssetWriter
    let assetWriterVideoInput:AVAssetWriterInput
    var assetWriterAudioInput:AVAssetWriterInput?
    private let assetWriterPixelBufferInput:AVAssetWriterInputPixelBufferAdaptor
    public let size: Size
    private let colorSwizzlingShader:ShaderProgram
    var videoEncodingIsFinished = false
    var audioEncodingIsFinished = false
    var markIsFinishedAfterProcessing = false
    private var startFrameTime: CMTime?
    public private(set) var recordedDuration: CMTime?
    private var previousFrameTime: CMTime?
    var encodingLiveVideo:Bool {
        didSet {
            assetWriterVideoInput.expectsMediaDataInRealTime = encodingLiveVideo
            assetWriterAudioInput?.expectsMediaDataInRealTime = encodingLiveVideo
        }
    }
    public private(set) var pixelBuffer:CVPixelBuffer? = nil
    public var dropFirstFrames: Int = 0
    public var waitUtilDataIsReadyForLiveVideo = false
    public private(set) var state = MovieOutputState.unknown
    public private(set) var renderFramebuffer:Framebuffer!
    
    public private(set) var audioSettings:[String:Any]? = nil
    public private(set) var audioSourceFormatHint:CMFormatDescription?
    
    public let movieProcessingContext:OpenGLContext
    public private(set) var videoPixelBufferCache = [(CVPixelBuffer, CMTime)]()
    public private(set) var videoSampleBufferCache = NSMutableArray()
    public private(set) var audioSampleBufferCache = [CMSampleBuffer]()
    public private(set) var cacheBuffersDuration: TimeInterval = 0
    
    var synchronizedEncodingDebug = false
    public private(set) var totalFramesAppended:Int = 0
    private var observations = [NSKeyValueObservation]()
    
    deinit {
        observations.forEach { $0.invalidate() }
        print("movie output deinit \(assetWriter.outputURL)")
    }
    var shouldWaitForEncoding: Bool {
        return !encodingLiveVideo || waitUtilDataIsReadyForLiveVideo
    }
    var preferredTransform: CGAffineTransform?
    
    public init(URL:Foundation.URL, size:Size, fileType:AVFileType = .mov, liveVideo:Bool = false, videoSettings:[String:Any]? = nil, videoNaturalTimeScale:CMTimeScale? = nil, audioSettings:[String:Any]? = nil, audioSourceFormatHint:CMFormatDescription? = nil) throws {

        print("movie output init \(URL)")

        imageProcessingShareGroup = sharedImageProcessingContext.context.sharegroup
        let movieProcessingContext = OpenGLContext()
        
        if movieProcessingContext.supportsTextureCaches() {
            self.colorSwizzlingShader = movieProcessingContext.passthroughShader
        } else {
            self.colorSwizzlingShader = crashOnShaderCompileFailure("MovieOutput"){try movieProcessingContext.programForVertexShader(defaultVertexShaderForInputs(1), fragmentShader:ColorSwizzlingFragmentShader)}
        }
        
        self.size = size
        
        assetWriter = try AVAssetWriter(url:URL, fileType:fileType)
        assetWriter.shouldOptimizeForNetworkUse = true
        
        var localSettings:[String:Any]
        if let videoSettings = videoSettings {
            localSettings = videoSettings
        } else {
            localSettings = [String:Any]()
        }
        
        localSettings[AVVideoWidthKey] = localSettings[AVVideoWidthKey] ?? size.width
        localSettings[AVVideoHeightKey] = localSettings[AVVideoHeightKey] ?? size.height
        localSettings[AVVideoCodecKey] =  localSettings[AVVideoCodecKey] ?? AVVideoCodecH264
        
        assetWriterVideoInput = AVAssetWriterInput(mediaType:.video, outputSettings:localSettings)
        assetWriterVideoInput.expectsMediaDataInRealTime = liveVideo
        
        // You should provide a naturalTimeScale if you have one for the current media.
        // Otherwise the asset writer will choose one for you and it may result in misaligned frames.
        if let naturalTimeScale = videoNaturalTimeScale {
            assetWriter.movieTimeScale = naturalTimeScale
            assetWriterVideoInput.mediaTimeScale = naturalTimeScale
            // This is set to make sure that a functional movie is produced, even if the recording is cut off mid-stream. Only the last second should be lost in that case.
            assetWriter.movieFragmentInterval = CMTime(seconds: 1, preferredTimescale: naturalTimeScale)
        }
        else {
            assetWriter.movieFragmentInterval = CMTime(seconds: 1, preferredTimescale: 1000)
        }
        
        encodingLiveVideo = liveVideo
        
        // You need to use BGRA for the video in order to get realtime encoding. I use a color-swizzling shader to line up glReadPixels' normal RGBA output with the movie input's BGRA.
        let sourcePixelBufferAttributesDictionary:[String:Any] = [kCVPixelBufferPixelFormatTypeKey as String:Int32(kCVPixelFormatType_32BGRA),
                                                                        kCVPixelBufferWidthKey as String:self.size.width,
                                                                        kCVPixelBufferHeightKey as String:self.size.height]
        
        assetWriterPixelBufferInput = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput:assetWriterVideoInput, sourcePixelBufferAttributes:sourcePixelBufferAttributesDictionary)
        assetWriter.add(assetWriterVideoInput)
        
        self.audioSettings = audioSettings
        self.audioSourceFormatHint = audioSourceFormatHint
        
        self.movieProcessingContext = movieProcessingContext
    }
    
    public func startRecording(sync: Bool = false, manualControlState: Bool = false, _ completionCallback:((_ started: Bool, _ error: Error?) -> Void)? = nil) {
        // Don't do this work on the movieProcessingContext queue so we don't block it.
        // If it does get blocked framebuffers will pile up from live video and after it is no longer blocked (this work has finished)
        // we will be able to accept framebuffers but the ones that piled up will come in too quickly resulting in most being dropped.
        let block = { () -> Void in
            do {
                guard self.assetWriter.status != .cancelled else {
                    throw MovieOutputError.startWritingError(assetWriterError: nil)
                }
                
                let observation = self.assetWriter.observe(\.error) { [weak self] writer, _ in
                    guard let self = self, let error = writer.error else { return }
                    self.delegate?.movieOutputWriterError(self, error: error)
                }
                self.observations.append(observation)
                
                if let preferredTransform = self.preferredTransform {
                    self.assetWriterVideoInput.transform = preferredTransform
                }
                print("MovieOutput starting writing...")
                var success = false
                try NSObject.catchException {
                    success = self.assetWriter.startWriting()
                }
                
                if(!success) {
                    throw MovieOutputError.startWritingError(assetWriterError: self.assetWriter.error)
                }
                
                guard self.assetWriterPixelBufferInput.pixelBufferPool != nil else {
                    /*
                     When the pixelBufferPool returns nil, check the following:
                     1. the the output file of the AVAssetsWriter doesn't exist.
                     2. use the pixelbuffer after calling startSessionAtTime: on the AVAssetsWriter.
                     3. the settings of AVAssetWriterInput and AVAssetWriterInputPixelBufferAdaptor are correct.
                     4. the present times of appendPixelBuffer uses are not the same.
                     https://stackoverflow.com/a/20110179/1275014
                     */
                    throw MovieOutputError.pixelBufferPoolNilError
                }
                
                if !manualControlState {
                    self.state = .writing
                } else {
                    self.state = .idle
                }
                
                print("MovieOutput started writing")
                
                completionCallback?(true, nil)
            } catch {
                self.assetWriter.cancelWriting()
                
                print("MovieOutput failed to start writing. error:\(error)")
                
                completionCallback?(false, error)
            }
        }
        
        if sync {
            block()
        } else {
            MovieOutput.assetWriterQueue.async(execute: block)
        }
    }
    
    public func startCachingWithoutWriting(duration: TimeInterval) {
        print("MovieOutput starting caching. duration:\(duration)")
        state = .caching
        cacheBuffersDuration = duration
    }
    
    public func finishCachingAndStartWriting() {
        print("MovieOutput finish caching and start writing. cached buffer: videoPixelBuffers:\(videoPixelBufferCache.count) audioSampleBuffer:\(audioSampleBufferCache.count) videoSampleBuffers:\(videoSampleBufferCache.count)")
        state = .writing
    }
    
    public func finishRecording(_ completionCallback:(() -> Void)? = nil) {
        MovieOutput.assetWriterQueue.async {
            self._cleanBufferCaches()
            guard self.state == .writing,
                self.assetWriter.status == .writing else {
                    completionCallback?()
                    return
            }
            
            self.audioEncodingIsFinished = true
            self.videoEncodingIsFinished = true
            
            self.state = .finished
            
            if let lastFrame = self.previousFrameTime {
                // Resolve black frames at the end. Without this the end timestamp of the session's samples could be either video or audio.
                // Documentation: "You do not need to call this method; if you call finishWriting without
                // calling this method, the session's effective end time will be the latest end timestamp of
                // the session's samples (that is, no samples will be edited out at the end)."
                self.assetWriter.endSession(atSourceTime: lastFrame)
            }
            
            if let lastFrame = self.previousFrameTime, let startFrame = self.startFrameTime {
                self.recordedDuration = lastFrame - startFrame
            }
            self.assetWriter.finishWriting {
                completionCallback?()
            }
            print("MovieOutput finished writing. Total frames appended:\(self.totalFramesAppended)")
        }
    }
    
    public func cancelRecording(_ completionCallback:(() -> Void)? = nil) {
        MovieOutput.assetWriterQueue.async { [weak self] in
            guard let self = self else {
                completionCallback?()
                return
            }
            self._cleanBufferCaches()
            self.state = .canceled
            self.audioEncodingIsFinished = true
            self.videoEncodingIsFinished = true
            if self.assetWriter.status == .writing {
                self.assetWriter.cancelWriting()
            }
            completionCallback?()
            print("MovieOutput cancel writing")
        }
    }
    
    private func _cleanBufferCaches() {
        movieProcessingContext.runOperationAsynchronously { [weak self] in
            self?.videoPixelBufferCache.removeAll()
            self?.videoSampleBufferCache.removeAllObjects()
            self?.audioSampleBufferCache.removeAll()
        }
    }
    
    public func newFramebufferAvailable(_ framebuffer:Framebuffer, fromSourceIndex:UInt) {
        glFinish();
        
        let work = { [weak self] in
            if self?.state == .caching {
                self?._renderAndCache(framebuffer: framebuffer)
            } else {
                self?._processPixelBufferCache(framebuffer: framebuffer)
            }
            sharedImageProcessingContext.runOperationAsynchronously {
                framebuffer.unlock()
            }
        }

        if(self.encodingLiveVideo) {
            // This is done asynchronously to reduce the amount of work done on the sharedImageProcessingContext que
            // so we can decrease the risk of frames being dropped by the camera. I believe it is unlikely a backlog of framebuffers will occur
            // since the framebuffers come in much slower than during synchronized encoding.
            movieProcessingContext.runOperationAsynchronously(work)
        }
        else {
            // This is done synchronously to prevent framebuffers from piling up during synchronized encoding.
            // If we don't force the sharedImageProcessingContext queue to wait for this frame to finish processing it will
            // keep sending frames whenever isReadyForMoreMediaData = true but the movieProcessingContext queue would run when the system wants it to.
            movieProcessingContext.runOperationSynchronously(work)
        }
    }
    
    private func _renderAndCache(framebuffer: Framebuffer) {
        // Discard first n frames
        if dropFirstFrames > 0 {
            dropFirstFrames -= 1
            synchronizedEncodingDebugPrint("Drop one frame. Left dropFirstFrames:\(dropFirstFrames)")
            return
        }
        guard state == .caching || state == .writing, assetWriter.status == .writing, !videoEncodingIsFinished else {
            synchronizedEncodingDebugPrint("Guard fell through, dropping frame")
            return
        }
        guard let frameTime = framebuffer.timingStyle.timestamp?.asCMTime else { return }
        pixelBuffer = nil
        let pixelBufferStatus = CVPixelBufferPoolCreatePixelBuffer(nil, assetWriterPixelBufferInput.pixelBufferPool!, &pixelBuffer)
        guard pixelBuffer != nil && pixelBufferStatus == kCVReturnSuccess else {
            print("[Caching] WARNING: Unable to create pixel buffer, dropping frame")
            return
        }
        do {
            try renderIntoPixelBuffer(pixelBuffer!, framebuffer:framebuffer)
            videoPixelBufferCache.append((pixelBuffer!, frameTime))
            print("[Caching] appended new buffer at:\(frameTime.seconds)")
            while let firstBufferTime = videoPixelBufferCache.first?.1, CMTimeSubtract(frameTime, firstBufferTime).seconds > cacheBuffersDuration {
                let firstBuffer = videoPixelBufferCache.removeFirst()
                print("[Caching] caching video duration reach up to:\(cacheBuffersDuration) dropped frame at:\(firstBuffer.1.seconds)")
            }
        } catch {
            print("[Caching] WARNING: Trouble appending pixel buffer at time: \(frameTime) \(error)")
        }
        
        CVPixelBufferUnlockBaseAddress(pixelBuffer!, CVPixelBufferLockFlags(rawValue:CVOptionFlags(0)))
    }
    
    private func _processPixelBufferCache(framebuffer: Framebuffer) {
        // Discard first n frames
        if dropFirstFrames > 0 {
            dropFirstFrames -= 1
            synchronizedEncodingDebugPrint("Drop one frame. Left dropFirstFrames:\(self.dropFirstFrames)")
            return
        }
        
        guard state == .caching || state == .writing, assetWriter.status == .writing, !videoEncodingIsFinished else {
            synchronizedEncodingDebugPrint("Guard fell through, dropping frame")
            return
        }
        
        // Ignore still images and other non-video updates (do I still need this?)
        guard let frameTime = framebuffer.timingStyle.timestamp?.asCMTime else { return }
        
        // If two consecutive times with the same value are added to the movie, it aborts recording, so I bail on that case.
        guard (frameTime != previousFrameTime) else { return }
        
        if (previousFrameTime == nil) {
            // This resolves black frames at the beginning. Any samples recieved before this time will be edited out.
            let startFrameTime = videoPixelBufferCache.first?.1 ?? frameTime
            assetWriter.startSession(atSourceTime: startFrameTime)
            self.startFrameTime = startFrameTime
            delegate?.movieOutputDidStartWriting(self, at: startFrameTime)
        }
        
        previousFrameTime = frameTime

        guard (assetWriterVideoInput.isReadyForMoreMediaData || self.shouldWaitForEncoding) else {
            print("WARNING: Had to drop a frame at time \(frameTime)")
            return
        }
        
        while !assetWriterVideoInput.isReadyForMoreMediaData && shouldWaitForEncoding && !videoEncodingIsFinished {
            synchronizedEncodingDebugPrint("Video waiting...")
            // Better to poll isReadyForMoreMediaData often since when it does become true
            // we don't want to risk letting framebuffers pile up in between poll intervals.
            usleep(100000) // 0.1 seconds
            if markIsFinishedAfterProcessing {
                synchronizedEncodingDebugPrint("set videoEncodingIsFinished to true after processing")
                markIsFinishedAfterProcessing = false
                videoEncodingIsFinished = true
            }
        }
        
        if !videoPixelBufferCache.isEmpty {
            // If videoPixelBufferCache has too much buffers, processing current buffers to ease the burden of videoInput, or it will crash
            _appendPixelBuffersFromCache()
            _renderAndCache(framebuffer: framebuffer)
            _appendPixelBuffersFromCache()
        } else {
            _renderAndCache(framebuffer: framebuffer)
            _appendPixelBuffersFromCache()
        }
        
        if videoEncodingIsFinished {
            assetWriterVideoInput.markAsFinished()
        }
    }
    
    private func _appendPixelBuffersFromCache() {
        var appendedBufferCount = 0
        do {
            try NSObject.catchException {
                // Drain all cached buffers at first
                if !self.videoPixelBufferCache.isEmpty {
                    for (i, (buffer, time)) in self.videoPixelBufferCache.enumerated() {
                        print("appending video pixel buffer \(i+1)/\(self.videoPixelBufferCache.count) at:\(time.seconds)")
                        if (!self.assetWriterPixelBufferInput.append(buffer, withPresentationTime: time)) {
                            print("WARNING: Trouble appending pixel buffer at time: \(time) \(String(describing: self.assetWriter.error))")
                            break
                        }
                        appendedBufferCount += 1
                        if(self.synchronizedEncodingDebug) {
                            self.totalFramesAppended += 1
                        }
                    }
                }
            }
        }
        catch {
            print("WARNING: Trouble appending pixel buffer \(error)")
        }
        videoPixelBufferCache.removeFirst(appendedBufferCount)
    }
    
    func renderIntoPixelBuffer(_ pixelBuffer:CVPixelBuffer, framebuffer:Framebuffer) throws {
        // Is this the first pixel buffer we have recieved?
        if(renderFramebuffer == nil) {
            CVBufferSetAttachment(pixelBuffer, kCVImageBufferColorPrimariesKey, kCVImageBufferColorPrimaries_ITU_R_709_2, .shouldPropagate)
            CVBufferSetAttachment(pixelBuffer, kCVImageBufferYCbCrMatrixKey, kCVImageBufferYCbCrMatrix_ITU_R_601_4, .shouldPropagate)
            CVBufferSetAttachment(pixelBuffer, kCVImageBufferTransferFunctionKey, kCVImageBufferTransferFunction_ITU_R_709_2, .shouldPropagate)
        }
        
        let bufferSize = GLSize(self.size)
        var cachedTextureRef:CVOpenGLESTexture? = nil
        let _ = CVOpenGLESTextureCacheCreateTextureFromImage(kCFAllocatorDefault, self.movieProcessingContext.coreVideoTextureCache, pixelBuffer, nil, GLenum(GL_TEXTURE_2D), GL_RGBA, bufferSize.width, bufferSize.height, GLenum(GL_BGRA), GLenum(GL_UNSIGNED_BYTE), 0, &cachedTextureRef)
        let cachedTexture = CVOpenGLESTextureGetName(cachedTextureRef!)
        
        renderFramebuffer = try Framebuffer(context:self.movieProcessingContext, orientation:.portrait, size:bufferSize, textureOnly:false, overriddenTexture:cachedTexture)
        
        renderFramebuffer.activateFramebufferForRendering()
        clearFramebufferWithColor(Color.black)
        CVPixelBufferLockBaseAddress(pixelBuffer, CVPixelBufferLockFlags(rawValue:CVOptionFlags(0)))
        renderQuadWithShader(colorSwizzlingShader, uniformSettings:ShaderUniformSettings(), vertexBufferObject:movieProcessingContext.standardImageVBO, inputTextures:[framebuffer.texturePropertiesForOutputRotation(.noRotation)], context: movieProcessingContext)
        
        if movieProcessingContext.supportsTextureCaches() {
            glFinish()
        } else {
            glReadPixels(0, 0, renderFramebuffer.size.width, renderFramebuffer.size.height, GLenum(GL_RGBA), GLenum(GL_UNSIGNED_BYTE), CVPixelBufferGetBaseAddress(pixelBuffer))
        }
    }
    
    // MARK: Append buffer directly from CMSampleBuffer
    public func processVideoBuffer(_ sampleBuffer: CMSampleBuffer, shouldInvalidateSampleWhenDone:Bool) {
        let cache = {
            guard self.state == .caching || self.state == .writing,
                self.assetWriter.status == .writing,
                !self.videoEncodingIsFinished else {
                    self.synchronizedEncodingDebugPrint("Guard fell through, dropping frame")
                    return
            }
            
            let frameTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
            
            self.videoSampleBufferCache.add(sampleBuffer)
            print("[Caching] cache new video sample buffer at:\(frameTime.seconds)")
            if self.videoSampleBufferCache.count >= 13 && self.encodingLiveVideo {
                // Be careful of caching too much sample buffers from camera captureOutput. iOS has a hard limit of camera buffer count: 15.
                print("WARNING: almost reach system buffer limit: \(self.videoSampleBufferCache.count)/15")
            }
            while let firstBuffer = self.videoSampleBufferCache.firstObject, CMTimeSubtract(frameTime, CMSampleBufferGetPresentationTimeStamp(firstBuffer as! CMSampleBuffer)).seconds > self.cacheBuffersDuration {
                self.videoSampleBufferCache.removeObject(at: 0)
                print("[Caching] caching video duration reach up to:\(self.cacheBuffersDuration) dropped frame at:\(CMSampleBufferGetPresentationTimeStamp(firstBuffer as! CMSampleBuffer).seconds)")
            }
        }
        
        let work = {
            defer {
                if(shouldInvalidateSampleWhenDone) {
                    CMSampleBufferInvalidate(sampleBuffer)
                }
            }
            
            guard self.state == .caching || self.state == .writing,
                self.assetWriter.status == .writing,
                !self.videoEncodingIsFinished else {
                    self.synchronizedEncodingDebugPrint("Guard fell through, dropping frame")
                    return
            }
            
            let frameTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
            
            // If two consecutive times with the same value are added to the movie, it aborts recording, so I bail on that case.
            guard (frameTime != self.previousFrameTime) else { return }
            
            self.videoSampleBufferCache.add(sampleBuffer)
            
            if (self.previousFrameTime == nil) {
                // This resolves black frames at the beginning. Any samples recieved before this time will be edited out.
                let startFrameTime = self.videoSampleBufferCache.firstObject.map { CMSampleBufferGetPresentationTimeStamp($0 as! CMSampleBuffer) } ?? frameTime
                self.assetWriter.startSession(atSourceTime: startFrameTime)
                self.startFrameTime = startFrameTime
                self.delegate?.movieOutputDidStartWriting(self, at: startFrameTime)
            }
            
            self.previousFrameTime = frameTime
            
            guard (self.assetWriterVideoInput.isReadyForMoreMediaData || self.shouldWaitForEncoding) else {
                print("Had to drop a frame at time \(frameTime)")
                return
            }
            
            while(!self.assetWriterVideoInput.isReadyForMoreMediaData && self.shouldWaitForEncoding && !self.videoEncodingIsFinished) {
                self.synchronizedEncodingDebugPrint("Video waiting...")
                // Better to poll isReadyForMoreMediaData often since when it does become true
                // we don't want to risk letting framebuffers pile up in between poll intervals.
                usleep(100000) // 0.1 seconds
            }
            
            var appendedBufferCount = 0
            do {
                try NSObject.catchException {
                    // Drain all cached buffers at first
                    for (i, sampleBufferObject) in self.videoSampleBufferCache.enumerated() {
                        let sampleBuffer = sampleBufferObject as! CMSampleBuffer
                        let time = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
                        print("appending video sample buffer \(i+1)/\(self.videoSampleBufferCache.count) at:\(time.seconds)")
                        guard let buffer = CMSampleBufferGetImageBuffer(sampleBuffer) else {
                            print("WARNING: Cannot get pixel buffer from sampleBuffer:\(sampleBuffer)")
                            break
                        }
                        if (!self.assetWriterPixelBufferInput.append(buffer, withPresentationTime: time)) {
                            print("WARNING: Trouble appending pixel buffer at time: \(time) \(String(describing: self.assetWriter.error))")
                            break
                        }
                        appendedBufferCount += 1
                        if(self.synchronizedEncodingDebug) {
                            self.totalFramesAppended += 1
                        }
                    }
                }
            }
            catch {
                print("WARNING: Trouble appending video sample buffer at time: \(frameTime) \(error)")
            }
            self.videoSampleBufferCache.removeObjects(in: NSRange(0..<appendedBufferCount))
        }
        
        if(self.encodingLiveVideo) {
            movieProcessingContext.runOperationSynchronously(state == .caching ? cache : work)
        }
        else {
            (state == .caching ? cache : work)()
        }
    }
    
    // MARK: -
    // MARK: Audio support
    
    public func activateAudioTrack() throws {
        guard assetWriter.status != .writing && assetWriter.status != .completed else {
            throw MovieOutputError.activeAudioTrackError
        }
        assetWriterAudioInput = AVAssetWriterInput(mediaType:.audio, outputSettings:self.audioSettings, sourceFormatHint:self.audioSourceFormatHint)

        assetWriter.add(assetWriterAudioInput!)
        assetWriterAudioInput?.expectsMediaDataInRealTime = encodingLiveVideo
    }
    
    public func processAudioBuffer(_ sampleBuffer:CMSampleBuffer, shouldInvalidateSampleWhenDone:Bool) {
        let cache = {
            guard self.state == .caching || self.state == .writing,
                self.assetWriter.status == .writing,
                !self.audioEncodingIsFinished else {
                    self.synchronizedEncodingDebugPrint("Guard fell through, dropping audio sample")
                    if shouldInvalidateSampleWhenDone {
                        CMSampleBufferInvalidate(sampleBuffer)
                    }
                    return
            }
            let frameTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
            self.audioSampleBufferCache.append(sampleBuffer)
            while let firstBuffer = self.audioSampleBufferCache.first, CMTimeSubtract(frameTime, CMSampleBufferGetPresentationTimeStamp(firstBuffer)).seconds > self.cacheBuffersDuration {
                _ = self.audioSampleBufferCache.removeFirst()
                print("[Caching] caching audio duration reach up to:\(self.cacheBuffersDuration) dropped frame at:\(CMSampleBufferGetPresentationTimeStamp(firstBuffer).seconds)")
            }
        }
        
        let work = {
            guard self.state == .caching || self.state == .writing,
                self.assetWriter.status == .writing,
                !self.audioEncodingIsFinished,
                let assetWriterAudioInput = self.assetWriterAudioInput else {
                    self.synchronizedEncodingDebugPrint("Guard fell through, dropping audio sample")
                    if shouldInvalidateSampleWhenDone {
                        CMSampleBufferInvalidate(sampleBuffer)
                    }
                    return
            }
            
            let currentSampleTime = CMSampleBufferGetOutputPresentationTimeStamp(sampleBuffer)
            self.audioSampleBufferCache.append(sampleBuffer)
            
            guard (assetWriterAudioInput.isReadyForMoreMediaData || self.shouldWaitForEncoding) else {
                print("Had to delay a audio sample at time \(currentSampleTime)")
                return
            }
            
            while(!assetWriterAudioInput.isReadyForMoreMediaData && self.shouldWaitForEncoding && !self.audioEncodingIsFinished) {
                self.synchronizedEncodingDebugPrint("Audio waiting...")
                usleep(100000)
                if !assetWriterAudioInput.isReadyForMoreMediaData {
                    self.synchronizedEncodingDebugPrint("Audio still not ready, skip this runloop...")
                    return
                }
            }
            
            guard self.previousFrameTime != nil else {
                self.synchronizedEncodingDebugPrint("Add audio sample to pending queue but first video frame is not ready yet. Time:\(CMTimeGetSeconds(currentSampleTime))")
                return
            }
            
            self.synchronizedEncodingDebugPrint("Process audio sample output. Time:\(CMTimeGetSeconds(currentSampleTime))")
            
            var appendedBufferCount = 0
            do {
                try NSObject.catchException {
                    for (i, audioBuffer) in self.audioSampleBufferCache.enumerated() {
                        print("appending audio buffer \(i+1)/\(self.audioSampleBufferCache.count) at:\(CMSampleBufferGetOutputPresentationTimeStamp(audioBuffer).seconds)")
                        if (!assetWriterAudioInput.append(audioBuffer)) {
                            print("WARNING: Trouble appending audio sample buffer: \(String(describing: self.assetWriter.error))")
                            break
                        }
                        appendedBufferCount += 1
                        if shouldInvalidateSampleWhenDone {
                            CMSampleBufferInvalidate(audioBuffer)
                        }
                    }
                }
            }
            catch {
                print("WARNING: Trouble appending audio sample buffer: \(error)")
            }
            self.audioSampleBufferCache.removeFirst(appendedBufferCount)
        }
        
        if(self.encodingLiveVideo) {
            movieProcessingContext.runOperationSynchronously(state == .caching ? cache : work)
        }
        else {
            (state == .caching ? cache : work)()
        }
    }
    
    public func flushPendingAudioBuffers(shouldInvalidateSampleWhenDone: Bool) {
        guard let lastBuffer = audioSampleBufferCache.popLast() else { return }
        processAudioBuffer(lastBuffer, shouldInvalidateSampleWhenDone: shouldInvalidateSampleWhenDone)
    }
    
    // Note: This is not used for synchronized encoding, only live video.
    public func readyForNextAudioBuffer() -> Bool {
        return true
    }
    
    func synchronizedEncodingDebugPrint(_ string: String) {
        if(synchronizedEncodingDebug && !encodingLiveVideo) { print(string) }
    }
}


public extension Timestamp {
    init(_ time:CMTime) {
        self.value = time.value
        self.timescale = time.timescale
        self.flags = TimestampFlags(rawValue:time.flags.rawValue)
        self.epoch = time.epoch
    }
    
    var asCMTime:CMTime {
        get {
            return CMTimeMakeWithEpoch(value: value, timescale: timescale, epoch: epoch)
        }
    }
}
