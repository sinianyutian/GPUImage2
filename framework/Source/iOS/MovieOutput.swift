import AVFoundation
import CoreImage
import UIKit

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
    case writing
    case finished
    case canceled
}

public class MovieOutput: ImageConsumer, AudioEncodingTarget {
    private static let assetWriterQueue = DispatchQueue(label: "com.GPUImage2.MovieOutput.assetWriterQueue", qos: .userInitiated)
    public let sources = SourceContainer()
    public let maximumInputs:UInt = 1
    
    public weak var delegate: MovieOutputDelegate?
    
    public let url: URL
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
    private var ciFilter: CILookupFilter?
    private var cpuCIContext: CIContext?
    public private(set) var pixelBuffer:CVPixelBuffer? = nil
    public var waitUtilDataIsReadyForLiveVideo = false
    public private(set) var renderFramebuffer:Framebuffer!
    
    public private(set) var audioSettings:[String:Any]? = nil
    public private(set) var audioSourceFormatHint:CMFormatDescription?
    
    public static let movieProcessingContext: OpenGLContext = {
        var context: OpenGLContext?
        imageProcessingShareGroup = sharedImageProcessingContext.context.sharegroup
        sharedImageProcessingContext.runOperationSynchronously {
            context = OpenGLContext(queueLabel: "com.GPUImage2.MovieOutput.imageProcess")
        }
        imageProcessingShareGroup = nil
        return context!
    }()
    public private(set) var audioSampleBufferCache = [CMSampleBuffer]()
    public private(set) var cacheBuffersDuration: TimeInterval = 0
    public let disablePixelBufferAttachments: Bool
    private var pixelBufferPoolSemaphore = DispatchSemaphore(value: 1)
    
    var synchronizedEncodingDebug = false
    public private(set) var totalVideoFramesAppended = 0
    public private(set) var totalAudioFramesAppended = 0
    private var observations = [NSKeyValueObservation]()
    
    deinit {
        observations.forEach { $0.invalidate() }
        print("movie output deinit \(assetWriter.outputURL)")
    }
    var shouldWaitForEncoding: Bool {
        return !encodingLiveVideo || waitUtilDataIsReadyForLiveVideo
    }
    var preferredTransform: CGAffineTransform?
    private var isProcessing = false
    
    public init(URL:Foundation.URL, size:Size, fileType:AVFileType = .mov, liveVideo:Bool = false, videoSettings:[String:Any]? = nil, videoNaturalTimeScale:CMTimeScale? = nil, optimizeForNetworkUse: Bool = false, disablePixelBufferAttachments: Bool = true, audioSettings:[String:Any]? = nil, audioSourceFormatHint:CMFormatDescription? = nil) throws {

        print("movie output init \(URL)")
        self.url = URL
        
        if sharedImageProcessingContext.supportsTextureCaches() {
            self.colorSwizzlingShader = sharedImageProcessingContext.passthroughShader
        } else {
            self.colorSwizzlingShader = crashOnShaderCompileFailure("MovieOutput"){try sharedImageProcessingContext.programForVertexShader(defaultVertexShaderForInputs(1), fragmentShader:ColorSwizzlingFragmentShader)}
        }
        
        self.size = size
        
        assetWriter = try AVAssetWriter(url:URL, fileType:fileType)
        if optimizeForNetworkUse {
            // NOTE: this is neccessary for streaming play support, but it will slow down finish writing speed
            assetWriter.shouldOptimizeForNetworkUse = true
        }
        
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
        
        self.disablePixelBufferAttachments = disablePixelBufferAttachments
        
        self.audioSettings = audioSettings
        self.audioSourceFormatHint = audioSourceFormatHint
    }
    
    public func setupSoftwareLUTFilter(lutImage: UIImage, intensity: Double? = nil, brightnessFactor: Double? = nil, sync: Bool = true) {
        let block: () -> () = { [weak self] in
            if self?.cpuCIContext == nil {
                let colorSpace = CGColorSpaceCreateDeviceRGB()
                let options: [CIContextOption: AnyObject] = [
                    .workingColorSpace: colorSpace,
                    .outputColorSpace : colorSpace,
                    .useSoftwareRenderer : NSNumber(value: true)
                ]
                self?.cpuCIContext = CIContext(options: options)
            }
            self?.ciFilter = CILookupFilter(lutImage: lutImage, intensity: intensity, brightnessFactor: brightnessFactor)
        }
        if sync {
            Self.movieProcessingContext.runOperationSynchronously(block)
        } else {
            Self.movieProcessingContext.runOperationAsynchronously(block)
        }
    }
    
    public func cleanSoftwareFilter(sync: Bool = true) {
        let block: () -> () = { [weak self] in
            self?.ciFilter = nil
        }
        if sync {
            Self.movieProcessingContext.runOperationSynchronously(block)
        } else {
            Self.movieProcessingContext.runOperationAsynchronously(block)
        }
    }
    
    public func startRecording(sync: Bool = false, _ completionCallback:((_ started: Bool, _ error: Error?) -> Void)? = nil) {
        // Don't do this work on the movieProcessingContext queue so we don't block it.
        // If it does get blocked framebuffers will pile up from live video and after it is no longer blocked (this work has finished)
        // we will be able to accept framebuffers but the ones that piled up will come in too quickly resulting in most being dropped.
        let block = { [weak self] () -> Void in
            do {
                guard let self = self else { return }
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
                let assetWriter = self.assetWriter
                try NSObject.catchException {
                    success = assetWriter.startWriting()
                }
                
                if(!success) {
                    throw MovieOutputError.startWritingError(assetWriterError: self.assetWriter.error)
                }
                
                // NOTE: pixelBufferPool is not multi-thread safe, and it will be accessed in another thread in order to improve the performance
                self.pixelBufferPoolSemaphore.wait()
                defer {
                    self.pixelBufferPoolSemaphore.signal()
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
                
                print("MovieOutput started writing")
                
                completionCallback?(true, nil)
            } catch {
                self?.assetWriter.cancelWriting()
                
                print("MovieOutput failed to start writing. error:\(error)")
                
                completionCallback?(false, error)
            }
        }
        
        if sync {
            Self.movieProcessingContext.runOperationSynchronously(block)
        } else {
            Self.movieProcessingContext.runOperationAsynchronously(block)
        }
    }
    
    public func finishRecording(sync: Bool = false, _ completionCallback:(() -> Void)? = nil) {
        print("MovieOutput start finishing writing, optimizeForNetworkUse:\(assetWriter.shouldOptimizeForNetworkUse)")
        let block = {
            guard self.assetWriter.status == .writing else {
                completionCallback?()
                return
            }
            
            self.audioEncodingIsFinished = true
            self.videoEncodingIsFinished = true
            
            self.assetWriterAudioInput?.markAsFinished()
            self.assetWriterVideoInput.markAsFinished()
            
            if let lastFrame = self.previousFrameTime {
                // Resolve black frames at the end. Without this the end timestamp of the session's samples could be either video or audio.
                // Documentation: "You do not need to call this method; if you call finishWriting without
                // calling this method, the session's effective end time will be the latest end timestamp of
                // the session's samples (that is, no samples will be edited out at the end)."
                print("MovieOutput start endSession")
                self.assetWriter.endSession(atSourceTime: lastFrame)
            }
            
            if let lastFrame = self.previousFrameTime, let startFrame = self.startFrameTime {
                self.recordedDuration = lastFrame - startFrame
            }
            print("MovieOutput did start finishing writing. Total frames appended video::\(self.totalVideoFramesAppended) audio:\(self.totalAudioFramesAppended)")
            // Calling "finishWriting(AVAssetWriter A)" then "startWriting(AVAssetWriter B)" at the same time,
            // will cause NSInternalInconsistencyException with error code 0.
            // So we need to make sure these two methods will not run at the same time.
            let dispatchGroup = DispatchGroup()
            dispatchGroup.enter()
            self.assetWriter.finishWriting {
                print("MovieOutput did finish writing")
                dispatchGroup.leave()
                completionCallback?()
            }
            dispatchGroup.wait()
        }
        if sync {
            Self.movieProcessingContext.runOperationSynchronously(block)
        } else {
            Self.movieProcessingContext.runOperationAsynchronously(block)
        }
    }
    
    public func cancelRecording(sync: Bool = false, _ completionCallback:(() -> Void)? = nil) {
        let block = {
            self.audioEncodingIsFinished = true
            self.videoEncodingIsFinished = true
            print("MovieOutput cancel writing, state:\(self.assetWriter.status.rawValue)")
            if self.assetWriter.status == .writing {
                self.pixelBufferPoolSemaphore.wait()
                self.assetWriter.cancelWriting()
                self.pixelBufferPoolSemaphore.signal()
            }
            completionCallback?()
        }
        if sync {
            Self.movieProcessingContext.runOperationSynchronously(block)
        } else {
            Self.movieProcessingContext.runOperationAsynchronously(block)
        }
    }
    
    public func newFramebufferAvailable(_ framebuffer:Framebuffer, fromSourceIndex:UInt) {
        glFinish()
        
        if previousFrameTime == nil {
            debugPrint("starting process new framebuffer when previousFrameTime == nil")
        }
        
        let work = { [weak self] in
            _ = self?._processFramebuffer(framebuffer)
            sharedImageProcessingContext.runOperationAsynchronously {
                framebuffer.unlock()
            }
        }
        if encodingLiveVideo {
            // This is done asynchronously to reduce the amount of work done on the sharedImageProcessingContext queue,
            // so we can decrease the risk of frames being dropped by the camera. I believe it is unlikely a backlog of framebuffers will occur
            // since the framebuffers come in much slower than during synchronized encoding.
            sharedImageProcessingContext.runOperationAsynchronously(work)
        }
        else {
            // This is done synchronously to prevent framebuffers from piling up during synchronized encoding.
            // If we don't force the sharedImageProcessingContext queue to wait for this frame to finish processing it will
            // keep sending frames whenever isReadyForMoreMediaData = true but the movieProcessingContext queue would run when the system wants it to.
            sharedImageProcessingContext.runOperationSynchronously(work)
        }
    }
    
    func _processFramebuffer(_ framebuffer: Framebuffer) -> Bool {
        guard assetWriter.status == .writing, !videoEncodingIsFinished else {
            print("Guard fell through, dropping video frame. writer.state:\(self.assetWriter.status.rawValue) videoEncodingIsFinished:\(self.videoEncodingIsFinished)")
            return false
        }
        do {
            // Ignore still images and other non-video updates (do I still need this?)
            guard let frameTime = framebuffer.timingStyle.timestamp?.asCMTime else {
                print("Cannot get timestamp from framebuffer, dropping frame")
                return false
            }
            
            // If two consecutive times with the same value are added to the movie, it aborts recording, so I bail on that case.
            guard frameTime != previousFrameTime else {
                print("WARNING: frameTime is as same as previousFrameTIme")
                return true
            }
            
            if previousFrameTime == nil {
                // This resolves black frames at the beginning. Any samples recieved before this time will be edited out.
                assetWriter.startSession(atSourceTime: frameTime)
                startFrameTime = frameTime
                print("did start writing at:\(frameTime.seconds)")
                delegate?.movieOutputDidStartWriting(self, at: frameTime)
            }
            previousFrameTime = frameTime
            
            pixelBuffer = nil
            pixelBufferPoolSemaphore.wait()
            let pixelBufferStatus = CVPixelBufferPoolCreatePixelBuffer(nil, assetWriterPixelBufferInput.pixelBufferPool!, &pixelBuffer)
            pixelBufferPoolSemaphore.signal()
            guard pixelBuffer != nil && pixelBufferStatus == kCVReturnSuccess else {
                print("WARNING: Unable to create pixel buffer, dropping frame")
                return false
            }
            try renderIntoPixelBuffer(pixelBuffer!, framebuffer:framebuffer)
            guard assetWriterVideoInput.isReadyForMoreMediaData || shouldWaitForEncoding else {
                print("WARNING: Had to drop a frame at time \(frameTime)")
                return false
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
            let bufferInput = assetWriterPixelBufferInput
            var appendResult = false
            synchronizedEncodingDebugPrint("appending video framebuffer at:\(frameTime.seconds)")
            // NOTE: when NSException was triggered within NSObject.catchException, the object inside the block seems cannot be released correctly, so be careful not to trigger error, or directly use "self."
            try NSObject.catchException {
                appendResult = bufferInput.append(self.pixelBuffer!, withPresentationTime: frameTime)
            }
            if !appendResult {
                print("WARNING: Trouble appending pixel buffer at time: \(frameTime) \(String(describing: self.assetWriter.error))")
                return false
            }
            totalVideoFramesAppended += 1
            
            if videoEncodingIsFinished {
                assetWriterVideoInput.markAsFinished()
            }
            
            return true
        } catch {
            print("WARNING: Trouble appending pixel buffer \(error)")
            return false
        }
    }
    
    func renderIntoPixelBuffer(_ pixelBuffer:CVPixelBuffer, framebuffer:Framebuffer) throws {
        // Is this the first pixel buffer we have recieved?
        // NOTE: this will cause strange frame brightness blinking for the first few seconds, be careful about using this.
        if renderFramebuffer == nil && !disablePixelBufferAttachments {
            CVBufferSetAttachment(pixelBuffer, kCVImageBufferColorPrimariesKey, kCVImageBufferColorPrimaries_ITU_R_709_2, .shouldPropagate)
            CVBufferSetAttachment(pixelBuffer, kCVImageBufferYCbCrMatrixKey, kCVImageBufferYCbCrMatrix_ITU_R_601_4, .shouldPropagate)
            CVBufferSetAttachment(pixelBuffer, kCVImageBufferTransferFunctionKey, kCVImageBufferTransferFunction_ITU_R_709_2, .shouldPropagate)
        }
        
        let bufferSize = GLSize(self.size)
        var cachedTextureRef:CVOpenGLESTexture? = nil
        let ret = CVOpenGLESTextureCacheCreateTextureFromImage(kCFAllocatorDefault, sharedImageProcessingContext.coreVideoTextureCache, pixelBuffer, nil, GLenum(GL_TEXTURE_2D), GL_RGBA, bufferSize.width, bufferSize.height, GLenum(GL_BGRA), GLenum(GL_UNSIGNED_BYTE), 0, &cachedTextureRef)
        if ret != kCVReturnSuccess {
            print("ret error: \(ret), pixelBuffer: \(pixelBuffer)")
            return
        }
        let cachedTexture = CVOpenGLESTextureGetName(cachedTextureRef!)
        
        renderFramebuffer = try Framebuffer(context:sharedImageProcessingContext, orientation:.portrait, size:bufferSize, textureOnly:false, overriddenTexture:cachedTexture)
        
        renderFramebuffer.activateFramebufferForRendering()
        clearFramebufferWithColor(Color.black)
        CVPixelBufferLockBaseAddress(pixelBuffer, CVPixelBufferLockFlags(rawValue:CVOptionFlags(0)))
        renderQuadWithShader(colorSwizzlingShader, uniformSettings:ShaderUniformSettings(), vertexBufferObject:sharedImageProcessingContext.standardImageVBO, inputTextures:[framebuffer.texturePropertiesForOutputRotation(.noRotation)], context: sharedImageProcessingContext)
        
        if sharedImageProcessingContext.supportsTextureCaches() {
            glFinish()
        } else {
            glReadPixels(0, 0, renderFramebuffer.size.width, renderFramebuffer.size.height, GLenum(GL_RGBA), GLenum(GL_UNSIGNED_BYTE), CVPixelBufferGetBaseAddress(pixelBuffer))
        }
        CVPixelBufferUnlockBaseAddress(pixelBuffer, CVPixelBufferLockFlags(rawValue:CVOptionFlags(0)))
    }
    
    // MARK: Append buffer directly from CMSampleBuffer
    public func processVideoBuffer(_ sampleBuffer: CMSampleBuffer, shouldInvalidateSampleWhenDone:Bool) {
        let work = { [weak self] in
            _ = self?._processVideoSampleBuffer(sampleBuffer, shouldInvalidateSampleWhenDone: shouldInvalidateSampleWhenDone)
        }
        
        if encodingLiveVideo {
            Self.movieProcessingContext.runOperationSynchronously(work)
        } else {
            work()
        }
    }
    
    func _processVideoSampleBuffer(_ sampleBuffer: CMSampleBuffer, shouldInvalidateSampleWhenDone: Bool) -> Bool {
        defer {
            if shouldInvalidateSampleWhenDone {
                CMSampleBufferInvalidate(sampleBuffer)
            }
        }
        
        guard assetWriter.status == .writing, !videoEncodingIsFinished else {
            print("Guard fell through, dropping video frame. writer.state:\(self.assetWriter.status.rawValue) videoEncodingIsFinished:\(self.videoEncodingIsFinished)")
            return false
        }
        
        let frameTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
        
        // If two consecutive times with the same value are added to the movie, it aborts recording, so I bail on that case.
        guard (frameTime != previousFrameTime) else {
            print("Cannot get timestamp from framebuffer, dropping frame")
            return false
        }
        
        if previousFrameTime == nil {
            // This resolves black frames at the beginning. Any samples recieved before this time will be edited out.
            let startFrameTime = frameTime
            assetWriter.startSession(atSourceTime: startFrameTime)
            self.startFrameTime = startFrameTime
            delegate?.movieOutputDidStartWriting(self, at: startFrameTime)
        }
        
        previousFrameTime = frameTime
        
        guard (assetWriterVideoInput.isReadyForMoreMediaData || self.shouldWaitForEncoding) else {
            print("Had to drop a frame at time \(frameTime)")
            return false
        }
        
        while !assetWriterVideoInput.isReadyForMoreMediaData && shouldWaitForEncoding && !videoEncodingIsFinished {
            self.synchronizedEncodingDebugPrint("Video waiting...")
            // Better to poll isReadyForMoreMediaData often since when it does become true
            // we don't want to risk letting framebuffers pile up in between poll intervals.
            usleep(100000) // 0.1 seconds
        }
        let time = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
        synchronizedEncodingDebugPrint("appending video sample buffer at:\(time.seconds)")
        guard let buffer = CMSampleBufferGetImageBuffer(sampleBuffer) else {
            print("WARNING: Cannot get pixel buffer from sampleBuffer:\(sampleBuffer)")
            return false
        }
        if !assetWriterVideoInput.isReadyForMoreMediaData {
            print("WARNING: video input is not ready at time: \(time))")
            return false
        }
        if let ciFilter = ciFilter {
            let originalImage = CIImage(cvPixelBuffer: buffer)
            if let outputImage = ciFilter.applyFilter(on: originalImage), let ciContext = cpuCIContext {
                ciContext.render(outputImage, to: buffer)
            }
        }
        let bufferInput = assetWriterPixelBufferInput
        do {
            var appendResult = false
            try NSObject.catchException {
                appendResult = bufferInput.append(buffer, withPresentationTime: time)
            }
            if (!appendResult) {
                print("WARNING: Trouble appending pixel buffer at time: \(time) \(String(describing: assetWriter.error))")
                return false
            }
            totalVideoFramesAppended += 1
        } catch {
            print("WARNING: Trouble appending video sample buffer at time: \(time) \(error)")
            return false
        }
        return true
    }
    
    // MARK: -
    // MARK: Audio support
    
    public func activateAudioTrack() throws {
        guard assetWriter.status != .writing && assetWriter.status != .completed else {
            throw MovieOutputError.activeAudioTrackError
        }
        assetWriterAudioInput = AVAssetWriterInput(mediaType:.audio, outputSettings:self.audioSettings, sourceFormatHint:self.audioSourceFormatHint)
        let assetWriter = self.assetWriter
        let audioInpupt = self.assetWriterAudioInput!
        try NSObject.catchException {
            assetWriter.add(audioInpupt)
        }
        assetWriterAudioInput?.expectsMediaDataInRealTime = encodingLiveVideo
    }
    
    public func processAudioBuffer(_ sampleBuffer:CMSampleBuffer, shouldInvalidateSampleWhenDone:Bool) {
        let work = { [weak self] in
            _ = self?._processAudioSampleBuffer(sampleBuffer, shouldInvalidateSampleWhenDone: shouldInvalidateSampleWhenDone)
        }
        if encodingLiveVideo {
            Self.movieProcessingContext.runOperationAsynchronously(work)
        } else {
            work()
        }
    }
    
    func _processAudioSampleBuffer(_ sampleBuffer: CMSampleBuffer, shouldInvalidateSampleWhenDone: Bool) -> Bool {
        guard assetWriter.status == .writing, !audioEncodingIsFinished, let audioInput = assetWriterAudioInput else {
            print("Guard fell through, dropping audio sample, writer.state:\(assetWriter.status.rawValue) audioEncodingIsFinished:\(audioEncodingIsFinished)")
            return false
        }
        
        // Always accept audio buffer and cache it at first, since video frame might delay a bit
        audioSampleBufferCache.append(sampleBuffer)
        guard previousFrameTime != nil else {
            print("Process audio sample but first video frame is not ready yet. Time:\(CMSampleBufferGetOutputPresentationTimeStamp(sampleBuffer).seconds)")
            return true
        }
        
        var processedBufferCount = 0
        for audioBuffer in audioSampleBufferCache {
            let currentSampleTime = CMSampleBufferGetOutputPresentationTimeStamp(audioBuffer)
            guard audioInput.isReadyForMoreMediaData || shouldWaitForEncoding else {
                print("Had to delay a audio sample at time \(currentSampleTime)")
                break
            }
            
            while !audioInput.isReadyForMoreMediaData && shouldWaitForEncoding && !audioEncodingIsFinished {
                print("Audio waiting...")
                usleep(100000)
                if !audioInput.isReadyForMoreMediaData {
                    synchronizedEncodingDebugPrint("Audio still not ready, skip this runloop...")
                    break
                }
            }
            
            synchronizedEncodingDebugPrint("Process audio sample output. Time:\(currentSampleTime.seconds)")
            do {
                var appendResult = false
                try NSObject.catchException {
                    appendResult = audioInput.append(audioBuffer)
                }
                if !appendResult {
                    print("WARNING: Trouble appending audio sample buffer: \(String(describing: self.assetWriter.error))")
                    break
                }
                totalAudioFramesAppended += 1
                if shouldInvalidateSampleWhenDone {
                    CMSampleBufferInvalidate(audioBuffer)
                }
                processedBufferCount += 1
            }
            catch {
                print("WARNING: Trouble appending audio sample buffer: \(error)")
                break
            }
        }
        audioSampleBufferCache.removeFirst(processedBufferCount)
        return true
    }
    
    public func flushPendingAudioBuffers(shouldInvalidateSampleWhenDone: Bool) {
        guard let lastBuffer = audioSampleBufferCache.popLast() else { return }
        _ = _processAudioSampleBuffer(lastBuffer, shouldInvalidateSampleWhenDone: shouldInvalidateSampleWhenDone)
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
