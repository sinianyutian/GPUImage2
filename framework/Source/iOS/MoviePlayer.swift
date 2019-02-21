//
//  MoviePlayer.swift
//  DayCam
//
//  Created by 陈品霖 on 2019/1/30.
//  Copyright © 2019 rocry. All rights reserved.
//
import AVFoundation

public protocol MoviePlayerDelegate: class {
    func moviePlayerDidReadPixelBuffer(_ pixelBuffer: CVPixelBuffer, time: TimeInterval)
}

public typealias MoviePlayerTimeObserverCallback = (TimeInterval) -> Void

public struct MoviePlayerTimeObserver {
    let targetTime: TimeInterval
    let callback: MoviePlayerTimeObserverCallback
    let observerID: String
    init(targetTime: TimeInterval, callback: @escaping MoviePlayerTimeObserverCallback) {
        self.targetTime = targetTime
        self.callback = callback
        observerID = UUID.init().uuidString
    }
}

public class MoviePlayer: ImageSource {
    public let targets = TargetContainer()
    public var runBenchmark = false
    public var logEnabled = false
    public weak var delegate: MoviePlayerDelegate?
    public let asset: AVAsset
    public let player: AVPlayer
    public var isPlaying = false
    
    public var startTime: TimeInterval?
    public var endTime: TimeInterval?
    public var loop: Bool
    
    let playerItem: AVPlayerItem
    let videoOutput: AVPlayerItemVideoOutput
    var displayLink: CADisplayLink?
    
    let yuvConversionShader: ShaderProgram
    
    var totalTimeObservers = [MoviePlayerTimeObserver]()
    var timeObserversQueue = [MoviePlayerTimeObserver]()
    
    var timebaseInfo = mach_timebase_info_data_t()
    var totalFramesSent = 0
    var totalFrameTime: Double = 0.0
    public var playrate: Float = 1.0 {
        didSet {
            player.rate = playrate
        }
    }
    public var isMuted: Bool = false {
        didSet {
            player.isMuted = isMuted
        }
    }
    
    var movieFramebuffer: Framebuffer?
    var framebufferUserInfo: [AnyHashable:Any]?
    var observations = [NSKeyValueObservation]()
    
    public init(asset: AVAsset, loop: Bool = false) throws {
        debugPrint("movie player init \(asset)")
        self.asset = asset
        self.loop = loop
        self.yuvConversionShader = crashOnShaderCompileFailure("MoviePlayer") {
            try sharedImageProcessingContext.programForVertexShader(defaultVertexShaderForInputs(2),
                                                                    fragmentShader: YUVConversionFullRangeFragmentShader)
        }
        
        let outputSettings = [String(kCVPixelBufferPixelFormatTypeKey) : kCVPixelFormatType_420YpCbCr8BiPlanarFullRange]
        videoOutput = AVPlayerItemVideoOutput(outputSettings: outputSettings)
        videoOutput.suppressesPlayerRendering = true
        
        playerItem = AVPlayerItem(asset: asset)
        playerItem.add(videoOutput)
        playerItem.audioTimePitchAlgorithm = AVAudioTimePitchAlgorithmVarispeed
        player = AVPlayer(playerItem: playerItem)
        _setupObservers()
    }
    
    public convenience init(url: URL, loop: Bool = false) throws {
        let inputOptions = [AVURLAssetPreferPreciseDurationAndTimingKey: NSNumber(value: true)]
        let inputAsset = AVURLAsset(url: url, options: inputOptions)
        try self.init(asset: inputAsset, loop: loop)
    }
    
    deinit {
        debugPrint("movie player deinit \(asset)")
        pause()
        movieFramebuffer?.unlock()
        observations.forEach { $0.invalidate() }
        NotificationCenter.default.removeObserver(self)
    }
    
    // MARK: -
    // MARK: Playback control
    
    public func start() {
        isPlaying = true
        debugPrint("movie player start \(asset)")
        if displayLink != nil {
            displayLink?.remove(from: RunLoop.main, forMode: .commonModes)
        }
        displayLink = CADisplayLink(target: self, selector: #selector(displayLinkCallback))
        displayLink?.add(to: RunLoop.main, forMode: .commonModes)
        timeObserversQueue.removeAll()
        if let endTime = endTime {
            let endTimeObserver = MoviePlayerTimeObserver(targetTime: endTime) { [weak self] _ in
                if self?.loop == true {
                    self?.pause()
                    self?.start()
                } else {
                    self?.pause()
                }
            }
            timeObserversQueue.append(endTimeObserver)
        }
        for observer in totalTimeObservers {
            guard observer.targetTime >= startTime ?? 0 else {
                break
            }
            timeObserversQueue.append(observer)
        }
        seekToTime(startTime ?? 0, shouldPlayAfterSeeking: true)
    }
    
    public func pause() {
        isPlaying = false
        debugPrint("movie player pause \(asset)")
        player.pause()
        timeObserversQueue.removeAll()
        displayLink?.remove(from: RunLoop.current, forMode: .commonModes)
        displayLink?.invalidate()
        displayLink = nil
    }
    
    public func seekToTime(_ time: TimeInterval, shouldPlayAfterSeeking: Bool) {
        player.seek(to: CMTime(seconds: time, preferredTimescale: 600)) { [weak self] success in
            print("movie player did seek to time:\(time) success:\(success)")
            guard let self = self else { return }
            if shouldPlayAfterSeeking {
                self.player.rate = self.playrate
            }
        }
    }
    
    public func transmitPreviousImage(to target: ImageConsumer, atIndex: UInt) {
        // Not needed for movie inputs
    }
    
    func transmitPreviousFrame() {
        sharedImageProcessingContext.runOperationAsynchronously {
            if let movieFramebuffer = self.movieFramebuffer {
                self.updateTargetsWithFramebuffer(movieFramebuffer)
            }
        }
    }
    
    public func addTimeObserver(seconds: TimeInterval, callback: @escaping MoviePlayerTimeObserverCallback) -> MoviePlayerTimeObserver {
        let timeObserver = MoviePlayerTimeObserver(targetTime: seconds, callback: callback)
        totalTimeObservers.append(timeObserver)
        totalTimeObservers = totalTimeObservers.sorted { (lhs, rhs) in
            return lhs.targetTime > rhs.targetTime
        }
        return timeObserver
    }
    
    public func removeTimeObserver(timeObserver: MoviePlayerTimeObserver) {
        totalTimeObservers.removeAll { (observer) -> Bool in
            return observer.observerID == timeObserver.observerID
        }
        timeObserversQueue.removeAll { (observer) -> Bool in
            return observer.observerID == timeObserver.observerID
        }
    }
}

private extension MoviePlayer {
    // MARK: -
    // MARK: Thread configuration
    
    func nanosToAbs(_ nanos: UInt64) -> UInt64 {
        return nanos * UInt64(timebaseInfo.denom) / UInt64(timebaseInfo.numer)
    }
    
    func _setupObservers() {
        observations.append(player.observe(\AVPlayer.rate) { [weak self] _, _ in
            self?.playerRateDidChange()
        })
        observations.append(player.observe(\AVPlayer.status) { [weak self] _, _ in
            self?.playerStatusDidChange()
        })
        NotificationCenter.default.addObserver(self, selector: #selector(playerDidPlayToEnd), name: .AVPlayerItemDidPlayToEndTime, object: nil)
        NotificationCenter.default.addObserver(self, selector: #selector(playerStalled), name: .AVPlayerItemPlaybackStalled, object: nil)
    }
    
    func playerRateDidChange() {
        debugPrint("rate change to:\(player.rate) asset:\(asset) status:\(player.status.rawValue)")
        resumeIfNeeded()
    }
    
    func playerStatusDidChange() {
        debugPrint("status change to:\(player.status.rawValue) asset:\(asset)")
        resumeIfNeeded()
    }
    
    func resumeIfNeeded() {
        guard player.status == .readyToPlay && isPlaying == true && player.rate != playrate else { return }
        player.rate = playrate
    }
    
    // MARK: -
    // MARK: Internal processing functions
    
    func process(movieFrame: CVPixelBuffer, with sampleTime: CMTime) {
        delegate?.moviePlayerDidReadPixelBuffer(movieFrame, time: CMTimeGetSeconds(sampleTime))
        
        let bufferHeight = CVPixelBufferGetHeight(movieFrame)
        let bufferWidth = CVPixelBufferGetWidth(movieFrame)
        CVPixelBufferLockBaseAddress(movieFrame, CVPixelBufferLockFlags(rawValue: CVOptionFlags(0)))
        
        let conversionMatrix = colorConversionMatrix601FullRangeDefault
        
        let startTime = CFAbsoluteTimeGetCurrent()
        
        var luminanceGLTexture: CVOpenGLESTexture?
        
        glActiveTexture(GLenum(GL_TEXTURE0))
        
        let luminanceGLTextureResult = CVOpenGLESTextureCacheCreateTextureFromImage(kCFAllocatorDefault, sharedImageProcessingContext.coreVideoTextureCache, movieFrame, nil, GLenum(GL_TEXTURE_2D), GL_LUMINANCE, GLsizei(bufferWidth), GLsizei(bufferHeight), GLenum(GL_LUMINANCE), GLenum(GL_UNSIGNED_BYTE), 0, &luminanceGLTexture)
        
        if(luminanceGLTextureResult != kCVReturnSuccess || luminanceGLTexture == nil) {
            print("Could not create LuminanceGLTexture")
            return
        }
        
        let luminanceTexture = CVOpenGLESTextureGetName(luminanceGLTexture!)
        
        glBindTexture(GLenum(GL_TEXTURE_2D), luminanceTexture)
        glTexParameterf(GLenum(GL_TEXTURE_2D), GLenum(GL_TEXTURE_WRAP_S), GLfloat(GL_CLAMP_TO_EDGE));
        glTexParameterf(GLenum(GL_TEXTURE_2D), GLenum(GL_TEXTURE_WRAP_T), GLfloat(GL_CLAMP_TO_EDGE));
        
        let luminanceFramebuffer: Framebuffer
        do {
            luminanceFramebuffer = try Framebuffer(context: sharedImageProcessingContext, orientation: .portrait, size: GLSize(width: GLint(bufferWidth), height: GLint(bufferHeight)), textureOnly: true, overriddenTexture: luminanceTexture)
        } catch {
            print("Could not create a framebuffer of the size (\(bufferWidth), \(bufferHeight)), error: \(error)")
            return
        }
        
        var chrominanceGLTexture: CVOpenGLESTexture?
        
        glActiveTexture(GLenum(GL_TEXTURE1))
        
        let chrominanceGLTextureResult = CVOpenGLESTextureCacheCreateTextureFromImage(kCFAllocatorDefault, sharedImageProcessingContext.coreVideoTextureCache, movieFrame, nil, GLenum(GL_TEXTURE_2D), GL_LUMINANCE_ALPHA, GLsizei(bufferWidth / 2), GLsizei(bufferHeight / 2), GLenum(GL_LUMINANCE_ALPHA), GLenum(GL_UNSIGNED_BYTE), 1, &chrominanceGLTexture)
        
        if(chrominanceGLTextureResult != kCVReturnSuccess || chrominanceGLTexture == nil) {
            print("Could not create ChrominanceGLTexture")
            return
        }
        
        let chrominanceTexture = CVOpenGLESTextureGetName(chrominanceGLTexture!)
        
        glBindTexture(GLenum(GL_TEXTURE_2D), chrominanceTexture)
        glTexParameterf(GLenum(GL_TEXTURE_2D), GLenum(GL_TEXTURE_WRAP_S), GLfloat(GL_CLAMP_TO_EDGE));
        glTexParameterf(GLenum(GL_TEXTURE_2D), GLenum(GL_TEXTURE_WRAP_T), GLfloat(GL_CLAMP_TO_EDGE));
        
        let chrominanceFramebuffer: Framebuffer
        do {
            chrominanceFramebuffer = try Framebuffer(context: sharedImageProcessingContext,
                                                     orientation: .portrait,
                                                     size: GLSize(width:GLint(bufferWidth), height:GLint(bufferHeight)),
                                                     textureOnly: true,
                                                     overriddenTexture: chrominanceTexture)
        } catch {
            print("Could not create a framebuffer of the size (\(bufferWidth), \(bufferHeight)), error: \(error)")
            return
        }
        
        movieFramebuffer?.unlock()
        let framebuffer = sharedImageProcessingContext.framebufferCache.requestFramebufferWithProperties(orientation: .portrait, size: GLSize(width: GLint(bufferWidth), height: GLint(bufferHeight)), textureOnly: false)
        framebuffer.lock()
        
        convertYUVToRGB(shader: yuvConversionShader,
                        luminanceFramebuffer: luminanceFramebuffer,
                        chrominanceFramebuffer: chrominanceFramebuffer,
                        resultFramebuffer: framebuffer,
                        colorConversionMatrix: conversionMatrix)
        CVPixelBufferUnlockBaseAddress(movieFrame, CVPixelBufferLockFlags(rawValue: CVOptionFlags(0)))
        
        framebuffer.timingStyle = .videoFrame(timestamp: Timestamp(sampleTime))
        framebuffer.userInfo = framebufferUserInfo
        movieFramebuffer = framebuffer
        
        updateTargetsWithFramebuffer(framebuffer)
        
        if(runBenchmark || logEnabled) {
            totalFramesSent += 1
        }
        
        if runBenchmark {
            let currentFrameTime = (CFAbsoluteTimeGetCurrent() - startTime)
            totalFrameTime += currentFrameTime
            print("Average frame time :\(1000.0 * totalFrameTime / Double(totalFramesSent)) ms")
            print("Current frame time :\(1000.0 * currentFrameTime) ms")
        }
    }
    
    @objc func displayLinkCallback(displayLink: CADisplayLink) {
        sharedImageProcessingContext.runOperationAsynchronously {
            let currentTime = self.player.currentTime()
            if self.videoOutput.hasNewPixelBuffer(forItemTime: currentTime) {
                guard let pixelBuffer = self.videoOutput.copyPixelBuffer(forItemTime: currentTime, itemTimeForDisplay: nil) else {
                    print("Failed to copy pixel buffer at time:\(currentTime)")
                    return
                }
                self._notifyTimeObserver(with: currentTime)
                self.process(movieFrame: pixelBuffer, with: currentTime)
            }
        }
    }
    
    @objc func playerDidPlayToEnd(notification: Notification) {
        guard loop && isPlaying && (endTime == nil || player.currentTime() == playerItem.asset.duration) else { return }
        start()
    }
    
    @objc func playerStalled(notification: Notification) {
        print("player was stalled. notification:\(notification)")
    }
    
    func _notifyTimeObserver(with sampleTime: CMTime) {
        let currentTime = CMTimeGetSeconds(sampleTime)
        while let lastObserver = timeObserversQueue.last, lastObserver.targetTime <= currentTime {
            timeObserversQueue.removeLast()
            DispatchQueue.main.async {
                lastObserver.callback(currentTime)
            }
        }
    }
}
