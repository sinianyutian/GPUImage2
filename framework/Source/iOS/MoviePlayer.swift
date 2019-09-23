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

public class MoviePlayer: AVPlayer, ImageSource {
    public let targets = TargetContainer()
    public var runBenchmark = false
    public var logEnabled = false
    public weak var delegate: MoviePlayerDelegate?
    public var startTime: TimeInterval?
    public var endTime: TimeInterval?
    public var loop = false
    public private(set) var asset: AVAsset?
    public private(set) var isPlaying = false
    
    private(set) var playerItem: AVPlayerItem?
    var videoOutput: AVPlayerItemVideoOutput?
    var displayLink: CADisplayLink?
    
    lazy var framebufferGenerator = FramebufferGenerator()
    
    var totalTimeObservers = [MoviePlayerTimeObserver]()
    var timeObserversQueue = [MoviePlayerTimeObserver]()
    
    var timebaseInfo = mach_timebase_info_data_t()
    var totalFramesSent = 0
    var totalFrameTime: Double = 0.0
    public var playrate: Float = 1.0
    public var assetDuration: TimeInterval {
        return asset?.duration.seconds ?? 0
    }
    public var isReadyToPlay: Bool {
        return status == .readyToPlay
    }
    public var videoOrientation: ImageOrientation {
        guard let asset = asset else { return .portrait }
        return asset.imageOrientation ?? .portrait
    }
    
    var framebufferUserInfo: [AnyHashable:Any]?
    var observations = [NSKeyValueObservation]()
    
    struct SeekingInfo: Equatable {
        let time: CMTime
        let toleranceBefore: CMTime
        let toleranceAfter: CMTime
        let shouldPlayAfterSeeking: Bool
        
        public static func == (lhs: MoviePlayer.SeekingInfo, rhs: MoviePlayer.SeekingInfo) -> Bool {
            return lhs.time.seconds == rhs.time.seconds
                && lhs.toleranceBefore.seconds == rhs.toleranceBefore.seconds
                && lhs.toleranceAfter.seconds == rhs.toleranceAfter.seconds
                && lhs.shouldPlayAfterSeeking == rhs.shouldPlayAfterSeeking
        }
    }
    var nextSeeking: SeekingInfo?
    public var isSeeking = false
    public var disableGPURender = false
    
    public override init() {
        debugPrint("movie player init")
        // Make sure player it intialized on the main thread, or it might cause KVO crash
        assert(Thread.isMainThread)
        super.init()
    }
    
    override public init(playerItem item: AVPlayerItem?) {
        self.playerItem = item
        // Make sure player it intialized on the main thread, or it might cause KVO crash
        assert(Thread.isMainThread)
        super.init(playerItem: item)
        replaceCurrentItem(with: item)
    }
    
    deinit {
        debugPrint("movie player deinit \(String(describing: asset))")
        stop()
        _removePlayerObservers()
    }
    
    // MARK: Data Source
    public func replaceCurrentItem(with url: URL) {
        let inputAsset = AVURLAsset(url: url)
        let playerItem = AVPlayerItem(asset: inputAsset, automaticallyLoadedAssetKeys: [AVURLAssetPreferPreciseDurationAndTimingKey])
        replaceCurrentItem(with: playerItem)
    }
    
    override public func replaceCurrentItem(with item: AVPlayerItem?) {
        if isPlaying {
            stop()
        }
        self.videoOutput.map { self.playerItem?.remove($0) }
        self.playerItem = item
        self.asset = item?.asset
        if let item = item {
            if !disableGPURender {
                let outputSettings = [String(kCVPixelBufferPixelFormatTypeKey) : kCVPixelFormatType_420YpCbCr8BiPlanarFullRange]
                let videoOutput = AVPlayerItemVideoOutput(outputSettings: outputSettings)
                videoOutput.suppressesPlayerRendering = true
                item.add(videoOutput)
                item.audioTimePitchAlgorithm = .varispeed
                self.videoOutput = videoOutput
            } else {
                self.videoOutput = nil
            }
            _setupPlayerObservers()
        } else {
            self.videoOutput = nil
            _removePlayerObservers()
        }
        
        super.replaceCurrentItem(with: item)
    }
    
    // MARK: -
    // MARK: Playback control
    
    override public func play() {
        if displayLink == nil {
            start()
        } else {
            resume()
        }
    }
    
    override public func playImmediately(atRate rate: Float) {
        playrate = rate
        start()
    }
    
    public func start() {
        guard playerItem != nil else {
            assert(playerItem != nil)
            debugPrint("ERROR! player hasn't been setup before starting")
            return
        }
        isPlaying = true
        debugPrint("movie player start \(String(describing: asset))")
        _setupDisplayLinkIfNeeded()
        _resetTimeObservers()
        seekToTime(startTime ?? 0, shouldPlayAfterSeeking: true)
    }
    
    public func resume() {
        isPlaying = true
        rate = playrate
        debugPrint("movie player resume \(String(describing: asset))")
    }
    
    override public func pause() {
        isPlaying = false
        guard rate != 0 else { return }
        debugPrint("movie player pause \(String(describing: asset))")
        super.pause()
    }
    
    public func stop() {
        pause()
        debugPrint("movie player stop \(String(describing: asset))")
        timeObserversQueue.removeAll()
        displayLink?.invalidate()
        displayLink = nil
        isSeeking = false
        nextSeeking = nil
    }
    
    public func seekToTime(_ time: TimeInterval, shouldPlayAfterSeeking: Bool) {
        let targetTime = CMTime(seconds: time, preferredTimescale: 600)
        if shouldPlayAfterSeeking {
            // 0.1s has 3 frames tolerance for 30 FPS video, it should be enough if there is no sticky video
            let toleranceTime = CMTime(seconds: 0.1, preferredTimescale: 600)
            isPlaying = true
            nextSeeking = SeekingInfo(time: targetTime, toleranceBefore: toleranceTime, toleranceAfter: toleranceTime, shouldPlayAfterSeeking: shouldPlayAfterSeeking)
        } else {
            nextSeeking = SeekingInfo(time: targetTime, toleranceBefore: .zero, toleranceAfter: .zero, shouldPlayAfterSeeking: shouldPlayAfterSeeking)
        }
        actuallySeekToTime()
    }
    
    func actuallySeekToTime() {
        // Avoid seeking choppy when fast seeking
        // https://developer.apple.com/library/archive/qa/qa1820/_index.html#//apple_ref/doc/uid/DTS40016828    
        guard !isSeeking, let seekingInfo = nextSeeking, isReadyToPlay else { return }
        isSeeking = true
        seek(to: seekingInfo.time, toleranceBefore:seekingInfo.toleranceBefore, toleranceAfter: seekingInfo.toleranceAfter) { [weak self] success in
//            debugPrint("movie player did seek to time:\(seekingInfo.time.seconds) success:\(success) shouldPlayAfterSeeking:\(seekingInfo.shouldPlayAfterSeeking)")
            guard let self = self else { return }
            if seekingInfo.shouldPlayAfterSeeking && self.isPlaying {
                self._resetTimeObservers()
                self.rate = self.playrate
            }
            
            self.isSeeking = false
            
            if seekingInfo != self.nextSeeking {
                self.actuallySeekToTime()
            } else {
                self.nextSeeking = nil
            }
        }
    }
    
    public func transmitPreviousImage(to target: ImageConsumer, atIndex: UInt) {
        // Not needed for movie inputs
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
    
    public func removeAllTimeObservers() {
        sharedImageProcessingContext.runOperationAsynchronously { [weak self] in
            self?.timeObserversQueue.removeAll()
            self?.totalTimeObservers.removeAll()
        }
    }
}

private extension MoviePlayer {
    func _setupDisplayLinkIfNeeded() {
        if displayLink == nil {
            displayLink = CADisplayLink(target: self, selector: #selector(displayLinkCallback))
            displayLink?.add(to: RunLoop.main, forMode: .common)
        }
    }
    
    func _setupPlayerObservers() {
        _removePlayerObservers()
        NotificationCenter.default.addObserver(self, selector: #selector(playerDidPlayToEnd), name: .AVPlayerItemDidPlayToEndTime, object: nil)
        NotificationCenter.default.addObserver(self, selector: #selector(playerStalled), name: .AVPlayerItemPlaybackStalled, object: nil)
        observations.append(observe(\.status) { [weak self] _, _ in
            self?.playerStatusDidChange()
        })
        observations.append(observe(\.rate) { [weak self] _, _ in
            self?.playerRateDidChange()
        })
        if let playerItem = playerItem {
            observations.append(playerItem.observe(\AVPlayerItem.status) { [weak self] _, _ in
                self?.playerItemStatusDidChange()
            })
        }
    }
    
    func _removePlayerObservers() {
        NotificationCenter.default.removeObserver(self, name: .AVPlayerItemDidPlayToEndTime, object: nil)
        NotificationCenter.default.removeObserver(self, name: .AVPlayerItemPlaybackStalled, object: nil)
        observations.forEach { $0.invalidate() }
        observations.removeAll()
    }
    
    func _resetTimeObservers() {
        timeObserversQueue.removeAll()
        for observer in totalTimeObservers {
            guard observer.targetTime >= (startTime ?? 0) && observer.targetTime <= endTime ?? assetDuration else {
                continue
            }
            timeObserversQueue.append(observer)
        }
        if let endTime = endTime {
            let endTimeObserver = MoviePlayerTimeObserver(targetTime: endTime) { [weak self] _ in
                if self?.loop == true && self?.isPlaying == true {
                    self?.pause()
                    self?.start()
                } else {
                    self?.pause()
                }
            }
            let insertIndex: Int = timeObserversQueue.reversed().firstIndex { endTime < $0.targetTime } ?? 0
            timeObserversQueue.insert(endTimeObserver, at: insertIndex)
        }
    }
    
    func playerRateDidChange() {
//        debugPrint("rate change to:\(player.rate) asset:\(asset) status:\(player.status.rawValue)")
        resumeIfNeeded()
    }
    
    func playerStatusDidChange() {
        debugPrint("Player status change to:\(status.rawValue) asset:\(String(describing: asset))")
        resumeIfNeeded()
    }
    
    func playerItemStatusDidChange() {
        debugPrint("PlayerItem status change to:\(String(describing: currentItem?.status.rawValue)) asset:\(String(describing: asset))")
        resumeIfNeeded()
    }
    
    func resumeIfNeeded() {
        guard isReadyToPlay && isPlaying == true && rate != playrate else { return }
        rate = playrate
    }
    
    // MARK: -
    // MARK: Internal processing functions
    
    func _process(movieFrame: CVPixelBuffer, with sampleTime: CMTime) {
        delegate?.moviePlayerDidReadPixelBuffer(movieFrame, time: CMTimeGetSeconds(sampleTime))
        
        let startTime = CFAbsoluteTimeGetCurrent()
        if runBenchmark || logEnabled {
            totalFramesSent += 1
        }
        defer {
            if runBenchmark {
                let currentFrameTime = (CFAbsoluteTimeGetCurrent() - startTime)
                totalFrameTime += currentFrameTime
                print("Average frame time :\(1000.0 * totalFrameTime / Double(totalFramesSent)) ms")
                print("Current frame time :\(1000.0 * currentFrameTime) ms")
            }
        }
        
        guard !disableGPURender, let framebuffer = framebufferGenerator.generateFromYUVBuffer(movieFrame, frameTime: sampleTime, videoOrientation: videoOrientation) else { return }
        framebuffer.userInfo = framebufferUserInfo
        
        updateTargetsWithFramebuffer(framebuffer)
    }
    
    @objc func displayLinkCallback(displayLink: CADisplayLink) {
        guard currentItem != nil else {
            stop()
            return
        }
        if !disableGPURender {
            sharedImageProcessingContext.runOperationAsynchronously { [weak self] in
                self?._displayLinkCallback(displayLink)
            }
        } else {
            _displayLinkCallback(displayLink)
        }
    }
    
    private func _displayLinkCallback(_ displayLink: CADisplayLink) {
        let playTime = currentTime()
        if self.videoOutput?.hasNewPixelBuffer(forItemTime: playTime) == true {
            guard let pixelBuffer = videoOutput?.copyPixelBuffer(forItemTime: playTime, itemTimeForDisplay: nil) else {
                print("Failed to copy pixel buffer at time:\(playTime)")
                return
            }
            _notifyTimeObserver(with: playTime)
            _process(movieFrame: pixelBuffer, with: playTime)
        }
    }
    
    @objc func playerDidPlayToEnd(notification: Notification) {
        guard loop && isPlaying && (endTime == nil || currentTime().seconds == assetDuration) else { return }
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

public extension AVAsset {
    var imageOrientation: ImageOrientation? {
        guard let videoTrack = tracks(withMediaType: AVMediaType.video).first else {
            return nil
        }
        let trackTransform = videoTrack.preferredTransform
        switch (trackTransform.a, trackTransform.b, trackTransform.c, trackTransform.d) {
        case (1, 0, 0, 1): return .portrait
        case (1, 0, 0, -1): return .portraitUpsideDown
        case (0, 1, -1, 0): return .landscapeLeft
        case (0, -1, 1, 0): return .landscapeRight
        default:
            print("ERROR: unsupport transform!\(trackTransform)")
            return .portrait
        }
    }
    
    // For original orientation is different with preferred image orientation when it is landscape
    var originalOrientation: ImageOrientation? {
        guard let videoTrack = tracks(withMediaType: AVMediaType.video).first else {
            return nil
        }
        let trackTransform = videoTrack.preferredTransform
        switch (trackTransform.a, trackTransform.b, trackTransform.c, trackTransform.d) {
        case (1, 0, 0, 1): return .portrait
        case (1, 0, 0, -1): return .portraitUpsideDown
        case (0, 1, -1, 0): return .landscapeRight
        case (0, -1, 1, 0): return .landscapeLeft
        default:
            print("ERROR: unsupport transform!\(trackTransform)")
            return .portrait
        }
    }
}
