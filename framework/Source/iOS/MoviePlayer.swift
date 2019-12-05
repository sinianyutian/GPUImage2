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

private var looperDict = [MoviePlayer: AVPlayerLooper]()

public class MoviePlayer: AVQueuePlayer, ImageSource {
    public let targets = TargetContainer()
    public var runBenchmark = false
    public var logEnabled = false
    public weak var delegate: MoviePlayerDelegate?
    public var startTime: TimeInterval?
    public var endTime: TimeInterval?
    /// Whether to loop play.
    public var loop = false
    public var asset: AVAsset? { return currentItem?.asset }
    public private(set) var isPlaying = false
    public var lastPlayerItem: AVPlayerItem?
    public var playableItem: AVPlayerItem? { currentItem ?? lastPlayerItem }
    
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
    public var didPlayToEnd: Bool {
        return currentTime().seconds >= assetDuration
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
        print("movie player init")
        // Make sure player it intialized on the main thread, or it might cause KVO crash
        assert(Thread.isMainThread)
        super.init()
    }
    
    override public init(playerItem item: AVPlayerItem?) {
        // Make sure player it intialized on the main thread, or it might cause KVO crash
        assert(Thread.isMainThread)
        super.init(playerItem: item)
        replaceCurrentItem(with: item)
    }
    
    deinit {
        print("movie player deinit \(String(describing: asset))")
        pause()
        displayLink?.invalidate()
        _removePlayerObservers()
    }
    
    // MARK: Data Source
    public func replaceCurrentItem(with url: URL) {
        let inputAsset = AVURLAsset(url: url)
        let playerItem = AVPlayerItem(asset: inputAsset, automaticallyLoadedAssetKeys: [AVURLAssetPreferPreciseDurationAndTimingKey])
        replaceCurrentItem(with: playerItem)
    }
    
    override public func insert(_ item: AVPlayerItem, after afterItem: AVPlayerItem?) {
        insert(item, after: afterItem, disableGPURender: disableGPURender)
    }
    
    public func insert(_ item: AVPlayerItem, after afterItem: AVPlayerItem?, disableGPURender: Bool) {
        if !disableGPURender {
            _setupPlayerItemVideoOutput(for: item)
        }
        item.audioTimePitchAlgorithm = .varispeed
        lastPlayerItem = item
        self.disableGPURender = disableGPURender
        _setupPlayerObservers(playerItem: item)
        super.insert(item, after: afterItem)
        print("insert new item(\(item.duration.seconds)s):\(item) afterItem:\(String(describing: afterItem)) disableGPURender:\(disableGPURender) itemsCount:\(items().count)")
    }
    
    override public func replaceCurrentItem(with item: AVPlayerItem?) {
        replaceCurrentItem(with: item, disableGPURender: disableGPURender)
    }
    
    public func replaceCurrentItem(with item: AVPlayerItem?, disableGPURender: Bool) {
        if isPlaying {
            stop()
        }
        lastPlayerItem = item
        if let item = item {
            if !disableGPURender {
                _setupPlayerItemVideoOutput(for: item)
            }
            item.audioTimePitchAlgorithm = .varispeed
            _setupPlayerObservers(playerItem: item)
        } else {
            _removePlayerObservers()
        }
        self.disableGPURender = disableGPURender
        print("replace current item with newItem(\(item?.duration.seconds ?? 0)s)):\(String(describing: item)) disableGPURender:\(disableGPURender) itemsCount:\(items().count)")
        super.replaceCurrentItem(with: item)
    }
    
    public func replayLastItem() {
        guard let playerItem = lastPlayerItem else { return }
        remove(playerItem)
        insert(playerItem, after: nil)
        let start = startTime ?? 0
        if playerItem.currentTime().seconds != start {
            seekToTime(start, shouldPlayAfterSeeking: true)
        } else {
            play()
        }
        print("replay last item:\(playerItem)")
    }
    
    override public func remove(_ item: AVPlayerItem) {
        super.remove(item)
        print("remove item:\(item)")
    }
    
    override public func removeAllItems() {
        super.removeAllItems()
        print("remove all items")
    }
    
    override public func advanceToNextItem() {
        super.advanceToNextItem()
        print("advance to next item")
    }
    
    // MARK: -
    // MARK: Playback control
    
    override public func play() {
        if displayLink == nil || didPlayToEnd {
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
        if actionAtItemEnd == .advance, currentItem == nil, let playerItem = lastPlayerItem {
            insert(playerItem, after: nil)
        }
        
        guard currentItem != nil else {
            // Sometime the player.items() seems still 0 even if insert was called, but it won't result in crash, just print a error log for information.
            print("ERROR! player currentItem is nil")
            return
        }
        isPlaying = true
        print("movie player start duration:\(String(describing: asset?.duration.seconds)) \(String(describing: asset))")
        _setupDisplayLinkIfNeeded()
        _resetTimeObservers()
        if loop {
            if let playerItem = lastPlayerItem {
                looperDict[self]?.disableLooping()
                let start = CMTime(seconds: startTime ?? 0, preferredTimescale: 600)
                let end = CMTime(seconds: endTime ?? assetDuration, preferredTimescale: 600)
                let looper = AVPlayerLooper(player: self, templateItem: playerItem, timeRange: CMTimeRange(start: start, end: end))
                looperDict[self] = looper
            }
        }
        if currentTime().seconds != (startTime ?? 0)  {
            seekToTime(startTime ?? 0, shouldPlayAfterSeeking: true)
        } else {
            rate = playrate
        }
    }
    
    public func resume() {
        isPlaying = true
        rate = playrate
        print("movie player resume \(String(describing: asset))")
    }
    
    override public func pause() {
        isPlaying = false
        guard rate != 0 else { return }
        print("movie player pause \(String(describing: asset))")
        super.pause()
    }
    
    public func stop() {
        pause()
        print("movie player stop \(String(describing: asset))")
        _timeObserversUpdate { [weak self] in
            self?.timeObserversQueue.removeAll()
        }
        displayLink?.invalidate()
        displayLink = nil
        isSeeking = false
        nextSeeking = nil
        looperDict[self]?.disableLooping()
        looperDict[self] = nil
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
        if assetDuration <= 0 {
            print("cannot seek since assetDuration is 0. currentItem:\(String(describing: currentItem))")
        } else {
            actuallySeekToTime()
        }
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
        _timeObserversUpdate { [weak self] in
            guard let self = self else { return }
            self.totalTimeObservers.append(timeObserver)
            self.totalTimeObservers = self.totalTimeObservers.sorted { (lhs, rhs) in
                return lhs.targetTime > rhs.targetTime
            }
            if self.isPlaying {
                if let lastIndex = self.timeObserversQueue.firstIndex(where: { $0.targetTime >= seconds }) {
                    self.timeObserversQueue.insert(timeObserver, at: lastIndex)
                } else {
                    self.timeObserversQueue.append(timeObserver)
                }
            }
        }
        return timeObserver
    }
    
    public func removeTimeObserver(timeObserver: MoviePlayerTimeObserver) {
        _timeObserversUpdate { [weak self] in
            self?.totalTimeObservers.removeAll { $0.observerID == timeObserver.observerID }
            self?.timeObserversQueue.removeAll { $0.observerID == timeObserver.observerID }
        }
    }
    
    public func removeAllTimeObservers() {
        _timeObserversUpdate { [weak self] in
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
    
    func _setupPlayerItemVideoOutput(for item: AVPlayerItem) {
        let outputSettings = [String(kCVPixelBufferPixelFormatTypeKey) : kCVPixelFormatType_420YpCbCr8BiPlanarFullRange]
        let videoOutput = AVPlayerItemVideoOutput(outputSettings: outputSettings)
        videoOutput.suppressesPlayerRendering = true
        item.add(videoOutput)
    }
    
    func _setupPlayerObservers(playerItem: AVPlayerItem?) {
        _removePlayerObservers()
        NotificationCenter.default.addObserver(self, selector: #selector(playerDidPlayToEnd), name: .AVPlayerItemDidPlayToEndTime, object: nil)
        NotificationCenter.default.addObserver(self, selector: #selector(playerStalled), name: .AVPlayerItemPlaybackStalled, object: nil)
        observations.append(observe(\.status) { [weak self] _, _ in
            self?.playerStatusDidChange()
        })
        observations.append(observe(\.rate) { [weak self] _, _ in
            self?.playerRateDidChange()
        })
        if let item = playerItem {
            observations.append(item.observe(\AVPlayerItem.status) { [weak self] _, _ in
                self?.playerItemStatusDidChange(item)
            })
        }
    }
    
    func _removePlayerObservers() {
        NotificationCenter.default.removeObserver(self, name: .AVPlayerItemDidPlayToEndTime, object: nil)
        NotificationCenter.default.removeObserver(self, name: .AVPlayerItemPlaybackStalled, object: nil)
        observations.forEach { $0.invalidate() }
        observations.removeAll()
    }
    
    /// NOTE: all time observer operations will be executed in main queue
    func _timeObserversUpdate(_ block: @escaping () -> Void) {
        if Thread.isMainThread {
            block()
        } else {
            DispatchQueue.main.async {
                block()
            }
        }
    }
    
    func _resetTimeObservers() {
        _timeObserversUpdate { [weak self] in
            guard let self = self else { return }
            self.timeObserversQueue.removeAll()
            for observer in self.totalTimeObservers {
                guard observer.targetTime >= (self.startTime ?? 0) && observer.targetTime <= self.endTime ?? self.assetDuration else {
                    continue
                }
                self.timeObserversQueue.append(observer)
            }
            if !self.loop, let endTime = self.endTime {
                let endTimeObserver = MoviePlayerTimeObserver(targetTime: endTime) { [weak self] _ in
                    if self?.loop == true && self?.isPlaying == true {
                        self?.pause()
                        self?.start()
                    } else {
                        self?.pause()
                    }
                }
                let insertIndex: Int = self.timeObserversQueue.reversed().firstIndex { endTime < $0.targetTime } ?? 0
                self.timeObserversQueue.insert(endTimeObserver, at: insertIndex)
            }
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
    
    func playerItemStatusDidChange(_ playerItem: AVPlayerItem) {
        debugPrint("PlayerItem status change to:\(playerItem.status.rawValue) asset:\(playerItem.asset)")
        if playerItem == currentItem {
            resumeIfNeeded()
        }
    }
    
    func resumeIfNeeded() {
        guard isReadyToPlay && isPlaying == true && rate != playrate else { return }
        if nextSeeking != nil {
            actuallySeekToTime()
        } else {
            rate = playrate
        }
    }
    
    // MARK: -
    // MARK: Internal processing functions
    
    func _process(videoOutput: AVPlayerItemVideoOutput, at playTime: CMTime) {
        guard let pixelBuffer = videoOutput.copyPixelBuffer(forItemTime: playTime, itemTimeForDisplay: nil) else {
            print("Failed to copy pixel buffer at time:\(playTime)")
            return
        }
        
        delegate?.moviePlayerDidReadPixelBuffer(pixelBuffer, time: CMTimeGetSeconds(playTime))
        
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
        
        guard !disableGPURender, let framebuffer = framebufferGenerator.generateFromYUVBuffer(pixelBuffer, frameTime: playTime, videoOrientation: videoOrientation) else { return }
        framebuffer.userInfo = framebufferUserInfo
        
        updateTargetsWithFramebuffer(framebuffer)
    }
    
    @objc func displayLinkCallback(displayLink: CADisplayLink) {
        guard currentItem != nil else {
            stop()
            return
        }
        
        let playTime = currentTime()
//        debugPrint("playtime:\(playTime.seconds)")
        guard playTime.seconds > 0 else { return }
        if let videoOutput = videoOutput, videoOutput.hasNewPixelBuffer(forItemTime: playTime) == true {
            if !disableGPURender {
                sharedImageProcessingContext.runOperationAsynchronously { [weak self] in
                    self?._process(videoOutput: videoOutput, at: playTime)
                }
            } else {
                _process(videoOutput: videoOutput, at: playTime)
            }
        }
        if playTime.seconds > 0 {
            _notifyTimeObserver(with: playTime)
        }
    }
    
    var videoOutput: AVPlayerItemVideoOutput? {
        return currentItem?.outputs.first(where: { $0 is AVPlayerItemVideoOutput }) as? AVPlayerItemVideoOutput
    }
    
    @objc func playerDidPlayToEnd(notification: Notification) {
        print("player did play to end. notification:\(notification)")
        guard (notification.object as? AVPlayerItem) == currentItem else { return }
        guard loop && isPlaying && (endTime == nil || currentTime().seconds == assetDuration) else { return }
        start()
    }
    
    @objc func playerStalled(notification: Notification) {
        print("player was stalled. notification:\(notification)")
        guard (notification.object as? AVPlayerItem) == currentItem else { return }
    }
    
    func _notifyTimeObserver(with sampleTime: CMTime) {
        let currentTime = sampleTime.seconds
        _timeObserversUpdate { [weak self] in
            while let lastObserver = self?.timeObserversQueue.last, lastObserver.targetTime <= currentTime {
                self?.timeObserversQueue.removeLast()
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
