//
//  MovieCache.swift
//  GPUImage2
//
//  Created by 陈品霖 on 2020/3/27.
//

import Foundation
import AVFoundation

public enum MovieCacheError: Error {
    case invalidState
    case emptyMovieOutput
}

public class MovieCache: ImageConsumer, AudioEncodingTarget {
    public let sources = SourceContainer()
    public let maximumInputs: UInt = 1
    public private(set) var movieOutput: MovieOutput?
    public private(set) lazy var framebufferCache = [Framebuffer]()
    public private(set) lazy var videoSampleBufferCache = NSMutableArray()
    public private(set) lazy var audioSampleBufferCache = [CMSampleBuffer]()
    public private(set) var cacheBuffersDuration: TimeInterval = 0
    public enum State: String {
        case unknown
        case idle
        case caching
        case writing
        case stopped
        case canceled
    }
    public private(set) var state = State.unknown
    
    public init() {
        print("MovieCache init")
    }
    
    public func startCaching(duration: TimeInterval) {
        MovieOutput.movieProcessingContext.runOperationAsynchronously { [weak self] in
            self?._startCaching(duration: duration)
        }
    }
    
    public func setMovieOutput(_ movieOutput: MovieOutput) {
        MovieOutput.movieProcessingContext.runOperationAsynchronously { [weak self] in
            self?._setMovieOutput(movieOutput)
        }
    }
    
    public func startWriting(_ completionCallback:((_ error: Error?) -> Void)? = nil) {
        MovieOutput.movieProcessingContext.runOperationAsynchronously { [weak self] in
            self?._startWriting(completionCallback)
        }
    }
    
    public func stopWriting(_ completionCallback:((Error?) -> Void)? = nil) {
        MovieOutput.movieProcessingContext.runOperationAsynchronously { [weak self] in
            self?._stopWriting(completionCallback)
        }
    }
    
    public func cancelWriting(_ completionCallback:(() -> Void)? = nil) {
        MovieOutput.movieProcessingContext.runOperationAsynchronously { [weak self] in
            self?._cancelWriting(completionCallback)
        }
    }
    
    public func stopCaching() {
        MovieOutput.movieProcessingContext.runOperationAsynchronously { [weak self] in
            self?._stopCaching()
        }
    }
}

extension MovieCache {
    public func newFramebufferAvailable(_ framebuffer: Framebuffer, fromSourceIndex: UInt) {
//        debugPrint("get new framebuffer time:\(framebuffer.timingStyle.timestamp?.asCMTime.seconds ?? .zero)")
        guard shouldProcessBuffer else { return }
        glFinish()
        _cacheFramebuffer(framebuffer)
        _writeFramebuffers()
    }
    
    public func activateAudioTrack() throws {
        try movieOutput?.activateAudioTrack()
    }
    
    public func processAudioBuffer(_ sampleBuffer: CMSampleBuffer, shouldInvalidateSampleWhenDone: Bool) {
        guard shouldProcessBuffer else { return }
        _cacheAudioSampleBuffer(sampleBuffer)
        _writeAudioSampleBuffers(shouldInvalidateSampleWhenDone)
    }
    
    public func processVideoBuffer(_ sampleBuffer: CMSampleBuffer, shouldInvalidateSampleWhenDone:Bool) {
        guard shouldProcessBuffer else { return }
        _cacheVideoSampleBuffer(sampleBuffer)
        _writeVideoSampleBuffers(shouldInvalidateSampleWhenDone)
    }
    
    public func readyForNextAudioBuffer() -> Bool {
        guard shouldProcessBuffer else { return false }
        return true
    }
}

private extension MovieCache {
    var shouldProcessBuffer: Bool {
        return state != .unknown && state != .idle
    }
    
    func _tryTransitingState(to newState: State) -> Bool {
        guard state != newState else { return true }
        switch (state, newState) {
        case (.unknown, .idle), (.unknown, .caching), (.unknown, .writing),
             (.idle, .caching), (.idle, .writing), (.idle, .canceled),
             (.caching, .writing), (.caching, .stopped), (.caching, .canceled), (.caching, .idle),
             (.writing, .stopped), (.writing, .canceled),
             (.stopped, .idle), (.stopped, .writing),
             (.canceled, .idle), (.canceled, .writing):
            debugPrint("state transite from:\(state) to:\(newState)")
            state = newState
            return true
        default:
            assertionFailure()
            print("ERROR: invalid state transition from:\(state) to:\(newState)")
            return false
        }
    }
    
    func _startCaching(duration: TimeInterval) {
        guard _tryTransitingState(to: .caching) else { return }
        print("start caching")
        cacheBuffersDuration = duration
    }
    
    func _setMovieOutput(_ movieOutput: MovieOutput) {
        guard state != .writing || self.movieOutput == nil else {
            assertionFailure("Should not set MovieOutput during writing")
            return
        }
        print("set movie output")
        self.movieOutput = movieOutput
    }
    
    func _startWriting(_ completionCallback:((_ error: Error?) -> Void)? = nil) {
        guard _tryTransitingState(to: .writing) else {
            completionCallback?(MovieCacheError.invalidState)
            return
        }
        guard movieOutput != nil else {
            print("movie output is not ready yet, waiting...")
            completionCallback?(nil)
            return
        }
        print("start writing")
        movieOutput?.startRecording(sync: true) { _, error in
            completionCallback?(error)
        }
    }
    
    func _stopWriting(_ completionCallback:((Error?) -> Void)? = nil) {
        guard _tryTransitingState(to: .stopped), movieOutput != nil else {
            completionCallback?(MovieCacheError.invalidState)
            return
        }
        print("stop writing. videoFramebuffers:\(framebufferCache.count) audioSampleBuffers:\(audioSampleBufferCache.count) videoSampleBuffers:\(videoSampleBufferCache.count)")
        movieOutput?.finishRecording(sync: true) {
            completionCallback?(nil)
        }
        movieOutput = nil
    }
    
    func _cancelWriting(_ completionCallback:(() -> Void)? = nil) {
        guard _tryTransitingState(to: .canceled), let movieOutput = movieOutput else {
            completionCallback?()
            self.movieOutput = nil
            return
        }
        print("cancel writing")
        movieOutput.cancelRecording(sync: true) {
            completionCallback?()
        }
        self.movieOutput = nil
    }
    
    func _stopCaching() {
        guard _tryTransitingState(to: .idle) else { return }
        print("stop caching")
        _cleanBufferCaches()
    }
    
    func _cleanBufferCaches() {
        print("Clean all buffers framebufferCache:\(framebufferCache.count) audioSampleBuffer:\(audioSampleBufferCache.count) videoSampleBuffers:\(videoSampleBufferCache.count)")
        sharedImageProcessingContext.runOperationSynchronously {
            self.framebufferCache.forEach { $0.unlock() }
            self.framebufferCache.removeAll()
            self.videoSampleBufferCache.removeAllObjects()
            self.audioSampleBufferCache.removeAll()
        }
    }
    
    func _cacheFramebuffer(_ framebuffer: Framebuffer) {
        guard let frameTime = framebuffer.timingStyle.timestamp?.asCMTime else {
            print("Cannot get timestamp from framebuffer, dropping frame")
            return
        }
        framebufferCache.append(framebuffer)
        while let firstBufferTime = framebufferCache.first?.timingStyle.timestamp?.asCMTime, CMTimeSubtract(frameTime, firstBufferTime).seconds > cacheBuffersDuration {
//            debugPrint("dropping oldest video framebuffer time:\(firstBufferTime.seconds)")
            _ = framebufferCache.removeFirst()
        }
    }
    
    func _writeFramebuffers() {
        guard state == .writing else { return }
        var appendedBufferCount = 0
        for framebuffer in framebufferCache {
            guard movieOutput?._processFramebuffer(framebuffer) == true else { break }
            appendedBufferCount += 1
            framebuffer.unlock()
            // NOTE: don't occupy too much GPU time, if it is already accumulate lots of framebuffer.
            // So that it can reduce frame drop and video frames brightness flashing.
            guard sharedImageProcessingContext.alreadyExecuteTime < 1.0 / 40.0 else { break }
        }
        framebufferCache.removeFirst(appendedBufferCount)
    }
    
    func _cacheAudioSampleBuffer(_ sampleBuffer: CMSampleBuffer) {
        let frameTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
        audioSampleBufferCache.append(sampleBuffer)
        while let firstBuffer = audioSampleBufferCache.first, CMTimeSubtract(frameTime, CMSampleBufferGetPresentationTimeStamp(firstBuffer)).seconds > cacheBuffersDuration {
//            debugPrint("dropping oldest audio buffer time:\(CMSampleBufferGetPresentationTimeStamp(firstBuffer)).seconds))")
            _ = audioSampleBufferCache.removeFirst()
        }
    }
    
    func _writeAudioSampleBuffers(_ shouldInvalidateSampleWhenDone: Bool) {
        guard state == .writing else { return }
        var appendedBufferCount = 0
        for audioBuffer in audioSampleBufferCache {
            //                        debugPrint("[Caching] appending audio buffer \(i+1)/\(self.audioSampleBufferCache.count) at:\(CMSampleBufferGetOutputPresentationTimeStamp(audioBuffer).seconds)")
            guard movieOutput?._processAudioSampleBuffer(audioBuffer, shouldInvalidateSampleWhenDone: shouldInvalidateSampleWhenDone) == true else { break }
            appendedBufferCount += 1
        }
        audioSampleBufferCache.removeFirst(appendedBufferCount)
    }
    
    func _cacheVideoSampleBuffer(_ sampleBuffer: CMSampleBuffer) {
        let frameTime = CMSampleBufferGetPresentationTimeStamp(sampleBuffer)
        videoSampleBufferCache.add(sampleBuffer)
        //            debugPrint("[Caching] cache new video sample buffer at:\(frameTime.seconds)")
        if videoSampleBufferCache.count >= 13 {
            // Be careful of caching too much sample buffers from camera captureOutput. iOS has a hard limit of camera buffer count: 15.
            //                debugPrint("WARNING: almost reach system buffer limit: \(self.videoSampleBufferCache.count)/15")
        }
        while let firstBuffer = videoSampleBufferCache.firstObject, CMTimeSubtract(frameTime, CMSampleBufferGetPresentationTimeStamp(firstBuffer as! CMSampleBuffer)).seconds > cacheBuffersDuration {
//            debugPrint("dropping oldest video buffer time:\(CMSampleBufferGetPresentationTimeStamp(firstBuffer as! CMSampleBuffer).seconds)")
            videoSampleBufferCache.removeObject(at: 0)
        }
    }
    
    private func _writeVideoSampleBuffers(_ shouldInvalidateSampleWhenDone: Bool) {
        guard state == .writing else { return }
        var appendedBufferCount = 0
        // Drain all cached buffers at first
        for sampleBufferObject in videoSampleBufferCache {
            let sampleBuffer = sampleBufferObject as! CMSampleBuffer
            guard movieOutput?._processVideoSampleBuffer(sampleBuffer, shouldInvalidateSampleWhenDone: shouldInvalidateSampleWhenDone) == true else { break }
            appendedBufferCount += 1
        }
        videoSampleBufferCache.removeObjects(in: NSRange(0..<appendedBufferCount))
    }
}
