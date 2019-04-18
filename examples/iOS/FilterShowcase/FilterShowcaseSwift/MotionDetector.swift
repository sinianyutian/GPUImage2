//
//  MotionDetector.swift
//  FilterShowcase
//
//  Created by RoCry on 4/18/19.
//  Copyright © 2019 Sunset Lake Software. All rights reserved.
//

import Foundation
import CoreMotion

final class MotionDetector {
    static let shared = MotionDetector()

    lazy private var queue: OperationQueue = {
        let q = OperationQueue()
        q.maxConcurrentOperationCount = 1
        q.name = "motion"
        return q
    }()
    lazy private var manager = CMMotionManager()

    init() {

    }

    func start(callback: @escaping (Double) -> Void) {
        guard manager.isDeviceMotionAvailable else {
            return
        }

        manager.deviceMotionUpdateInterval = 0.01
        manager.startDeviceMotionUpdates(to: queue) { (data, error) in

            guard let data = data, error == nil else {
                return
            }

            let rotation = .pi - atan2(data.gravity.x, data.gravity.y)

            print(String(format: "rotation: %.2f, (%.2f, %.2f)", rotation,  data.gravity.x, data.gravity.y))
            DispatchQueue.main.async {
                callback(rotation)
            }
        }
    }

    func stop() {
        guard manager.isDeviceMotionAvailable else {
            return
        }

        manager.stopDeviceMotionUpdates()
    }
}
