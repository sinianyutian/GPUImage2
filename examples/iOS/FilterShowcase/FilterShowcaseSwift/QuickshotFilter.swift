//
//  QuickshotFilter.swift
//  FilterShowcase
//
//  Created by RoCry on 4/18/19.
//  Copyright © 2019 Sunset Lake Software. All rights reserved.
//

import Foundation
import QuartzCore
import GPUImage

final class QuickshotFilter: OperationGroup {
    private let crop = Crop()
    private let transform = TransformOperation()

    var angle: Float = 0.0 {
        didSet {
            _updateAngle(angle)
        }
    }

    override init() {
        super.init()

        configureGroup { input, output in
            input --> transform --> crop --> output
        }
    }

    func _updateAngle(_ inputAngle: Float) {
        if inputAngle == 0 {
            crop.locationOfCropInPixels = nil
            crop.cropSizeInPixels = nil
            transform.transform = .identity
            return
        }
        let angle = abs(inputAngle)

        // TODO: use real w/h
        let w0: Float = 480.0
        let h0: Float = 640.0

        let ratio = h0/w0

        // new rect width/height
        let w1 = w0 / (cos(angle) + ratio * sin(angle))
        let h1 = w1 * ratio

        let a = sqrt(pow(w0, 2.0) + pow(h0, 2.0)) / 2.0
        // upper case for angle
        let A = atan(h0/w0)
        let B = Float.pi - angle - A
        let b = a / sin(B) * sin(A)
        let c = a - b
        let d = c / sin(angle) * sin(B)
        let e = d * sin(angle)
        let f = w1 * sin(angle) * cos(angle) - e
        let g = c / cos(angle) * cos(A)
        let h = a / sin(B) * sin(angle) - g
        let i = tan(angle) * f
        let j = d / sin(B) * sin(A)
        let k = w0 - h - g - j
        let l = f / tan(angle)

        let x = inputAngle > 0 ? h - i : k - l
        let y = f

        crop.locationOfCropInPixels = Position(Float(x), Float(y))
        crop.cropSizeInPixels = Size(width: Float(w1), height: Float(h1))

        print("angle: \(Int(angle/Float.pi * 180)), x: \(x), y: \(y), size: \(w1)*\(h1)")
        transform.transform = Matrix4x4(CGAffineTransform(rotationAngle: CGFloat(inputAngle)))
    }
}
