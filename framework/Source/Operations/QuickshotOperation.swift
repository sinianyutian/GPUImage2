//
//  QuickshotOperation.swift
//  GPUImage
//
//  Created by RoCry on 4/23/19.
//  Copyright © 2019 Sunset Lake Software LLC. All rights reserved.
//

#if os(Linux)
#if GLES
import COpenGLES.gles2
#else
import COpenGL
#endif
#else
#if GLES
import OpenGLES
import CoreGraphics
#else
import OpenGL.GL3
#endif
#endif

open class QuickshotOpeartion: BasicOperation {
    let rotationProvider: (() -> Float)
    public var transform:Matrix4x4 = Matrix4x4.identity { didSet { uniformSettings["transformMatrix"] = transform } }
    public var anchorTopLeft = false
    public var ignoreAspectRatio = false
    var normalizedImageVertices:[GLfloat]!

    public var cropSizeInPixels: Size?
    public var locationOfCropInPixels: Position?

    public init(rotationProvider: @escaping (() -> Float)) {
        self.rotationProvider = rotationProvider

        super.init(vertexShader:TransformVertexShader, fragmentShader:PassthroughFragmentShader, numberOfInputs:1)

        ({transform = Matrix4x4.identity})()
    }

    override open func renderFrame() {
        let inputFramebuffer:Framebuffer = inputFramebuffers[0]!
        let inputSize = inputFramebuffer.sizeForTargetOrientation(.portrait)
        updateRotation(inputSize: inputSize)

        let finalCropSize:GLSize
        let normalizedOffsetFromOrigin:Position
        if let cropSize = cropSizeInPixels, let locationOfCrop = locationOfCropInPixels {
            let glCropSize = GLSize(cropSize)
            finalCropSize = GLSize(width:min(inputSize.width, glCropSize.width), height:min(inputSize.height, glCropSize.height))
            normalizedOffsetFromOrigin = Position(locationOfCrop.x / Float(inputSize.width), locationOfCrop.y / Float(inputSize.height))
        } else if let cropSize = cropSizeInPixels {
            let glCropSize = GLSize(cropSize)
            finalCropSize = GLSize(width:min(inputSize.width, glCropSize.width), height:min(inputSize.height, glCropSize.height))
            normalizedOffsetFromOrigin = Position(Float(inputSize.width / 2 - finalCropSize.width / 2) / Float(inputSize.width), Float(inputSize.height / 2 - finalCropSize.height / 2) / Float(inputSize.height))
        } else {
            finalCropSize = inputSize
            normalizedOffsetFromOrigin  = Position.zero
        }
        let normalizedCropSize = Size(width:Float(finalCropSize.width) / Float(inputSize.width), height:Float(finalCropSize.height) / Float(inputSize.height))

        let bufferSize:GLSize
        if abs(abs(Double(inputSize.width)/Double(inputSize.height)) - abs(Double(finalCropSize.width)/Double(finalCropSize.height))) < 0.01 {
            bufferSize = inputSize
        } else {
            bufferSize = finalCropSize
        }

        renderFramebuffer = sharedImageProcessingContext.framebufferCache.requestFramebufferWithProperties(orientation:.portrait, size:bufferSize, stencil:false)

        let textureProperties = InputTextureProperties(textureCoordinates:inputFramebuffer.orientation.rotationNeededForOrientation(.portrait).croppedTextureCoordinates(offsetFromOrigin:normalizedOffsetFromOrigin, cropSize:normalizedCropSize), texture:inputFramebuffer.texture)
        configureFramebufferSpecificUniforms(inputFramebuffer)

        renderFramebuffer.activateFramebufferForRendering()
        clearFramebufferWithColor(backgroundColor)
        renderQuadWithShader(shader, uniformSettings:uniformSettings, vertices:normalizedImageVertices, inputTextures:[textureProperties])
        releaseIncomingFramebuffers()
    }

    override open func configureFramebufferSpecificUniforms(_ inputFramebuffer:Framebuffer) {
        let outputRotation = overriddenOutputRotation ?? inputFramebuffer.orientation.rotationNeededForOrientation(.portrait)
        var aspectRatio = inputFramebuffer.aspectRatioForRotation(outputRotation)
        if(ignoreAspectRatio) {
            aspectRatio = 1
        }
        let orthoMatrix = orthographicMatrix(-1.0, right:1.0, bottom:-1.0 * aspectRatio, top:1.0 * aspectRatio, near:-1.0, far:1.0, anchorTopLeft:anchorTopLeft)
        normalizedImageVertices = normalizedImageVerticesForAspectRatio(aspectRatio)

        uniformSettings["orthographicMatrix"] = orthoMatrix
    }

    func normalizedImageVerticesForAspectRatio(_ aspectRatio:Float) -> [GLfloat] {
        // [TopLeft.x, TopLeft.y, TopRight.x, TopRight.y, BottomLeft.x, BottomLeft.y, BottomRight.x, BottomRight.y]
        if(anchorTopLeft) {
            return [0.0, 0.0, 1.0, 0.0, 0.0,  GLfloat(aspectRatio), 1.0,  GLfloat(aspectRatio)]
        }
        else {
            return [-1.0, GLfloat(-aspectRatio), 1.0, GLfloat(-aspectRatio), -1.0,  GLfloat(aspectRatio), 1.0,  GLfloat(aspectRatio)]
        }
    }

    private func updateRotation(inputSize: GLSize) {
        let inputAngle = rotationProvider()
        if inputAngle == 0 {
            locationOfCropInPixels = nil
            cropSizeInPixels = nil
            transform = .identity
            return
        }
        let angle = max(min(inputAngle, .pi / 6), -.pi / 6)
        let absAngle = abs(angle)

        let w0: Float = Float(inputSize.width)
        let h0: Float = Float(inputSize.height)

        let ratio = h0/w0

        // new rect width/height
        let w1 = w0 / (cos(absAngle) + ratio * sin(absAngle))
        let h1 = w1 * ratio

        let a = sqrt(pow(w0, 2.0) + pow(h0, 2.0)) / 2.0
        // upper case for angle
        let A = atan(h0/w0)
        let B = Float.pi - absAngle - A
        let b = a / sin(B) * sin(A)
        let c = a - b
        let d = c / sin(absAngle) * sin(B)
        let e = d * sin(absAngle)
        let f = w1 * sin(absAngle) * cos(absAngle) - e
        let g = c / cos(absAngle) * cos(A)
        let h = a / sin(B) * sin(absAngle) - g
        let i = tan(absAngle) * f
        let j = d / sin(B) * sin(A)
        let k = w0 - h - g - j
        let l = f / tan(absAngle)

        let x = angle > 0 ? h - i : k - l
        let y = f

        locationOfCropInPixels = Position(Float(x), Float(y))
        cropSizeInPixels = Size(width: Float(w1), height: Float(h1))

        debugPrint("angle: \(Int(angle/Float.pi * 180)), x: \(x), y: \(y), size: \(w1)*\(h1)")
        transform = Matrix4x4(CGAffineTransform(rotationAngle: CGFloat(angle)))
    }
}


