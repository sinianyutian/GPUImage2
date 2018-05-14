//
//  ResizeCrop.swift
//  Alamofire
//
//  Created by rocry on 5/14/18.
//

open class ResizeCrop: BasicOperation {
    public var cropSizeInPixels: Size?
    
    public init() {
        super.init(fragmentShader:PassthroughFragmentShader, numberOfInputs:1)
    }
    
    override open func renderFrame() {
        let inputFramebuffer:Framebuffer = inputFramebuffers[0]!
        let inputSize = inputFramebuffer.sizeForTargetOrientation(.portrait)
        
        let finalCropSize:GLSize
        let normalizedOffsetFromOrigin:Position
        if let cropSize = cropSizeInPixels {
            let glCropSize: GLSize
            
            let ratioW = cropSize.width / Float(inputSize.width)
            let ratioH = cropSize.height / Float(inputSize.height)
            if ratioW > ratioH {
                glCropSize = GLSize(width: inputSize.width, height: GLint(Float(inputSize.width) * (cropSize.height / cropSize.width)))
            } else {
                glCropSize = GLSize(width: GLint(Float(inputSize.height) * (cropSize.width / cropSize.height)), height: inputSize.height)
            }
            
            finalCropSize = GLSize(width:min(inputSize.width, glCropSize.width), height:min(inputSize.height, glCropSize.height))
            normalizedOffsetFromOrigin = Position(Float(inputSize.width / 2 - finalCropSize.width / 2) / Float(inputSize.width),
                                                  Float(inputSize.height / 2 - finalCropSize.height / 2) / Float(inputSize.height))
        } else {
            finalCropSize = inputSize
            normalizedOffsetFromOrigin  = Position.zero
        }
        let normalizedCropSize = Size(width:Float(finalCropSize.width) / Float(inputSize.width), height:Float(finalCropSize.height) / Float(inputSize.height))
        
        renderFramebuffer = sharedImageProcessingContext.framebufferCache.requestFramebufferWithProperties(orientation:.portrait, size:finalCropSize, stencil:false)
        
        let textureProperties = InputTextureProperties(textureCoordinates:inputFramebuffer.orientation.rotationNeededForOrientation(.portrait).croppedTextureCoordinates(offsetFromOrigin:normalizedOffsetFromOrigin, cropSize:normalizedCropSize), texture:inputFramebuffer.texture)
        
        renderFramebuffer.activateFramebufferForRendering()
        clearFramebufferWithColor(backgroundColor)
        renderQuadWithShader(shader, uniformSettings:uniformSettings, vertexBufferObject:sharedImageProcessingContext.standardImageVBO, inputTextures:[textureProperties])
        releaseIncomingFramebuffers()
    }
}
