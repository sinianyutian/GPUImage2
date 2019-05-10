open class ResizeCrop: BasicOperation {
    public var useCropSizeAsFinal = false
    public var cropSizeInPixels: Size?
    
    public init() {
        super.init(fragmentShader:PassthroughFragmentShader, numberOfInputs:1)
    }
    
    override open func renderFrame() {
        let inputFramebuffer:Framebuffer = inputFramebuffers[0]!
        let inputGLSize = inputFramebuffer.sizeForTargetOrientation(.portrait)
        let inputSize = Size(inputGLSize)

        let (normalizedOffsetFromOrigin, finalCropSize, normalizedCropSize) = calculateFinalFrame(inputSize: inputSize)

        renderFramebuffer = sharedImageProcessingContext.framebufferCache.requestFramebufferWithProperties(
            orientation: .portrait,
            size: GLSize(finalCropSize),
            stencil: false)
        
        let textureProperties = InputTextureProperties(textureCoordinates:inputFramebuffer.orientation.rotationNeededForOrientation(.portrait).croppedTextureCoordinates(offsetFromOrigin:normalizedOffsetFromOrigin, cropSize:normalizedCropSize), texture:inputFramebuffer.texture)
        
        renderFramebuffer.activateFramebufferForRendering()
        clearFramebufferWithColor(backgroundColor)
        renderQuadWithShader(shader, uniformSettings:uniformSettings, vertexBufferObject:sharedImageProcessingContext.standardImageVBO, inputTextures:[textureProperties])
        releaseIncomingFramebuffers()
    }

    public func calculateFinalFrame(inputSize: Size) -> (Position, Size, Size) {
        let finalCropSize: Size
        let normalizedCropSize: Size
        let normalizedOffsetFromOrigin: Position

        if let cropSize = cropSizeInPixels {
            let glCropSize: Size

            if useCropSizeAsFinal {
                // finalCropSize might be resized
                glCropSize = cropSize
            } else {
                // finalCropSize won't be resized
                let ratioW = cropSize.width / inputSize.width
                let ratioH = cropSize.height / inputSize.height
                if ratioW > ratioH {
                    glCropSize = Size(width: inputSize.width, height: inputSize.width * (cropSize.height / cropSize.width))
                } else {
                    glCropSize = Size(width: inputSize.height * (cropSize.width / cropSize.height), height: inputSize.height)
                }
            }

            finalCropSize = Size(width:min(inputSize.width, glCropSize.width), height:min(inputSize.height, glCropSize.height))
            
            // Scale finalCropSize to inputSize to crop original content
            let aspectFitRatioToOrigin = min(inputSize.width / finalCropSize.width, inputSize.height / finalCropSize.height)
            let cropSizeInOrigin = Size(width: finalCropSize.width * aspectFitRatioToOrigin, height: finalCropSize.height * aspectFitRatioToOrigin)
            normalizedCropSize = Size(width: cropSizeInOrigin.width / inputSize.width, height: cropSizeInOrigin.height / inputSize.height)
            normalizedOffsetFromOrigin = Position((inputSize.width / 2 - cropSizeInOrigin.width / 2) / inputSize.width,
                                                  (inputSize.height / 2 - cropSizeInOrigin.height / 2) / inputSize.height)
        } else {
            finalCropSize = inputSize
            normalizedOffsetFromOrigin  = Position.zero
            normalizedCropSize = Size(width: 1, height: 1)
        }

        return (normalizedOffsetFromOrigin, finalCropSize, normalizedCropSize)
    }
}
