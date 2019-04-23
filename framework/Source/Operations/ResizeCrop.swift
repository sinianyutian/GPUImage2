open class ResizeCrop: BasicOperation {
    public var cropSizeInPixels: Size?
    
    public init() {
        super.init(fragmentShader:PassthroughFragmentShader, numberOfInputs:1)
    }
    
    override open func renderFrame() {
        let inputFramebuffer:Framebuffer = inputFramebuffers[0]!
        let inputGLSize = inputFramebuffer.sizeForTargetOrientation(.portrait)
        let inputSize = Size(inputGLSize)

        let (normalizedOffsetFromOrigin, finalCropSize) = calculateFinalFrame(inputSize: inputSize)
        let normalizedCropSize = Size(width: finalCropSize.width / inputSize.width, height: finalCropSize.height / inputSize.height)

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

    public func calculateFinalFrame(inputSize: Size) -> (Position, Size) {
        let finalCropSize: Size
        let normalizedOffsetFromOrigin: Position

        if let cropSize = cropSizeInPixels {
            let glCropSize: Size

            let ratioW = cropSize.width / inputSize.width
            let ratioH = cropSize.height / inputSize.height
            if ratioW > ratioH {
                glCropSize = Size(width: inputSize.width, height: inputSize.width * (cropSize.height / cropSize.width))
            } else {
                glCropSize = Size(width: inputSize.height * (cropSize.width / cropSize.height), height: inputSize.height)
            }

            finalCropSize = Size(width:min(inputSize.width, glCropSize.width), height:min(inputSize.height, glCropSize.height))
            normalizedOffsetFromOrigin = Position((inputSize.width / 2 - finalCropSize.width / 2) / inputSize.width,
                                                  (inputSize.height / 2 - finalCropSize.height / 2) / inputSize.height)
        } else {
            finalCropSize = inputSize
            normalizedOffsetFromOrigin  = Position.zero
        }

        return (normalizedOffsetFromOrigin, finalCropSize)
    }
}
