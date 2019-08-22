//
//  FramebufferGenerator.swift
//  GPUImage2
//
//  Created by 陈品霖 on 2019/8/22.
//

import CoreMedia

public class FramebufferGenerator {
    lazy var yuvConversionShader = _setupShader()
    private(set) var outputSize: GLSize?
    private(set) var pixelBufferPool: CVPixelBufferPool?
    private var renderFramebuffer: Framebuffer?
    
    public init() {
        
    }
    
    public func generateFromYUVBuffer(_ yuvPixelBuffer: CVPixelBuffer, frameTime: CMTime, videoOrientation: ImageOrientation) -> Framebuffer? {
        var framebuffer: Framebuffer?
        sharedImageProcessingContext.runOperationSynchronously {
            framebuffer = _generateFromYUVBuffer(yuvPixelBuffer, frameTime: frameTime, videoOrientation: videoOrientation)
        }
        return framebuffer
    }
    
    public func convertToPixelBuffer(_ framebuffer: Framebuffer) -> CVPixelBuffer? {
        var pixelBuffer: CVPixelBuffer?
        sharedImageProcessingContext.runOperationSynchronously {
            pixelBuffer = _convertToPixelBuffer(framebuffer)
        }
        return pixelBuffer
    }
}

private extension FramebufferGenerator {
    func _setupShader() -> ShaderProgram? {
        var yuvConversionShader: ShaderProgram?
        sharedImageProcessingContext.runOperationSynchronously {
            yuvConversionShader = crashOnShaderCompileFailure("MoviePlayer") {
                try sharedImageProcessingContext.programForVertexShader(defaultVertexShaderForInputs(2),
                                                                        fragmentShader: YUVConversionFullRangeFragmentShader)
            }
        }
        return yuvConversionShader
    }
    
    func _generateFromYUVBuffer(_ yuvPixelBuffer: CVPixelBuffer, frameTime: CMTime, videoOrientation: ImageOrientation) -> Framebuffer? {
        guard let yuvConversionShader = yuvConversionShader else {
            debugPrint("ERROR! yuvConversionShader hasn't been setup before starting")
            return nil
        }
        let originalOrientation = videoOrientation.originalOrientation
        let bufferHeight = CVPixelBufferGetHeight(yuvPixelBuffer)
        let bufferWidth = CVPixelBufferGetWidth(yuvPixelBuffer)
        let conversionMatrix = colorConversionMatrix601FullRangeDefault
        CVPixelBufferLockBaseAddress(yuvPixelBuffer, CVPixelBufferLockFlags(rawValue: CVOptionFlags(0)))
        defer {
            CVPixelBufferUnlockBaseAddress(yuvPixelBuffer, CVPixelBufferLockFlags(rawValue: CVOptionFlags(0)))
            CVOpenGLESTextureCacheFlush(sharedImageProcessingContext.coreVideoTextureCache, 0)
        }
        
        glActiveTexture(GLenum(GL_TEXTURE0))
        var luminanceGLTexture: CVOpenGLESTexture?
        let luminanceGLTextureResult = CVOpenGLESTextureCacheCreateTextureFromImage(kCFAllocatorDefault, sharedImageProcessingContext.coreVideoTextureCache, yuvPixelBuffer, nil, GLenum(GL_TEXTURE_2D), GL_LUMINANCE, GLsizei(bufferWidth), GLsizei(bufferHeight), GLenum(GL_LUMINANCE), GLenum(GL_UNSIGNED_BYTE), 0, &luminanceGLTexture)
        if luminanceGLTextureResult != kCVReturnSuccess || luminanceGLTexture == nil {
            print("Could not create LuminanceGLTexture")
            return nil
        }
        
        let luminanceTexture = CVOpenGLESTextureGetName(luminanceGLTexture!)
        
        glBindTexture(GLenum(GL_TEXTURE_2D), luminanceTexture)
        glTexParameterf(GLenum(GL_TEXTURE_2D), GLenum(GL_TEXTURE_WRAP_S), GLfloat(GL_CLAMP_TO_EDGE));
        glTexParameterf(GLenum(GL_TEXTURE_2D), GLenum(GL_TEXTURE_WRAP_T), GLfloat(GL_CLAMP_TO_EDGE));
        
        let luminanceFramebuffer: Framebuffer
        do {
            luminanceFramebuffer = try Framebuffer(context: sharedImageProcessingContext,
                                                   orientation: originalOrientation,
                                                   size: GLSize(width: GLint(bufferWidth), height: GLint(bufferHeight)),
                                                   textureOnly: true,
                                                   overriddenTexture: luminanceTexture)
        } catch {
            print("Could not create a framebuffer of the size (\(bufferWidth), \(bufferHeight)), error: \(error)")
            return nil
        }
        
        glActiveTexture(GLenum(GL_TEXTURE1))
        var chrominanceGLTexture: CVOpenGLESTexture?
        let chrominanceGLTextureResult = CVOpenGLESTextureCacheCreateTextureFromImage(kCFAllocatorDefault, sharedImageProcessingContext.coreVideoTextureCache, yuvPixelBuffer, nil, GLenum(GL_TEXTURE_2D), GL_LUMINANCE_ALPHA, GLsizei(bufferWidth / 2), GLsizei(bufferHeight / 2), GLenum(GL_LUMINANCE_ALPHA), GLenum(GL_UNSIGNED_BYTE), 1, &chrominanceGLTexture)
        
        if chrominanceGLTextureResult != kCVReturnSuccess || chrominanceGLTexture == nil {
            print("Could not create ChrominanceGLTexture")
            return nil
        }
        
        let chrominanceTexture = CVOpenGLESTextureGetName(chrominanceGLTexture!)
        
        glBindTexture(GLenum(GL_TEXTURE_2D), chrominanceTexture)
        glTexParameterf(GLenum(GL_TEXTURE_2D), GLenum(GL_TEXTURE_WRAP_S), GLfloat(GL_CLAMP_TO_EDGE));
        glTexParameterf(GLenum(GL_TEXTURE_2D), GLenum(GL_TEXTURE_WRAP_T), GLfloat(GL_CLAMP_TO_EDGE));
        
        let chrominanceFramebuffer: Framebuffer
        do {
            chrominanceFramebuffer = try Framebuffer(context: sharedImageProcessingContext,
                                                     orientation: originalOrientation,
                                                     size: GLSize(width:GLint(bufferWidth), height:GLint(bufferHeight)),
                                                     textureOnly: true,
                                                     overriddenTexture: chrominanceTexture)
        } catch {
            print("Could not create a framebuffer of the size (\(bufferWidth), \(bufferHeight)), error: \(error)")
            return nil
        }
        
        let portraitSize: GLSize
        switch videoOrientation.rotationNeededForOrientation(.portrait) {
        case .noRotation, .rotate180, .flipHorizontally, .flipVertically:
            portraitSize = GLSize(width: GLint(bufferWidth), height: GLint(bufferHeight))
        case .rotateCounterclockwise, .rotateClockwise, .rotateClockwiseAndFlipVertically, .rotateClockwiseAndFlipHorizontally:
            portraitSize = GLSize(width: GLint(bufferHeight), height: GLint(bufferWidth))
        }
        
        let framebuffer = sharedImageProcessingContext.framebufferCache.requestFramebufferWithProperties(orientation: .portrait, size: portraitSize, textureOnly: false)
        
        convertYUVToRGB(shader: yuvConversionShader,
                        luminanceFramebuffer: luminanceFramebuffer,
                        chrominanceFramebuffer: chrominanceFramebuffer,
                        resultFramebuffer: framebuffer,
                        colorConversionMatrix: conversionMatrix)
        framebuffer.timingStyle = .videoFrame(timestamp: Timestamp(frameTime))
        return framebuffer
    }
    
    func _convertToPixelBuffer(_ framebuffer: Framebuffer) -> CVPixelBuffer? {
        if pixelBufferPool == nil || outputSize?.width != framebuffer.size.width || outputSize?.height != framebuffer.size.height {
            outputSize = framebuffer.size
            pixelBufferPool = _createPixelBufferPool(framebuffer.size.width, framebuffer.size.height, FourCharCode(kCVPixelFormatType_32BGRA), 3)
        }
        guard let pixelBufferPool = pixelBufferPool else { return nil }
        var outPixelBuffer: CVPixelBuffer?
        let pixelBufferStatus = CVPixelBufferPoolCreatePixelBuffer(nil, pixelBufferPool, &outPixelBuffer)
        guard let pixelBuffer = outPixelBuffer, pixelBufferStatus == kCVReturnSuccess else {
            print("WARNING: Unable to create pixel buffer, dropping frame")
            return nil
        }
        
        do {
            if renderFramebuffer == nil {
                CVBufferSetAttachment(pixelBuffer, kCVImageBufferColorPrimariesKey, kCVImageBufferColorPrimaries_ITU_R_709_2, .shouldPropagate)
                CVBufferSetAttachment(pixelBuffer, kCVImageBufferYCbCrMatrixKey, kCVImageBufferYCbCrMatrix_ITU_R_601_4, .shouldPropagate)
                CVBufferSetAttachment(pixelBuffer, kCVImageBufferTransferFunctionKey, kCVImageBufferTransferFunction_ITU_R_709_2, .shouldPropagate)
            }
            
            let bufferSize = framebuffer.size
            var cachedTextureRef: CVOpenGLESTexture?
            let _ = CVOpenGLESTextureCacheCreateTextureFromImage(kCFAllocatorDefault, sharedImageProcessingContext.coreVideoTextureCache, pixelBuffer, nil, GLenum(GL_TEXTURE_2D), GL_RGBA, bufferSize.width, bufferSize.height, GLenum(GL_BGRA), GLenum(GL_UNSIGNED_BYTE), 0, &cachedTextureRef)
            let cachedTexture = CVOpenGLESTextureGetName(cachedTextureRef!)
            
            renderFramebuffer = try Framebuffer(context: sharedImageProcessingContext, orientation:.portrait, size:bufferSize, textureOnly:false, overriddenTexture:cachedTexture)
            
            renderFramebuffer?.activateFramebufferForRendering()
            clearFramebufferWithColor(Color.black)
            CVPixelBufferLockBaseAddress(pixelBuffer, CVPixelBufferLockFlags(rawValue:CVOptionFlags(0)))
            renderQuadWithShader(sharedImageProcessingContext.passthroughShader, uniformSettings: ShaderUniformSettings(), vertexBufferObject: sharedImageProcessingContext.standardImageVBO, inputTextures: [framebuffer.texturePropertiesForOutputRotation(.noRotation)], context: sharedImageProcessingContext)
            
            glFinish()
        }
        catch {
            print("WARNING: Trouble appending pixel buffer at time: \(framebuffer.timingStyle.timestamp?.seconds() ?? 0) \(error)")
        }
        
        CVPixelBufferUnlockBaseAddress(pixelBuffer, CVPixelBufferLockFlags(rawValue: CVOptionFlags(0)))
        return pixelBuffer
    }
    
    func _createPixelBufferPool(_ width: Int32, _ height: Int32, _ pixelFormat: FourCharCode, _ maxBufferCount: Int32) -> CVPixelBufferPool? {
        var outputPool: CVPixelBufferPool? = nil
        
        let sourcePixelBufferOptions: NSDictionary = [kCVPixelBufferPixelFormatTypeKey: pixelFormat,
                                                      kCVPixelBufferWidthKey: width,
                                                      kCVPixelBufferHeightKey: height,
                                                      kCVPixelFormatOpenGLESCompatibility: true,
                                                      kCVPixelBufferIOSurfacePropertiesKey: NSDictionary()]
        
        let pixelBufferPoolOptions: NSDictionary = [kCVPixelBufferPoolMinimumBufferCountKey: maxBufferCount]
        
        CVPixelBufferPoolCreate(kCFAllocatorDefault, pixelBufferPoolOptions, sourcePixelBufferOptions, &outputPool)
        
        return outputPool
    }
}

public extension ImageOrientation {
    var originalOrientation: ImageOrientation {
        switch self {
        case .portrait, .portraitUpsideDown:
            return self
        case .landscapeLeft:
            return .landscapeRight
        case .landscapeRight:
            return .landscapeLeft
        }
    }
}
