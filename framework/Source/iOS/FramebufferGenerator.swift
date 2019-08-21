//
//  FramebufferGenerator.swift
//  GPUImage2
//
//  Created by 陈品霖 on 2019/8/22.
//

import CoreMedia

public class FramebufferGenerator {
    lazy var yuvConversionShader = _setupShader()
    
    public init() {
        
    }
    
    public func generateFromPixelBuffer(_ movieFrame: CVPixelBuffer, frameTime: CMTime, videoOrientation: ImageOrientation) -> Framebuffer? {
        var framebuffer: Framebuffer?
        sharedImageProcessingContext.runOperationSynchronously {
            framebuffer = _generateFromPixelBuffer(movieFrame, frameTime: frameTime, videoOrientation: videoOrientation)
        }
        return framebuffer
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
    
    func _generateFromPixelBuffer(_ movieFrame: CVPixelBuffer, frameTime: CMTime, videoOrientation: ImageOrientation) -> Framebuffer? {
        guard let yuvConversionShader = yuvConversionShader else {
            debugPrint("ERROR! yuvConversionShader hasn't been setup before starting")
            return nil
        }
        let originalOrientation = videoOrientation.originalOrientation
        let bufferHeight = CVPixelBufferGetHeight(movieFrame)
        let bufferWidth = CVPixelBufferGetWidth(movieFrame)
        let conversionMatrix = colorConversionMatrix601FullRangeDefault
        CVPixelBufferLockBaseAddress(movieFrame, CVPixelBufferLockFlags(rawValue: CVOptionFlags(0)))
        defer {
            CVPixelBufferUnlockBaseAddress(movieFrame, CVPixelBufferLockFlags(rawValue: CVOptionFlags(0)))
            CVOpenGLESTextureCacheFlush(sharedImageProcessingContext.coreVideoTextureCache, 0)
        }
        
        glActiveTexture(GLenum(GL_TEXTURE0))
        var luminanceGLTexture: CVOpenGLESTexture?
        let luminanceGLTextureResult = CVOpenGLESTextureCacheCreateTextureFromImage(kCFAllocatorDefault, sharedImageProcessingContext.coreVideoTextureCache, movieFrame, nil, GLenum(GL_TEXTURE_2D), GL_LUMINANCE, GLsizei(bufferWidth), GLsizei(bufferHeight), GLenum(GL_LUMINANCE), GLenum(GL_UNSIGNED_BYTE), 0, &luminanceGLTexture)
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
        let chrominanceGLTextureResult = CVOpenGLESTextureCacheCreateTextureFromImage(kCFAllocatorDefault, sharedImageProcessingContext.coreVideoTextureCache, movieFrame, nil, GLenum(GL_TEXTURE_2D), GL_LUMINANCE_ALPHA, GLsizei(bufferWidth / 2), GLsizei(bufferHeight / 2), GLenum(GL_LUMINANCE_ALPHA), GLenum(GL_UNSIGNED_BYTE), 1, &chrominanceGLTexture)
        
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
