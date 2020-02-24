//
//  CILookupFilter.swift
//  DayCam
//
//  Created by 陈品霖 on 2020/2/23.
//  Copyright © 2020 rocry. All rights reserved.
//

import Foundation

public class CILookupFilter {
    private var lutFilter: CIFilter?
    public private(set) var intensity: Double?
    public private(set) var brightnessFactor: Double?
    // Use "ColorMatrix(Alpha) + Composite" filters for color LUT
    private var alphaFilter: CIFilter?
    private var compositeFilter: CIFilter?
    private lazy var alphaColorMatrix = [CGFloat]()
    // Use "ColorControl(Brightness)" filter for black and white LUT
    private var brightnessFilter: CIFilter?
    
    init(lutImage: UIImage, intensity: Double? = nil, brightnessFactor: Double? = nil) {
        self.intensity = intensity
        self.brightnessFactor = brightnessFactor
        lutFilter = CIFilter.filter(with: lutImage)
        if let intensity = intensity {
            if let factor = brightnessFactor {
                brightnessFilter = CIFilter(name: "CIColorControls")
                brightnessFilter?.setDefaults()
                let adjustedBrightness = -factor + factor * intensity
                brightnessFilter?.setValue(NSNumber(value: adjustedBrightness), forKey: kCIInputBrightnessKey)
            } else {
                alphaColorMatrix = [0, 0, 0, CGFloat(intensity)]
                alphaFilter = CIFilter(name: "CIColorMatrix")
                alphaFilter?.setDefaults()
                alphaFilter?.setValue(CIVector(values: &alphaColorMatrix, count: 4), forKey: "inputAVector")
                
                compositeFilter = CIFilter(name: "CISourceOverCompositing")
                compositeFilter?.setDefaults()
            }
        }
    }
    
    func applyFilter(on image: CIImage) -> CIImage? {
        lutFilter?.setValue(image, forKey: kCIInputImageKey)
        if intensity == nil {
            return lutFilter?.outputImage
        } else {
            if brightnessFactor != nil {
                brightnessFilter?.setValue(lutFilter?.outputImage, forKey: kCIInputImageKey)
                return brightnessFilter?.outputImage
            } else {
                alphaFilter?.setValue(lutFilter?.outputImage, forKey: kCIInputImageKey)
                compositeFilter?.setValue(alphaFilter?.outputImage, forKey: kCIInputImageKey)
                compositeFilter?.setValue(image, forKey: kCIInputBackgroundImageKey)
                return compositeFilter?.outputImage
            }
        }
    }
}

public extension CIFilter {
    static func filter(with lutUIImage: UIImage) -> CIFilter? {
        guard let lutCGImage = lutUIImage.cgImage else {
            print("ERROR: Invalid colorLUT");
            return nil
        }
        let size = 64
        let lutWidth = lutCGImage.width
        let lutHeight = lutCGImage.height
        let rowCount = lutHeight / size
        let columnCount = lutWidth / size

        guard lutWidth % size == 0 && lutHeight % size == 0 && rowCount * columnCount == size else {
            print("ERROR: Invalid colorLUT image size, width:\(lutWidth) height:\(lutHeight)");
            return nil
        }

        guard let bitmap  = getBytesFromImage(image: lutUIImage) else {
            print("ERROR: Cannot get byte from image")
            return nil
        }
        
        let floatSize = MemoryLayout<Float>.size
        let cubeData = UnsafeMutablePointer<Float>.allocate(capacity: size * size * size * 4 * floatSize)
        var z = 0
        var bitmapOffset = 0

        for _ in 0 ..< rowCount {
            for y in 0 ..< size {
                let tmp = z
                for _ in 0 ..< columnCount {
                    for x in 0 ..< size {
                        let alpha = Float(bitmap[bitmapOffset]) / 255.0
                        let red = Float(bitmap[bitmapOffset+1]) / 255.0
                        let green = Float(bitmap[bitmapOffset+2]) / 255.0
                        let blue = Float(bitmap[bitmapOffset+3]) / 255.0

                        let dataOffset = (z * size * size + y * size + x) * 4

                        cubeData[dataOffset + 3] = alpha
                        cubeData[dataOffset + 2] = red
                        cubeData[dataOffset + 1] = green
                        cubeData[dataOffset + 0] = blue
                        bitmapOffset += 4
                    }
                    z += 1
                }
                z = tmp
            }
            z += columnCount
        }

        // create CIColorCube Filter
        let colorCubeData = NSData(bytesNoCopy: cubeData, length: size * size * size * 4 * floatSize, freeWhenDone: true)
        guard let filter = CIFilter(name: "CIColorCube") else {
            print("ERROR: Cannot get CIColorCube filter")
            return nil
        }
        filter.setValue(colorCubeData, forKey: "inputCubeData")
        filter.setValue(size, forKey: "inputCubeDimension")
        return filter
    }
    
    static func getBytesFromImage(image: UIImage?) -> [UInt8]? {
        var pixelValues: [UInt8]?
        if let imageRef = image?.cgImage {
            let width = Int(imageRef.width)
            let height = Int(imageRef.height)
            let bitsPerComponent = 8
            let bytesPerRow = width * 4
            let totalBytes = height * bytesPerRow

            let bitmapInfo = CGImageAlphaInfo.premultipliedLast.rawValue | CGBitmapInfo.byteOrder32Little.rawValue
            let colorSpace = CGColorSpaceCreateDeviceRGB()
            var intensities = [UInt8](repeating: 0, count: totalBytes)

            let contextRef = CGContext(data: &intensities, width: width, height: height, bitsPerComponent: bitsPerComponent, bytesPerRow: bytesPerRow, space: colorSpace, bitmapInfo: bitmapInfo)
            contextRef?.draw(imageRef, in: CGRect(x: 0.0, y: 0.0, width: CGFloat(width), height: CGFloat(height)))

            pixelValues = intensities
        }
        return pixelValues
    }
}
