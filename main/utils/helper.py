import cv2
import numpy as np
from turbojpeg import TurboJPEG, TJFLAG_PROGRESSIVE, TJPF_BGR


jpeg = TurboJPEG()

def split_bytes(byte_data:bytearray, num_chunks:int):
    chunk_size = len(byte_data) // num_chunks
    return [byte_data[i:i + int(chunk_size)] for i in range(0, len(byte_data), int(chunk_size))]


def turbojpeg_encode(frame:np.ndarray, quality:int):
    jpeg = TurboJPEG()
    buffer = jpeg.encode(
        frame,
        quality=int(quality),
        pixel_format=TJPF_BGR,
        flags=TJFLAG_PROGRESSIVE
    )
    return buffer

def opencv_encode(frame:np.ndarray, quality:int):
    encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), quality]
    result, comp_image = cv2.imencode('.jpg', frame, encode_param)
    return result.tobytes()

def jpeg_encode(encoder_type:str, frame:np.ndarray, quality:int):
    if encoder_type == 'turbojpeg':
        return turbojpeg_encode(frame, quality)
    elif encoder_type == 'opencv':
        return opencv_encode(frame, quality)
    else:
        raise ValueError(f"Invalid encoder type: {encoder_type}")
