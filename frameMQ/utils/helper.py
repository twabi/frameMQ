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

def sanitize_json_string(json_str):
    """
    Sanitize a JSON string by removing or replacing invalid control characters.

    Args:
        json_str (str): The JSON string to sanitize

    Returns:
        str: Sanitized JSON string
    """
    if not isinstance(json_str, str):
        return json_str

    # Define a translation table to remove or replace control characters
    control_chars = {
        # Remove null bytes
        '\x00': '',
        # Replace other common problematic control characters with spaces
        '\x01': ' ', '\x02': ' ', '\x03': ' ', '\x04': ' ',
        '\x05': ' ', '\x06': ' ', '\x07': ' ', '\x08': ' ',
        '\x0b': ' ', '\x0c': ' ', '\x0e': ' ', '\x0f': ' ',
        # Preserve valid whitespace characters
        '\x09': '\t',  # tab
        '\x0a': '\n',  # newline
        '\x0d': '\r',  # carriage return
    }

    # Create a translation table
    trans_table = str.maketrans(control_chars)

    # Apply the translation
    sanitized = json_str.translate(trans_table)
    return sanitized