from threading import Thread, Lock
import numpy as np
from main.models.models import EncodeParams


from main.utils.helper import split_bytes, jpeg_encode


class FrameEncode:
    def __init__(self, frame: np.ndarray, params: EncodeParams):
        self.frame = frame
        self.quality = params.quality
        self.chunk_num = params.chunk_num
        self.encoder_type = params.encoder_type
        self.chunk_array = []
        self.frame_num = 0
        self.last_processed_frame_num = -1  # Track the last processed frame
        self.lock = Lock()  # Add thread safety

        self.stopped = True

    def start(self):
        self.stopped = False
        Thread(target=self.encode, daemon=True).start()
        return self

    def update_frame(self, new_frame: np.ndarray, frame_num: int):
        """Update the frame with a new frame and its number"""
        with self.lock:
            self.frame = new_frame
            self.frame_num = frame_num

    def encode(self):
        try:
            while not self.stopped:
                with self.lock:
                    current_frame_num = self.frame_num
                    current_frame = self.frame

                # Only process if this is a new frame
                if current_frame_num > self.last_processed_frame_num:
                    if 0 <= self.quality <= 100:
                        raise ValueError("Quality must be between 0 and 100")
                    if self.encoder_type not in ["turbojpeg", "opencv"]:
                        raise ValueError("Invalid encoder type")

                    buffer = jpeg_encode(self.encoder_type, current_frame, self.quality)
                    chunks = split_bytes(buffer, self.chunk_num)

                    with self.lock:
                        self.chunk_array = chunks
                        self.last_processed_frame_num = current_frame_num

                if self.stopped:
                    break
        except Exception as e:
            raise e

    def stop(self):
        self.stopped = True

    def get_chunks(self):
        """Thread-safe method to get current chunks"""
        with self.lock:
            return self.chunk_array.copy()

    def get_current_frame_num(self):
        """Thread-safe method to get current frame number"""
        with self.lock:
            return self.frame_num