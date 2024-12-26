from threading import Thread, Lock
from typing import Optional

import numpy as np
from main.models.models import EncodeParams

from concurrent.futures import ThreadPoolExecutor
from main.utils.helper import split_bytes, jpeg_encode


class FrameEncode:
    def __init__(self,
                 frame: Optional[np.ndarray],
                 params: EncodeParams,
                 frame_transfer=None
                 ):
        self.frame = frame
        self.quality = params.quality
        self.chunk_num = params.chunk_num
        self.encoder_type = params.encoder_type
        self.chunk_array = []
        self.frame_num = 0
        self.last_processed_frame_num = -1  # Track the last processed frame
        self.lock = Lock()  # Add thread safety

        self.frame_transfer = frame_transfer

        self.stopped = True

    def start(self):
        self.stopped = False
        Thread(target=self.encode, daemon=True).start()
        return self

    def update_frame(self, new_frame: np.ndarray, frame_num: int):
        """Update the frame with a new frame and its number"""
        self.frame = new_frame
        self.frame_num = frame_num

    def encode(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            while not self.stopped:
                try:
                    with self.lock:
                        current_frame_num = self.frame_num
                        current_frame = self.frame

                    if current_frame is not None and current_frame_num > self.last_processed_frame_num:
                        if not (0 <= self.quality <= 100):
                            raise ValueError("Quality must be between 0 and 100")

                        if self.encoder_type not in ["turbojpeg", "opencv"]:
                            raise ValueError("Invalid encoder type")

                        buffer = jpeg_encode(self.encoder_type, current_frame, self.quality)
                        chunks = split_bytes(buffer, self.chunk_num)

                        self.frame_transfer.update_frame(chunks, current_frame_num)
                except Exception as e:
                    self.stop()
                    raise e

    def stop(self):
        self.stopped = True