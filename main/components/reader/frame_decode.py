import threading
from threading import Thread, Lock
import numpy as np
from turbojpeg import TJPF_BGR, TurboJPEG

from main.components.reader.frame_show import FrameShow

from concurrent.futures import ThreadPoolExecutor



jpeg = TurboJPEG()


class FrameDecode:
    def __init__(self,
                 frame_shower:FrameShow=None
                 ):
        self.frame = None
        self.chunk_array = []
        self.frame_num = 0

        self.chunks = {}
        self.last_processed_frame_num = -1  # Track the last processed frame
        self.lock = Lock()  # Add thread safety

        self.frame_shower = frame_shower
        self.payload = None
        self.image_data = None

        self.stopped = True

    def start(self):
        self.stopped = False
        Thread(target=self.decode(), daemon=True).start()
        return self

    def update_frame(self, new_frame: np.ndarray, frame_num: int):
        """Update the frame with a new frame and its number"""
        self.frame = new_frame
        self.frame_num = frame_num

    def update_data(self, payload, image_data):
        self.payload = payload
        self.image_data = image_data

    @staticmethod
    def deserializer(im_bytes):
        try:
            return jpeg.decode(im_bytes, pixel_format=TJPF_BGR)
        except Exception as e:
            print(f"Deserialization error: {e}")
            return None

    def add_chunk(self, payload, i_bytes):
        message_uuid = payload["message_uuid"]
        chunk_num = payload["chunk_num"]
        total_chunks = payload["total_chunks"]

        with self.lock:
            if message_uuid not in self.chunks:
                self.chunks[message_uuid] = {}
            self.chunks[message_uuid][chunk_num] = i_bytes

            if len(self.chunks[message_uuid]) == total_chunks:
                buffer = b''.join(self.chunks[message_uuid][i] for i in sorted(self.chunks[message_uuid]))
                image = self.deserializer(buffer)
                if image is not None:
                    self.image_data = image
                    self.frame_shower.update_image(image, payload)

                del self.chunks[message_uuid]

    def decode(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            while not self.stopped:
                try:
                    threading.Thread(target=self.add_chunk, args=(self.payload, self.image_data), daemon=True).start()

                except Exception as e:
                    self.stop()
                    raise e

    def stop(self):
        self.stopped = True