import threading
import time
import base64
from collections import defaultdict
from turbojpeg import TurboJPEG, TJPF_BGR
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict
from main.components.reader.frame_show import FrameShow
from main.models.models import RetrieveParams


class FrameRetrieve:
    def __init__(self,
                 params: RetrieveParams,
                 frame_show: Optional[FrameShow] = None,
                 max_workers: int = 4):
        self.frame_num = 0
        self.frame_show = frame_show
        self.topic = params.topic
        self.reader = params.reader
        self.chunks: Dict[str, Dict] = defaultdict(dict)
        self.image = None
        self.data = None
        self.stopped = False

        # Create a thread pool for parallel processing
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)

        # Create a single TurboJPEG instance to be reused
        self._jpeg = TurboJPEG()

        # Pre-compile the base64 padding
        self._base64_padding = b'=' * 3

        self.reader.subscribe([self.topic])
        self.payload_array = []
        self.lock = threading.Lock()

    def deserialize_image(self, im_bytes):
        try:
            return self._jpeg.decode(im_bytes, pixel_format=TJPF_BGR)
        except Exception as e:
            print(f"Deserialization error: {e}")
            return None

    def process_complete_message(self, message_uuid: str, chunks: Dict):
        """Process a complete message once all chunks are received."""
        # Combine chunks efficiently
        buffer = b''.join(chunks[i] for i in sorted(chunks))

        # Deserialize image
        image = self.deserialize_image(buffer)
        if image is not None:
            with self.lock:
                self.image = image
                # if self.frame_show:
                #     self.frame_show.update_image(image=image, payload=self.data)

    def add_chunk(self, payload: dict, i_bytes: bytes):
        """Add a chunk to the message buffer and process if complete."""
        message_uuid = payload["message_uuid"]
        chunk_num = payload["chunk_num"]
        total_chunks = payload["total_chunks"]

        with self.lock:
            self.chunks[message_uuid][chunk_num] = i_bytes
            if len(self.chunks[message_uuid]) == total_chunks:
                # Process complete message in thread pool
                chunks = self.chunks[message_uuid]
                del self.chunks[message_uuid]
                self.thread_pool.submit(self.process_complete_message, message_uuid, chunks)

    def process_message(self, msg):
        """Process a single message from the reader."""
        if msg is None:
            return

        try:
            payload = msg.value
            payload['consume_time'] = time.time() * 1000

            # Decode base64 more efficiently
            image_data = base64.b64decode(payload["message"], validate=False)

            # Submit chunk processing to thread pool
            self.thread_pool.submit(self.add_chunk, payload, image_data)

            del payload["message"]
            self.data = payload
            self.payload_array.append(payload)

        except Exception as e:
            raise e

    def get(self):
        """Main message processing loop."""
        while not self.stopped:
            for msg in self.reader:
                if self.stopped:
                    break
                self.process_message(msg)

    def start(self):
        """Start the frame retrieval process."""
        self.stopped = False
        threading.Thread(target=self.get, daemon=True).start()

    def stop(self):
        """Stop the frame retrieval process and clean up resources."""
        self.stopped = True
        self.thread_pool.shutdown(wait=False)
        self.reader.close()