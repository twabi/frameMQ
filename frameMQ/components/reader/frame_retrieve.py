import json
import threading
import time
import base64
from collections import defaultdict
from turbojpeg import TurboJPEG, TJPF_BGR
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict
from frameMQ.components.reader.frame_show import FrameShow
from frameMQ.models.models import RetrieveParams


class FrameRetrieve:
    def __init__(self,
                 params: RetrieveParams,
                 frame_show: Optional[FrameShow] = None,
                 max_workers: int = 4):
        self.frame_num = 0
        self.frame_show = frame_show
        self.topic = params.topic

        self.reader = params.reader
        self.reader_type = params.reader_type

        if self.reader_type == 'mqtt':
            self.reader.on_message = self.on_message_mqtt
            self.reader.enable_logger()
        else:
            self.reader.subscribe([self.topic])

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
            payload = msg
            payload['consume_time'] = time.time() * 1000

            try:
                image_data = base64.b64decode(payload["message"], validate=True)
            except (base64.binascii.Error, TypeError):
                return

            # Submit chunk processing to thread pool
            self.thread_pool.submit(self.add_chunk, payload, image_data)

            del payload["message"]
            self.data = payload
            self.payload_array.append(payload)

        except Exception as e:
            raise e

    def is_valid_utf8(self, payload_bytes):
        try:
            payload_bytes.decode('utf-8')  # Attempt to decode as UTF-8
            return True
        except UnicodeDecodeError:
            return False

    def on_message_mqtt(self, client, userdata, message):
        """Callback for MQTT messages."""
        try:
            if self.is_valid_utf8(message.payload):
                payload = json.loads(message.payload.decode('utf-8'))
                self.process_message(payload)
        except Exception as e:
            print(f"Error processing message: {e}")

    def on_message_kafka(self):
        """Main message processing loop."""
        while not self.stopped:
            for msg in self.reader:
                if self.stopped:
                    break
                payload = msg.value
                # print(payload)
                self.process_message(payload)

    def start(self):
        """Start the frame retrieval process."""
        self.stopped = False
        try:
            if self.reader_type == 'mqtt':
                self.reader.loop_start()
            elif self.reader_type == 'kafka':
                threading.Thread(target=self.on_message_kafka, daemon=True).start()
            else:
                raise ValueError("Invalid reader type. Must be 'kafka' or 'mqtt'.")
        except Exception as e:
            print("reader: ", e)
            raise e

    def stop(self):
        """Stop the frame retrieval process and clean up resources."""
        self.stopped = True
        self.thread_pool.shutdown(wait=False)
        try:
            if self.reader_type == 'kafka':
                self.reader.close()
            elif self.reader_type == 'mqtt':
                self.reader.loop_stop()
                self.reader.disconnect()
            else:
                raise ValueError("Invalid reader type. Must be 'kafka' or 'mqtt'.")
        except Exception as e:
            print("reader: ", e)
            raise e