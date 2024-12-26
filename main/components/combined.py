import base64
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

import cv2

from main.models.models import CaptureParams, ProducerMetric, EncodeParams, TransferParams
from main.utils.helper import jpeg_encode, split_bytes


class FrameCombined:
    def __init__(self, capture_params:CaptureParams,
                 encode_params:EncodeParams,
                 transfer_params:TransferParams
                 ):
        self.frame = None
        self.frame_num = 0

        # Capture parameters
        self.source = capture_params.source

        self.fps = capture_params.fps
        self.codec = capture_params.codec
        self.platform = capture_params.platform
        self.level = capture_params.level

        # Encode parameters
        self.quality = encode_params.quality
        self.chunk_num = encode_params.chunk_num
        self.encoder_type = encode_params.encoder_type

        # Transfer parameters
        self.brokers = transfer_params.brokers
        self.partitions = transfer_params.partitions
        self.topic = transfer_params.topic
        self.writer = transfer_params.writer


        self.metrics = []
        self.ack_array = []

        # some processing here
        string_source = 'v4l2src' if self.platform == 'linux' else 'ksvideosrc device-index=0'
        self.width = self.get_dimensions(self.level)[0]
        self.height = self.get_dimensions(self.level)[1]
        self.fps = self.get_dimensions(self.level)[2]

        self.pipeline = f"{string_source} ! image/jpeg, width={self.width}, height={self.height}, framerate={self.fps}/1 ! jpegdec ! videoconvert ! video/x-raw, format=BGR ! appsink"
        self.cap = cv2.VideoCapture(self.pipeline, cv2.CAP_GSTREAMER)

        # start with the assumption that the capture is not stopped
        self.stopped = True


    @staticmethod
    def get_dimensions(level:int):
        if level == 1:
            return 1280, 720, 30
        elif level == 2:
            return 1920, 1080, 60
        else:
            return 640, 360, 30

    def start(self):
        self.stopped = False
        Thread(target=self.capture, daemon=True).start()
        return self

    def acked(self, payload):
        if payload is None:
            print("Failed to send message.")
        else:
            self.ack_array.append(payload)

    def encode(self, frame):
        buffer = jpeg_encode(self.encoder_type, frame, self.quality)
        chunks = split_bytes(buffer, self.chunk_num)

        key = str(uuid.uuid1()).encode('utf-8')
        time_stamp = int(time.time() * 1000)

        payloads = [
            ProducerMetric(
                quality=self.quality,
                message_uuid=key.decode('utf-8'),
                message_num=self.frame_num,
                level=self.level,
                chunk_num=i,
                total_chunks=len(chunks),
                brokers=len(self.brokers),
                partitions=self.partitions,
                produce_time=time_stamp,
                message_size=len(chunk),
                message=base64.b64encode(chunk).decode('utf-8')
            ).to_dict() for i, chunk in enumerate(chunks)]

        return payloads, key

    def transfer(self, payloads, key):
        for payload in payloads:
            self.writer.send(
                topic=self.topic,
                value=json.dumps(payload).encode('utf-8'),
                key=key
            ).add_callback(self.acked)
            self.metrics.append({k: v for k, v in payload.items() if k != "message"})

            self.writer.flush()
            self.chunk_array = []

    def capture(self):

        try:
            if not self.cap.isOpened():
                raise Exception("Could not open video device")

            with ThreadPoolExecutor(max_workers=3) as executor:
                while not self.stopped:
                    ret, frame = self.cap.read()
                    self.frame = frame

                    if not ret:
                        continue

                    # Submit the encode task
                    future_encode = executor.submit(self.encode, frame)

                    # Process the result of encoding
                    payloads, key = future_encode.result()

                    # Submit the send_payload task
                    executor.submit(self.transfer, payloads, key)


        except Exception as e:
            self.stop()
            raise e

    def stop(self):
        self.stopped = True
        self.cap.release()
