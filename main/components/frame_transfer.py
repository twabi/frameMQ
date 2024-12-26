from threading import Thread
import base64
import uuid
import time
import json
from main.models.models import TransferParams, ProducerMetric


class FrameTransfer:
    def __init__(self, transfer:TransferParams, start_time:float):
        self.start_time = start_time
        self.chunk_array = transfer.frame
        self.quality = transfer.quality
        self.frame_num = transfer.frame_number
        self.level = transfer.level
        self.brokers = transfer.brokers
        self.partitions = transfer.partitions
        self.topic = transfer.topic

        self.writer = transfer.writer

        self.metrics = []
        self.ack_array = []


        self.stopped = True

    def acked(self, payload):
        if payload is None:
            print("Failed to send message.")
        else:
            self.ack_array.append(payload)

    def start(self):
        self.stopped = False
        Thread(target=self.transfer, daemon=True).start()
        return self

    def update_frame(self, new_frame: bytes, frame_num: int):
        """Update the frame with a new frame and its number"""
        self.chunk_array = new_frame
        self.frame_num = frame_num

    def transfer(self):
        try:
            while not self.stopped:
                if len(self.chunk_array) > 0:
                    key = str(uuid.uuid1()).encode('utf-8')
                    time_stamp = int(time.time() * 1000)

                    payloads = [
                        ProducerMetric(
                            quality = self.quality,
                            message_uuid = key.decode('utf-8'),
                            message_num = self.frame_num,
                            level = self.level,
                            chunk_num = i,
                            total_chunks= len(self.chunk_array),
                            brokers = len(self.brokers),
                            partitions = self.partitions,
                            produce_time = time_stamp,
                            message_size = len(chunk),
                            message = base64.b64encode(chunk).decode('utf-8')
                    ).to_dict() for i, chunk in enumerate(self.chunk_array)]

                    for payload in payloads:
                        self.writer.send(
                            topic=self.topic,
                            value=json.dumps(payload).encode('utf-8'),
                            key=key
                        ).add_callback(self.acked)
                        self.metrics.append({k: v for k, v in payload.items() if k != "message"})

                        self.writer.flush()
                        self.chunk_array = []
                    print("time taken: ", time.time() - self.start_time)

                if self.stopped:
                    break

        except Exception as e:
            self.stop()
            raise e

    def stop(self):
        self.stopped = True