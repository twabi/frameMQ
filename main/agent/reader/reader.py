import json
import threading
import time

from kafka import KafkaProducer, KafkaConsumer

from main.components.reader.frame_decode import FrameDecode
from main.components.reader.frame_retrieve import FrameRetrieve
from main.components.reader.frame_show import FrameShow
from main.models.models import CaptureParams, EncodeParams, TransferParams, WriterParams, RetrieveParams, ReaderParams
import platform



class Reader:
    def __init__(self, params: ReaderParams):
        consumer = KafkaConsumer(
            bootstrap_servers=params.brokers,
            auto_offset_reset='latest',
            enable_auto_commit=False,
            group_id=params.group_id,
            key_deserializer=lambda x: x.decode('utf-8'),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            max_partition_fetch_bytes=10485880,
            fetch_max_bytes=5048588,
            fetch_max_wait_ms=5,
            receive_buffer_bytes=10485880,
            send_buffer_bytes=10485880
        )

        self.stopped = True
        self.metrics = []



        self.frame_show = FrameShow()
        self.frame_decode = FrameDecode(frame_shower=self.frame_show)
        self.frame_retrieve = FrameRetrieve(
            params=RetrieveParams(
                reader=consumer
            ),
            frame_decode=self.frame_decode)

        self.thread = None

    def start(self):
        self.stopped = False
        self.frame_decode.start()
        self.frame_retrieve.start()
        self.frame_show.start()
        self.thread = threading.Thread(target=self.run_threads, daemon=True)
        self.thread.start()

    def run_threads(self):
        try:
            while not self.stopped:
                self.metrics = self.frame_show.metrics
        except Exception as e:
            self.stop()
            print("writer: ", e)

    def stop(self):
        self.stopped = True
        self.frame_show.stop()
        self.frame_decode.stop()
        self.frame_retrieve.stop()
        if self.thread and self.thread.is_alive():
            self.thread.join()
