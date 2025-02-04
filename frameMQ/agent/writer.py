import json
import threading
import time

from kafka import KafkaProducer, KafkaConsumer
import paho.mqtt.client as mqtt
from frameMQ.components.writer.frame_capture import FrameCapture
from frameMQ.components.writer.frame_transfer import FrameTransfer
from frameMQ.components.writer.notif_consumer import NotifConsumer
from frameMQ.models.models import CaptureParams, EncodeParams, TransferParams, WriterParams
import platform
import logging



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

class Writer:
    def __init__(self, params: WriterParams):
        self._initialize_params(params)
        self._initialize_components(params)
        self.params = params
        self.stopped = True
        self.metrics = []
        self.thread = None

    def _initialize_params(self, params: WriterParams):
        """Initialize all parameters for the Writer."""
        current_platform = platform.system().lower()
        self.capture_params = CaptureParams(
            source=params.source,
            fps=30,
            codec="h264",
            platform=current_platform,
            level=1
        )

        quality = 90
        self.encode_params = EncodeParams(
            encoder_type=params.encoder_type,
            quality=quality,
            chunk_num=100
        )

        self.producer = self._create_producer(params)

        self.transfer_params = TransferParams(
            brokers=params.brokers,
            topic=params.topic,
            partitions=1,
            level=1,
            quality=quality,
            frame_number=0,
            frame=[],
            writer=self.producer,
            writer_type=params.writer_type
        )

    @staticmethod
    def _create_producer(params: WriterParams) -> KafkaProducer | mqtt.Client:
        try:

            if params.writer_type not in ['kafka', 'mqtt']:
                raise ValueError("Invalid writer type. Must be 'kafka' or 'mqtt'.")

            if params.writer_type == 'mqtt':
                broker_host = params.brokers[0].split(":")[0]
                broker_port = int(params.brokers[0].split(":")[1])

                producer = mqtt.Client()
                producer.connect(broker_host, broker_port)
                return producer
            elif params.writer_type == 'kafka':
                producer = KafkaProducer(
                    bootstrap_servers=params.brokers,
                    compression_type='zstd',
                    max_request_size=10485880,
                    batch_size=5048588,
                    linger_ms=5,
                    send_buffer_bytes=5048588,
                    receive_buffer_bytes=5048588
                )
                return producer
            else:
                raise ValueError("Invalid writer type. Must be 'kafka' or 'mqtt'.")
        except Exception as e:
            print("writer: ", e)
            raise e

    def _initialize_components(self, params: WriterParams):
        """Initialize all components for frame capture, encode, and transfer."""
        self.frame_transfer = FrameTransfer(
            start_time=0,
            transfer=self.transfer_params
        )
        self.frame_capture = FrameCapture(
            capture_params=self.capture_params,
            encode_params=self.encode_params
        )
        print("writer: ", params.optimizer)
        if params.optimizer != 'none':

            if params.writer_type == 'kafka':
                notif_consumer = KafkaConsumer(
                        bootstrap_servers=params.brokers,
                        auto_offset_reset='latest',
                        enable_auto_commit=False,
                        group_id='b-group',
                        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                        max_partition_fetch_bytes=10485880,
                        fetch_max_bytes=10485880,
                        fetch_max_wait_ms=1,
                        receive_buffer_bytes=10485880,
                        send_buffer_bytes=10485880
                    )
            elif params.writer_type == 'mqtt':
                notif_consumer = mqtt.Client()
                notif_consumer.connect(params.brokers[0].split(":")[0], int(params.brokers[0].split(":")[1]))
                notif_consumer.subscribe(params.notification_topic)

            else:
                raise ValueError("Invalid writer type. Must be 'kafka' or 'mqtt'.")

            self.notif_consumer = NotifConsumer(
                writer_type=params.writer_type,
                reader=notif_consumer,
            )


    def start(self):
        self.stopped = False
        self.frame_capture.start()
        self.frame_transfer.start()

        if self.params.optimizer != 'none':
            self.notif_consumer.start()

        self.thread = threading.Thread(target=self.run_threads, daemon=True)
        self.thread.start()

    def run_threads(self):
        try:
            while not self.stopped:
                logging.info("Writer running")
                self.metrics = self.frame_transfer.metrics
                self.frame_transfer.update_frame(
                    self.frame_capture.chunk_array, self.frame_capture.frame_num)

                if self.params.optimizer != 'none':

                    if self.notif_consumer.notif is not None:
                        print("writer: ", self.notif_consumer.notif)
                        self.frame_transfer.partitions = self.notif_consumer.notif['num_partitions'] if self.params.writer_type == 'kafka' else 1
                        self.frame_capture.update_params(self.notif_consumer.notif['chunks'], self.notif_consumer.notif['quality'], self.notif_consumer.notif['level'])

                time.sleep(0.15)
        except Exception as e:
            self.stop()
            print("writer: ", e)

    def stop(self):
        self.stopped = True
        self.frame_capture.stop()
        self.frame_transfer.stop()

        if self.params.optimizer != 'none':
            self.notif_consumer.stop()

        if self.thread and self.thread.is_alive():
            self.thread.join()
