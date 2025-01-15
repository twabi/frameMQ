import threading

from kafka import KafkaProducer
import paho.mqtt.client as mqtt
from main.components.writer.frame_capture import FrameCapture
from main.components.writer.frame_transfer import FrameTransfer
from main.models.models import CaptureParams, EncodeParams, TransferParams, WriterParams
import platform


class Writer:
    def __init__(self, params: WriterParams):
        self._initialize_params(params)
        self._initialize_components()
        self.stopped = True
        self.metrics = []
        self.thread = None

    def _initialize_params(self, params: WriterParams):
        """Initialize all parameters for the Writer."""
        current_platform = platform.system().lower()
        self.capture_params = CaptureParams(
            source=0,
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

    def _initialize_components(self):
        """Initialize all components for frame capture, encode, and transfer."""
        self.frame_transfer = FrameTransfer(
            start_time=0,
            transfer=self.transfer_params
        )
        self.frame_capture = FrameCapture(
            capture_params=self.capture_params,
            encode_params=self.encode_params
        )

    def start(self):
        self.stopped = False
        self.frame_capture.start()
        self.frame_transfer.start()
        self.thread = threading.Thread(target=self.run_threads, daemon=True)
        self.thread.start()

    def run_threads(self):
        try:
            while not self.stopped:
                self.metrics = self.frame_transfer.metrics
                self.frame_transfer.update_frame(
                    self.frame_capture.chunk_array, self.frame_capture.frame_num)
        except Exception as e:
            self.stop()
            print("writer: ", e)

    def stop(self):
        self.stopped = True
        self.frame_capture.stop()
        self.frame_transfer.stop()
        if self.thread and self.thread.is_alive():
            self.thread.join()
