import json
import threading
from queue import Queue, Empty, Full
from kafka import KafkaConsumer
import time
import paho.mqtt.client as mqtt

from frameMQ.components.reader.frame_retrieve import FrameRetrieve
from frameMQ.components.reader.frame_show import FrameShow
from frameMQ.models.models import ReaderParams, RetrieveParams, PSOParams, TrackedParams, OptimizeTargetParams, \
    GeneralParams
from frameMQ.optimizers.PSO.pso import NetworkManagerPSO

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)



class Reader:
    def __init__(self, params: ReaderParams, buffer_size: int = 100):
        # Configure Kafka consumer with optimized settings
        self.consumer = self._create_consumer(params)
        # print("consumer created")
        self.topic = params.topic
        self.params = params

        # Initialize state
        self.stopped = True
        self.metrics = []
        self.image_queue = Queue(maxsize=buffer_size)

        # Initialize components
        self._init_components(params)
        self.optimizer = None
        if params.optimizer == 'pso':
            self.optimizer = NetworkManagerPSO(
                params=PSOParams(
                    num_particles=10,
                    tracked_params=TrackedParams(),
                    optimize_target_params=OptimizeTargetParams(),
                    general_params=GeneralParams(
                        consumer_group='y-group',
                        brokers=params.brokers,
                        reader_type=params.reader_type
                    )
                )
            )


        # Thread management
        self.threads = []
        self._lock = threading.Lock()

    @staticmethod
    def _create_consumer(params: ReaderParams) -> KafkaConsumer | mqtt.Client:
        try:

            if params.reader_type not in ['kafka', 'mqtt']:
                raise ValueError("Invalid reader type. Must be 'kafka' or 'mqtt'.")

            if params.reader_type == 'mqtt':
                broker_host = params.brokers[0].split(":")[0]
                broker_port = int(params.brokers[0].split(":")[1])

                consumer = mqtt.Client()
                consumer.connect(broker_host, broker_port)

                # print("mqtt connected and ready to subscribe")
                consumer.subscribe(params.topic)
                return consumer

            elif params.reader_type == 'kafka':
                consumer =  KafkaConsumer(
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
                    send_buffer_bytes=10485880,
                    api_version=(0, 10),
                    request_timeout_ms=35000,
                    session_timeout_ms=30000,
                    connections_max_idle_ms=60000
                )
                return consumer

        except Exception as e:
            # print("reader: ", e)
            raise e

    def _init_components(self, params: ReaderParams):
        """Initialize frame show and retrieve components."""
        self.frame_show = FrameShow(
            start_time=time.time(),
            save_video=params.save_video
        )

        self.frame_retrieve = FrameRetrieve(
            params=RetrieveParams(
                reader=self.consumer,
                topic=self.topic,
                reader_type=params.reader_type
            ),
            frame_show=self.frame_show,
            max_workers=4
        )



    def _process_frame(self) -> None:
        """Process frames from the frame retriever."""
        while not self.stopped:
            try:
                with self._lock:
                    image = self.frame_retrieve.image
                    data = self.frame_retrieve.data

                if self.params.reader_type == 'mqtt':
                    logging.info("processing frame")
                    # print(data)

                if image is not None and data is not None:
                    # Use non-blocking put with timeout
                    try:
                        self.image_queue.put((image, data), timeout=0.1)
                        # print("looping...")

                        if self.optimizer is not None:
                            self.optimizer.quality = data['quality']
                            self.optimizer.current_level = data['level']
                            self.optimizer.chunk_number = data['chunk_num']
                            self.optimizer.frame_size = data['message_size']
                            self.optimizer.latency = (data['consume_time'] - data['produce_time'])

                    except Full:
                        # Skip frame if queue is full
                        continue
            except Exception as e:
                logging.info(f"Frame processing error: {e}")

            time.sleep(0.1)

    def _display_frame(self) -> None:
        """Display frames from the queue."""
        while not self.stopped:
            try:
                # Use non-blocking get with timeout
                image, data = self.image_queue.get(timeout=0.1)
                self.frame_show.update_image(image, data)

                self.metrics = self.frame_show.metrics

            except Empty:
                continue
            except Exception as e:
                # print(f"Frame display error: {e}")
                if not self.stopped:
                    time.sleep(0.1)


    def start(self) -> None:
        """Start all components and processing threads."""
        self.stopped = False

        # Start components
        self.frame_retrieve.start()
        self.frame_show.start()

        if self.params.optimizer != 'none':
            self.optimizer.start()

        # Start processing threads
        self.threads = [
            threading.Thread(target=self._process_frame, daemon=True),
            threading.Thread(target=self._display_frame, daemon=True),
        ]

        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        """Stop all components and threads."""
        self.stopped = True

        # Stop components first
        try:
            self.frame_show.stop()
            self.frame_retrieve.stop()

            if self.params.optimizer != 'none':
                self.optimizer.stop()
        except Exception as e:
            logging.info(f"Error stopping components: {e}")

        # Clear queue with timeout
        while not self.image_queue.empty():
            try:
                self.image_queue.get(timeout=0.1)  # Use timeout to prevent infinite blocking
            except Empty:
                break

        # Wait for threads to finish with timeout
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=2.0)  # Reduced timeout to 2 seconds
                if thread.is_alive():
                    logging.warning(f"Thread {thread.name} did not terminate within timeout")

        # Close Kafka consumer or MQTT client
        try:
            if self.params.reader_type == 'mqtt':
                self.consumer.loop_stop()
                self.consumer.disconnect()
            else:
                self.consumer.close()
        except Exception as e:
            logging.info(f"Error closing consumer: {e}")

        # Clear threads list
        self.threads = []