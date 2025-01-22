import threading
import time
import random
import json
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions
from kafka import KafkaProducer
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

from frameMQ.models.models import PSOParams
from frameMQ.optimizers.PSO.particle import Particle

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)


class NetworkManagerPSO:
    def __init__(
            self,
            params: PSOParams
    ):
        self.stopped = False

        self.consumer_group = params.general_params.consumer_group
        self.monitored_topic = params.general_params.monitored_topic
        self.sleep_interval = params.general_params.sleep_interval
        self.update_threshold = params.general_params.update_threshold

        self.latency = params.tracked_params.latency
        self.chunk_number = params.tracked_params.chunk_number
        self.quality = params.tracked_params.quality
        self.frame_size = params.tracked_params.frame_size
        self.current_level = params.tracked_params.current_level

        # PSO Parameters
        self.num_particles = params.num_particles
        self.inertia_weight = params.inertia_weight
        self.cognitive_coeff = params.cognitive_coeff
        self.social_coeff = params.social_coeff

        # Target Metrics (moved to class constants)
        self.FRAME_THRESHOLD = params.optimize_target_params.FRAME_THRESHOLD
        self.TARGET_RATIO = params.optimize_target_params.TARGET_RATIO
        self.TARGET_LATENCY = params.optimize_target_params.TARGET_LATENCY
        self.TARGET_QUALITY = params.optimize_target_params.TARGET_QUALITY
        self.TARGET_PARTITIONS = params.optimize_target_params.TARGET_PARTITIONS
        self.TARGET_CHUNKS = params.optimize_target_params.TARGET_CHUNKS
        # this is the level of aspect ratio of
        # the video where 0 is the lowest and 2 the highest
        self.TARGET_LEVEL = params.optimize_target_params.TARGET_LEVEL
        self.LATENCY_THRESHOLD = params.optimize_target_params.LATENCY_THRESHOLD

        # State tracking
        self.last_optimization = None
        self.optimization_count = 0
        self.number_of_consumers = 1
        self.number_of_partitions = self.get_topic_info()

        # Initialize Kafka clients with optimized settings
        brokers = params.general_params.brokers

        # TODO change with the writer type to also support mqtt
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=brokers,
            request_timeout_ms=5000,  # Reduced timeout
            api_version_auto_timeout_ms=5000
        )
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

        # Thread pool for parallel processing
        self.thread_pool = ThreadPoolExecutor(max_workers=4)

        # Initialize particles with bounds checking
        self.particles = self._initialize_bounded_particles()
        self.global_best = float('inf')
        self.global_best_position = None
        self.global_positions = []
        self.lock = threading.Lock()

        # Cache for expensive operations
        self.topic_info_cache = {}
        self.consumer_info_cache = {}
        self.monitor_thread = None

        logging.info("NetworkManagerPSO initialized with optimized settings.")

    def _initialize_bounded_particles(self):
        return [
            Particle(
                position={
                    'partitions': max(2, min(400, random.randint(2, 400))),
                    'chunk_number': max(10, min(200, random.randint(10, 200))),
                    'quality': max(15, min(95, random.randint(15, 95))),
                    'level': max(1, min(2, random.randint(0, 2)))
                },
                velocity={
                    'partitions': random.uniform(-10, 10),
                    'chunk_number': random.uniform(-5, 5),
                    'quality': random.uniform(-2, 2),
                    'level': random.uniform(-0.5, 0.5)
                }
            ) for _ in range(self.num_particles)
        ]

    def start(self):
        self.monitor_thread = threading.Thread(target=self.monitor_and_adjust, daemon=True)
        self.monitor_thread.start()
        logging.info("NetworkManagerPSO started.")
        return self

    def test(self):
        while True:
            print("Hello")
            time.sleep(1)

    def stop(self):
        self.stopped = True
        try:

            self.admin_client.delete_topics(topics=[self.monitored_topic])
            self.cleanup()
            logging.info(f"Deleted topic '{self.monitored_topic}'.")
        except Exception as e:
            logging.warning(f"Failed to delete topic '{self.monitored_topic}': {e}")
        finally:
            self.monitor_thread.join()
            self.admin_client.close()
            self.producer.close()
            logging.info("Kafka clients closed. NetworkManagerPSO stopped.")

    def update_params(self, quality: int, level: int, chunk_number: int, message_size: int, latency: int):
        with self.lock:
            self.quality = quality
            self.current_level = level
            self.chunk_number = chunk_number
            self.frame_size = message_size
            self.latency = latency

            print(f"Updated params: quality={quality}, level={level}, chunk_number={chunk_number}, message_size={message_size}, latency={latency}")

    @lru_cache(maxsize=128)
    def get_topic_info(self):
        try:
            topic_info = self.admin_client.describe_topics([self.monitored_topic])
            return len(topic_info[0]['partitions']) if topic_info else 0
        except Exception as e:
            logging.error(f"Error fetching topic info: {e}")
            return 1

    def create_or_alter_topic(self, topic_name, num_partitions):
        """
        Create a new topic or alter an existing topic's partition count.

        Parameters:
            topic_name (str): The name of the Kafka topic.
            num_partitions (int): The desired number of partitions.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            existing_topics = self.admin_client.list_topics()
            if topic_name in existing_topics:
                current_partitions = self.number_of_partitions
                self.number_of_partitions = current_partitions
                if num_partitions > current_partitions:
                    self.admin_client.create_partitions({
                        topic_name: NewPartitions(total_count=num_partitions)
                    })
                    logging.info(f"Altered topic '{topic_name}' to have {num_partitions} partitions.")
                else:
                    logging.info(f"Topic '{topic_name}' already has {current_partitions} partitions.")
            else:
                new_topic = NewTopic(
                    name=topic_name,
                    num_partitions=num_partitions,
                    replication_factor=1  # Adjust as per your Kafka cluster
                )
                self.admin_client.create_topics([new_topic])
                logging.info(f"Created topic '{topic_name}' with {num_partitions} partitions.")
            return True
        except Exception as e:
            # logging.error(f"Failed to create or alter topic '{topic_name}': {e}")
            return False

    def fitness_function(self, particle):
        quality = particle.position['quality']
        partitions = particle.position['partitions']
        chunks = particle.position['chunk_number']
        level = particle.position['level']

        # Normalized metrics
        latency_ratio = (self.latency - self.TARGET_LATENCY) / self.TARGET_LATENCY
        quality_ratio = quality / self.TARGET_QUALITY
        partitions_ratio = partitions / self.TARGET_PARTITIONS
        chunks_ratio = abs(chunks - self.TARGET_CHUNKS) / self.TARGET_CHUNKS
        level_ratio = level / self.TARGET_LEVEL

        # Calculate final fitness
        fitness = (
                3.0 * latency_ratio ** 2 +  # Increased weight for latency
                0.8 * (1 - quality_ratio) ** 2 +  # Quality impact
                2.5 * partitions_ratio ** 2 +  # Reduced weight for partitions
                4.5 * chunks_ratio ** 2 +  # Minor impact for chunks
                0.8 * (1 - level_ratio) ** 2  # Strong influence of level
        )

        return fitness

    def notify_producer_consumer(self, num_partitions, chunk_num, quality, level):
        message = {
            'num_partitions': num_partitions,
            'segmentation': True,
            'chunks': chunk_num,
            'quality': quality,
            'level': level,
            'timestamp': time.time()
        }

        try:
            self.producer.send("notification", value=message)
            self.producer.flush()
            logging.info(f"Sent notification: {message}")
        except Exception as e:
            logging.error(f"Failed to send notification: {e}")

    def monitor_and_adjust(self):
        while not self.stopped:
            latency_ratio = (self.latency - self.TARGET_LATENCY) / self.TARGET_LATENCY

            while latency_ratio > self.LATENCY_THRESHOLD:
                logging.info(F"Current latency: {self.latency}")
                if self.latency <= 0:
                    logging.info("Latency is 0. Skipping optimization.")
                    time.sleep(0.1)
                    continue

                for particle in self.particles:
                    self._process_particle(particle)

                    sleep_time = max(0.1, min(self.sleep_interval,
                                              int(self.latency / self.TARGET_LATENCY * 0.1)))
                    time.sleep(sleep_time)

                self.last_optimization = time.time()
                self.optimization_count += 1

                # Adaptive sleep based on optimization success
                sleep_time = 2 if latency_ratio > 0.5 else 0.5
                time.sleep(sleep_time)

    def update_self(self, position):
        logging.info("Optimizing conditions...")
        # I should only update the conditions based on global best if it has improved
        new_partitions = int(position['partitions'])
        new_chunks = int(position['chunk_number'])
        new_quality = int(position['quality'])
        new_level = int(position['level'])

        current_parts = self.number_of_partitions

        logging.info(
            f"Optimized conditions: parts:{new_partitions}, chunk:{new_chunks}, qual:{new_quality}, lvl:{new_level}")

        self.notify_producer_consumer(new_partitions, new_chunks, new_quality, new_level)

        if current_parts < new_partitions < self.TARGET_PARTITIONS:
            self.create_or_alter_topic(self.monitored_topic, new_partitions)

    def _process_particle(self, particle):
        """Process individual particle updates"""
        fitness = self.fitness_function(particle)
        logging.info(f"Fitness: {fitness}")
        with self.lock:
            # Update personal best
            if fitness < particle.p_best:
                particle.p_best = fitness
                particle.p_best_position = particle.position.copy()

                # Update global best
                if fitness < self.global_best:
                    self.global_best = fitness
                    self.global_best_position = particle.position.copy()

                    if self.global_best_position not in self.global_positions:
                        self.global_positions.append(particle.position.copy())
                    for p in self.particles:
                        p.g_best_position = self.global_best_position.copy()

                    # Update particle state
        particle.update_velocity(
            w=self.inertia_weight,
            c1=self.cognitive_coeff,
            c2=self.social_coeff
        )
        particle.update_position()

        self.update_self(particle.position)

        logging.info(" ")
        logging.info(f"Current latency: {self.latency}")
        logging.info(f"Particle fitness: {fitness}")
        logging.info(f"Global best: {self.global_best}")

    def cleanup(self):
        """Clean up resources"""
        self.thread_pool.shutdown(wait=True)
        self.producer.close()
        self.admin_client.close()