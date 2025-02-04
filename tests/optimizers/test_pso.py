import time
import unittest

from kafka import KafkaConsumer
from frameMQ.models.models import PSOParams, GeneralParams, TrackedParams, OptimizeTargetParams
from frameMQ.optimizers.PSO.pso import NetworkManagerPSO
from kafka.admin import KafkaAdminClient, NewTopic



class TestNetworkManagerPSO(unittest.TestCase):
    def setUp(self):
        # Define test parameters
        reader_type = 'kafka'
        self.params = PSOParams(
            general_params=GeneralParams(
                consumer_group="test_group",
                monitored_topic="test_topic",
                brokers=['133.41.117.50:9092', '133.41.117.50:9093',
                                   '133.41.117.50:9094', '133.41.117.50:9095'] if reader_type == 'kafka' else ['133.41.117.94:1884'],
                sleep_interval=1,
                update_threshold=0.5,
                reader_type=reader_type
            ),
            tracked_params=TrackedParams(
                latency=50,
                chunk_number=10,
                quality=80,
                frame_size=5000,
                current_level=1
            ),
            num_particles=10,
            inertia_weight=0.5,
            cognitive_coeff=1.5,
            social_coeff=1.5,
            optimize_target_params=OptimizeTargetParams(
                FRAME_THRESHOLD=10,
                TARGET_RATIO=0.9,
                TARGET_LATENCY=30,
                TARGET_QUALITY=85,
                TARGET_PARTITIONS=3,
                TARGET_CHUNKS=15,
                TARGET_LEVEL=2,
                LATENCY_THRESHOLD=0.2
            )
        )
        self.manager = NetworkManagerPSO(self.params)

    def setup_kafka(self):
        """Ensure Kafka is ready for testing."""
        admin_client = KafkaAdminClient(
            bootstrap_servers=self.params.general_params.brokers
        )
        existing_topics = admin_client.list_topics()
        if self.params.general_params.monitored_topic not in existing_topics:
            admin_client.create_topics(
                [NewTopic(name=self.params.general_params.monitored_topic, num_partitions=3, replication_factor=1)]
            )

    def test_start_and_stop(self):
        """Test the start and stop methods."""
        self.manager.start()
        time.sleep(2)  # Let the thread run briefly
        self.manager.stop()

    def test_update_params(self):
        """Test updating tracked parameters."""
        self.manager.update_params(quality=90, level=2, chunk_number=20, message_size=6000, latency=25)
        assert self.manager.quality == 90
        assert self.manager.current_level == 2
        assert self.manager.chunk_number == 20
        assert self.manager.frame_size == 6000
        assert self.manager.latency == 25

    def test_fitness_function(self):
        """Test the fitness function."""
        particle = self.manager.particles[0]
        fitness = self.manager.fitness_function(particle)
        assert isinstance(fitness, float)

    def test_notify_producer_consumer(self):
        """Test producer notification."""
        self.manager.notify_producer_consumer(
            num_partitions=5, chunk_num=20, quality=85, level=2
        )
        consumer = KafkaConsumer(
            "notification",
            bootstrap_servers=self.params.general_params.brokers,
            group_id="test_group"
        )
        messages = []
        for msg in consumer:
            messages.append(msg)
            break  # Only consume one message
        assert len(messages) > 0
        consumer.close()

    def test_create_or_alter_topic(self):
        """Test topic creation or alteration."""
        topic_name = "test_topic_new"
        success = self.manager.create_or_alter_topic(topic_name, num_partitions=3)
        assert success
