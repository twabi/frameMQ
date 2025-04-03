import time
import unittest

from frameMQ.agent.reader import Reader
from frameMQ.models.models import ReaderParams
from frameMQ.utils.helper import save_metrics_to_csv

class TestReader(unittest.TestCase):
    def setUp(self):
        kafka_ip = '172.16.0.13' #
        mqtt_ip = '172.16.0.13' #
        self.reader_type = 'mqtt'
        self.optimizer = 'none'
        self.save_video = True
        brokers = [
            f'{kafka_ip}:9092', f'{kafka_ip}:9096',
            f'{kafka_ip}:9094', f'{kafka_ip}:9095'
        ] if self.reader_type == 'kafka' else [f'{mqtt_ip}:1883']

        self.reader = Reader(
            params=ReaderParams(
                group_id='y-group',
                brokers=brokers,
                topic='video-trans',
                reader_type=self.reader_type,
                optimizer=self.optimizer,
                save_video=self.save_video
            )
        )

    def test_start_and_stop(self):
        self.reader.start()
        time.sleep(2)  # Allow some time for the threads to initialize
        self.assertFalse(self.reader.stopped, "Writer should be running after start.")

        self.reader.stop()
        self.assertTrue(self.reader.stopped, "Writer should be stopped after stop.")

    def test_write_read(self):
        self.reader.start()
        time.sleep(100)  # Simulate some runtime
        filepath = save_metrics_to_csv(f'reader-{self.reader_type}-{self.optimizer}', self.reader.metrics)
        self.reader.stop()

        
        print(f"Metrics saved to: {filepath}")
        self.assertTrue(len(self.reader.metrics) > 0, "Metrics should have recorded data.")
