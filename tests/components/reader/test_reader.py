import time
import unittest

from frameMQ.agent.reader import Reader
from frameMQ.models.models import ReaderParams


class TestReader(unittest.TestCase):
    def setUp(self):
        reader_type = 'kafka'
        brokers = [
            '133.41.117.50:9092', '133.41.117.50:9093',
            '133.41.117.50:9094', '133.41.117.50:9095'
        ] if reader_type == 'kafka' else ['133.41.117.94:1884']

        self.reader = Reader(
            params=ReaderParams(
                group_id='y-group',
                brokers=brokers,
                topic='video-trans',
                reader_type=reader_type,
                optimizer='none'
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
        self.reader.stop()

        print(
            len(self.reader.metrics)
        )
        self.assertTrue(len(self.reader.metrics) > 0, "Metrics should have recorded data.")
