import time
import unittest

from main.agent.reader.reader import Reader
from main.agent.writer.writer import Writer
from main.models.models import WriterParams, ReaderParams


class TestReader(unittest.TestCase):
    def setUp(self):

        self.reader = Reader(
            params=ReaderParams(
                group_id='y-group',
                brokers=[
                    '133.41.117.50:9092', '133.41.117.50:9093',
                    '133.41.117.50:9094', '133.41.117.50:9095'
                ],
                topic='video-trans'
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
