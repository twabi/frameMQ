import time
import unittest
from frameMQ.agent.writer import Writer
from frameMQ.models.models import WriterParams


class TestWriter(unittest.TestCase):
    def setUp(self):

        writer_type = 'mqtt'
        brokers = [
                    '133.41.117.50:9092', '133.41.117.50:9093',
                    '133.41.117.50:9094', '133.41.117.50:9095'
                ] if writer_type == 'kafka' else ['133.41.117.94:1884']

        self.writer = Writer(
            params=WriterParams(
                brokers=brokers,
                topic='video-trans',
                encoder_type='turbojpeg',
                writer_type=writer_type,
                optimizer='pso'
            )
        )

    def test_start_and_stop(self):
        self.writer.start()
        time.sleep(2)  # Allow some time for the threads to initialize
        self.assertFalse(self.writer.stopped, "Writer should be running after start.")

        self.writer.stop()
        self.assertTrue(self.writer.stopped, "Writer should be stopped after stop.")

    def test_write(self):
        self.writer.start()
        time.sleep(100)  # Simulate some runtime
        self.writer.stop()

        # print(
            #self.writer.metrics
        #)
        self.assertTrue(len(self.writer.metrics) > 0, "Metrics should have recorded data.")
