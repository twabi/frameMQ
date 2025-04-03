import time
import unittest
from frameMQ.agent.writer import Writer
from frameMQ.models.models import WriterParams
from frameMQ.utils.helper import save_metrics_to_csv

class TestWriter(unittest.TestCase):
    def setUp(self):
        kafka_ip = '172.16.0.13' #
        mqtt_ip = '172.16.0.13'
        self.writer_type = 'mqtt'
        self.optimizer = 'none'
        self.save_video = True
        brokers = [
            f'{kafka_ip}:9092', f'{kafka_ip}:9096',
            f'{kafka_ip}:9094', f'{kafka_ip}:9095'
        ] if self.writer_type == 'kafka' else [f'{mqtt_ip}:1883']

        self.writer = Writer(
            params=WriterParams(
                source=1,
                brokers=brokers,
                topic='video-trans',
                encoder_type='turbojpeg',
                writer_type=self.writer_type,
                optimizer=self.optimizer,
                save_video=self.save_video
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
        time.sleep(100)  # Reduced from 100 to 5 seconds for testing
        self.writer.stop()

        print(self.writer.metrics)
        filepath = save_metrics_to_csv(f'writer-{self.writer_type}-{self.optimizer}', self.writer.metrics)
        print(f"Metrics saved to: {filepath}")

        self.assertTrue(len(self.writer.metrics) > 0, "Metrics should have recorded data.")
