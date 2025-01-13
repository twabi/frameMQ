import unittest
import time
from kafka import KafkaProducer
from main.models.models import TransferParams
from main.components.writer.frame_transfer import FrameTransfer


class TestFrameTransfer(unittest.TestCase):
    def setUp(self):
        # Create real test chunks
        self.test_chunks = [b'chunk1', b'chunk2', b'chunk3', b'chunk4']

        # Setup a real Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=['133.41.117.50:9092', '133.41.117.50:9093',
               '133.41.117.50:9094', '133.41.117.50:9095'],
            compression_type='zstd',
            max_request_size=10485880,
            batch_size=5048588,
            linger_ms=5,
            send_buffer_bytes=5048588,
            receive_buffer_bytes=5048588
        )

        # Create transfer parameters
        self.params = TransferParams(
            frame=self.test_chunks,
            quality=80,
            frame_number=1,
            level=1,
            brokers=1,
            partitions=1,
            topic='video-10',
            writer=producer
        )

    def tearDown(self):
        if hasattr(self, 'producer'):
            self.producer.close()

    def test_initialization(self):
        transfer = FrameTransfer(self.params)

        self.assertEqual(transfer.chunk_array, self.test_chunks)
        self.assertEqual(transfer.quality, 80)
        self.assertEqual(transfer.frame_num, 1)
        self.assertEqual(transfer.level, 1)
        self.assertEqual(transfer.brokers, 1)
        self.assertEqual(transfer.partitions, 1)
        self.assertEqual(transfer.topic, 'video-10')
        self.assertTrue(transfer.stopped)
        self.assertEqual(len(transfer.metrics), 0)
        self.assertEqual(len(transfer.ack_array), 0)

    def test_transfer_process(self):
        transfer = FrameTransfer(self.params)
        transfer.start()

        # Wait for transfer to process chunks
        time.sleep(1)

        # Verify metrics were created
        self.assertEqual(len(transfer.metrics), len(self.test_chunks))

        # Check metric contents
        for metric in transfer.metrics:
            self.assertEqual(metric['quality'], 80)
            self.assertEqual(metric['message_num'], 1)
            self.assertEqual(metric['level'], 1)
            self.assertEqual(metric['total_chunks'], len(self.test_chunks))
            self.assertEqual(metric['brokers'], 1)
            self.assertEqual(metric['partitions'], 1)

        # Verify chunks were cleared
        self.assertEqual(len(transfer.chunk_array), 0)

        transfer.stop()

    def test_multiple_transfers(self):
        transfer = FrameTransfer(self.params)
        transfer.start()

        # First transfer
        time.sleep(1)
        first_metrics_count = len(transfer.metrics)

        # Add new chunks
        new_chunks = [b'chunk5', b'chunk6']
        transfer.chunk_array = new_chunks
        time.sleep(1)

        # Verify new metrics were added
        self.assertEqual(len(transfer.metrics), first_metrics_count + len(new_chunks))

        transfer.stop()

    def test_ack_callback(self):
        transfer = FrameTransfer(self.params)

        # Test successful ack
        test_payload = {"test": "data"}
        transfer.acked(test_payload)
        self.assertEqual(len(transfer.ack_array), 1)
        self.assertEqual(transfer.ack_array[0], test_payload)

        # Test failed ack
        transfer.acked(None)
        self.assertEqual(len(transfer.ack_array), 1)  # Should not have added None

    def test_stop_and_restart(self):
        transfer = FrameTransfer(self.params)

        # Start transfer
        transfer.start()
        self.assertFalse(transfer.stopped)

        # Stop transfer
        transfer.stop()
        self.assertTrue(transfer.stopped)

        # Restart transfer
        transfer.start()
        self.assertFalse(transfer.stopped)

        # Add new chunks and verify they're processed
        transfer.chunk_array = [b'new_chunk']
        time.sleep(1)

        self.assertTrue(len(transfer.metrics) > 0)
        transfer.stop()
