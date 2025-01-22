import unittest
import time
import json
import base64
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from threading import Event
from frameMQ.models.models import TransferParams
from frameMQ.components.writer.frame_transfer import FrameTransfer


class TestFrameTransfer(unittest.TestCase):
    def setUp(self):
        # Create test chunks
        self.test_chunks = [b'chunk1', b'chunk2', b'chunk3', b'chunk4']
        self.start_time = time.time()

        # Setup Kafka producer
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=['133.41.117.50:9092', '133.41.117.50:9093',
                               '133.41.117.50:9094', '133.41.117.50:9095'],
            compression_type='zstd',
            max_request_size=10485880,
            batch_size=5048588,
            linger_ms=5,
            send_buffer_bytes=5048588,
            receive_buffer_bytes=5048588
        )

        # Setup MQTT client
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect("133.41.117.94", 1884, 60)
        self.mqtt_client.loop_start()

        # Create Kafka transfer parameters
        self.kafka_params = TransferParams(
            frame=self.test_chunks,
            quality=80,
            frame_number=1,
            level=1,
            brokers=['133.41.117.50:9092', '133.41.117.50:9093',
                               '133.41.117.50:9094', '133.41.117.50:9095'],
            partitions=1,
            topic='video-test',
            writer_type='kafka',
            writer=self.kafka_producer
        )

        # Create MQTT transfer parameters
        self.mqtt_params = TransferParams(
            frame=self.test_chunks,
            quality=80,
            frame_number=1,
            level=1,
            brokers=["133.41.117.94"],
            partitions=1,
            topic='video-test',
            writer_type='mqtt',
            writer=self.mqtt_client
        )

        # Event for MQTT message reception
        self.message_received = Event()
        self.received_messages = []

    def tearDown(self):
        # Clean up Kafka producer
        if hasattr(self, 'kafka_producer'):
            self.kafka_producer.close()

        # Clean up MQTT client
        if hasattr(self, 'mqtt_client'):
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

    def test_initialization(self):
        # Test Kafka initialization
        kafka_transfer = FrameTransfer(self.kafka_params, self.start_time)
        self.assertEqual(kafka_transfer.chunk_array, self.test_chunks)
        self.assertEqual(kafka_transfer.quality, 80)
        self.assertEqual(kafka_transfer.frame_num, 1)
        self.assertEqual(kafka_transfer.level, 1)
        self.assertEqual(kafka_transfer.partitions, 1)
        self.assertEqual(kafka_transfer.topic, 'video-test')
        self.assertTrue(kafka_transfer._stopped)
        self.assertEqual(len(kafka_transfer.metrics), 0)
        self.assertEqual(len(kafka_transfer.ack_array), 0)

        # Test MQTT initialization
        mqtt_transfer = FrameTransfer(self.mqtt_params, self.start_time)
        self.assertEqual(mqtt_transfer.writer_type, 'mqtt')
        self.assertIsInstance(mqtt_transfer.writer, mqtt.Client)

    def test_update_frame(self):
        transfer = FrameTransfer(self.kafka_params, self.start_time)
        new_chunks = [b'new_chunk1', b'new_chunk2']
        new_frame_num = 2

        transfer.update_frame(new_chunks, new_frame_num)
        self.assertEqual(transfer.chunk_array, new_chunks)
        self.assertEqual(transfer.frame_num, new_frame_num)

    def test_kafka_transfer_process(self):
        transfer = FrameTransfer(self.kafka_params, self.start_time)
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

        # Verify chunks were cleared
        self.assertEqual(len(transfer.chunk_array), 0)

        transfer.stop()

    def on_mqtt_message(self, client, userdata, message):
        try:
            payload = json.loads(message.payload.decode('utf-8'))
            self.received_messages.append(payload)
            self.message_received.set()
        except json.JSONDecodeError:
            pass

    def test_mqtt_transfer_process(self):
        # Subscribe to test topic
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.subscribe(self.mqtt_params.topic)

        transfer = FrameTransfer(self.mqtt_params, self.start_time)
        transfer.start()

        # Wait for messages to be received
        time.sleep(1)

        # Verify metrics were created
        self.assertEqual(len(transfer.metrics), len(self.test_chunks))

        # Verify received messages
        self.assertTrue(len(self.received_messages) > 0)

        # Check received message format
        for msg in self.received_messages:
            self.assertIn('quality', msg)
            self.assertIn('message_uuid', msg)
            self.assertIn('message_num', msg)
            self.assertIn('level', msg)
            self.assertIn('chunk_num', msg)
            self.assertIn('total_chunks', msg)
            self.assertIn('message', msg)

            # Verify we can decode the message
            decoded_message = base64.b64decode(msg['message'])
            self.assertIsInstance(decoded_message, bytes)

        transfer.stop()

    def test_invalid_writer_type(self):
        invalid_params = TransferParams(
            frame=self.test_chunks,
            quality=80,
            frame_number=1,
            level=1,
            brokers=['133.41.117.50:9092', '133.41.117.50:9093',
                               '133.41.117.50:9094', '133.41.117.50:9095'],
            partitions=1,
            topic='video-test',
            writer_type='invalid',
            writer=None
        )

        transfer = FrameTransfer(invalid_params, self.start_time)
        transfer.start()

        # Wait for potential error
        time.sleep(0.5)

        with self.assertRaises(ValueError):
            transfer._send_payload({}, b'test')

        transfer.stop()

    def test_concurrent_updates(self):
        transfer = FrameTransfer(self.kafka_params, self.start_time)
        transfer.start()

        # Update frame multiple times in quick succession
        for i in range(5):
            new_chunks = [f"chunk{i}".encode() for j in range(3)]
            transfer.update_frame(new_chunks, i)
            time.sleep(0.1)

        # Wait for processing
        time.sleep(1)

        # Verify all updates were processed
        self.assertTrue(len(transfer.metrics) > 0)
        self.assertEqual(len(transfer.chunk_array), 0)

        transfer.stop()

    def test_stop_and_restart(self):
        transfer = FrameTransfer(self.kafka_params, self.start_time)

        # First start
        transfer.start()
        self.assertFalse(transfer._stopped)

        # Stop
        transfer.stop()
        self.assertTrue(transfer._stopped)

        # Restart
        transfer = FrameTransfer(self.kafka_params, self.start_time)
        transfer.start()
        self.assertFalse(transfer._stopped)

        # Process some data
        new_chunks = [b'test_chunk']
        transfer.update_frame(new_chunks, 1)
        time.sleep(1)

        self.assertTrue(len(transfer.metrics) > 0)
        transfer.stop()
