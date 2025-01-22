import unittest
import time
import json
import base64
import numpy as np
import paho.mqtt.client as mqtt
from kafka import KafkaConsumer
from threading import Event
from frameMQ.models.models import RetrieveParams
from frameMQ.components.reader.frame_retrieve import FrameRetrieve


class TestFrameRetrieve(unittest.TestCase):
    def setUp(self):
        # Create test image data
        self.test_image = np.zeros((480, 640, 3), dtype=np.uint8)  # Create a black image
        self.test_image[100:200, 100:200] = [255, 0, 0]  # Add a red square

        # Setup MQTT client
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.connect("133.41.117.94", 1884, 60)
        self.mqtt_client.loop_start()

        # Setup Kafka consumer
        self.kafka_consumer = KafkaConsumer(
            'video-test',
            bootstrap_servers=['133.41.117.50:9092', '133.41.117.50:9093',
                               '133.41.117.50:9094', '133.41.117.50:9095'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='test-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Create MQTT parameters
        self.mqtt_params = RetrieveParams(
            topic='video-test',
            reader_type='mqtt',
            reader=self.mqtt_client
        )

        # Create Kafka parameters
        self.kafka_params = RetrieveParams(
            topic='video-test',
            reader_type='kafka',
            reader=self.kafka_consumer
        )

        # Event for message reception
        self.message_received = Event()
        self.received_messages = []

    def tearDown(self):
        # Clean up MQTT client
        if hasattr(self, 'mqtt_client'):
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()

        # Clean up Kafka consumer
        if hasattr(self, 'kafka_consumer'):
            self.kafka_consumer.close()

    def create_test_message(self, chunk_num, total_chunks):
        """Create a test message with image chunk"""
        message_uuid = "test-uuid"
        # Encode a portion of the test image
        chunk_size = len(self.test_image.tobytes()) // total_chunks
        start_idx = chunk_num * chunk_size
        end_idx = start_idx + chunk_size
        image_chunk = self.test_image.tobytes()[start_idx:end_idx]

        return {
            "quality": 90,
            "message_uuid": message_uuid,
            "message_num": 1,
            "level": 1,
            "chunk_num": chunk_num,
            "total_chunks": total_chunks,
            "brokers": 1,
            "partitions": 1,
            "produce_time": int(time.time() * 1000),
            "message": base64.b64encode(image_chunk).decode('utf-8')
        }

    def test_initialization(self):
        # Test MQTT initialization
        mqtt_retrieve = FrameRetrieve(self.mqtt_params)
        self.assertEqual(mqtt_retrieve.topic, 'video-test')
        self.assertEqual(mqtt_retrieve.reader_type, 'mqtt')
        self.assertIsInstance(mqtt_retrieve.reader, mqtt.Client)
        self.assertFalse(mqtt_retrieve.stopped)

        # Test Kafka initialization
        kafka_retrieve = FrameRetrieve(self.kafka_params)
        self.assertEqual(kafka_retrieve.topic, 'video-test')
        self.assertEqual(kafka_retrieve.reader_type, 'kafka')
        self.assertIsInstance(kafka_retrieve.reader, KafkaConsumer)
        self.assertFalse(kafka_retrieve.stopped)

    def test_process_message(self):
        retrieve = FrameRetrieve(self.mqtt_params)

        # Create and process test message
        test_message = self.create_test_message(0, 1)
        retrieve.process_message(test_message)

        # Verify payload was stored
        self.assertEqual(len(retrieve.payload_array), 1)
        stored_payload = retrieve.payload_array[0]
        self.assertEqual(stored_payload['quality'], 90)
        self.assertEqual(stored_payload['message_num'], 1)
        self.assertNotIn('message', stored_payload)  # Message should be removed
        self.assertIn('consume_time', stored_payload)

    def test_invalid_message_handling(self):
        retrieve = FrameRetrieve(self.mqtt_params)

        # Test with invalid base64
        invalid_message = self.create_test_message(0, 1)
        invalid_message['message'] = 'invalid-base64'

        # Should not raise exception but skip processing
        retrieve.process_message(invalid_message)
        self.assertIsNone(retrieve.image)

    def test_stop_and_restart(self):
        retrieve = FrameRetrieve(self.mqtt_params)

        # Start
        retrieve.start()
        self.assertFalse(retrieve.stopped)

        # Stop
        retrieve.stop()
        self.assertTrue(retrieve.stopped)

        # Restart
        retrieve = FrameRetrieve(self.mqtt_params)
        retrieve.start()
        self.assertFalse(retrieve.stopped)

        # Clean up
        retrieve.stop()

    def test_concurrent_message_processing(self):
        retrieve = FrameRetrieve(self.mqtt_params, max_workers=4)
        total_chunks = 8

        # Send multiple chunks concurrently
        for i in range(total_chunks):
            test_message = self.create_test_message(i, total_chunks)
            retrieve.process_message(test_message)

        # Wait for processing to complete
        time.sleep(1)

        # Verify all messages were processed
        self.assertEqual(len(retrieve.payload_array), total_chunks)

    def test_utf8_validation(self):
        retrieve = FrameRetrieve(self.mqtt_params)

        # Test valid UTF-8
        valid_payload = json.dumps({'test': 'value'}).encode('utf-8')
        self.assertTrue(retrieve.is_valid_utf8(valid_payload))

        # Test invalid UTF-8
        invalid_payload = b'\xff\xff\xff\xff'
        self.assertFalse(retrieve.is_valid_utf8(invalid_payload))

    def test_image_deserialization(self):
        retrieve = FrameRetrieve(self.mqtt_params)

        # Test with valid JPEG data
        from turbojpeg import TurboJPEG
        jpeg = TurboJPEG()

        # Create a test JPEG
        jpeg_data = jpeg.encode(self.test_image)
        decoded_image = retrieve.deserialize_image(jpeg_data)

        self.assertIsNotNone(decoded_image)
        self.assertEqual(decoded_image.shape, self.test_image.shape)

        # Test with invalid data
        invalid_result = retrieve.deserialize_image(b'invalid-jpeg-data')
        self.assertIsNone(invalid_result)
