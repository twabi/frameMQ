import json
import time
import unittest

from kafka import KafkaConsumer

from frameMQ.agent.writer import Writer
from frameMQ.components.reader.frame_decode import FrameDecode
from frameMQ.components.reader.frame_retrieve import FrameRetrieve
from frameMQ.models.models import RetrieveParams, WriterParams


class TestFrameRetrieve(unittest.TestCase):
    def setUp(self):
        reader = KafkaConsumer(
            bootstrap_servers=['133.41.117.50:9092', '133.41.117.50:9093',
               '133.41.117.50:9094', '133.41.117.50:9095'],
            auto_offset_reset='latest',
            enable_auto_commit=False,
            group_id='y-group',
            key_deserializer=lambda x: x.decode('utf-8'),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            max_partition_fetch_bytes=10485880,
            fetch_max_bytes=5048588,
            fetch_max_wait_ms=5,
            receive_buffer_bytes=10485880,
            send_buffer_bytes=10485880
        )

        self.frame_retrieve = FrameRetrieve(
            params=RetrieveParams(
                reader=reader,
                topic="video-trans"
            ),
            frame_decode=FrameDecode()
        )

        self.writer = Writer(
            params=WriterParams(
                brokers=[
                    '133.41.117.50:9092', '133.41.117.50:9093',
                    '133.41.117.50:9094', '133.41.117.50:9095'
                ],
                topic='video-trans',
                encoder_type='turbojpeg'
            )
        )


    def test_get_frame(self):
        self.frame_retrieve.start()
        self.writer.start()

        time.sleep(15)

        self.frame_retrieve.stop()
        self.writer.stop()

        print(
            self.frame_retrieve.payload_array
        )

        self.assertTrue(len(self.frame_retrieve.payload_array) > 0)
        self.assertTrue(self.frame_retrieve.stopped)


