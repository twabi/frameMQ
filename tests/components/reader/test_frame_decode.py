import json
import time
import unittest

import numpy as np
from kafka import KafkaConsumer

from main.agent.writer.writer import Writer
from main.components.reader.frame_decode import FrameDecode
from main.components.reader.frame_retrieve import FrameRetrieve
from main.components.reader.frame_show import FrameShow
from main.models.models import RetrieveParams, WriterParams


class TestFrameDecode(unittest.TestCase):
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

        self.frame_decode = FrameDecode(
            frame_shower=None
        )
        self.frame_retrieve = FrameRetrieve(
            params=RetrieveParams(
                reader=reader,
                topic="video-trans"
            ),
            frame_decode=None
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

    def test_decode(self):
        # Test the static deserializer method
        self.frame_retrieve.start()
        self.writer.start()

        time.sleep(15)

        self.frame_retrieve.stop()
        self.writer.stop()

        self.frame_decode.start()

        retrieved_array = self.frame_retrieve.payload_array
        for item in retrieved_array:
            self.frame_decode.update_data(
                item['dict'],
                item['image']
            )


        self.frame_decode.stop()
        array = self.frame_decode.image_array

        print(" ")
        print(" ")
        print("decoded: ", len(array))
        #self.assertTrue(len(array) > 0, "Image array should have data.")

        #self.assertIsInstance(array[0], np.ndarray, "Image array should contain numpy arrays.")




