import unittest
import numpy as np
import time

from main.components.writer.frame_encode import FrameEncode
from main.models.models import EncodeParams


class TestFrameEncode(unittest.TestCase):
    def setUp(self):
        # Create a simple test image as numpy array
        self.test_frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
        self.params = EncodeParams(
            quality=80,
            chunk_num=4,
            encoder_type="turbojpeg"
        )

    def test_initialization(self):
        encoder = FrameEncode(self.test_frame, self.params)

        self.assertEqual(encoder.quality, 80)
        self.assertEqual(encoder.chunk_num, 4)
        self.assertEqual(encoder.encoder_type, "turbojpeg")
        self.assertEqual(encoder.frame_num, 0)
        self.assertEqual(encoder.last_processed_frame_num, -1)
        self.assertTrue(encoder.stopped)
        self.assertEqual(len(encoder.chunk_array), 0)
        np.testing.assert_array_equal(encoder.frame, self.test_frame)

    def test_frame_processing(self):
        encoder = FrameEncode(self.test_frame, self.params)
        encoder.start()

        # Wait for initial frame to be processed
        time.sleep(0.1)

        # Check if chunks were created
        chunks = encoder.get_chunks()
        self.assertTrue(len(chunks) > 0)
        self.assertEqual(len(chunks), self.params.chunk_num)

        # Create a new frame
        new_frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
        encoder.update_frame(new_frame, 1)

        # Wait for new frame to be processed
        time.sleep(0.1)

        # Verify frame number was updated
        self.assertEqual(encoder.get_current_frame_num(), 1)

        # Stop the encoder
        encoder.stop()

    def test_multiple_frame_updates(self):

        number_of_frames = 5
        encoder = FrameEncode(self.test_frame, self.params)
        encoder.start()

        print("chunks: ", len(encoder.chunk_array))
        # Update frame several times
        for i in range(1, number_of_frames):
            new_frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
            encoder.update_frame(new_frame, i)
            time.sleep(0.1)  # Give time for processing

            self.assertEqual(encoder.get_current_frame_num(), i)
            chunks = encoder.get_chunks()

        print("chunks: ", len(encoder.chunk_array))
        self.assertEqual(len(encoder.chunk_array), number_of_frames)
        encoder.stop()

    def test_duplicate_frame_number(self):
        encoder = FrameEncode(self.test_frame, self.params)
        encoder.start()

        # Process initial frame
        time.sleep(0.1)
        initial_chunks = encoder.get_chunks()

        # Try to update with same frame number
        new_frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
        encoder.update_frame(new_frame, 0)
        time.sleep(0.1)

        # Chunks should not have changed
        np.testing.assert_array_equal(initial_chunks, encoder.get_chunks())

        encoder.stop()

    def test_stop_and_restart(self):
        encoder = FrameEncode(self.test_frame, self.params)
        encoder.start()

        # Let it process initial frame
        time.sleep(0.1)

        # Stop the encoder
        encoder.stop()
        self.assertTrue(encoder.stopped)

        # Restart the encoder
        encoder.start()
        self.assertFalse(encoder.stopped)

        # Update frame after restart
        new_frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
        encoder.update_frame(new_frame, 1)
        time.sleep(0.1)

        # Verify it's still processing frames
        chunks = encoder.get_chunks()
        self.assertTrue(len(chunks) > 0)

        encoder.stop()
