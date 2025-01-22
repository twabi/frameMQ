import unittest
import numpy as np
import time

from frameMQ.components.reader.frame_show import FrameShow


class TestFrameShow(unittest.TestCase):
    def setUp(self):
        """Set up a FrameShow instance with a start time."""
        self.start_time = time.time()
        self.frame_show = FrameShow(self.start_time)

    def tearDown(self):
        """Stop the FrameShow pipeline after each test."""
        self.frame_show.stop()

    def test_start_and_stop(self):
        """Test starting and stopping the FrameShow pipeline."""
        self.frame_show.start()
        time.sleep(0.1)  # Allow the thread to start
        self.assertFalse(self.frame_show.stopped)

        self.frame_show.stop()
        self.assertTrue(self.frame_show.stopped)

    def test_update_image(self):
        """Test updating the image and payload."""
        dummy_image = np.zeros((720, 1280, 3), dtype=np.uint8)  # Create a dummy black frame
        dummy_payload = {"test": "data"}

        self.frame_show.update_image(dummy_image, dummy_payload)
        with self.frame_show.lock:
            self.assertTrue(np.array_equal(self.frame_show.image, dummy_image))
            self.assertEqual(self.frame_show.payload, dummy_payload)

    def test_display_frame(self):
        """Test the display function with a single frame."""
        dummy_image = np.random.randint(0, 256, (720, 1280, 3), dtype=np.uint8)  # Random frame
        dummy_payload = {"frame_id": 1}

        self.frame_show.update_image(dummy_image, dummy_payload)
        self.frame_show.start()
        time.sleep(0.2)  # Allow the thread to process the frame

        # Check that the payload was updated with 'show_time'
        with self.frame_show.lock:
            self.assertGreater(len(self.frame_show.metrics), 0)
            self.assertIn("show_time", self.frame_show.metrics[-1])
            self.assertEqual(self.frame_show.metrics[-1]["frame_id"], 1)

        self.frame_show.stop()

    def test_resize_frame(self):
        """Test that frames are resized correctly."""
        dummy_image = np.zeros((640, 480, 3), dtype=np.uint8)  # Non-1280x720 frame
        dummy_payload = {"frame_id": 2}

        self.frame_show.update_image(dummy_image, dummy_payload)
        self.frame_show.start()
        time.sleep(0.2)  # Allow the thread to process the frame

        # Verify that the frame is processed and payload updated
        with self.frame_show.lock:
            self.assertGreater(len(self.frame_show.metrics), 0)
            self.assertIn("show_time", self.frame_show.metrics[-1])
            self.assertEqual(self.frame_show.metrics[-1]["frame_id"], 2)

        self.frame_show.stop()

    def test_pipeline_error_handling(self):
        """Test the pipeline's handling of invalid frames."""
        invalid_image = None  # No frame
        dummy_payload = {"frame_id": 3}

        self.frame_show.update_image(invalid_image, dummy_payload)
        self.frame_show.start()
        time.sleep(0.2)  # Allow the thread to process

        # Verify that no metrics were added
        with self.frame_show.lock:
            self.assertEqual(len(self.frame_show.metrics), 0)

        self.frame_show.stop()

