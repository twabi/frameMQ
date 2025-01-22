import unittest
import time
import cv2
import numpy as np
from threading import Lock
from frameMQ.components.writer.frame_capture import FrameCapture
from frameMQ.models.models import CaptureParams, EncodeParams


class TestFrameCapture(unittest.TestCase):
    def setUp(self):
        # Create a virtual video device for testing
        self.test_frame = np.zeros((720, 1280, 3), dtype=np.uint8)
        # Draw something on the frame to make it more realistic
        cv2.rectangle(self.test_frame, (100, 100), (200, 200), (0, 255, 0), -1)

        # Basic capture parameters
        self.params = CaptureParams(
            source=0,
            fps=30,
            codec="h264",
            platform="linux",
            level=1
        )

        self.encoder_params = EncodeParams(
            encoder_type="turbojpeg",
            quality=90,
            chunk_num=100
        )


    def test_initialization(self):
        capture = FrameCapture(capture_params=self.params, encode_params=self.encoder_params)

        # Test initial state
        self.assertEqual(capture.quality, 90)
        self.assertEqual(capture.chunk_num, 100)
        self.assertEqual(capture.encoder_type, "turbojpeg")
        self.assertEqual(capture.frame_num, 0)
        self.assertEqual(capture.last_processed_frame_num, -1)
        self.assertTrue(capture.stopped)

        # Test dimensions
        self.assertEqual(capture.width, 1280)
        self.assertEqual(capture.height, 720)
        self.assertEqual(capture.fps, 30)

    def test_get_dimensions(self):
        test_cases = [
            (1, (1280, 720, 30)),  # HD
            (2, (1920, 1080, 60)),  # Full HD
            (3, (640, 360, 30)),  # Default case
            (0, (640, 360, 30)),  # Invalid level should return default
            (-1, (640, 360, 30)),  # Negative level should return default
        ]

        for level, expected in test_cases:
            with self.subTest(level=level):
                result = FrameCapture.get_dimensions(level)
                self.assertEqual(result, expected)

    def test_update_params(self):
        capture = FrameCapture(capture_params=self.params, encode_params=self.encoder_params)

        # Test updating parameters
        new_chunk_num = 200
        new_quality = 80
        new_level = 2

        capture.update_params(new_chunk_num, new_quality, new_level)

        self.assertEqual(capture.chunk_num, new_chunk_num)
        self.assertEqual(capture.quality, new_quality)
        self.assertEqual(capture.level, new_level)

    def test_pipeline_string_generation(self):
        # Test Linux pipeline
        linux_params = CaptureParams(
            source=0, fps=30, codec="h264", platform="linux", level=1
        )
        linux_capture = FrameCapture(capture_params=linux_params, encode_params=self.encoder_params)
        expected_linux = "v4l2src ! image/jpeg, width=1280, height=720, framerate=30/1 ! jpegdec ! videoconvert ! video/x-raw, format=BGR ! appsink"
        self.assertEqual(linux_capture.pipeline, expected_linux)

    def test_capture_lifecycle(self):
        capture = FrameCapture(capture_params=self.params, encode_params=self.encoder_params)

        # Test initial state
        self.assertTrue(capture.stopped)

        # Test start
        capture.start()
        self.assertFalse(capture.stopped)

        # Give some time for frames to be captured
        time.sleep(0.5)

        # Test stop
        capture.stop()
        self.assertTrue(capture.stopped)

        # Verify cleanup
        self.assertFalse(capture.cap.isOpened())

    def test_chunk_generation(self):
        capture = FrameCapture(capture_params=self.params, encode_params=self.encoder_params)
        capture.start()

        # Give some time for frames to be processed
        time.sleep(0.5)

        # Verify chunks are being generated
        self.assertGreater(len(capture.chunk_array), 0)

        # Verify chunk sizes
        total_chunks = len(capture.chunk_array) - 1
        self.assertLessEqual(total_chunks, capture.chunk_num)

        capture.stop()

