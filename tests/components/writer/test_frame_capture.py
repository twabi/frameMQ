import time
import unittest

from frameMQ.components.writer.frame_capture import FrameCapture
from frameMQ.models.models import CaptureParams


class TestFrameCapture(unittest.TestCase):
    def setUp(self):
        self.params = CaptureParams(
            source=0,
            fps=30,
            codec="h264",
            platform="linux",  # Change to "windows" if testing on Windows
            level=1
        )

    def test_initialization(self):
        capture = FrameCapture(self.params)
        self.assertEqual(capture.width, 1280)
        self.assertEqual(capture.height, 720)
        self.assertEqual(capture.fps, 30)
        self.assertTrue(capture.stopped)
        self.assertEqual(capture.platform, "linux")

    def test_get_dimensions(self):
        test_cases = [
            (1, (1280, 720, 30)),
            (2, (1920, 1080, 60)),
            (3, (640, 360, 30)),
            (0, (640, 360, 30)),
        ]

        for level, expected in test_cases:
            with self.subTest(level=level):
                result = FrameCapture.get_dimensions(level)
                self.assertEqual(result, expected)

    def test_capture_lifecycle(self):
        print("Starting the test")
        capture = FrameCapture(self.params)

        self.assertTrue(capture.stopped)
        capture.start()

        self.assertFalse(capture.stopped)
        time.sleep(1)

        self.assertIsNotNone(capture.frame)
        capture.stop()

        self.assertTrue(capture.stopped)
        self.assertIsNone(capture.frame)


    def test_pipeline_string(self):
        capture = FrameCapture(self.params)
        expected_pipeline = "v4l2src ! image/jpeg, width=1280, height=720, framerate=30/1 ! jpegdec ! videoconvert ! video/x-raw, format=BGR ! appsink"
        self.assertEqual(capture.pipeline, expected_pipeline)
