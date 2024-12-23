from threading import Thread

import cv2

from main.models.models import CaptureParams


class FrameCapture:
    def __init__(self, params:CaptureParams):
        self.frame = None

        # Capture parameters
        self.source = params.source

        self.fps = params.fps
        self.codec = params.codec
        self.platform = params.platform
        self.level = params.level

        # some processing here
        string_source = 'v4l2src' if self.platform == 'linux' else 'ksvideosrc device-index=0'
        self.width = self.get_dimensions(self.level)[0]
        self.height = self.get_dimensions(self.level)[1]
        self.fps = self.get_dimensions(self.level)[2]

        self.pipeline = f"{string_source} ! image/jpeg, width={self.width}, height={self.height}, framerate={self.fps}/1 ! jpegdec ! videoconvert ! video/x-raw, format=BGR ! appsink"
        self.cap = cv2.VideoCapture(self.pipeline, cv2.CAP_GSTREAMER)

        # start with the assumption that the capture is not stopped
        self.stopped = True


    @staticmethod
    def get_dimensions(level:int):
        if level == 1:
            return 1280, 720, 30
        elif level == 2:
            return 1920, 1080, 60
        else:
            return 640, 360, 30

    def start(self):
        self.stopped = False
        Thread(target=self.capture, daemon=True).start()
        return self

    def capture(self):
        try:
            if not self.cap.isOpened():
                raise Exception("Could not open video device")

            while not self.stopped:
                ret, frame = self.cap.read()
                self.frame = frame

                if not ret:
                    continue

                if self.stopped:
                    break


        except Exception as e:
            raise e

    def stop(self):
        self.stopped = True
        self.cap.release()
