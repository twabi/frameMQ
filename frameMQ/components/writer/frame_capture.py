from threading import Thread, Lock

import cv2

from frameMQ.models.models import CaptureParams, EncodeParams
from concurrent.futures import ThreadPoolExecutor
from frameMQ.utils.helper import split_bytes, jpeg_encode
from frameMQ.components.common.video_saver import VideoSaver



class FrameCapture:
    def __init__(self, capture_params:CaptureParams,
                    encode_params: EncodeParams,
                    save_video: bool = False
                ):
        self.frame = None
        self.frame_num = 0

        self.quality = encode_params.quality
        self.chunk_num = encode_params.chunk_num
        self.encoder_type = encode_params.encoder_type
        self.chunk_array = []
        self.frame_num = 0
        self.last_processed_frame_num = -1  # Track the last processed frame
        self.lock = Lock()  # Add thread safety

        # Capture parameters
        self.source = capture_params.source

        self.fps = capture_params.fps
        self.codec = capture_params.codec
        self.platform = capture_params.platform
        self.level = capture_params.level

        # some processing here
        string_source = 'v4l2src' if self.platform == 'linux' else f'ksvideosrc device-index={self.source}'
        self.width = self.get_dimensions(self.level)[0]
        self.height = self.get_dimensions(self.level)[1]
        self.fps = self.get_dimensions(self.level)[2]


        self.pipeline = f"{string_source} ! image/jpeg, width={self.width}, height={self.height}, framerate={self.fps}/1 ! jpegdec ! videoconvert ! video/x-raw, format=BGR ! appsink"
        self.cap = cv2.VideoCapture(self.pipeline, cv2.CAP_GSTREAMER)

        # Initialize video saver only if save_video is True
        self.video_saver = None
        if save_video:
            self.video_saver = VideoSaver(output_dir="output_videos/original")
            self.video_saver.start_recording(
                filename="original_video.mp4",
                fps=self.fps,
                width=self.width,
                height=self.height
            )

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

    def update_params(self, chunk_number, quality, level):
        with self.lock:
            self.chunk_num = chunk_number
            self.quality = quality
            self.level = level

    def encode(self, frame, frame_num):
        try:
            with self.lock:
                current_frame_num = frame_num
                current_frame = frame

            print(f"Encoding frame {self.chunk_num} chunks, quality {self.quality}")
            if current_frame is not None and current_frame_num > self.last_processed_frame_num:
                # # print(f"Encoding frame {self.chunk_num} chunks, quality {self.quality}")
                buffer = jpeg_encode(self.encoder_type, current_frame, self.quality)
                chunks = split_bytes(buffer, self.chunk_num)

                self.chunk_array = chunks
                self.frame_num = current_frame_num
        except Exception as e:
            self.stop()
            raise e

    def capture(self):
        try:
            if not self.cap.isOpened():
                self.stop()
                raise Exception("Could not open video device")

            if not (0 <= self.quality <= 100):
                # print(f"Quality must be between 0 and 100")
                self.stop()
                raise ValueError("Quality must be between 0 and 100")

            if self.encoder_type not in ["turbojpeg", "opencv"]:
                self.stop()
                raise ValueError("Invalid encoder type")

            with ThreadPoolExecutor(max_workers=4) as executor:
                while not self.stopped:
                    ret, frame = self.cap.read()

                    with self.lock:
                        if frame is None:
                            continue
                        else:
                            local_frame = frame.copy()
                            local_frame_num = self.frame_num
                            self.frame_num += 1
                            
                            # Save frame to video
                            if self.video_saver is not None:
                                self.video_saver.write_frame(local_frame)

                    executor.submit(self.encode, local_frame, local_frame_num)

        except Exception as e:
            self.stop()
            raise e

    def stop(self):
        self.stopped = True
        self.cap.release()
        if self.video_saver is not None:
            self.video_saver.stop_recording()
