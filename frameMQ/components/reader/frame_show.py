import threading
import time
import gi
gi.require_version('Gst', '1.0')  # Specify the version
from gi.repository import Gst, GLib
import logging

import cv2
from frameMQ.components.common.video_saver import VideoSaver

# Initialize GStreamer once
Gst.init(None)

class FrameShow:
    def __init__(self, start_time: float, save_video: bool = False):
        self.image = None
        self.payload = None
        self.stopped = False
        self.lock = threading.Lock()
        self.metrics = []
        self.start_time = start_time
        self.frame_count = 0

        # Initialize video saver only if save_video is True
        self.video_saver = None
        if save_video:
            self.video_saver = VideoSaver(output_dir="output_videos/received/")
            self.video_saver.start_recording(
                filename="received_video.mp4",
                fps=30,
                width=1280,
                height=720,
                is_reader=True
            )

        # Create GStreamer pipeline with optimized settings
        self.pipeline = Gst.parse_launch(
            'appsrc name=appsrc format=GST_FORMAT_TIME is-live=true block=true '
            'caps=video/x-raw,format=BGR,width=1280,height=720,framerate=30/1 ! '
            'videorate ! video/x-raw,framerate=30/1 ! videoconvert !'
            'queue max-size-buffers=0 max-size-time=0 max-size-bytes=0 ! '
            'autovideosink sync=false'
        )

        # Retrieve appsrc element
        self.appsrc = self.pipeline.get_by_name('appsrc')
        if not self.appsrc:
            raise ValueError("Failed to retrieve appsrc from pipeline")

        # Configure appsrc properties for better performance
        self.appsrc.set_property("format", Gst.Format.TIME)
        self.appsrc.set_property("is-live", True)
        self.appsrc.set_property("block", True)

        # Start the pipeline
        self.pipeline.set_state(Gst.State.PLAYING)

    def start(self):
        self.stopped = False
        threading.Thread(target=self.display, daemon=True).start()

    def update_image(self, image, payload):
        with self.lock:
            self.image = image
            self.payload = payload

    def display(self):
        duration = Gst.SECOND // 30 
        while not self.stopped:
            start_time = time.time()
            with self.lock:
                frame = getattr(self, 'image', None)
                payload = getattr(self, 'payload', None)

            if frame is not None and payload is not None:
                # Convert frame to bytes
                # Check if the frame already has the required dimensions before resizing
                if frame.shape[1] != 1280 or frame.shape[0] != 720:
                    frame_resized = cv2.resize(frame, (1280, 720), interpolation=cv2.INTER_AREA)
                else:
                    frame_resized = frame  # No need to resize if it's already 1280x720

                # Create Gst.Buffer from frame bytes
                buffer = Gst.Buffer.new_wrapped(frame_resized.tobytes())
                # Calculate proper PTS based on frame count and FPS
                pts = self.frame_count * duration
                buffer.pts = pts
                buffer.duration = duration
                
                # Save frame to video if video_saver is initialized
                if self.video_saver is not None:
                    self.video_saver.write_frame_reader(frame_resized, pts, duration)

                # Push buffer to appsrc
                ret = self.appsrc.emit('push-buffer', buffer)
                if ret != Gst.FlowReturn.OK:
                    # print(f"Failed to push buffer: {ret}")
                    self.stop()
                    break

                # Update payload with show time
                payload['show_time'] = time.time() * 1000
                self.metrics.append(payload)

                self.frame_count += 1

            # Calculate sleep time to maintain FPS
            elapsed = time.time() - start_time
            fps_time = max(1/(30 - elapsed), 0)
            sleep_time = min(fps_time, 0.013)
            time.sleep(sleep_time)

    def stop(self):
        self.stopped = True
        try:
            # Send end-of-stream signal
            self.appsrc.emit('end-of-stream')
            
            # Wait for pipeline to process end-of-stream
            time.sleep(0.1)
            
            # Stop pipeline
            self.pipeline.set_state(Gst.State.NULL)
            
            # Wait for state change to complete
            state_change = self.pipeline.get_state(timeout=Gst.CLOCK_TIME_NONE)
            if state_change[0] != Gst.StateChangeReturn.SUCCESS:
                logging.warning("Failed to stop GStreamer pipeline")
                
            # Stop video recording if video_saver is initialized
            if self.video_saver is not None:
                self.video_saver.stop_recording()
            
        except Exception as e:
            logging.info(f"Error stopping frame show: {e}")
