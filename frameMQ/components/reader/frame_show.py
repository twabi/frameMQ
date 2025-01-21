import threading
import time
import gi
gi.require_version('Gst', '1.0')  # Specify the version
from gi.repository import Gst, GLib

import cv2

# Initialize GStreamer once
Gst.init(None)

class FrameShow:
    def __init__(self, start_time: float):
        self.image = None
        self.payload = None
        self.stopped = False
        self.lock = threading.Lock()
        self.metrics = []
        self.start_time = start_time

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

                # Push buffer to appsrc
                ret = self.appsrc.emit('push-buffer', buffer)
                if ret != Gst.FlowReturn.OK:
                    print(f"Failed to push buffer: {ret}")
                    self.stop()
                    break

                # Update payload with show time
                payload['show_time'] = time.time() * 1000
                self.metrics.append(payload)

            # Calculate sleep time to maintain FPS
            elapsed = time.time() - start_time
            fps_time = max(1/(30 - elapsed), 0)
            sleep_time = min(fps_time, 0.013)
            time.sleep(sleep_time)

    def stop(self):
        self.stopped = True
        self.appsrc.emit('end-of-stream')
        self.pipeline.set_state(Gst.State.NULL)
