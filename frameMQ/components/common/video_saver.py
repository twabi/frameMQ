import cv2
import os
from typing import Optional
import logging
import threading
from queue import Queue, Empty
import gi
import time
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

class VideoSaver:
    def __init__(self, output_dir: str = "output_videos"):
        """
        Initialize the VideoSaver.
        
        Args:
            output_dir (str): Directory to save the videos
        """
        self.output_dir = output_dir
        self.writer: Optional[cv2.VideoWriter] = None
        self.frame_count = 0
        self.fps = 30  # Default FPS
        self.width = 1280  # Default width
        self.height = 720  # Default height
        self.codec = cv2.VideoWriter_fourcc(*'mp4v')
        
        # Thread management
        self.frame_queue = Queue(maxsize=100)  # Buffer up to 100 frames
        self.thread = None
        self.stopped = True
        self.is_reader = False
        
        # GStreamer pipeline components
        self.save_pipeline = None
        self.save_appsrc = None
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
    def start_recording(self, filename: str, fps: int = 30, width: int = 1280, height: int = 720, is_reader: bool = False):
        """
        Start recording a new video.
        
        Args:
            filename (str): Name of the output video file
            fps (int): Frames per second
            width (int): Video width
            height (int): Video height
            is_reader (bool): Whether the video is a reader video
        """
        if self.writer is not None or self.save_pipeline is not None:
            self.stop_recording()
            
        self.fps = fps
        self.width = width
        self.height = height
        self.frame_count = 0
        self.is_reader = is_reader
        
        output_path = os.path.join(self.output_dir, filename)
        
        if not is_reader:
            # Writer implementation using OpenCV
            self.writer = cv2.VideoWriter(
                output_path,
                self.codec,
                self.fps,
                (self.width, self.height)
            )
            
            # Start the writer thread
            self.stopped = False
            self.thread = threading.Thread(target=self._writer_thread, daemon=True)
            self.thread.start()
        else:
            
            self.save_pipeline = Gst.parse_launch(
                'appsrc name=save_src format=GST_FORMAT_TIME is-live=true block=true '
                'caps=video/x-raw,format=BGR,width=1280,height=720,framerate=30/1 ! '
                'videoconvert ! x264enc key-int-max=30 tune=zerolatency speed-preset=ultrafast ! '
                'video/x-h264,profile=baseline ! h264parse ! '
                'mp4mux faststart=true ! '
                f'filesink location={output_path} sync=false'
            )
            
            self.save_appsrc = self.save_pipeline.get_by_name('save_src')
            if not self.save_appsrc:
                raise ValueError("Failed to retrieve save_src from pipeline")
            self.save_appsrc.set_property("format", Gst.Format.TIME)
            self.save_appsrc.set_property("is-live", True)
            self.save_appsrc.set_property("block", True)
            self.save_pipeline.set_state(Gst.State.PLAYING)
            
        logging.info(f"Started recording to {output_path}")
        
    def write_frame(self, frame):
        """
        Write a frame to the video queue.
        
        Args:
            frame: The frame to write (numpy array)
        """
        if (self.writer is None and self.save_pipeline is None) or self.stopped:
            logging.warning("No active recording. Call start_recording first.")
            return
            
        if frame is None:
            logging.warning("Received None frame, skipping.")
            return
            
        # Resize frame if necessary
        if frame.shape[1] != self.width or frame.shape[0] != self.height:
            frame = cv2.resize(frame, (self.width, self.height))
            
        if not self.is_reader:
            # Add frame to queue (non-blocking)
            try:
                self.frame_queue.put(frame, block=False)
            except:
                # Queue is full, skip this frame
                pass
        else:
            # For reader, write directly using GStreamer
            self.write_frame_reader(frame, self.frame_count * (Gst.SECOND // self.fps))
            self.frame_count += 1
        
    def write_frame_reader(self, frame, pts, duration):
        """
        Write a frame using GStreamer pipeline.
        
        Args:
            frame: The frame to write (numpy array)
            pts: Presentation timestamp
        """
        if not self.save_pipeline or self.stopped:
            return
            
        buffer_save = Gst.Buffer.new_wrapped(frame.tobytes())
        buffer_save.pts = pts
        buffer_save.duration = duration
        print(f"buffer_save.duration: {buffer_save.duration}")
        ret_save = self.save_appsrc.emit('push-buffer', buffer_save)
        if ret_save != Gst.FlowReturn.OK:
            logging.error("Failed to push buffer to save pipeline")
            self.stop_recording()
            
    def _writer_thread(self):
        """Thread function that writes frames from the queue to the video file."""
        while not self.stopped:
            try:
                # Get frame from queue with timeout
                frame = self.frame_queue.get(timeout=0.1)
                
                # Write frame to video
                self.writer.write(frame)
                self.frame_count += 1
                
                # Mark task as done
                self.frame_queue.task_done()
                
            except Empty:
                # Queue is empty, continue waiting
                continue
            except Exception as e:
                logging.error(f"Error in writer thread: {e}")
                break
                
    def stop_recording(self):
        """Stop recording and release resources."""
        self.stopped = True
        
        if not self.is_reader:
            # Stop writer implementation
            if self.writer is not None:
                # Wait for thread to finish
                if self.thread and self.thread.is_alive():
                    self.thread.join(timeout=1.0)
                    
                # Release writer
                self.writer.release()
                self.writer = None
        else:
            # Stop reader implementation
            if self.save_pipeline is not None:
                try:
                    # Send end-of-stream signal
                    self.save_appsrc.emit('end-of-stream')
                    
                    # Wait for pipeline to process end-of-stream
                    time.sleep(0.1)
                    
                    # Stop pipeline
                    self.save_pipeline.set_state(Gst.State.NULL)
                    
                    # Wait for state change to complete
                    state_change = self.save_pipeline.get_state(timeout=Gst.CLOCK_TIME_NONE)
                    if state_change[0] != Gst.StateChangeReturn.SUCCESS:
                        logging.warning("Failed to stop GStreamer pipeline")
                except Exception as e:
                    logging.error(f"Error stopping GStreamer pipeline: {e}")
                finally:
                    self.save_pipeline = None
                    self.save_appsrc = None
                
        logging.info(f"Stopped recording. Total frames: {self.frame_count}")
            
    def __del__(self):
        """Cleanup when the object is destroyed."""
        self.stop_recording() 