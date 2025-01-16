from threading import Thread, Condition
import base64
import uuid
import time
import json
from typing import List, Dict, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from main.models.models import TransferParams, ProducerMetric


@dataclass
class FrameTransfer:
    transfer: TransferParams
    start_time: float

    def __post_init__(self):
        self.chunk_array = self.transfer.frame
        self.quality = self.transfer.quality
        self.frame_num = self.transfer.frame_number
        self.level = self.transfer.level
        self.brokers = self.transfer.brokers
        self.partitions = self.transfer.partitions
        self.topic = self.transfer.topic
        self.writer_type = self.transfer.writer_type
        self.writer = self.transfer.writer

        self.metrics: List[Dict[str, Any]] = []
        self.ack_array: List[Any] = []

        self._condition = Condition()
        self._stopped = True
        self._executor = ThreadPoolExecutor(max_workers=4)  # Adjust based on needs

    def acked(self, payload: Any) -> None:
        """Callback for successful message delivery"""
        if payload is not None:
            self.ack_array.append(payload)
        else:
            print("Failed to send message.")

    def start(self) -> 'FrameTransfer':
        """Start the transfer process"""
        self._stopped = False
        Thread(target=self._transfer_loop, daemon=True).start()
        return self

    def update_frame(self, new_frame: bytes, frame_num: int) -> None:
        """Update the frame with new data"""
        with self._condition:
            self.chunk_array = new_frame
            self.frame_num = frame_num
            self._condition.notify()


    def _create_payload(self, chunk: bytes, chunk_num: int, total_chunks: int,
                        key: bytes, timestamp: int) -> Dict[str, Any]:
        """Create a single payload for a chunk"""
        return {
            "quality": self.quality,
            "message_uuid": key.decode('utf-8'),
            "message_num": self.frame_num,
            "level": self.level,
            "chunk_num":chunk_num,
            "total_chunks":total_chunks,
            "brokers":len(self.brokers),
            "partitions":self.partitions,
            "produce_time":timestamp,
            "message_size":len(chunk),
            "message":base64.b64encode(chunk).decode('utf-8')
        }

    def _send_payload(self, payload: Dict[str, Any], key: bytes) -> None:
        """Send a single payload"""
        try:
            if self.writer_type == 'mqtt':
                # Convert payload to JSON string with proper formatting
                json_payload = json.dumps(payload, separators=(',', ':'))

                # Encode as UTF-8 bytes
                encoded_payload = json_payload.encode('utf-8')

                self.writer.publish(
                    topic=self.topic,
                    payload=encoded_payload,
                    retain=True,
                )

                # Wait for message to be delivered
                print(f"Published message {payload['message_uuid']}")

            elif self.writer_type == 'kafka':
                self.writer.send(
                    topic=self.topic,
                    value=json.dumps(payload).encode('utf-8'),
                    key=key
                ).add_callback(self.acked)
            else:
                raise ValueError("Invalid writer type. Must be 'kafka' or 'mqtt'.")

            # Store metrics without message content
            self.metrics.append({k: v for k, v in payload.items() if k != "message"})
        except Exception as e:
            print("Failed to send payload: ", e)
            raise e

    def _transfer_loop(self) -> None:
        """Main transfer loop with optimized batch processing"""
        try:
            while not self._stopped:
                with self._condition:
                    # Wait for new data or stop signal
                    if self._condition.wait_for(lambda: len(self.chunk_array) > 0 or self._stopped, timeout=1.0):
                        if self._stopped:
                            break

                        key = str(uuid.uuid1()).encode('utf-8')
                        timestamp = int(time.time() * 1000)
                        chunks = self.chunk_array
                        total_chunks = len(chunks)

                        # Create all payloads first
                        payloads = [
                            self._create_payload(chunk, i, total_chunks, key, timestamp)
                            for i, chunk in enumerate(chunks)
                        ]

                        # Send payloads in parallel
                        try:
                            list(self._executor.map(
                                lambda p: self._send_payload(p, key),
                                payloads
                            ))
                        finally:
                            if self.writer_type == 'kafka':
                                self.writer.flush()
                            self.chunk_array = []

        except Exception as e:
            self.stop()
            raise e

    def stop(self) -> None:
        """Stop the transfer process and clean up resources"""
        self._stopped = True
        with self._condition:
            self._condition.notify_all()
        self._executor.shutdown(wait=True)

        if self.writer_type == 'mqtt':
            self.writer.disconnect()