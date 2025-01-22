import threading
from threading import Thread


class NotifConsumer:

    def __init__(self, writer_type, reader):
        self.reader = reader
        self.writer_type = writer_type
        self.notif = None
        self.payload = None
        self.stopped = False
        self.lock = threading.Lock()
        self.thread = None  # Store thread reference

    def start(self):
        # Create daemon thread
        if self.writer_type == 'kafka':
            self.thread = Thread(target=self.notif_listen_kafka, daemon=True)
        else:
            self.thread = Thread(target=self.notif_listen_mqtt, daemon=True)

        self.thread.start()
        return self

    def notif_listen_kafka(self):
        self.reader.subscribe(['notification'])
        while not self.stopped:
            try:
                # Use poll with timeout to make the loop interruptible
                messages = self.reader.poll(timeout_ms=1000)

                if not messages:
                    continue

                for topic_partition, msgs in messages.items():
                    for msg in msgs:
                        if msg is None:
                            continue
                        with self.lock:
                            self.notif = msg.value

            except Exception as e:
                print(f"Error in Kafka notification listener: {e}")
                if self.stopped:
                    break

    def notif_listen_mqtt(self):
        try:
            self.reader.loop_start()
            self.reader.subscribe('notification')
            self.reader.on_message = self.on_message_mqtt

            # Replace loop_forever with a controlled loop
            while not self.stopped:
                # Sleep briefly to prevent CPU spinning
                threading.Event().wait(0.1)

        except Exception as e:
            print(f"Error in MQTT notification listener: {e}")
        finally:
            if self.reader:
                self.reader.loop_stop()
                self.reader.disconnect()

    def on_message_mqtt(self, client, userdata, message):
        try:
            print(f"Received message '{type(message.topic)}': {type(message.payload)}")
            with self.lock:
                self.notif = message.payload
        except Exception as e:
            print(f"Error processing message: {e}")

    def stop(self):
        self.stopped = True

        # Clean up Kafka consumer if needed
        if self.writer_type == 'kafka':
            try:
                self.reader.close()
            except Exception as e:
                print(f"Error closing Kafka consumer: {e}")

        # Wait for thread to finish with timeout
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)