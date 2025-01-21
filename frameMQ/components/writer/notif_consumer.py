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

    def start(self):
        if self.writer_type == 'kafka':
            Thread(target=self.notif_listen_kafka, args=()).start()
        else:
            Thread(target=self.notif_listen_mqtt, args=()).start()
        return self

    def notif_listen_kafka(self):
        self.reader.subscribe(['notification'])
        while not self.stopped:
            print("notif    ")
            for msg in self.reader:
                if msg is None:
                    print("None")
                    continue
                else:
                    notif = msg.value
                    self.notif = notif

            if self.stopped:
                break

    def notif_listen_mqtt(self):
        self.reader.loop_start()
        self.reader.subscribe('notification')
        self.reader.on_message = self.on_message_mqtt
        self.reader.loop_forever()

    def on_message_mqtt(self, client, userdata, message):
        try:
            print(f"Received message '{type(message.topic)}': {type(message.payload)}")
            self.notif = message.payload
        except Exception as e:
            print(f"Error processing message: {e}")

    def stop(self):
        self.stopped = True
