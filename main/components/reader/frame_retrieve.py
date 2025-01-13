import threading
import time
import base64


from main.components.reader.frame_decode import FrameDecode
from main.models.models import RetrieveParams


class FrameRetrieve:
    def __init__(self,
                 params: RetrieveParams,
                 frame_decode:FrameDecode=None
                 ):
        self.frame_num = 0

        self.topic = params.topic
        self.reader = params.reader

        self.frame_decode = frame_decode

        self.reader.subscribe([self.topic])
        self.payload_array = []

        self.lock = threading.Lock()
        self.stopped = False

    def start(self):
        self.stopped = False
        threading.Thread(target=self.get, daemon=True).start()

    def get(self):
        while not self.stopped:
            for msg in self.reader:
                if msg is None:
                    continue

                payload = msg.value
                try:
                    image_data = base64.b64decode(payload["message"])
                    payload['consume_time'] = time.time() * 1000

                    del payload["message"]
                    #self.frame_decode.update_data(payload, image_data)
                    self.payload_array.append({
                        "dict": payload,
                        "image": image_data
                    })
                except Exception as e:
                    raise e

                if self.stopped:
                    break

    def stop(self):
        self.stopped = True
        self.reader.close()
