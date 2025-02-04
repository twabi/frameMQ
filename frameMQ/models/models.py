from dataclasses import dataclass
from typing import List, Callable, Any, Optional

from kafka import KafkaProducer, KafkaConsumer
import paho.mqtt.client as mqtt

from frameMQ.utils.enums import CompressionType


@dataclass
class KafkaConsumerParams:
    bootstrap_servers: List[str]
    auto_offset_reset: str = 'latest'
    enable_auto_commit: bool = False
    group_id: str = 'a-group'
    value_deserializer: Callable[[bytes], Any] = lambda x: x
    max_partition_fetch_bytes: int = 10485880
    fetch_max_bytes: int = 10485880
    fetch_max_wait_ms: int = 1
    receive_buffer_bytes: int = 10485880
    send_buffer_bytes: int = 10485880


@dataclass
class KafkaProducerParams:
    bootstrap_servers: List[str]
    compression_type: CompressionType = CompressionType.ZSTD
    max_request_size: int = 10485880
    batch_size: int = 5048588
    linger_ms: int = 5
    send_buffer_bytes: int = 5048588
    receive_buffer_bytes: int = 5048588


@dataclass
class CaptureParams:
    source: int
    fps: int
    codec: str = 'h264'
    platform: str = 'linux'
    level:int = 1

@dataclass
class EncodeParams:
    encoder_type: str
    quality: int
    chunk_num: int

@dataclass
class ProducerMetric:
    quality: int
    message_uuid: str
    message_num:int
    level: int
    chunk_num: int
    total_chunks: int
    brokers: int
    partitions: int
    produce_time: int
    message_size: int
    message: str

    def to_dict(self):
        return {
            "quality": self.quality,
            "message_uuid": self.message_uuid,
            "message_num": self.message_num,
            "level": self.level,
            "chunk_num": self.chunk_num,
            "total_chunks": self.total_chunks,
            "brokers": self.brokers,
            "partitions": self.partitions,
            "produce_time": self.produce_time,
            "message_size": self.message_size,
            "message": self.message
        }

    @staticmethod
    def from_dict(data: dict):
        return ProducerMetric(
            quality=data["quality"],
            message_uuid=data["message_uuid"],
            message_num=data["message_num"],
            level=data["level"],
            chunk_num=data["chunk_num"],
            total_chunks=data["total_chunks"],
            brokers=data["brokers"],
            partitions=data["partitions"],
            produce_time=data["produce_time"],
            message_size=data["message_size"],
            message=data["message"]
        )


@dataclass
class TransferParams:
    quality: int
    level: int
    brokers: List[str]
    partitions: int
    frame_number: int
    frame: List[bytes]
    topic: str
    writer: KafkaProducer | mqtt.Client
    writer_type: str = 'kafka'

    def update(self, quality: int, level: int, brokers: List[str],
               partitions: int, frame_number: int, frame: List[bytes], topic: str):
        self.quality = quality
        self.level = level
        self.brokers = brokers
        self.partitions = partitions
        self.frame_number = frame_number
        self.frame = frame

@dataclass
class WriterParams:
    brokers: List[str]
    topic: str
    encoder_type: Optional[str] = 'turbojpeg'
    writer_type: str = 'kafka'
    optimizer: str = 'none'
    source: int = 0
    notification_topic: str = 'notification'



@dataclass
class RetrieveParams:
    topic: str
    reader: KafkaConsumer | mqtt.Client
    reader_type: str = 'kafka'

@dataclass
class ReaderParams:
    brokers: List[str]
    topic: str
    group_id: str = 'y-group'
    reader_type:str = 'kafka'
    optimizer: str = 'none'
    notification_topic: str = 'notification'

@dataclass
class OptimizeTargetParams:
    FRAME_THRESHOLD:int = 50000
    TARGET_RATIO:float  = 0.08
    TARGET_LATENCY:int  = 60
    TARGET_QUALITY:int  = 55
    TARGET_PARTITIONS:int  = 300
    TARGET_CHUNKS:int  = 500
    # this is the level of aspect ratio of
    # the video where 0 is the lowest and 2 the highest
    TARGET_LEVEL:float  = 1.001
    LATENCY_THRESHOLD:float  = 0.5

@dataclass
class TrackedParams:
    # metrics from the consumer
    latency: int = 0
    frame_size: int = 0
    quality: int = 50
    current_level: int = 1
    chunk_number: int = 5


@dataclass
class GeneralParams:
    # kafka parameters
    consumer_group: str
    brokers: List[str]
    monitored_topic: str = "video-trans"
    sleep_interval: int = 5
    update_threshold: float = 0.7
    reader_type: str = 'kafka'

@dataclass
class PSOParams:
    tracked_params: TrackedParams
    optimize_target_params: OptimizeTargetParams
    general_params: GeneralParams

    num_particles: int = 10
    inertia_weight: float = 0.5
    cognitive_coeff: float = 1.0
    social_coeff: float = 2.0





