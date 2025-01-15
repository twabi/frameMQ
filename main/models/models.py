import json
from dataclasses import dataclass
from typing import List, Callable, Any, Optional

from kafka import KafkaProducer, KafkaConsumer
import paho.mqtt.client as mqtt

from main.utils.enums import CompressionType


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

    def update_level(self, level: int):
        self.level = level

@dataclass
class EncodeParams:
    encoder_type: str
    quality: int
    chunk_num: int

    def update(self, quality: int, chunk_num: int):
        self.quality = quality
        self.chunk_num = chunk_num

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



@dataclass
class RetrieveParams:
    topic: str
    reader: KafkaConsumer | mqtt.Client
    reader_type: str = 'kafka'

@dataclass
class ReaderParams:
    brokers: List[str]
    topic: str
    group_id: str
    reader_type:str = 'kafka'

