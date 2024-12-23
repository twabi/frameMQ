import json
from dataclasses import dataclass
from typing import List, Callable, Any

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