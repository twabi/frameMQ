from enum import Enum


class CompressionType(Enum):
    LZ4 = 'lz4'
    ZSTD = 'zstd'
    GZIP = 'gzip'
    SNAPPY = 'snappy'
