# FrameMQ

FrameMQ is a Python library that provides a simplified architecture for real-time video transmission over pub/sub platforms (currently supporting Apache Kafka and MQTT). It includes optional PSO (Particle Swarm Optimization) for automatic parameter tuning to achieve smoother transmission.

## Features

- Support for multiple pub/sub platforms:
  - Apache Kafka
  - MQTT
- Real-time video frame transmission
- Configurable video encoding parameters
- Optional PSO-based optimization for transmission parameters
- Thread-safe implementation
- Customizable buffer sizes and quality settings
- Extensible architecture for adding new pub/sub platforms
- Performance metrics tracking

## System Requirements

### Base Requirements
- Python 3.9 or higher
- For Kafka support: Running Kafka cluster
- For MQTT support: Running MQTT broker
- For video encoding: TurboJPEG library
- For video display: OpenCV with GStreamer support

### GStreamer Dependencies
The reader component requires OpenCV built with GStreamer support. Follow these steps to set up the required dependencies:

1. Install GStreamer and development packages:
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y \
    libgstreamer1.0-0 \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    gstreamer1.0-tools \
    gstreamer1.0-x \
    gstreamer1.0-alsa \
    gstreamer1.0-gl \
    gstreamer1.0-gtk3 \
    gstreamer1.0-qt5 \
    gstreamer1.0-pulseaudio \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libgstreamer-plugins-good1.0-dev \
    libgstreamer-plugins-bad1.0-dev
```

2. Install OpenCV dependencies:
```bash
sudo apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    python3-dev \
    python3-numpy \
    libtbb2 \
    libtbb-dev \
    libavcodec-dev \
    libavformat-dev \
    libswscale-dev \
    libdc1394-22-dev \
    libxvidcore-dev \
    libx264-dev \
    libgtk-3-dev \
    libjpeg-dev \
    libpng-dev \
    libtiff-dev \
    libv4l-dev \
    v4l-utils \
    qv4l2 \
    libgphoto2-dev \
    libfaac-dev \
    libmp3lame-dev \
    libtheora-dev \
    libvorbis-dev \
    libopencore-amrnb-dev \
    libopencore-amrwb-dev \
    libxine2-dev \
    libtbb-dev
```

3. Build and install OpenCV with GStreamer support:
```bash
# Clone OpenCV and OpenCV contrib repositories
git clone https://github.com/opencv/opencv.git
git clone https://github.com/opencv/opencv_contrib.git

# Create build directory
cd opencv
mkdir build
cd build

# Configure with CMake
cmake -D CMAKE_BUILD_TYPE=RELEASE \
    -D CMAKE_INSTALL_PREFIX=/usr/local \
    -D OPENCV_EXTRA_MODULES_PATH=../../opencv_contrib/modules \
    -D WITH_GSTREAMER=ON \
    -D WITH_LIBV4L=ON \
    -D BUILD_opencv_python3=ON \
    -D INSTALL_PYTHON_EXAMPLES=OFF \
    -D INSTALL_C_EXAMPLES=OFF \
    -D OPENCV_ENABLE_NONFREE=ON \
    -D OPENCV_PYTHON3_INSTALL_PATH=/usr/local/lib/python3.x/dist-packages \
    -D WITH_GTK=ON \
    -D WITH_QT=OFF \
    -D BUILD_EXAMPLES=OFF ..

# Build and install
make -j$(nproc)
sudo make install
sudo ldconfig
```

## Installation

### Using pip (once published)
```bash
pip install FrameMQ
```

### From source
```bash
# Clone the repository
git clone https://github.com/twabi/frameMQ.git
cd framemq

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

### Requirements
A `requirements.txt` file is provided with the repository containing all necessary Python dependencies. Key dependencies include:

```text
kafka-python==2.0.2
paho-mqtt==1.6.1
numpy==1.24.3
PyTurboJPEG==1.7.2
opencv-python  # Will be replaced by your GStreamer-enabled OpenCV build
```

## Quick Start

### Basic Writer Example

```python
from frameMQ.agent.writer import Writer
from frameMQ.models.models import WriterParams

# Initialize writer with Kafka
writer = Writer(
    params=WriterParams(
        brokers=['localhost:9092'],
        topic='video-stream',
        encoder_type='turbojpeg',
        writer_type='kafka',
        optimizer='none'
    )
)

writer.start()
# Your application logic here
writer.stop()
```

### Basic Reader Example

```python
from frameMQ.agent.reader import Reader
from frameMQ.models.models import ReaderParams

# Initialize reader with Kafka
reader = Reader(
    params=ReaderParams(
        group_id='my-group',
        brokers=['localhost:9092'],
        topic='video-stream',
        reader_type='kafka',
        optimizer='none'
    )
)

reader.start()
# Your application logic here
reader.stop()
```

## Configuration

### Writer Parameters

```python
WriterParams(
    brokers: List[str],          # List of broker addresses
    topic: str,                  # Topic name for publishing
    encoder_type: str = 'turbojpeg',  # Video encoder type
    writer_type: str = 'kafka',  # 'kafka' or 'mqtt'
    optimizer: str = 'none',     # 'none' or 'pso'
    source: int = 0,            # Video source (e.g., camera index)
    notification_topic: str = 'notification'  # Topic for optimization notifications
)
```

### Reader Parameters

```python
ReaderParams(
    brokers: List[str],         # List of broker addresses
    topic: str,                 # Topic name for subscribing
    group_id: str,              # Consumer group ID
    reader_type: str = 'kafka', # 'kafka' or 'mqtt'
    optimizer: str = 'none',    # 'none' or 'pso'
    notification_topic: str = 'notification'
)
```

## PSO Optimization

The library includes a Particle Swarm Optimization (PSO) implementation that automatically tunes transmission parameters for optimal performance. To enable PSO:

1. Set `optimizer='pso'` in your Writer/Reader parameters
2. The PSO optimizer will automatically adjust:
   - Number of partitions (Kafka only)
   - Chunk size
   - Video quality
   - Frame resolution level

## Advanced Usage

### Using MQTT

```python
# Writer with MQTT
writer = Writer(
    params=WriterParams(
        brokers=['mqtt-broker:1883'],  # Format: 'ip-address:port'
        topic='video-stream',
        writer_type='mqtt',
        optimizer='none'
    )
)

# Reader with MQTT
reader = Reader(
    params=ReaderParams(
        brokers=['mqtt-broker:1883'],
        topic='video-stream',
        group_id='my-group',
        reader_type='mqtt',
        optimizer='none'
    )
)
print(reader.metrics) # Get performance metrics

```

### With PSO Optimization

```python
# Enable PSO optimization
writer = Writer(
    params=WriterParams(
        brokers=['localhost:9092'],
        topic='video-stream',
        writer_type='kafka',
        optimizer='pso'  # Enable PSO
    )

print(writer.metrics) # Get performance metrics
```

## Performance Considerations

- For Kafka, adjust `buffer_size` based on your video quality and network capacity
- MQTT is recommended for smaller payloads and simpler setups
- PSO optimization can help balance quality vs. performance automatically
- Consider using multiple partitions in Kafka for better parallelism

## Extending the Library
- Its possible to add support for new pub/sub platforms by extending the `Writer` and `Reader` classes
- Its also possible to add new video encoders by adding the necessary encoding logic in the helper functions. Ensure compatibility with the existing interface.
- The PSO optimizer can be extended to include additional parameters for optimization.
- Its also possible to use other optimization algorithms in place of PSO, just ensure the interface is compatible

## Contact
For more information, please contact the author at [itwabi@gmail.com](mailto:itwabi@gmail.com).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

To contribute:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request


## License Recommendation
MIT License: https://opensource.org/licenses/MIT

