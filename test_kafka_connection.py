from kafka import KafkaClient, KafkaConsumer, KafkaProducer, TopicPartition
import time
import socket

# List of all broker addresses
bootstrap_servers = [
    '172.16.0.13:9092',
    '172.16.0.13:9094',
    '172.16.0.13:9095',
    '172.16.0.13:9096'
]

def test_socket_connection(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"Socket error: {str(e)}")
        return False

def test_connection():
    print("Testing connection to Kafka brokers...")
    
    # First test basic socket connectivity
    for server in bootstrap_servers:
        host, port = server.split(':')
        port = int(port)
        print(f"\nTesting socket connection to {server}...")
        if test_socket_connection(host, port):
            print(f"Socket connection successful to {server}")
        else:
            print(f"Socket connection failed to {server}")
    
    # Test consumer connection
    try:
        print("\nTrying to connect consumer to video-trans topic...")
        consumer = KafkaConsumer(
            bootstrap_servers=','.join(bootstrap_servers),
            enable_auto_commit=False,
            group_id=None,  # Disable consumer groups
            api_version=(0, 10),
            request_timeout_ms=15000
        )
        # Assign specific partition
        partition = TopicPartition('video-trans', 0)
        consumer.assign([partition])
        
        print("Successfully connected consumer to topic")
        print("Attempting to get topic metadata...")
        topics = consumer.topics()
        print(f"Available topics: {topics}")
        
        # Try to get the beginning and end offsets
        print("\nChecking partition offsets...")
        beginning_offsets = consumer.beginning_offsets([partition])
        end_offsets = consumer.end_offsets([partition])
        print(f"Beginning offsets: {beginning_offsets}")
        print(f"End offsets: {end_offsets}")
        
        consumer.close()
    except Exception as e:
        print(f"Consumer error: {str(e)}")

    # Test producer connection
    try:
        print("\nTrying to connect producer to video-trans topic...")
        producer = KafkaProducer(
            bootstrap_servers=','.join(bootstrap_servers),
            api_version=(0, 10),
            request_timeout_ms=15000
        )
        print("Successfully connected producer")
        producer.close()
    except Exception as e:
        print(f"Producer error: {str(e)}")

if __name__ == "__main__":
    test_connection() 