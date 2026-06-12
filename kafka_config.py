# config/kafka_config.py

# Network address for our Docker-based Kafka broker
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Name of our streaming data channel
CRICKET_TOPIC = 'live-cricket-stream'

# Local server configurations for our FastAPI application
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8000