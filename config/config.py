import os

from dotenv import load_dotenv

load_dotenv()
# --- CONFIGURATION ---
BASE_DIRECTORY = os.getenv('HOST_DATA_DIRECTORY')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'file_changes')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
STATE_FILE = os.getenv('STATE_FILE', 'csv_kafka_state.pkl')
CHUNK_SIZE_ROWS = int(os.getenv('CHUNK_SIZE_ROWS', 1000))
MAX_CHUNK_BYTES = int(os.getenv('MAX_CHUNK_BYTES', 1000000))
