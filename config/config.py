import os

from dotenv import load_dotenv

load_dotenv()
# --- CONFIGURATION ---
BASE_DIRECTORY = os.getenv('DATA_DIRECTORY')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'file_changes')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
STATE_FILE = os.getenv('STATE_FILE', '/app/state/csv_kafka_state.pkl')
CHUNK_SIZE_ROWS = int(os.getenv('CHUNK_SIZE_ROWS', 1000))
MAX_CHUNK_BYTES = int(os.getenv('MAX_CHUNK_BYTES', 1000000))

# SEF / schema-evolution specific configuration
# Topic for schema evolution events consumed by the Schema Evolution Framework (SEF)
SEF_SCHEMA_TOPIC = os.getenv('SEF_SCHEMA_TOPIC', 'sef_schema_events')
# Logical name of the source system; propagated into the SEF event header
SEF_SOURCE_SYSTEM = os.getenv('SEF_SOURCE_SYSTEM', 'filewatcher')
# Optional logical domain / subject area for grouping datasets
SEF_DOMAIN = os.getenv('SEF_DOMAIN', 'default')

# ---- WATCHDOG OBSERVER SELECTION ----
def _default_observer() -> str:
    # In Docker Desktop bind mounts, inotify often misses events; polling is more reliable.
    if os.path.exists("/.dockerenv"):
        return "polling"
    return "inotify"

WATCHDOG_OBSERVER = os.getenv("WATCHDOG_OBSERVER", _default_observer()).strip().lower()
USE_POLLING_OBSERVER = WATCHDOG_OBSERVER in {"polling", "poll", "true", "1", "yes", "y"}

# Polling interval for PollingObserver (best-effort; watchdog versions vary)
WATCHDOG_POLL_INTERVAL_SEC = float(os.getenv("WATCHDOG_POLL_INTERVAL_SEC", "1.0"))
