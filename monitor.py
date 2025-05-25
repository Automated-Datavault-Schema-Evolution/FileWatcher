import os
import threading
import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaProducer
from logger import log
import hashlib
import pickle

#CONFIG
#TODO: setup via dedicated utils-platform and env-file
BASE_DIRECTORY = 'C:\\Users\\alexm\Desktop\\repos\\automated_datavault_schema_evolution\\data'
KAFKA_TOPIC = 'file_changes'
KAFKA_BROKER = 'localhost:9092'


#Kafka-Setup
#TODO: Created dedicated utils file for reuse
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

state_lock = threading.Lock() # For safe concurrent access

#Row-Hashing for Change Detection --> General approach as it could be that no unique identifier is stored in baseline csv
#TODO: refactor to dedicated utils file for reuse
file_row_hashes={}

def row_hash(row):
    return hashlib.md5(','.join(map(str, row)).encode()).hexdigest()

def get_new_rows_by_hash(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        log.error(f"Failed to read {file_path}: {e}")
        return None

    with state_lock:
        hashes_sent = set(file_row_hashes.get(file_path, []))
    new_rows = []
    new_hashes = set()
    for _, row in df.iterrows():
        h = row_hash(row)
        new_hashes.add(h)
        if h not in hashes_sent:
            new_rows.append(row.values.tolist())

    with state_lock:
        file_row_hashes[file_path] = list(new_hashes)
    return pd.DataFrame(new_rows, columns=df.columns) if new_rows else None

#TODO: refactor for dedicated utils file and reuse
def initial_crawl_and_send():
    # Only do this if no state exists yet
    if os.path.exists(STATE_FILE):
        log.info("State file already exists; skipping initial crawl.")
        return
    log.info("No state file found. Performing initial crawl of directory...")
    for filename in os.listdir(BASE_DIRECTORY):
        if filename.endswith('.csv'):
            file_path = os.path.join(BASE_DIRECTORY, filename)
            try:
                df = pd.read_csv(file_path)
                if not df.empty:
                    payload = df.to_csv(index=False)
                    producer.send(KAFKA_TOPIC, payload.encode('utf-8'))
                    log.info(f"Sent entire file {file_path} ({len(df)} rows) to Kafka (initial crawl).")
                    hashes = [row_hash(row) for _, row in df.iterrows()]
                    with state_lock:
                        file_row_hashes[file_path] = hashes
                else:
                    log.info(f"File {file_path} is empty. Skipping.")
            except Exception as e:
                log.error(f"Initial crawl failed for {file_path}: {e}")
    save_state(file_row_hashes)

#persist state
#Store your offset data (row count, last ID, or hashes) in a local pkl
#Update this file after every successful Kafka send
#On process startup, load this checkpoint file.
#TODO: refactor to dedicated utils file
#TODO: scalability for larger files? AND/OR multiple files at once?
STATE_FILE = 'csv_kafka_state.pkl'

def save_state(state):
    with state_lock:
        with open(STATE_FILE, 'wb') as f:
            pickle.dump(state, f)

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return pickle.load(f)
    return {}

# file_path -> set of row hashes sent
file_row_hashes = load_state()

def get_new_rows_by_hash(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        log.error(f"Failed to read {file_path}: {e}")
        return None

    hashes_sent = set(file_row_hashes.get(file_path, []))
    new_rows = []
    new_hashes = set()
    for _, row in df.iterrows():
        h = row_hash(row)
        new_hashes.add(h)
        if h not in hashes_sent:
            new_rows.append(row.values.tolist())

    # Update hashes for this file (all hashes in file, not just new ones)
    file_row_hashes[file_path] = list(new_hashes)
    return pd.DataFrame(new_rows, columns=df.columns) if new_rows else None

class CSVHashEventHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            log.info(f"Detected modification: {event.src_path}")
            new_rows_df = get_new_rows_by_hash(event.src_path)
            if new_rows_df is not None and not new_rows_df.empty:
                payload = new_rows_df.to_csv(index=False)
                producer.send(KAFKA_TOPIC, payload.encode('utf-8'))
                log.info(f"Sent {len(new_rows_df)} new/changed rows from {event.src_path} to Kafka.")
                save_state(file_row_hashes)
            else:
                log.info(f"No new rows to send for {event.src_path}")

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            log.info(f"Detected new CSV file: {event.src_path}")
            new_rows_df = get_new_rows_by_hash(event.src_path)
            if new_rows_df is not None and not new_rows_df.empty:
                payload = new_rows_df.to_csv(index=False)
                producer.send(KAFKA_TOPIC, payload.encode('utf-8'))
                log.info(f"Sent {len(new_rows_df)} rows from new file {event.src_path} to Kafka.")
                save_state(file_row_hashes)
            else:
                log.info(f"No new rows to send for new file {event.src_path}")

def monitor_directory():
    # Initialize state for existing files
    for filename in os.listdir(BASE_DIRECTORY):
        if filename.endswith('.csv'):
            file_path = os.path.join(BASE_DIRECTORY, filename)
            # On startup, load all hashes (do NOT send them again)
            try:
                df = pd.read_csv(file_path)
                hashes = [row_hash(row) for _, row in df.iterrows()]
                file_row_hashes[file_path] = hashes
            except Exception as e:
                log.error(f"Initial load failed for {file_path}: {e}")
    save_state(file_row_hashes)

    event_handler = CSVHashEventHandler()
    observer = Observer()
    observer.schedule(event_handler, BASE_DIRECTORY, recursive=False)
    observer.start()
    log.info(f"Monitoring directory: {BASE_DIRECTORY}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    producer.close()

if __name__ == "__main__":
    crawl_thread = threading.Thread(target=initial_crawl_and_send)
    crawl_thread.start()
    monitor_directory()