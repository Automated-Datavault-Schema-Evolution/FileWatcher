import os
import threading
import time

import pandas as pd
from logger import log
from watchdog.observers import Observer

from config.config import BASE_DIRECTORY
from monitor.event_handlers import CSVHashEventHandler
from monitor.initial_setup import initial_crawl_and_send
from utils.hash_utils import row_hash
from utils.kafka_utils import get_kafka_producer
from utils.state_utils import save_state, load_state, state_lock


def monitor_directory(file_row_hashes, producer):
    log.info("Monitoring starting up. BASE_DIRECTORY=%s", BASE_DIRECTORY)
    for filename in os.listdir(BASE_DIRECTORY):
        if filename.endswith('.csv'):
            file_path = os.path.join(BASE_DIRECTORY, filename)
            try:
                df = pd.read_csv(file_path)
                hashes = [row_hash(row) for _, row in df.iterrows()]
                file_row_hashes[file_path] = hashes
                log.debug(f"Loaded {len(hashes)} hashes for {file_path}")
            except Exception as e:
                log.error(f"Initial load failed for {file_path}: {e}")
    save_state(file_row_hashes)

    event_handler = CSVHashEventHandler(file_row_hashes, state_lock, producer)
    observer = Observer()
    observer.schedule(event_handler, BASE_DIRECTORY, recursive=False)
    observer.start()
    log.info(f"Monitoring directory: {BASE_DIRECTORY}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.critical("Keyboard interrupt received, stopping observer.")
        observer.stop()
    finally:
        observer.join()
        try:
            producer.close()
            log.info("Kafka producer closed cleanly.")
        except Exception as e:
            log.critical(f"Failed to close Kafka producer: {e}")
        log.info("Observer stopped.")


def main():
    file_row_hashes = load_state()
    producer = get_kafka_producer()

    # Start initial crawl in a thread (or just call directly if you don't need async)
    crawl_thread = threading.Thread(target=initial_crawl_and_send, args=(file_row_hashes, producer))
    crawl_thread.start()
    # Optionally: crawl_thread.join() before continuing, if you want to wait for crawl to finish

    monitor_directory(file_row_hashes, producer)


if __name__ == "__main__":
    main()
