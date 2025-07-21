import os
import threading
import time

from logger import log
from watchdog.observers import Observer

from config.config import BASE_DIRECTORY
from monitor.event_handlers import CSVHashEventHandler
from monitor.initial_setup import initial_crawl_and_send
from utils.kafka_utils import get_kafka_producer
from utils.state_utils import save_state, load_state, state_lock


def monitor_directory(file_offsets, producer):
    log.info(f"Monitoring starting up. BASE_DIRECTORY={BASE_DIRECTORY}")
    for filename in os.listdir(BASE_DIRECTORY):
        log.debug(f"Found entry during startup scan: {filename}")
        if filename.endswith('.csv'):
            file_path = os.path.join(BASE_DIRECTORY, filename)
            try:
                with open(file_path, encoding='utf-8') as f:
                    n_rows = sum(1 for _ in f) - 1  # skip header row
                if n_rows < 0:
                    n_rows = 0

                file_offsets[file_path] = n_rows
                log.info(f"Set last offset {file_path}: {n_rows}")
            except Exception as e:
                log.error(f"Could not set offset for {file_path}: {e}")
        else:
            log.debug(f"Ignoring non CSV entry: {filename}")
    save_state(file_offsets)

    log.debug("State saved after initial directory scan")

    log.debug("Initializing watchdog observer and event handler")
    event_handler = CSVHashEventHandler(file_offsets, state_lock, producer)
    observer = Observer()
    observer.schedule(event_handler, BASE_DIRECTORY, recursive=False)
    observer.start()
    log.info(f"Monitoring directory: {BASE_DIRECTORY}")
    try:
        while True:
            time.sleep(0.01)
    except KeyboardInterrupt:
        log.critical("Keyboard interrupt received, stopping observer.")
        observer.stop()
    finally:
        log.debug("Waiting for observer thread to terminate")
        observer.join()
        try:
            producer.close()
            log.info("Kafka producer closed cleanly.")
        except Exception as e:
            log.critical(f"Failed to close Kafka producer: {e}")
        log.info("Observer stopped.")


def main():
    # file_row_hashes = load_state()
    file_offsets = load_state()
    log.debug(f"Loaded state for {len(file_offsets)} files")
    producer = get_kafka_producer()
    log.debug("Kafka producer initialized")

    # Start initial crawl in a thread (or just call directly if you don't need async)
    crawl_thread = threading.Thread(target=initial_crawl_and_send, args=(file_offsets, producer))
    crawl_thread.start()
    log.debug("Started initial crawl thread")
    # Optionally: crawl_thread.join() before continuing, if you want to wait for crawl to finish

    log.debug("Starting directory monitoring")
    monitor_directory(file_offsets, producer)


if __name__ == "__main__":
    main()
