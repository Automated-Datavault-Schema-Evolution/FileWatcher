import threading

from logger import log

from core.monitoring import run_monitor_loop
from monitor.initial_setup import initial_crawl_and_send
from utils.kafka_utils import get_kafka_producer
from utils.state_utils import load_state


def main():
    file_offsets = load_state()
    log.debug(f"Loaded state for {len(file_offsets)} files")

    producer = get_kafka_producer()
    log.debug("Kafka producer initialized")

    # Start initial crawl in a thread (original behavior)
    crawl_thread = threading.Thread(target=initial_crawl_and_send, args=(file_offsets, producer))
    crawl_thread.start()
    log.debug("Started initial crawl thread")

    log.debug("Starting directory monitoring")
    run_monitor_loop(file_offsets, producer)


if __name__ == "__main__":
    main()
