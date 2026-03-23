import time

from logger import log

from config.config import BASE_DIRECTORY, USE_POLLING_OBSERVER, WATCHDOG_POLL_INTERVAL_SEC
from helper.filesystem_helper import scan_initial_offsets
from helper.watchdog_helper import create_observer
from monitor.event_handlers import CSVHashEventHandler
from utils.state_utils import save_state, state_lock


def prime_offsets_from_directory(file_offsets: dict) -> None:
    """Populate file_offsets with offsets from disk, then persist.

    Preserves the original startup scan semantics.
    """

    scanned = scan_initial_offsets(BASE_DIRECTORY)
    file_offsets.update(scanned)
    save_state(file_offsets)
    log.debug("State saved after initial directory scan")


def run_monitor_loop(file_offsets: dict, producer) -> None:
    """Run the watchdog observer loop.

    This is a direct extraction of the former monitor_directory() function.
    """

    log.info(f"Monitoring starting up. BASE_DIRECTORY={BASE_DIRECTORY}")
    prime_offsets_from_directory(file_offsets)

    log.debug("Initializing watchdog observer and event handler")
    event_handler = CSVHashEventHandler(file_offsets, state_lock, producer)
    observer = create_observer(USE_POLLING_OBSERVER, WATCHDOG_POLL_INTERVAL_SEC)
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
