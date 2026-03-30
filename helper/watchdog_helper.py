from watchdog.observers import Observer as InotifyObserver

try:
    from watchdog.observers.polling import PollingObserver
except Exception:
    PollingObserver = None

from logger import log


def create_observer(use_polling: bool, poll_interval_sec: float):
    """Create a watchdog observer.

    Preserves previous fallback behavior across watchdog versions.
    """

    if use_polling:
        if PollingObserver is None:
            log.warning("PollingObserver not available; falling back to default Observer.")
            return InotifyObserver()

        try:
            return PollingObserver(timeout=poll_interval_sec)
        except TypeError:
            return PollingObserver()

    return InotifyObserver()
