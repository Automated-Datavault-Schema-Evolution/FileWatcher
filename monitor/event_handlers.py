import os
import time
import psutil
from concurrent.futures import ThreadPoolExecutor

from logger import log
from watchdog.events import FileSystemEventHandler

from config.config import KAFKA_TOPIC, CHUNK_SIZE_ROWS, MAX_CHUNK_BYTES
from utils.hash_utils import get_new_rows_by_offset
from utils.kafka_utils import send_df_in_chunks, send_schema_notification_for_file, build_schema_notification, \
    send_schema_notification
from utils.state_utils import save_state


def _wait_file_stable(path: str, stable_for_s: float = 0.2, timeout_s: float = 2.0) -> bool:
    """
    Return True if (size, mtime) stay unchanged for stable_for_s within timeout_s.
    Helps avoid reading partially rewritten CSV headers.
    """
    deadline = time.time() + timeout_s
    last = None
    stable_since = None

    while time.time() < deadline:
        try:
            st = os.stat(path)
        except FileNotFoundError:
            return False

        cur = (st.st_size, st.st_mtime)
        if cur == last:
            if stable_since is None:
                stable_since = time.time()
            elif (time.time() - stable_since) >= stable_for_s:
                return True
        else:
            last = cur
            stable_since = None

        time.sleep(0.05)

    return False

# Determine thread pool size based on current CPU load. If the CPU is heavily
# utilized the executor will use fewer workers. At minimum one worker is
# available and at most it uses all logical CPUs.
def _dynamic_worker_count():
    cpu_count = os.cpu_count() or 1
    # psutil.cpu_percent with a small interval gives a recent utilization value
    try:
        usage = psutil.cpu_percent(interval=0.1)
    except Exception:
        usage = 0
    available_ratio = max(0.1, (100 - usage) / 100)
    workers = max(1, int(cpu_count * available_ratio))
    log.debug(
        f"Initializing ThreadPoolExecutor with {workers} workers – CPU usage {usage}%"
    )
    return workers

executor = ThreadPoolExecutor(max_workers=_dynamic_worker_count())


class CSVHashEventHandler(FileSystemEventHandler):
    def __init__(self, file_offsets, state_lock, producer):
        super().__init__()
        self.file_offsets = file_offsets
        self.state_lock = state_lock
        self.producer = producer
        self.schema_fingerprints = {}

    def process_append(self, file_path, sef_handle=True):
        log.debug(f"Processing append for {file_path}")

        if sef_handle:
            # Emit schema snapshot only when fingerprint changed (dedupe)
            self._maybe_send_schema_notification(file_path)

        with self.state_lock:
            last_idx = self.file_offsets.get(file_path, 0)

        df_new = get_new_rows_by_offset(file_path, last_idx)
        if not df_new.empty:
            log.debug(f"Sending {len(df_new)} new rows from {file_path}")
            send_df_in_chunks(
                df_new,
                self.producer,
                KAFKA_TOPIC,
                chunk_size_rows=CHUNK_SIZE_ROWS,
                max_bytes=MAX_CHUNK_BYTES,
                source_file=file_path,
            )
            with self.state_lock:
                self.file_offsets[file_path] = last_idx + len(df_new)
            save_state(self.file_offsets)
            log.debug(f"Updated offset for {file_path} to {self.file_offsets[file_path]}")
        else:
            log.debug(f"No new rows found for {file_path}")

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            log.info(f"Detected modification: {event.src_path}")
            start = time.time()
            executor.submit(self.process_append, event.src_path, True)
            log.info(f"Processed modified event in {time.time() - start:.2f}s")
        else:
            log.debug(f"Ignored modification event for {event.src_path}")

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            log.info(f"Detected new CSV file (created): {event.src_path}")
            start = time.time()
            self._send_schema_notification_and_cache(event.src_path)
            executor.submit(self.process_append, event.src_path, False)
            log.info(f"Processed new file event in {time.time() - start:.2f}s")
        else:
            log.debug(f"Ignored creation event for {event.src_path}")

    def on_moved(self, event):
        if event.is_directory:
            log.debug(f"Ignored move event for directory {event.dest_path}")
            return

        if event.dest_path.endswith('.csv'):
            log.info(f"Detected new CSV file (moved): {event.dest_path}")
            start = time.time()
            self._send_schema_notification_and_cache(event.dest_path)
            executor.submit(self.process_append, event.dest_path, False)
            log.info(f"Processed moved file event in {time.time() - start:.2f}s")
        else:
            log.debug(f"Ignored move event for {event.dest_path}")

    def _send_schema_notification_and_cache(self, file_path: str) -> None:
        if not _wait_file_stable(file_path):
            log.debug(f"File not stable yet for schema read: {file_path}")
            return

        notification = build_schema_notification(file_path)
        if notification is None:
            log.debug(f"No schema notification built for {file_path}; skipping send.")
            return

        fingerprint = notification.get("header", {}).get("schema_fingerprint")
        if fingerprint:
            with self.state_lock:
                self.schema_fingerprints[file_path] = fingerprint

        # Send exactly what we fingerprinted (no second read)
        send_schema_notification(self.producer, notification)

    def _maybe_send_schema_notification(self, file_path: str) -> None:
        if not _wait_file_stable(file_path):
            log.debug(f"File not stable yet for schema read: {file_path}")
            return

        notification = build_schema_notification(file_path)
        if notification is None:
            log.debug(f"No schema notification built for {file_path}; skipping send.")
            return

        fingerprint = notification.get("header", {}).get("schema_fingerprint")
        if not fingerprint:
            log.debug(f"No schema fingerprint available for {file_path}; skipping send.")
            return

        with self.state_lock:
            previous_fingerprint = self.schema_fingerprints.get(file_path)
            if previous_fingerprint == fingerprint:
                log.debug(f"No schema change detected for {file_path}; skipping schema event.")
                return

            log.info(f"Schema change detected for {file_path}; sending schema event.")
            self.schema_fingerprints[file_path] = fingerprint

        # Send exactly what we fingerprinted (no second read)
        send_schema_notification(self.producer, notification)