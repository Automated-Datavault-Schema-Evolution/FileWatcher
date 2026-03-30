"""Watchdog handlers that delegate event decisions to pure file-event helpers."""

from __future__ import annotations

import time

from logger import log
from watchdog.events import FileSystemEventHandler

from config.config import CHUNK_SIZE_ROWS, KAFKA_TOPIC, MAX_CHUNK_BYTES
from domain.file_events import fingerprint_changed, is_csv_path, next_offset
from helper.concurrency_helper import create_executor
from helper.filesystem_helper import wait_file_stable
from utils.hash_utils import get_new_rows_by_offset
from utils.kafka_utils import build_schema_notification, send_df_in_chunks, send_schema_notification
from utils.state_utils import save_state

executor = create_executor()


class CSVHashEventHandler(FileSystemEventHandler):
    """Translate filesystem changes into schema and row events."""

    def __init__(self, file_offsets, state_lock, producer):
        super().__init__()
        self.file_offsets = file_offsets
        self.state_lock = state_lock
        self.producer = producer
        self.schema_fingerprints = {}

    def process_append(self, file_path, sef_handle=True):
        """Send new rows and optional schema notifications for an updated file."""
        log.debug(f"Processing append for {file_path}")
        if sef_handle:
            self._maybe_send_schema_notification(file_path)

        with self.state_lock:
            last_idx = self.file_offsets.get(file_path, 0)

        df_new = get_new_rows_by_offset(file_path, last_idx)
        if df_new.empty:
            log.debug(f"No new rows found for {file_path}")
            return

        send_df_in_chunks(
            df_new,
            self.producer,
            KAFKA_TOPIC,
            chunk_size_rows=CHUNK_SIZE_ROWS,
            max_bytes=MAX_CHUNK_BYTES,
            source_file=file_path,
        )
        with self.state_lock:
            self.file_offsets[file_path] = next_offset(last_idx, len(df_new))
        save_state(self.file_offsets)
        log.debug(f"Updated offset for {file_path} to {self.file_offsets[file_path]}")

    def on_modified(self, event):
        """Schedule append processing when an existing CSV file changes."""
        if event.is_directory or not is_csv_path(event.src_path):
            log.debug(f"Ignored modification event for {event.src_path}")
            return
        log.info(f"Detected modification: {event.src_path}")
        start = time.time()
        executor.submit(self.process_append, event.src_path, True)
        log.info(f"Processed modified event in {time.time() - start:.2f}s")

    def on_created(self, event):
        """Handle newly created CSV files."""
        if event.is_directory or not is_csv_path(event.src_path):
            log.debug(f"Ignored creation event for {event.src_path}")
            return
        log.info(f"Detected new CSV file (created): {event.src_path}")
        start = time.time()
        self._send_schema_notification_and_cache(event.src_path)
        executor.submit(self.process_append, event.src_path, False)
        log.info(f"Processed new file event in {time.time() - start:.2f}s")

    def on_moved(self, event):
        """Treat moved CSV files like newly created files at the destination path."""
        if event.is_directory or not is_csv_path(event.dest_path):
            log.debug(f"Ignored move event for {getattr(event, 'dest_path', event.src_path)}")
            return
        log.info(f"Detected new CSV file (moved): {event.dest_path}")
        start = time.time()
        self._send_schema_notification_and_cache(event.dest_path)
        executor.submit(self.process_append, event.dest_path, False)
        log.info(f"Processed moved file event in {time.time() - start:.2f}s")

    def _send_schema_notification_and_cache(self, file_path: str) -> None:
        """Build, cache, and publish the latest schema snapshot for ``file_path``."""
        if not wait_file_stable(file_path):
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
        send_schema_notification(self.producer, notification)

    def _maybe_send_schema_notification(self, file_path: str) -> None:
        """Publish a schema event only when the observed fingerprint changed."""
        if not wait_file_stable(file_path):
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
            previous = self.schema_fingerprints.get(file_path)
            if not fingerprint_changed(previous, fingerprint):
                log.debug(f"No schema change detected for {file_path}; skipping schema event.")
                return
            self.schema_fingerprints[file_path] = fingerprint

        log.info(f"Schema change detected for {file_path}; sending schema event.")
        send_schema_notification(self.producer, notification)
