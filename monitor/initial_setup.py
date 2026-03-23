"""Initial crawl that seeds the pipeline before watchdog takes over."""

from __future__ import annotations

import concurrent.futures

import pandas as pd
from logger import log

from config.config import BASE_DIRECTORY, CHUNK_SIZE_ROWS, KAFKA_TOPIC, MAX_CHUNK_BYTES
from domain.file_events import collect_csv_paths
from utils.hash_utils import row_hash
from utils.kafka_utils import send_df_in_chunks, send_schema_notification_for_file
from utils.state_utils import load_state, save_state, state_lock


def process_single_file_initial(file_path, producer, file_row_hashes):
    """Send the current contents of one CSV file during the bootstrap crawl."""
    log.debug(f"Initial processing of {file_path}")
    send_schema_notification_for_file(producer, file_path)
    try:
        df = pd.read_csv(file_path)
        log.debug(f"Loaded {len(df)} rows from {file_path}")
        if df.empty:
            log.info(f"File {file_path} is empty. Skipping.")
            return

        send_df_in_chunks(
            df,
            producer,
            KAFKA_TOPIC,
            chunk_size_rows=CHUNK_SIZE_ROWS,
            max_bytes=MAX_CHUNK_BYTES,
            source_file=file_path,
        )
        hashes = [row_hash(row) for _, row in df.iterrows()]
        with state_lock:
            file_row_hashes[file_path] = hashes
        log.debug(f"Stored {len(hashes)} row hashes for {file_path}")
    except Exception as exc:
        log.critical(f"Initial crawl failed for {file_path}: {exc}")



def initial_crawl_and_send(file_row_hashes, producer):
    """Perform the initial directory crawl when no persisted watcher state exists."""
    if load_state():
        log.info("State file already exists; skipping initial crawl.")
        return

    log.info("No state or empty file found. Performing initial crawl of directory...")
    files_to_process = collect_csv_paths(BASE_DIRECTORY)
    log.debug(f"Found {len(files_to_process)} files for initial crawl")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for file_path in files_to_process:
            executor.submit(process_single_file_initial, file_path, producer, file_row_hashes)
    save_state(file_row_hashes)
    log.info("Initial crawl and send completed.")
