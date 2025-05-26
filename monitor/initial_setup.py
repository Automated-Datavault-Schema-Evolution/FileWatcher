import concurrent.futures
import os

import pandas as pd
from logger import log

from config.config import BASE_DIRECTORY, KAFKA_TOPIC, CHUNK_SIZE_ROWS, MAX_CHUNK_BYTES
from utils.hash_utils import row_hash
from utils.kafka_utils import send_df_in_chunks
from utils.state_utils import save_state, state_lock, load_state


def process_single_file_initial(file_path, producer, file_row_hashes):
    try:
        df = pd.read_csv(file_path)
        if not df.empty:
            send_df_in_chunks(df, producer, KAFKA_TOPIC, chunk_size_rows=CHUNK_SIZE_ROWS,
                              max_bytes=MAX_CHUNK_BYTES, source_file=file_path)
            hashes = [row_hash(row) for _, row in df.iterrows()]
            with state_lock:
                file_row_hashes[file_path] = hashes
        else:
            log.info(f"File {file_path} is empty. Skipping.")
    except Exception as e:
        log.critical(f"Initial crawl failed for {file_path}: {e}")


def initial_crawl_and_send(file_row_hashes, producer):
    # Only do this if no state exists yet
    state = load_state()
    if state:
        log.info("State file already exists; skipping initial crawl.")
        return
    log.info("No state or empty file found. Performing initial crawl of directory...")
    files_to_process = [
        os.path.join(BASE_DIRECTORY, f)
        for f in os.listdir(BASE_DIRECTORY)
        if f.endswith('.csv')
    ]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for file_path in files_to_process:
            executor.submit(process_single_file_initial, file_path, producer, file_row_hashes)
    save_state(file_row_hashes)
    log.info("Initial crawl and send completed.")
