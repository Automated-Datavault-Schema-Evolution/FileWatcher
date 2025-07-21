import time
from concurrent.futures import ThreadPoolExecutor

from logger import log
from watchdog.events import FileSystemEventHandler

from config.config import KAFKA_TOPIC, CHUNK_SIZE_ROWS, MAX_CHUNK_BYTES
from utils.hash_utils import get_new_rows_by_offset
from utils.kafka_utils import send_df_in_chunks
from utils.state_utils import save_state

# TODO: Dynamic allocation based current CPU-LOAD
executor = ThreadPoolExecutor(max_workers=4)


class CSVHashEventHandler(FileSystemEventHandler):
    def __init__(self, file_offsets, state_lock, producer):
        super().__init__()
        # self.file_row_hashes = file_row_hashes
        self.file_offsets = file_offsets
        self.state_lock = state_lock
        self.producer = producer

    def process_append(self, file_path):
        log.debug(f"Processing append for {file_path}")
        with self.state_lock:
            last_idx = self.file_offsets.get(file_path, 0)
        log.debug(f"Last known row index for {file_path}: {last_idx}")

        df_new = get_new_rows_by_offset(file_path, last_idx)
        if not df_new.empty:
            log.debug(f"Sending {len(df_new)} new rows from {file_path}")
            send_df_in_chunks(df_new, self.producer, KAFKA_TOPIC, chunk_size_rows=CHUNK_SIZE_ROWS,
                              max_bytes=MAX_CHUNK_BYTES, source_file=file_path)
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
            # new_rows_df = get_new_rows_by_hash(event.src_path, self.file_row_hashes, self.state_lock)
            # if new_rows_df is not None and not new_rows_df.empty:
            #    send_df_in_chunks(new_rows_df, self.producer, KAFKA_TOPIC, chunk_size_rows=CHUNK_SIZE_ROWS,
            #                      max_bytes=MAX_CHUNK_BYTES)
            #    save_state(self.file_row_hashes)
            #    log.info(f"Processed modified event in {time.time() - start:.2f}s")
            # else:
            #    log.debug(f"No new rows to send for {event.src_path}")
            executor.submit(self.process_append, event.src_path)
            log.info(f"Processed modified event in {time.time() - start:.2f}s")
        else:
            log.debug(f"Ignored modification event for {event.src_path}")

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            log.info(f"Detected new CSV file: {event.src_path}")
            start = time.time()
            # new_rows_df = get_new_rows_by_hash(event.src_path, self.file_row_hashes, self.state_lock)
            # if new_rows_df is not None and not new_rows_df.empty:
            #     send_df_in_chunks(new_rows_df, self.producer, KAFKA_TOPIC, chunk_size_rows=CHUNK_SIZE_ROWS,
            #                       max_bytes=MAX_CHUNK_BYTES)
            #     save_state(self.file_row_hashes)
            #     log.info(f"Processed new file event in {time.time() - start:.2f}s")
            # else:
            #     log.debug(f"No new rows to send for new file {event.src_path}")
            executor.submit(self.process_append, event.src_path)
            log.info(f"Processed new file event in {time.time() - start:.2f}s")
        else:
            log.debug(f"Ignored creation event for {event.src_path}")
