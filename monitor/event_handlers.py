import time

from logger import log
from watchdog.events import FileSystemEventHandler

from config.config import KAFKA_TOPIC, CHUNK_SIZE_ROWS, MAX_CHUNK_BYTES
from utils.hash_utils import get_new_rows_by_hash
from utils.kafka_utils import send_df_in_chunks
from utils.state_utils import save_state


class CSVHashEventHandler(FileSystemEventHandler):
    def __init__(self, file_row_hashes, state_lock, producer):
        super().__init__()
        self.file_row_hashes = file_row_hashes
        self.state_lock = state_lock
        self.producer = producer

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            log.info(f"Detected modification: {event.src_path}")
            start = time.time()
            new_rows_df = get_new_rows_by_hash(event.src_path, self.file_row_hashes, self.state_lock)
            if new_rows_df is not None and not new_rows_df.empty:
                send_df_in_chunks(new_rows_df, self.producer, KAFKA_TOPIC, chunk_size_rows=CHUNK_SIZE_ROWS,
                                  max_bytes=MAX_CHUNK_BYTES)
                save_state(self.file_row_hashes)
                log.info(f"Processed modified event in {time.time() - start:.2f}s")
            else:
                log.debug(f"No new rows to send for {event.src_path}")

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            log.info(f"Detected new CSV file: {event.src_path}")
            start = time.time()
            new_rows_df = get_new_rows_by_hash(event.src_path, self.file_row_hashes, self.state_lock)
            if new_rows_df is not None and not new_rows_df.empty:
                send_df_in_chunks(new_rows_df, self.producer, KAFKA_TOPIC, chunk_size_rows=CHUNK_SIZE_ROWS,
                                  max_bytes=MAX_CHUNK_BYTES)
                save_state(self.file_row_hashes)
                log.info(f"Processed new file event in {time.time() - start:.2f}s")
            else:
                log.debug(f"No new rows to send for new file {event.src_path}")
