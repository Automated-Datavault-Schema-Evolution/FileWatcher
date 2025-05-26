import hashlib
import threading

import pandas as pd
from logger import log


def row_hash(row):
    hashed = hashlib.md5(','.join(map(str, row)).encode()).hexdigest()
    # log.debug(f"[DIAGNOSTIC] Hashed row: {row.values.tolist()} -> {hashed}")
    return hashed


def get_new_rows_by_offset(file_path, last_row_idx):
    # Efficiently read just the new rows (header always included, so skip 1+N)
    try:
        df = pd.read_csv(file_path, skiprows=range(1, last_row_idx + 1))
        log.debug(f"Loaded {len(df)} new rows from {file_path}")
        return df
    except Exception as e:
        log.error(f"Failed to read {file_path}: {e}")
        return pd.DataFrame()


def get_new_rows_by_hash(file_path, file_row_hashes, state_lock):
    log.debug(f"(Thread: {threading.get_ident()}) Reading file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        log.debug(f"Loaded {len(df)} rows from {file_path}")
    except Exception as e:
        log.error(f"Failed to read {file_path}: {e}")
        return None

    with state_lock:
        hashes_sent = set(file_row_hashes.get(file_path, []))

    new_rows = []
    new_hashes = set()
    for _, row in df.iterrows():
        h = row_hash(row)
        new_hashes.add(h)
        if h not in hashes_sent:
            new_rows.append(row.values.tolist())

    with state_lock:
        file_row_hashes[file_path] = list(new_hashes)

    if new_rows:
        log.debug(f"Identified {len(new_rows)} new rows for {file_path}.")
        return pd.DataFrame(new_rows, columns=df.columns)
    else:
        log.debug(f"No new rows found for {file_path}.")
        return None
