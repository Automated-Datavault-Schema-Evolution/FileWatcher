"""Hash and delta helpers for CSV row detection."""

from __future__ import annotations

import hashlib
import threading

import pandas as pd
from logger import log



def row_hash(row):
    """Return a stable hash for one CSV row."""
    return hashlib.md5(','.join(map(str, row)).encode()).hexdigest()



def get_new_rows_by_offset(file_path, last_row_idx):
    """Read only the rows that were appended after ``last_row_idx``."""
    try:
        df = pd.read_csv(file_path, skiprows=range(1, last_row_idx + 1))
        log.debug(f"Loaded {len(df)} new rows from {file_path}")
        return df
    except Exception as exc:
        log.error(f"Failed to read {file_path}: {exc}")
        return pd.DataFrame()



def get_new_rows_by_hash(file_path, file_row_hashes, state_lock):
    """Return rows whose content hash has not been observed before."""
    log.debug(f"(Thread: {threading.get_ident()}) Reading file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        log.debug(f"Loaded {len(df)} rows from {file_path}")
    except Exception as exc:
        log.error(f"Failed to read {file_path}: {exc}")
        return None

    with state_lock:
        hashes_sent = set(file_row_hashes.get(file_path, []))

    new_rows = []
    new_hashes = set()
    for _, row in df.iterrows():
        hashed = row_hash(row)
        new_hashes.add(hashed)
        if hashed not in hashes_sent:
            new_rows.append(row.values.tolist())

    with state_lock:
        file_row_hashes[file_path] = list(new_hashes)

    if new_rows:
        log.debug(f"Identified {len(new_rows)} new rows for {file_path}.")
        return pd.DataFrame(new_rows, columns=df.columns)

    log.debug(f"No new rows found for {file_path}.")
    return None
