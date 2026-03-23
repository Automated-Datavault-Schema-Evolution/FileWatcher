import os
import time

from logger import log


def wait_file_stable(path: str, stable_for_s: float = 0.2, timeout_s: float = 2.0) -> bool:
    """Return True if (size, mtime) stay unchanged for stable_for_s within timeout_s.

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


def count_csv_data_rows(file_path: str) -> int:
    """Count CSV rows excluding header row.

    Matches previous behavior: read as UTF-8 and subtract 1 for header.
    """

    with open(file_path, encoding="utf-8") as f:
        n_rows = sum(1 for _ in f) - 1
    if n_rows < 0:
        n_rows = 0
    return n_rows


def scan_initial_offsets(base_directory: str) -> dict:
    """Scan base directory and compute initial offsets for CSV files.

    Returns a mapping of absolute file_path -> n_data_rows.
    """

    offsets: dict[str, int] = {}
    for filename in os.listdir(base_directory):
        log.debug(f"Found entry during startup scan: {filename}")
        if not filename.endswith(".csv"):
            log.debug(f"Ignoring non CSV entry: {filename}")
            continue

        file_path = os.path.join(base_directory, filename)
        try:
            offsets[file_path] = count_csv_data_rows(file_path)
            log.info(f"Set last offset {file_path}: {offsets[file_path]}")
        except Exception as e:
            log.error(f"Could not set offset for {file_path}: {e}")

    return offsets
