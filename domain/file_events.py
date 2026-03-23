"""Pure functions for classifying filesystem events and state updates."""

from __future__ import annotations

import os
from typing import Iterable


def is_csv_path(path: str | None) -> bool:
    """Return ``True`` when the path points to a CSV file."""
    return bool(path) and str(path).lower().endswith('.csv')



def collect_csv_paths(base_directory: str, entries: Iterable[str] | None = None) -> list[str]:
    """Return absolute CSV paths under ``base_directory`` in deterministic order."""
    names = sorted(entries if entries is not None else os.listdir(base_directory))
    return [os.path.join(base_directory, name) for name in names if is_csv_path(name)]



def next_offset(last_offset: int, rows_written: int) -> int:
    """Return the next persisted row offset for a file."""
    return max(0, int(last_offset)) + max(0, int(rows_written))



def fingerprint_changed(previous: str | None, current: str | None) -> bool:
    """Return ``True`` when a schema fingerprint changed and a new event should be emitted."""
    return bool(current) and previous != current
