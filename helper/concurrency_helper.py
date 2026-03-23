import os

import psutil
from concurrent.futures import ThreadPoolExecutor

from logger import log


def dynamic_worker_count() -> int:
    """Determine thread pool size based on current CPU load.

    Preserves previous behavior: compute a ratio from psutil cpu_percent and
    scale logical CPU count; at least 1 worker.
    """

    cpu_count = os.cpu_count() or 1
    try:
        usage = psutil.cpu_percent(interval=0.1)
    except Exception:
        usage = 0

    available_ratio = max(0.1, (100 - usage) / 100)
    workers = max(1, int(cpu_count * available_ratio))
    log.debug(f"Initializing ThreadPoolExecutor with {workers} workers – CPU usage {usage}%")
    return workers


def create_executor() -> ThreadPoolExecutor:
    """Create a ThreadPoolExecutor using dynamic_worker_count()."""

    return ThreadPoolExecutor(max_workers=dynamic_worker_count())
