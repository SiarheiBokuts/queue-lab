"""Work for the "json" scenario that keeps a core busy, run in a pool of processes.

One Python process runs Python code on one core at a time (the GIL), and even the C-written json module holds the GIL
while it works, so a service that computes keeps the reading process free and sends the work to other processes. The
pool size is the number of cores, at most CPU_THREADS (the same budget in every language).

This module imports nothing heavy on purpose: every process of the pool imports it when it starts.
"""

import json
import os
from concurrent.futures import ProcessPoolExecutor


def work(document: str, rounds: int) -> int:
    """Parse the JSON document and write it back to text, `rounds` times. Runs in a process of the pool."""
    text = document
    for _ in range(rounds):
        text = json.dumps(json.loads(text), separators=(",", ":"))
    return len(text)


def make_pool(size: int) -> ProcessPoolExecutor:
    """A pool with one process per core, at most `size` (the CPU_THREADS budget, the same in every language)."""
    return ProcessPoolExecutor(max_workers=min(size, os.cpu_count() or 1))