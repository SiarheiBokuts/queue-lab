"""CPU-bound work for the "cpu" scenario, run in a pool of processes.

One Python process runs Python code on one core at a time (the GIL), so a service that computes keeps the reading
process free and sends the computing to other processes, which is what this does. The pool size is the number of
cores, at most MAX_PROCESSES (every process costs memory, and a worker container has a memory limit).

This module imports nothing heavy on purpose: every process of the pool imports it when it starts.
"""

import hashlib
import os
from concurrent.futures import ProcessPoolExecutor

MAX_PROCESSES = 4
SEED = b"queuelab"  # starting value of the hash chain, the same in every language


def burn(rounds: int) -> None:
    """Compute `rounds` SHA-256 hashes in a row, each of the previous one. Runs in a process of the pool."""
    h = SEED
    for _ in range(rounds):
        h = hashlib.sha256(h).digest()


def make_pool() -> ProcessPoolExecutor:
    """A pool with one process per core (at most MAX_PROCESSES)."""
    return ProcessPoolExecutor(max_workers=min(MAX_PROCESSES, os.cpu_count() or 1))
