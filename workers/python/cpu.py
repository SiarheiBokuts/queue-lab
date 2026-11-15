"""CPU-bound work for the "cpu" scenario, run in a pool of processes.

One Python process runs Python code on one core at a time (the GIL), so a service that computes keeps the reading
process free and sends the computing to other processes, which is what this does. The pool size is the number of
cores, at most MAX_PROCESSES (every process costs memory, and a worker container has a memory limit).

This module imports nothing heavy on purpose: every process of the pool imports it when it starts.
"""

import os
from concurrent.futures import ProcessPoolExecutor

MAX_PROCESSES = 4
SEED = 2463534242  # starting value of the xorshift chain, the same in every language
MASK = 0xFFFFFFFF  # Python ints have no size, so the result is cut to 32 bits like in the other languages


def burn(rounds: int) -> None:
    """Run `rounds` steps of a 32-bit xorshift, each step uses the previous one. Runs in a process of the pool.

    Plain integer code in the language itself, no library does the work, so this measures the language and not
    a fast C or assembly routine behind it."""
    x = SEED
    for _ in range(rounds):
        x ^= (x << 13) & MASK
        x ^= x >> 17
        x ^= (x << 5) & MASK
    return x


def make_pool() -> ProcessPoolExecutor:
    """A pool with one process per core (at most MAX_PROCESSES)."""
    return ProcessPoolExecutor(max_workers=min(MAX_PROCESSES, os.cpu_count() or 1))
