"""queue-lab worker (Python): the entry point. One image, two roles, chosen by the ROLE environment variable.

    consumer.py   the handler: reads messages, verifies them, runs the processing scenario
    producer.py   sends the messages
    settings.py   everything read from the environment (the worker contract, see backend/app/contract.py)
    reporting.py  once-per-second progress reports to the backend
    lib.py        pure helpers: message format, latency buckets, loss and duplicate detection (unit tested)
"""

import signal
import sys

import consumer
import producer
import reporting
import settings


def on_term(*_) -> None:
    """Ask the loops to stop; docker stop sends SIGTERM."""
    reporting.stop.set()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    print(f"{settings.ROLE} {settings.WID} starting", flush=True)
    (consumer.run if settings.ROLE == "consumer" else producer.run)()
    sys.exit(0)