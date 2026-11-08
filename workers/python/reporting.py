"""Progress reporting shared by the producer and the consumer: the stop flag, the lock and the HTTP report."""

import json
import threading
import time
import urllib.request
from collections.abc import Callable

import settings

stop = threading.Event()  # set on SIGTERM/SIGINT so loops can finish and send a final report
lock = threading.Lock()  # protects the counters shared by the work loop and the reporter thread


def post(payload: dict) -> None:
    """POST a progress report to the backend. A failed report is logged and skipped, never fatal."""
    try:
        request = urllib.request.Request(
            settings.REPORT, json.dumps(payload).encode(), {"Content-Type": "application/json"}
        )
        urllib.request.urlopen(request, timeout=3).read()
    except Exception as exc:  # noqa: BLE001
        print("report failed:", exc, flush=True)


def start_reporter(send: Callable[[], None], keep_going: Callable[[], bool] = lambda: True) -> None:
    """Call `send` once per second on a background thread until the worker stops."""

    def loop() -> None:
        while not stop.is_set() and keep_going():
            time.sleep(1)
            send()

    threading.Thread(target=loop, daemon=True).start()
