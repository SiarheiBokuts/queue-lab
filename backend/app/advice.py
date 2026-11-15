"""Warnings about a setup: what is likely to happen with the numbers the user typed.

Nothing here refuses a setup. The lab lets you try any number; these notes only say what to expect, so a run that goes
badly is not a surprise. Every threshold is in `config.py`.
"""

from . import config
from .models import SetupRequest


def _size(nbytes: float) -> str:
    """Bytes as a short text, for example "12.5 GiB"."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if nbytes < 1024 or unit == "TiB":
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{int(nbytes)} B"
        nbytes /= 1024
    return ""


def _duration(seconds: float) -> str:
    if seconds < 90:
        return f"{round(seconds)} s"
    if seconds < 5400:
        return f"{round(seconds / 60)} min"
    return f"{seconds / 3600:.1f} h"


def warnings(req: SetupRequest) -> list[str]:
    """Plain sentences about what a setup will probably do. An empty list means nothing stands out."""
    out: list[str] = []
    data = req.messages * req.message_size
    if data > config.WARN_DATA_BYTES:
        out.append(
            f"About {_size(data)} will pass through Kafka. The broker keeps it on the disk of Docker, which may fill "
            "up; delete the queue after the run or send less."
        )
    if req.message_size > config.KAFKA_MAX_MESSAGE_BYTES:
        out.append(
            f"A message of {_size(req.message_size)} is above the default limit of the broker "
            f"(about {config.KAFKA_MAX_MESSAGE_BYTES // 1_000_000} MB). Kafka will refuse it and the run will fail, unless the broker "
            "is set up for bigger messages."
        )
    if req.rate > 0:
        sending = req.messages / req.rate
        if sending > config.RUN_TIMEOUT_S:
            out.append(
                f"At {req.rate:,} messages per second the sending alone takes about {_duration(sending)}. A run is "
                f"stopped after {_duration(config.RUN_TIMEOUT_S)}, so not every message will be sent."
            )
        if req.rate > config.WARN_RATE_PER_SENDER * req.producer.count:
            out.append(
                f"One sender usually cannot send more than about {config.WARN_RATE_PER_SENDER:,} messages per second. "
                f"Expect less than {req.rate:,}, or add senders."
            )
    elif req.messages > config.WARN_MESSAGES_FOR_TIMEOUT:
        out.append(
            f"{req.messages:,} messages is a long run. If it takes more than {_duration(config.RUN_TIMEOUT_S)} it is "
            "stopped and the rest is not counted."
        )
    if req.partitions > config.WARN_PARTITIONS:
        out.append(
            f"{req.partitions:,} partitions: every partition costs the broker memory and file handles, so a broker in "
            f"one container gets slow above about {config.WARN_PARTITIONS}."
        )
    if req.workers.concurrency > config.WARN_CONCURRENCY:
        out.append(
            f"{req.workers.concurrency:,} messages at once per worker is {req.workers.concurrency * req.workers.count:,} "
            "at the same time in total. If each one is a call to a database, a real service keeps 10 to 50 "
            "connections, not hundreds."
        )
    if req.users < req.messages and req.users < req.workers.concurrency:
        out.append(
            f"Only {req.users:,} users: the messages of a user are handled one after another, so at most {req.users:,} "
            "run at the same time, less than the batch size."
        )
    return out
