"""The once-per-second report of a worker or a sender, checked before it touches the run.

`/internal/report` can be reached by anything on the sandbox network, so what arrives is not trusted: wrong types,
missing fields, lists of a million entries or numbers that are not finite must end in a 4xx answer and change nothing,
never in an exception halfway through updating the run (that would leave the run with half of a report).
"""

import json
import math
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StringConstraints, ValidationError, field_validator

from . import config, security

# The names the app gives its own containers: `queuelab-<queue>-w1`, `queuelab-<queue>-producer1`.
Name = Annotated[str, StringConstraints(pattern=security.CONTAINER_NAME_PATTERN)]
RunId = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9]{1,64}$")]
# Strict: a number is a JSON number, never the text "100" or `true`, which pydantic would otherwise convert.
Count = Annotated[int, Field(strict=True, ge=0, le=2**62)]  # a counter that only grows, as big as a JSON number can be
Seconds = Annotated[float, Field(strict=True, ge=0, le=4e9)]
Milliseconds = Annotated[float, Field(strict=True, ge=0, le=1e15)]

MAX_PARTITIONS = 100_000  # the form has no cap on partitions, so the lists below follow the biggest reasonable queue
MAX_STREAMS = 200_000  # one stream is a sender and a partition
MAX_RANGES_PER_STREAM = 1000  # the same cap the workers keep (MAX_RANGES in the workers)


class _Report(BaseModel):
    model_config = ConfigDict(extra="ignore")

    runId: RunId
    worker: Name
    lang: Annotated[str, StringConstraints(pattern=r"^[a-z]{2,12}$")]
    ts: Seconds

    @field_validator("ts")
    @classmethod
    def _finite(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("not a finite number")
        return value


class ConsumerReport(_Report):
    role: Literal["consumer"]
    ready: StrictBool
    assigned: list[Annotated[int, Field(strict=True, ge=0, le=MAX_PARTITIONS)]] = Field(max_length=MAX_PARTITIONS)
    received: Count
    duplicates: Count
    ranges: dict[Annotated[str, StringConstraints(pattern=r"^\d{1,9}:\d{1,9}$")], list[tuple[Count, Count]]] = Field(
        max_length=MAX_STREAMS
    )
    overflow: Count
    errors: Count
    offsets: dict[Annotated[str, StringConstraints(pattern=r"^\d{1,9}$")], Count] = Field(max_length=MAX_PARTITIONS)
    hist: list[Count]
    final: StrictBool
    firstAt: Seconds | None = None
    lastAt: Seconds | None = None
    maxLatencyMs: Milliseconds | None = None
    orderViolations: Count = 0
    batchBusyMs: Milliseconds = 0
    batchWaitMs: Milliseconds = 0

    @field_validator("hist")
    @classmethod
    def _hist_has_the_buckets_of_the_run(cls, value: list[int]) -> list[int]:
        # One more than the edges (the last bucket is "above the last edge"): a different length would break the merge.
        if len(value) != len(config.HIST_EDGES) + 1:
            raise ValueError(f"the histogram must have {len(config.HIST_EDGES) + 1} buckets")
        return value

    @field_validator("ranges")
    @classmethod
    def _ranges_are_few_and_ordered(cls, value: dict) -> dict:
        for stream, ranges in value.items():
            if len(ranges) > MAX_RANGES_PER_STREAM:
                raise ValueError(f"stream {stream} has too many ranges")
            if any(first > last for first, last in ranges):
                raise ValueError(f"stream {stream} has a range that ends before it starts")
        return value


class ProducerReport(_Report):
    role: Literal["producer"]
    sent: Count
    delivered: Count
    errors: Count
    done: StrictBool
    started: Seconds | None = None
    finished: Seconds | None = None


class ReportError(ValueError):
    """The report cannot be used: the answer is a 4xx and the run is left as it was."""

    def __init__(self, message: str, status: int = 422):
        super().__init__(message)
        self.status = status


def parse_report(body: bytes) -> dict:
    """The body of a POST /internal/report as a checked dict with the same keys the workers send."""
    if len(body) > security.MAX_REPORT_BYTES:
        raise ReportError("The report is too large.", 413)
    try:
        raw = json.loads(body)
    except (ValueError, RecursionError) as exc:
        raise ReportError("The report is not valid JSON.", 400) from exc
    if not isinstance(raw, dict):
        raise ReportError("The report must be a JSON object.", 422)
    model = ProducerReport if raw.get("role") == "producer" else ConsumerReport
    try:
        return model.model_validate(raw).model_dump()
    except ValidationError as exc:
        first = exc.errors()[0]
        where = ".".join(str(part) for part in first["loc"])
        raise ReportError(f"The report is not valid: {where}: {first['msg']}", 422) from exc
