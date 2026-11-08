"""Unit tests for the pure helpers of the Python worker."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest  # noqa: E402
from lib import SeqTracker, bucket, build_header, pacing_chunk, pacing_delay, parse_edges, parse_header  # noqa: E402

EDGES = [0.5, 1, 2, 5, 10]


def test_parse_edges() -> None:
    assert parse_edges("0.5,1,2,5") == [0.5, 1.0, 2.0, 5.0]
    assert parse_edges(" 1 , 2 ,") == [1.0, 2.0]


@pytest.mark.parametrize(
    ("latency", "expected"),
    [(0.0, 0), (0.5, 0), (0.51, 1), (1, 1), (4.9, 3), (5, 3), (10, 4), (10.1, 5), (99999, 5)],
)
def test_bucket_boundaries(latency: float, expected: int) -> None:
    assert bucket(latency, EDGES) == expected


def test_header_round_trip() -> None:
    header = build_header("abc12345", 2, 3, 42, 1794147580123.456)
    message = header + b"x" * 100
    assert parse_header(message, "abc12345") == (2, 3, 42, 1794147580123.456)


def test_header_of_another_run_is_ignored() -> None:
    assert parse_header(build_header("other", 0, 0, 0, 1.0) + b"xx", "abc12345") is None


@pytest.mark.parametrize("garbage", [b"", b"no separators", b"a|b|c", b"run|0|x|y|z|pad", b"run|0|1|2|notanumber|pad"])
def test_malformed_header_returns_none(garbage: bytes) -> None:
    assert parse_header(garbage, "run") is None


def test_in_order_sequence_is_ok() -> None:
    t = SeqTracker()
    assert [t.observe(0, s) for s in range(5)] == ["ok"] * 5
    assert (t.gaps, t.duplicates) == (0, 0)


def test_gap_is_counted() -> None:
    t = SeqTracker()
    t.observe(0, 0)
    assert t.observe(0, 4) == "gap"  # 1, 2 and 3 were skipped
    assert t.gaps == 3
    assert t.observe(0, 5) == "ok"


def test_duplicate_is_counted() -> None:
    t = SeqTracker()
    t.observe(1, 0)
    t.observe(1, 1)
    assert t.observe(1, 1) == "duplicate"
    assert t.observe(1, 0) == "duplicate"
    assert t.duplicates == 2


def test_partitions_are_tracked_independently() -> None:
    t = SeqTracker()
    t.observe(0, 0)
    t.observe(1, 0)
    assert t.observe(0, 1) == "ok"
    assert t.observe(1, 1) == "ok"


def test_first_message_only_sets_the_baseline() -> None:
    """A worker that joins mid-run sees sequence 500 first; that must not count as 500 lost messages."""
    t = SeqTracker()
    assert t.observe(2, 500) == "ok"
    assert t.gaps == 0


def test_streams_of_different_producers_do_not_clash() -> None:
    """Two producers both numbering their messages from 0 on the same partition are not duplicates of each other."""
    t = SeqTracker()
    for seq in range(3):
        assert t.observe((0, 5), seq) == "ok"
        assert t.observe((1, 5), seq) == "ok"
    assert (t.gaps, t.duplicates) == (0, 0)


def test_no_rate_means_no_waiting() -> None:
    assert pacing_delay(1_000_000, 0, 0.001) == 0.0


def test_a_producer_ahead_of_its_rate_waits_for_the_difference() -> None:
    assert pacing_delay(1000, 1000, 0.25) == pytest.approx(0.75)  # 1000 messages should take 1 s; 0.25 s passed


def test_a_producer_behind_schedule_never_waits() -> None:
    assert pacing_delay(1000, 1000, 2.0) == 0.0


def test_the_pacing_chunk_is_about_ten_milliseconds_of_messages() -> None:
    assert pacing_chunk(0, 500) == 500
    assert pacing_chunk(10_000, 500) == 100
    assert pacing_chunk(1_000_000, 500) == 500  # never more than the maximum
    assert pacing_chunk(50, 500) == 1  # a slow rate still checks after every message


def test_a_user_always_lands_in_the_same_partition() -> None:
    from lib import user_key

    seen: dict[bytes, int] = {}
    for i in range(500):
        part = i % 6
        key = user_key(part, i, 6, 60)
        assert seen.setdefault(key, part) == part  # the same user, the same partition
    assert len(seen) == 60  # every user appears


def test_the_same_key_always_gets_the_same_lane() -> None:
    from lib import lane_for

    assert lane_for(b"user-7", 16) == lane_for(b"user-7", 16)
    assert all(0 <= lane_for(f"user-{n}".encode(), 8) < 8 for n in range(200))
    assert lane_for(None, 4) == lane_for(b"", 4)  # a message without a key is still handled


def test_the_cpu_work_is_a_chain_of_hashes_and_runs_in_a_pool_of_processes() -> None:
    import hashlib

    import cpu

    h = cpu.SEED
    for _ in range(3):
        h = hashlib.sha256(h).digest()
    cpu.burn(3)  # nothing to compare, it must simply run
    assert len(h) == 32
    with cpu.make_pool() as pool:
        assert pool.submit(cpu.burn, 50).result() is None
