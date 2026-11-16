"""Unit tests for the pure helpers of the Python worker."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest  # noqa: E402

from lib import SeqRanges, bucket, build_header, pacing_chunk, pacing_delay, parse_edges, parse_header  # noqa: E402

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


def test_numbers_one_after_another_make_one_range() -> None:
    seen = SeqRanges()
    assert all(seen.observe(0, 0, s) for s in range(5))
    assert seen.report() == {"0:0": [[0, 4]]}
    assert (seen.repeats, seen.overflow) == (0, 0)


def test_a_skipped_number_leaves_a_gap_between_two_ranges() -> None:
    seen = SeqRanges()
    for s in (0, 1, 2, 5, 6):
        seen.observe(0, 0, s)
    assert seen.report() == {"0:0": [[0, 2], [5, 6]]}  # 3 and 4 are in nobody's ranges: the lab counts them as lost


def test_a_number_seen_twice_is_a_repeat() -> None:
    seen = SeqRanges()
    for s in range(3):
        seen.observe(1, 0, s)
    assert seen.observe(1, 0, 1) is False
    assert seen.observe(1, 0, 0) is False
    assert seen.repeats == 2


def test_a_replayed_batch_fills_a_gap_and_the_ranges_are_merged() -> None:
    seen = SeqRanges()
    for s in (0, 1, 4, 5):
        seen.observe(0, 0, s)
    for s in (2, 3):  # the missing numbers arrive later
        assert seen.observe(0, 0, s) is True
    assert seen.report() == {"0:0": [[0, 5]]}


def test_streams_of_different_senders_and_partitions_do_not_clash() -> None:
    """Two senders both numbering their messages from 0 on the same partition are not repeats of each other."""
    seen = SeqRanges()
    for seq in range(3):
        assert seen.observe(0, 5, seq) and seen.observe(1, 5, seq) and seen.observe(0, 6, seq)
    assert seen.repeats == 0
    assert set(seen.report()) == {"0:5", "1:5", "0:6"}


def test_a_stream_with_too_many_gaps_stops_listing_but_keeps_counting() -> None:
    from lib import MAX_RANGES

    seen = SeqRanges()
    for s in range(0, MAX_RANGES * 2 + 10, 2):  # every second number: a range each
        seen.observe(0, 0, s)
    assert len(seen.report()["0:0"]) == MAX_RANGES
    assert seen.overflow > 0

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


def test_messages_are_grouped_by_user_and_keep_their_order() -> None:
    from lib import Item, group_by_user

    batch = [Item(b"user-1", 0, 1), Item(b"user-2", 0, 2), Item(b"user-1", 0, 3), Item(b"user-1", 0, 4)]
    groups = group_by_user(batch)
    assert [[i.seq for i in g] for g in groups] == [[1, 3, 4], [2]]  # one group per user, in the order sent
    assert group_by_user([]) == []


def test_batch_timing_measures_the_wait_for_the_slowest_group() -> None:
    from lib import batch_timing

    busy_ms, wait_ms = batch_timing(10.0, [10.5, 12.0, 11.0])
    assert (round(busy_ms), round(wait_ms)) == (2000, 1500)  # the batch took 2 s, 1.5 s after the first group was done
    assert batch_timing(10.0, [10.5]) == (500.0, 0.0)  # one group: nobody waits


def test_a_batch_without_groups_takes_no_time_and_does_not_crash() -> None:
    from lib import batch_timing

    assert batch_timing(10.0, []) == (0.0, 0.0)  # all messages belonged to another run


def test_a_user_handled_in_order_has_no_violation() -> None:
    from lib import Item, OrderCheck

    order = OrderCheck()
    assert all(order.check(Item(b"user-1", 0, seq)) for seq in (1, 2, 3, 3, 10))  # a repeat of 3 is not out of order
    assert order.violations == 0


def test_a_message_handled_after_a_later_one_is_a_violation() -> None:
    from lib import Item, OrderCheck

    order = OrderCheck()
    order.check(Item(b"user-1", 0, 5))
    assert order.check(Item(b"user-1", 0, 4)) is False
    assert order.violations == 1


def test_users_and_senders_are_checked_independently() -> None:
    from lib import Item, OrderCheck

    order = OrderCheck()
    order.check(Item(b"user-1", 0, 9))
    assert order.check(Item(b"user-2", 0, 1)) and order.check(Item(b"user-1", 1, 1))  # another user, another sender
    assert order.violations == 0

def test_the_json_work_parses_and_writes_the_document_and_runs_in_a_pool_of_processes() -> None:
    import json

    import cpu

    doc = '{"a":1,"b":[1,2,{"c":"x"}],"d":{"e":true}}'
    assert cpu.work(doc, 0) == len(doc)  # nothing to do: the document as it is
    assert cpu.work(doc, 3) == len(json.dumps(json.loads(doc), separators=(",", ":")))  # the same bytes come back
    with cpu.make_pool(2) as pool:
        assert pool.submit(cpu.work, doc, 5).result() == cpu.work(doc, 5)
