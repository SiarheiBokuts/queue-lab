"""Tests of comparing runs (any number) and of the automatic one-run-per-language comparison."""

import asyncio

import pytest
from pydantic import ValidationError

from app import analysis
from app.models import ComparisonRequest, SetupRequest, WorkersSpec
from app.runs import RunManager


def resources(lang: str, cpu_ms: float, mem: float, cpu_avg: float = 40.0) -> dict:
    return {
        f"queuelab-q-w1-{lang}": {"role": "consumer", "cpu_avg": cpu_avg, "cpu_peak": cpu_avg + 20, "mem_peak_mb": mem,
                                  "cpu_ms_per_1k": cpu_ms},
        f"queuelab-q-producer1-{lang}": {"role": "producer", "cpu_avg": 10.0, "cpu_peak": 30.0, "mem_peak_mb": 30.0,
                                         "cpu_ms_per_1k": 5.0},
    }  # fmt: skip


def stored(run_id: str, lang: str, total_s: float = 2.0, cpu_ms: float = 100.0, mem: float = 50.0, **cfg) -> dict:
    config = {"partitions": 6, "messages": 1000, "message_size": 256, "workers": {"lang": lang, "count": 3, "memory_mb": 300},
              "producer": {"lang": lang, "memory_mb": 256, "count": 1}, "scenario": {"type": "echo", "param": 0},
              "rate": 0, "compression": "none", "payload": "compressible"}  # fmt: skip
    config.update(cfg)
    return {
        "id": run_id, "config": config,
        "summary": {
            "sent": 1000, "received": 1000, "duplicates": 0, "lost": 0, "peak_recv_rate": 900, "p50": 5, "p99": 50,
            "latency_max_ms": 80.0, "peak_lag": 500,
            "window": {"send_s": 0.5, "total_s": total_s, "drain_s": total_s - 0.5},
            "resources": resources(lang, cpu_ms, mem), "network": {"producers_sent_mb": 1.0},
        },
    }  # fmt: skip


def rows_by_key(result: dict) -> dict:
    return {r["key"]: r for r in result["rows"]}


# ------------------------------------------------------------------------------------------------ ties and bounds
def test_runs_within_the_tie_band_of_the_best_are_all_marked_best() -> None:
    row = analysis.compare_row("x", "X", "ms", True, "", [100.0, 102.0, 400.0])
    assert row["winners"] == [0, 1]  # 102 is within 5% of 100: it is a tie for best, the screen does not call it "worse"
    assert row["ratios"][1] == 1.02
    assert row["ratios"][2] == 4.0


def test_equal_values_are_all_best_even_when_they_are_zero() -> None:
    row = analysis.compare_row("drain_s", "Tail", "s", True, "", [0, 0, 7.35])
    assert row["winners"] == [0, 1]


def test_a_latency_at_the_measurement_limit_is_a_lower_bound() -> None:
    limit = analysis.config.HIST_EDGES[-1]
    row = analysis.compare_row("p99", "p99", "ms", True, "", [200.0, float(limit)])
    assert row["capped"] == [False, True]
    assert analysis.compare_row("max_latency", "x", "ms", True, "", [1.0, 2.0])["capped"] == [False, False]  # only the histogram latencies are capped


def test_the_network_is_measured_at_the_broker_when_it_is_known() -> None:
    run = stored("a", "node")
    run["summary"]["network"] = {"producers_sent_mb": 9.0, "broker_received_mb": 18.0}
    assert analysis.run_metrics(run)["network_mb"] == 18.0


# ------------------------------------------------------------------------------------------------ scores
def test_the_faster_and_cheaper_run_wins_the_score() -> None:
    result = analysis.compare([stored("a", "python", total_s=4.0, cpu_ms=200.0), stored("b", "go", total_s=1.0, cpu_ms=50.0)])
    s = result["scores"]
    assert s["winner"] == 1
    assert s["overall"][1] > s["overall"][0]
    assert {c["key"] for c in s["categories"]} == {"speed", "latency", "efficiency"}


def test_a_run_that_lost_messages_cannot_win() -> None:
    fast_but_lossy = stored("a", "rust", total_s=0.5)
    fast_but_lossy["summary"]["lost"] = 10
    s = analysis.compare([fast_but_lossy, stored("b", "go", total_s=2.0)])["scores"]
    assert s["disqualified"] == [True, False]
    assert s["winner"] == 1


def test_a_close_race_has_no_winner() -> None:
    s = analysis.compare([stored("a", "python"), stored("b", "node")])["scores"]
    assert s["winner"] is None


# ------------------------------------------------------------------------------------------------ metrics
def test_run_metrics_has_every_standard_number() -> None:
    m = analysis.run_metrics(stored("a", "node"))
    assert m["e2e_rate"] == 500  # 1000 messages in 2 s
    assert m["send_rate"] == 2000  # 1000 messages in 0.5 s
    assert m["consumer_cpu_ms_per_1k"] == 100.0
    assert m["producer_mem_peak"] == 30.0
    assert m["network_mb"] == 1.0
    assert set(m) == {key for key, *_ in analysis.COMPARE_ROWS}


def test_duplicates_do_not_count_as_processed_in_the_speed() -> None:
    run = stored("a", "node")
    run["summary"]["received"], run["summary"]["duplicates"] = 1100, 100
    assert analysis.run_metrics(run)["e2e_rate"] == 500


# --------------------------------------------------------------------------------------------------- rows
def test_the_best_value_wins_and_the_others_are_rated_against_it() -> None:
    row = analysis.compare_row("x", "X", "ms", True, "", [100.0, 50.0, 200.0])  # lower is better
    assert row["best"] == 1
    assert row["ratios"] == [2.0, 1.0, 4.0]


def test_higher_is_better_flips_the_winner() -> None:
    row = analysis.compare_row("x", "X", "msg/s", False, "", [100, 50, 200])
    assert row["best"] == 2
    assert row["ratios"] == [2.0, 4.0, 1.0]


def test_values_within_five_percent_are_a_tie() -> None:
    assert analysis.compare_row("x", "X", "ms", True, "", [100, 104, 102])["best"] is None


def test_unknown_values_are_left_out() -> None:
    row = analysis.compare_row("x", "X", "ms", True, "", [None, 5, 10])
    assert row["best"] == 1
    assert row["ratios"] == [None, 1.0, 2.0]
    assert analysis.compare_row("x", "X", "ms", True, "", [None, 5, None])["best"] is None


def test_zero_against_something_wins_without_a_ratio() -> None:
    row = analysis.compare_row("lost", "Lost", "messages", True, "", [0, 10])
    assert row["best"] == 0
    assert row["ratios"] == [None, None]


# ---------------------------------------------------------------------------------------------- compare()
def test_only_the_language_differing_is_a_fair_comparison() -> None:
    result = analysis.compare([stored("a", "node"), stored("b", "python"), stored("c", "go")])
    assert result["fair"] is True
    assert result["names"] == ["Node.js", "Python", "Go"]
    assert [d["field"] for d in result["differences"]] == ["worker language", "producer language"]
    assert result["differences"][0]["values"] == ["node", "python", "go"]
    assert any("Only the language differed" in v["text"] for v in result["verdict"])


def test_a_different_setup_is_flagged_as_not_comparable() -> None:
    result = analysis.compare([stored("a", "node"), stored("b", "python", messages=5000)])
    assert result["fair"] is False
    assert any("message count" in v["text"] and v["level"] == "warn" for v in result["verdict"])


def test_a_different_rate_or_compression_is_flagged() -> None:
    result = analysis.compare([stored("a", "node"), stored("b", "python", rate=1000, compression="lz4")])
    fields = {d["field"] for d in result["differences"] if not d["expected"]}
    assert fields == {"target rate", "compression"}


def test_the_verdict_names_the_fastest_language_among_several() -> None:
    runs = [stored("a", "node", total_s=4.0), stored("b", "go", total_s=1.0), stored("c", "java", total_s=2.0)]
    result = analysis.compare(runs)
    text = next(v["text"] for v in result["verdict"] if "processed messages fastest" in v["text"])
    assert "Go processed messages fastest" in text
    assert "4.0x the speed of Node.js" in text
    assert result["wins"][1] > result["wins"][0]


def test_the_verdict_names_the_most_efficient_language() -> None:
    result = analysis.compare([stored("a", "node", cpu_ms=50.0), stored("b", "python", cpu_ms=200.0)])
    assert any(
        "Node.js needed the least CPU" in v["text"] and "4.0x less than Python" in v["text"] for v in result["verdict"]
    )


def test_identical_results_have_no_clear_winner() -> None:
    result = analysis.compare([stored("a", "node"), stored("b", "python")])
    assert any("no clear winner" in v["text"] for v in result["verdict"])
    assert result["wins"] == [0, 0]


def test_runs_with_the_same_language_are_named_by_id() -> None:
    result = analysis.compare([stored("run1", "node"), stored("run2", "node")])
    assert result["names"] == ["Run run1", "Run run2"]


def test_lost_messages_are_reported_as_bad() -> None:
    lossy = stored("b", "python")
    lossy["summary"]["lost"] = 7
    result = analysis.compare([stored("a", "node"), lossy])
    assert any(v["level"] == "bad" for v in result["verdict"])


# ------------------------------------------------------------------------------------- the request
def test_a_comparison_needs_two_different_languages() -> None:
    with pytest.raises(ValidationError, match="at least two languages"):
        ComparisonRequest(langs=["node"])
    with pytest.raises(ValidationError, match="only once"):
        ComparisonRequest(langs=["node", "node"])
    assert ComparisonRequest(langs=["go", "rust", "java"]).langs == ["go", "rust", "java"]


# ------------------------------------------------------------------------ the automatic comparison
class FakeManager(RunManager):
    """A manager whose runs finish instantly, to test the order and the bookkeeping of a comparison."""

    def __init__(self, statuses: list[str]):
        super().__init__(None, None, None, None)
        self.statuses = statuses
        self.started: list[tuple] = []

    async def start(self, req, group=None, label=None):
        self.started.append((req.workers.lang, req.producer.lang, group, label))
        status = self.statuses[len(self.started) - 1]
        self.current = {
            "id": f"r{len(self.started)}",
            "status": status,
            "error": "boom" if status == "failed" else None,
        }
        self.task = asyncio.create_task(asyncio.sleep(0))
        return {"id": self.current["id"]}


def comparison(langs=("node", "python"), follows=True) -> tuple[dict, ComparisonRequest]:
    creq = ComparisonRequest(setup=SetupRequest(workers=WorkersSpec(lang="python")), langs=list(langs),
                             cooldown_s=0, producer_follows=follows)  # fmt: skip
    cmp = {"id": "c1", "status": "running", "step": 0, "total": len(langs), "langs": list(langs), "run_ids": [],
           "cooldown_left": 0, "cooldown_s": 0, "cancel": False, "error": None}  # fmt: skip
    return cmp, creq


def test_every_language_runs_in_the_chosen_order_and_the_runs_are_grouped() -> None:
    m = FakeManager(["completed"] * 3)
    cmp, creq = comparison(langs=("go", "node", "rust"))
    asyncio.run(m._run_comparison(cmp, creq))
    assert cmp["status"] == "completed"
    assert cmp["run_ids"] == ["r1", "r2", "r3"]
    assert m.started == [("go", "go", "c1", "go"), ("node", "node", "c1", "node"), ("rust", "rust", "c1", "rust")]


def test_the_producer_can_keep_its_own_language() -> None:
    m = FakeManager(["completed", "completed"])
    cmp, creq = comparison(follows=False)
    asyncio.run(m._run_comparison(cmp, creq))
    assert {s[1] for s in m.started} == {"python"}  # the setup's producer language, untouched


def test_a_failed_run_stops_the_comparison() -> None:
    m = FakeManager(["completed", "failed", "completed"])
    cmp, creq = comparison(langs=("node", "python", "go"))
    asyncio.run(m._run_comparison(cmp, creq))
    assert cmp["status"] == "failed"
    assert cmp["error"] == "boom"
    assert len(m.started) == 2


def test_a_cancelled_comparison_does_not_start_the_next_run() -> None:
    m = FakeManager(["completed", "completed"])
    cmp, creq = comparison()
    cmp["cancel"] = True
    asyncio.run(m._run_comparison(cmp, creq))
    assert cmp["status"] == "cancelled"
    assert len(m.started) == 1


def test_the_comparison_view_hides_internal_state() -> None:
    m = FakeManager([])
    assert m.comparison_view() is None
    m.comparison, _ = comparison()
    view = m.comparison_view()
    assert view["langs"] == ["node", "python"]
    assert "cancel" not in view


def test_a_zero_cpu_measurement_is_treated_as_unknown() -> None:
    """A container that lived less than one sample has a CPU of exactly 0: that is no data, not a win."""
    run = stored("a", "go", cpu_ms=0.0)
    run["summary"]["network"] = {"producers_sent_mb": 0.0}
    m = analysis.run_metrics(run)
    assert m["consumer_cpu_ms_per_1k"] is None
    assert m["network_mb"] is None
    row = analysis.compare_row("x", "X", "ms", True, "", [m["consumer_cpu_ms_per_1k"], 12.0])
    assert row["best"] is None


def test_the_languages_of_a_comparison_are_drawn_at_random_unless_told_otherwise() -> None:
    import random

    from app.runs import comparison_order

    langs = ["python", "node", "java", "go", "rust"]
    assert comparison_order(langs, False) == langs  # the order given
    shuffled = comparison_order(langs, True, random.Random(7))
    assert sorted(shuffled) == sorted(langs)  # nothing lost, nothing doubled
    assert shuffled != langs
    assert langs == ["python", "node", "java", "go", "rust"]  # the input is not changed
    assert ComparisonRequest(langs=langs).random_order is True  # random is the default
