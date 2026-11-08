"""Tests of the run analysis (analysis.py) and of merging worker reports (runs.py). No Docker needed."""

from app import analysis, config
from app.models import ScenarioSpec, SetupRequest, WorkersSpec
from app.runs import RunManager

EDGES = config.HIST_EDGES


def manager() -> RunManager:
    return RunManager(None, None, None, None)  # the calculations under test never touch Docker, Kafka or SQLite


def worker(received, partitions, first=100.6, last=103.5, **extra):
    base = {
        "received": received,
        "duplicates": 0,
        "ranges": {f"0:{partitions[0]}": [[0, received - 1]]} if received else {},  # one stream per worker: no overlap
        "overflow": 0,
        "errors": 0,
        "assigned": partitions,
        "first_at": first,
        "last_at": last,
        "lang": "node",
        "max_latency": 120.0,
        "rate": 0,
    }
    return base | extra


def make_run(**over):
    req = SetupRequest(
        queue="q",
        partitions=4,
        messages=1000,
        workers=WorkersSpec(lang="node", count=2, memory_mb=300),
        scenario=ScenarioSpec(type="echo"),
    )
    run = {
        "id": "r1",
        "req": req,
        "status": "completed",
        "error": None,
        "t0": 100.0,
        "producers": {
            "queuelab-q-producer1": {
                "sent": 1000,
                "delivered": 1000,
                "errors": 0,
                "done": True,
                "started": 100.5,
                "finished": 101.5,
            }
        },
        "workers": {"queuelab-q-w1": worker(500, [0, 1]), "queuelab-q-w2": worker(500, [2, 3])},
        "samples": [],
        "csamples": [],
        "hist_total": [0] * (len(EDGES) + 1),
    }
    return run | over


# ----------------------------------------------------------------------------------------------- percentile
def test_percentile_of_an_empty_histogram_is_none() -> None:
    assert analysis.percentile([0] * 5, 0.5) is None


def test_percentile_returns_the_upper_edge_of_the_bucket() -> None:
    hist = [0] * (len(EDGES) + 1)
    hist[3] = 100  # all messages waited between 2 and 5 ms
    assert analysis.percentile(hist, 0.5) == EDGES[3]
    assert analysis.percentile(hist, 0.99) == EDGES[3]


def test_percentile_splits_between_buckets() -> None:
    hist = [0] * (len(EDGES) + 1)
    hist[0], hist[5] = 90, 10  # 90% fast, 10% slow
    assert analysis.percentile(hist, 0.5) == EDGES[0]
    assert analysis.percentile(hist, 0.95) == EDGES[5]


def test_percentile_of_the_overflow_bucket_is_the_last_edge() -> None:
    hist = [0] * (len(EDGES) + 1)
    hist[-1] = 10
    assert analysis.percentile(hist, 0.99) == EDGES[-1]


# -------------------------------------------------------------------------------------------------- totals
def test_totals_merge_the_ranges_of_all_workers() -> None:
    run = make_run()
    run["workers"]["queuelab-q-w1"]["ranges"] = {"0:0": [[0, 494]]}  # 500 received, 495 different: 5 repeats
    tot = analysis.totals(run)
    assert tot["received"] == 1000
    assert tot["duplicates"] == 5
    assert tot["unique"] == 995
    assert tot["producer_done"] is True


def test_a_batch_read_again_by_another_worker_counts_as_duplicates() -> None:
    run = make_run()
    run["workers"]["queuelab-q-w1"]["ranges"] = {"0:0": [[0, 499]]}
    run["workers"]["queuelab-q-w2"]["ranges"] = {"0:0": [[400, 899]]}  # 400-499 were read by both
    tot = analysis.totals(run)
    assert (tot["received"], tot["unique"], tot["duplicates"]) == (1000, 900, 100)


def test_unlisted_numbers_are_taken_as_unique() -> None:
    run = make_run()
    run["workers"]["queuelab-q-w1"]["ranges"] = {"0:0": [[0, 399]]}
    run["workers"]["queuelab-q-w1"]["overflow"] = 100  # counted but not listed: a stream had too many gaps
    assert analysis.totals(run)["unique"] == 400 + 100 + 500

def test_handle_report_updates_the_worker_and_the_histograms() -> None:
    m, run = manager(), make_run(workers={})
    m.current = run
    report = {
        "runId": "r1",
        "role": "consumer",
        "worker": "w",
        "lang": "python",
        "ts": 10.0,
        "received": 100,
        "duplicates": 0,
        "ranges": {"0:0": [[0, 99]]},
        "overflow": 0,
        "errors": 0,
        "offsets": {"0": 100},
        "assigned": [0],
        "ready": True,
        "hist": [1] + [0] * len(EDGES),
        "firstAt": 9.0,
        "lastAt": 10.0,
        "maxLatencyMs": 3.5,
    }
    run["hist_sec"] = [0] * (len(EDGES) + 1)
    m.handle_report(report)
    m.handle_report(report | {"ts": 11.0, "received": 300})
    w = run["workers"]["w"]
    assert w["received"] == 300
    assert w["rate"] == 200  # 200 messages in 1 second
    assert w["max_latency"] == 3.5
    assert run["hist_total"][0] == 2


def test_a_report_of_another_run_is_ignored() -> None:
    m, run = manager(), make_run(workers={})
    m.current = run
    m.handle_report({"runId": "other", "role": "consumer", "worker": "w"})
    assert run["workers"] == {}


# --------------------------------------------------------------------------------------------------- window
def test_window_is_measured_from_the_producer_start() -> None:
    win = analysis.window(make_run())
    assert win == {
        "send_start": 0.5,
        "send_end": 1.5,
        "first_processed": 0.6,
        "last_processed": 3.5,
        "send_s": 1.0,
        "total_s": 3.0,
        "drain_s": 2.0,
    }


def test_window_of_a_run_that_never_started_is_empty() -> None:
    assert analysis.window(make_run(t0=None)) == {}


def test_window_without_consumer_timestamps_has_no_processing_part() -> None:
    run = make_run()
    for w in run["workers"].values():
        w["first_at"] = w["last_at"] = None
    win = analysis.window(run)
    assert "last_processed" not in win
    assert win["send_s"] == 1.0


# ------------------------------------------------------------------------------------------------ resources
def cs(t, cpu, mem, dt=0.5, name="queuelab-q-w1", role="consumer", limit=300):
    return {
        "t": t,
        "dt": dt,
        "containers": [
            {
                "name": name,
                "role": role,
                "lang": "node",
                "cpu": cpu,
                "mem_mb": mem,
                "limit_mb": limit,
                "oom": False,
                "msgs_s": 0,
                "rx_kbs": 100.0,
                "tx_kbs": 10.0,
            }
        ],
    }


def test_resources_ignore_samples_outside_the_window() -> None:
    run = make_run(csamples=[cs(-1.0, 90, 50), cs(1.0, 40, 60), cs(2.0, 60, 70), cs(9.0, 99, 90)])
    win = analysis.window(run)  # window is 0.5 .. 3.5, padded by 0.5
    res = analysis.resource_stats(run, win, analysis.totals(run))["queuelab-q-w1"]
    assert res["samples"] == 2  # the samples at t=-1.0 and t=9.0 are outside
    assert res["cpu_peak"] == 60
    assert res["mem_peak_mb"] == 70


def test_resources_are_time_weighted() -> None:
    run = make_run(csamples=[cs(1.0, 100, 50, dt=1.0), cs(2.0, 0, 50, dt=3.0)])
    res = analysis.resource_stats(run, analysis.window(run), analysis.totals(run))["queuelab-q-w1"]
    assert res["cpu_avg"] == 25.0  # 1 s at 100% and 3 s at 0%
    assert res["cpu_seconds"] == 1.0
    assert res["mem_headroom_mb"] == 250.0


def test_cpu_cost_per_thousand_messages() -> None:
    run = make_run(csamples=[cs(1.0, 100, 50, dt=1.0)])
    res = analysis.resource_stats(run, analysis.window(run), analysis.totals(run))["queuelab-q-w1"]
    assert res["messages"] == 500
    assert res["cpu_ms_per_1k"] == 2000.0  # 1 CPU-second for 500 messages = 2000 ms per 1000


def test_peak_time_is_recorded() -> None:
    run = make_run(csamples=[cs(1.0, 10, 50), cs(2.0, 80, 50), cs(3.0, 20, 50)])
    res = analysis.resource_stats(run, analysis.window(run), analysis.totals(run))["queuelab-q-w1"]
    assert res["cpu_peak_at"] == 2.0


# ---------------------------------------------------------------------------------------------- distribution
def test_distribution_shares_and_partitions() -> None:
    run = make_run()
    run["workers"]["queuelab-q-w1"]["received"] = 750
    run["workers"]["queuelab-q-w2"]["received"] = 250
    dist = analysis.distribution(run, analysis.totals(run))
    assert [d["share"] for d in dist] == [75.0, 25.0]
    assert dist[0]["partitions"] == [0, 1]


# ---------------------------------------------------------------------------------------------------- notes
def summary_for(run, **over):
    tot, win = analysis.totals(run), analysis.window(run)
    res = analysis.resource_stats(run, win, tot)
    base = {
        "lost": 0,
        "peak_lag": 0,
        "peak_recv_rate": 500,
        "p50": 5,
        "p99": 100,
        "resources": res,
        "distribution": analysis.distribution(run, tot),
        "avg_recv_rate": 400,
        "latency_max_ms": 120.0,
        "peak_lag_at": 1.0,
    }
    return base | over


def texts(insights, level=None):
    return [i["text"] for i in insights if level is None or i["level"] == level]


def test_notes_confirm_that_every_message_arrived_once() -> None:
    run = make_run()
    out = analysis.notes(run, analysis.totals(run), summary_for(run))
    assert any("exactly once" in t for t in texts(out, "ok"))


def test_notes_report_loss_as_a_fact() -> None:
    run = make_run()
    run["workers"]["queuelab-q-w1"]["received"] = 400  # 100 messages never arrived
    out = analysis.notes(run, analysis.totals(run), summary_for(run, lost=100))
    assert any("100 messages were lost" in t for t in texts(out, "bad"))


def test_notes_say_when_a_run_did_not_finish() -> None:
    run = make_run()
    run["status"] = "cancelled"
    assert any("cancelled" in t for t in texts(analysis.notes(run, analysis.totals(run), summary_for(run)), "warn"))

# ----------------------------------------------------------------------------------- several producers
def producers(*reports):
    return {f"queuelab-q-producer{i + 1}": r for i, r in enumerate(reports)}


def report(sent, done, started=100.5, finished=None, errors=0):
    return {"sent": sent, "delivered": sent, "errors": errors, "done": done, "started": started, "finished": finished}


def test_several_producers_count_as_one() -> None:
    run = make_run(producers=producers(report(600, True, 100.4, 101.2), report(400, True, 100.6, 101.8)))
    run["req"] = SetupRequest(queue="q", partitions=4, messages=1000, producer={"count": 2})
    view = analysis.producer_view(run)
    assert (view["sent"], view["done"]) == (1000, True)
    assert (view["started"], view["finished"]) == (100.4, 101.8)  # first to start .. last to finish


def test_the_producers_are_not_done_until_every_one_is() -> None:
    run = make_run(producers=producers(report(600, True, finished=101.2), report(100, False)))
    run["req"] = SetupRequest(queue="q", partitions=4, messages=1000, producer={"count": 2})
    view = analysis.producer_view(run)
    assert view["done"] is False
    assert view["finished"] is None


def test_a_producer_that_has_not_reported_yet_keeps_the_run_open() -> None:
    run = make_run(producers=producers(report(500, True, finished=101.0)))
    run["req"] = SetupRequest(queue="q", partitions=4, messages=1000, producer={"count": 2})
    assert analysis.producer_view(run)["done"] is False


def test_the_window_uses_all_producers() -> None:
    run = make_run(producers=producers(report(500, True, 100.5, 101.0), report(500, True, 100.7, 102.0)))
    run["req"] = SetupRequest(queue="q", partitions=4, messages=1000, producer={"count": 2})
    win = analysis.window(run)
    assert (win["send_start"], win["send_end"], win["send_s"]) == (0.5, 2.0, 1.5)


# ----------------------------------------------------------------------------------- rate and network
def net_resources(sent_mb, received_mb, broker_in=0.0, broker_out=0.0):
    return {
        "queuelab-q-producer1": {"role": "producer", "net_out_mb": sent_mb, "net_in_mb": 0.1},
        "queuelab-kafka": {
            "role": "broker",
            "net_in_mb": broker_in or sent_mb,
            "net_out_mb": broker_out or received_mb,
        },
        "queuelab-q-w1": {"role": "consumer", "net_in_mb": received_mb, "net_out_mb": 0.1},
    }


def test_network_compares_the_data_with_what_went_over_the_wire() -> None:
    run = make_run()
    run["req"] = SetupRequest(queue="q", partitions=4, messages=1000, message_size=1024, compression="lz4")
    net = analysis.network(run, analysis.totals(run), analysis.window(run), net_resources(0.25, 1.0))
    assert net["needed_mb"] == 0.98  # 1000 x 1024 bytes = 0.977 MiB
    assert net["producers_sent_mb"] == 0.25
    assert net["workers_received_mb"] == 1.0
    assert net["compression_ratio"] == 3.92  # the wire carried about a quarter of the data
    assert net["needed_mbit_s"] == 7.8  # 0.98 MiB x 8 in 1.0 s of sending


