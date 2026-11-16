"""A report from a worker is checked before it touches the run: a bad one is a 4xx answer and changes nothing."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app import config, contract, main, reports, security
from app.models import ScenarioSpec, SetupRequest, WorkersSpec
from app.runs import RunManager

EDGES = config.HIST_EDGES


def consumer(**over) -> dict:
    report = {
        "runId": "r1", "worker": "queuelab-q-w1", "role": "consumer", "lang": "python", "ts": 10.0,
        "ready": True, "assigned": [0, 1], "received": 100, "duplicates": 0, "ranges": {"0:0": [[0, 99]]},
        "overflow": 0, "errors": 0, "offsets": {"0": 100}, "hist": [1] + [0] * len(EDGES), "final": False,
        "firstAt": 9.0, "lastAt": 10.0, "maxLatencyMs": 3.5, "orderViolations": 0, "batchBusyMs": 1.5, "batchWaitMs": 0.0,
    }  # fmt: skip
    return report | over


def producer(**over) -> dict:
    report = {
        "runId": "r1", "worker": "queuelab-q-producer1", "role": "producer", "lang": "go", "ts": 10.0,
        "sent": 50, "delivered": 40, "errors": 0, "done": False, "started": 9.0, "finished": None,
    }  # fmt: skip
    return report | over


def body(report) -> bytes:
    return json.dumps(report).encode()


def manager_with_run() -> tuple[RunManager, dict]:
    req = SetupRequest(
        queue="q", partitions=4, messages=1000, workers=WorkersSpec(lang="node", count=2, memory_mb=300),
        scenario=ScenarioSpec(type="echo"),
    )  # fmt: skip
    run = {
        "id": "r1", "req": req, "workers": {}, "producers": {},
        "hist_sec": [0] * (len(EDGES) + 1), "hist_total": [0] * (len(EDGES) + 1),
    }  # fmt: skip
    m = RunManager(None, None, None, None)
    m.current = run
    return m, run


# --------------------------------------------------------------------------------------------- parse_report
def test_a_good_report_of_each_role_is_accepted() -> None:
    assert reports.parse_report(body(consumer()))["received"] == 100
    assert reports.parse_report(body(producer()))["sent"] == 50


def test_the_report_models_have_exactly_the_fields_of_the_contract() -> None:
    """A field added to the contract but not to the model would be silently dropped on the way in."""
    assert set(reports.ConsumerReport.model_fields) == set(contract.CONSUMER_REPORT)
    assert set(reports.ProducerReport.model_fields) == set(contract.PRODUCER_REPORT)


def test_unknown_fields_are_dropped() -> None:
    assert "evil" not in reports.parse_report(body(consumer(evil="x" * 100)))


@pytest.mark.parametrize("raw", [b"", b"{", b"not json", b"\xff\xfe", b'{"a": ' + b"[" * 100_000, b"1" * 10_000])
def test_a_body_that_is_not_json_is_a_400(raw: bytes) -> None:
    with pytest.raises(reports.ReportError) as err:
        reports.parse_report(raw)
    assert err.value.status == 400


@pytest.mark.parametrize("raw", [b"[]", b"null", b"42", b'"text"', b"{}", b'{"role": "producer"}'])
def test_json_that_is_not_a_report_is_a_422(raw: bytes) -> None:
    with pytest.raises(reports.ReportError) as err:
        reports.parse_report(raw)
    assert err.value.status == 422


def test_a_body_over_the_limit_is_a_413() -> None:
    with pytest.raises(reports.ReportError) as err:
        reports.parse_report(b" " * (security.MAX_REPORT_BYTES + 1))
    assert err.value.status == 413


@pytest.mark.parametrize(
    "over",
    [
        {"received": "100"}, {"received": -1}, {"received": 1.5}, {"received": None}, {"received": 2**70},
        {"received": float("nan")}, {"ts": float("inf")}, {"ts": -5}, {"ts": "now"},
        {"worker": "../../etc"}, {"worker": "w"}, {"worker": "queuelab-" + "a" * 200}, {"worker": ["queuelab-a"]},
        {"runId": "../x"}, {"runId": 7}, {"lang": "<script>"},
        {"ready": "yes"}, {"final": 1.5}, {"assigned": "0,1"}, {"assigned": [-1]}, {"assigned": [1.5]},
        {"ranges": []}, {"ranges": {"x": [[0, 1]]}}, {"ranges": {"0:0": [[5, 1]]}}, {"ranges": {"0:0": [[1]]}},
        {"ranges": {"0:0": [[0, 1]] * 1001}},
        {"offsets": {"a": 1}}, {"offsets": {"0": -4}}, {"offsets": []},
        {"hist": [0] * 3}, {"hist": [0] * (len(EDGES) + 2)}, {"hist": "x"}, {"hist": [-1] + [0] * len(EDGES)},
        {"maxLatencyMs": "slow"}, {"firstAt": -1},
    ],
)  # fmt: skip
def test_a_report_with_a_wrong_field_is_a_422(over: dict) -> None:
    with pytest.raises(reports.ReportError) as err:
        reports.parse_report(json.dumps(consumer(**over), allow_nan=True).encode())
    assert err.value.status == 422


@pytest.mark.parametrize("missing", ["worker", "ts", "received", "ranges", "offsets", "hist", "assigned", "runId"])
def test_a_report_without_a_required_field_is_a_422(missing: str) -> None:
    report = consumer()
    del report[missing]
    with pytest.raises(reports.ReportError) as err:
        reports.parse_report(body(report))
    assert err.value.status == 422


def test_a_producer_report_is_checked_too() -> None:
    for over in ({"sent": "many"}, {"done": "later"}, {"delivered": -1}, {"started": "x"}):
        with pytest.raises(reports.ReportError):
            reports.parse_report(body(producer(**over)))


# ------------------------------------------------------------------------------------------- the route
def request_with(chunks: list[bytes]):
    async def stream():
        for chunk in chunks:
            yield chunk

    return SimpleNamespace(stream=stream)


def post(chunks: list[bytes]):
    return asyncio.run(main.report(request_with(chunks)))


@pytest.fixture
def live_run(monkeypatch):
    m, run = manager_with_run()
    monkeypatch.setattr(main, "runs", m)
    return run


def test_the_route_takes_a_good_report_into_the_run(live_run) -> None:
    assert post([body(consumer())]) == {}
    assert live_run["workers"]["queuelab-q-w1"]["received"] == 100


@pytest.mark.parametrize(
    ("chunks", "status"),
    [([b"{"], 400), ([b"[1, 2]"], 422), ([body(consumer(received="x"))], 422), ([b"x" * 600_000, b"x" * 600_000], 413)],
)
def test_the_route_answers_a_bad_report_with_a_4xx_and_leaves_the_run_alone(live_run, chunks, status) -> None:
    with pytest.raises(HTTPException) as err:
        post(chunks)
    assert err.value.status_code == status
    assert live_run["workers"] == {} and live_run["producers"] == {}
    assert live_run["hist_total"] == [0] * (len(EDGES) + 1)


def test_the_size_is_counted_as_it_arrives_not_taken_from_content_length(live_run) -> None:
    """A chunked request has no Content-Length at all; the route still stops reading at the limit."""
    big = [b"x" * 100_000] * 11
    with pytest.raises(HTTPException) as err:
        post(big)
    assert err.value.status_code == 413


def test_a_bad_report_does_not_stop_the_good_ones_that_follow(live_run) -> None:
    with pytest.raises(HTTPException):
        post([b"garbage"])
    post([body(consumer(received=7))])
    assert live_run["workers"]["queuelab-q-w1"]["received"] == 7


# ----------------------------------------------------------------------------------- handle_report itself
def test_a_report_of_a_container_the_run_did_not_start_is_dropped() -> None:
    m, run = manager_with_run()
    for name in ("queuelab-q-w3", "queuelab-other-w1", "queuelab-q-producer1", "queuelab-app"):
        assert m.handle_report(reports.parse_report(body(consumer(worker=name)))) is False
    assert m.handle_report(reports.parse_report(body(producer(worker="queuelab-q-w1")))) is False
    assert run["workers"] == {} and run["producers"] == {}  # invented names would also break "all workers are ready"


def test_a_report_for_the_expected_names_is_taken() -> None:
    m, run = manager_with_run()
    assert m.handle_report(reports.parse_report(body(consumer(worker="queuelab-q-w2")))) is True
    assert m.handle_report(reports.parse_report(body(producer()))) is True
    assert "queuelab-q-w2" in run["workers"] and "queuelab-q-producer1" in run["producers"]


def test_a_report_of_another_run_or_without_a_run_is_ignored() -> None:
    m, run = manager_with_run()
    assert m.handle_report(reports.parse_report(body(consumer(runId="old")))) is False
    m.current = None
    assert m.handle_report(reports.parse_report(body(consumer()))) is False
    assert run["workers"] == {}


def test_a_histogram_of_the_wrong_length_changes_nothing_even_when_it_gets_past_the_parser() -> None:
    m, run = manager_with_run()
    bad = reports.parse_report(body(consumer()))
    bad["hist"] = [5] * (len(EDGES) + 40)
    assert m.handle_report(bad) is False
    assert run["workers"] == {} and run["hist_sec"] == [0] * (len(EDGES) + 1) and run["hist_total"] == run["hist_sec"]


def test_the_rate_comes_from_two_reports() -> None:
    m, run = manager_with_run()
    m.handle_report(reports.parse_report(body(consumer())))
    m.handle_report(reports.parse_report(body(consumer(ts=11.0, received=300))))
    assert run["workers"]["queuelab-q-w1"]["rate"] == 200
    assert run["hist_total"][0] == 2
