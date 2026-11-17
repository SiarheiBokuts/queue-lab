"""Input validation of a run request."""

import pytest
from pydantic import ValidationError

from app import config
from app.models import ProducerSpec, ScenarioSpec, SettingsUpdate, SetupRequest, WorkersSpec


def test_defaults_are_valid() -> None:
    req = SetupRequest()
    assert req.scenario.type == "echo"
    assert req.workers.concurrency == 100


@pytest.mark.parametrize("field", ["partitions", "messages", "message_size"])
def test_numbers_cannot_be_zero_but_have_no_upper_limit(field: str) -> None:
    """The lab lets you try any big number; advice.py only warns about what to expect."""
    with pytest.raises(ValidationError):
        SetupRequest(**{field: 0})
    assert getattr(SetupRequest(**{field: 10**9}), field) == 10**9


def test_only_what_a_worker_cannot_read_is_refused() -> None:
    """A number a worker cannot parse would crash it, so the type limits stay: 32-bit, JavaScript safe integer, batch."""
    with pytest.raises(ValidationError):
        SetupRequest(partitions=2**31)
    with pytest.raises(ValidationError):
        SetupRequest(messages=2**53)
    with pytest.raises(ValidationError):
        WorkersSpec(concurrency=1_000_001)  # the Python Kafka client reads at most a million messages per call


def test_queue_names_are_restricted() -> None:
    for bad in ("", "has space", "a/b", "x" * 65):
        with pytest.raises(ValidationError):
            SetupRequest(queue=bad)


def test_only_the_listed_scenarios_exist() -> None:
    for kind in ("echo", "delay", "json"):
        assert ScenarioSpec(type=kind).type == kind
    with pytest.raises(ValidationError):
        ScenarioSpec(type="custom")  # running a user's own code is not part of the lab: everything is set in the form


def test_there_are_many_users_by_default() -> None:
    assert SetupRequest().users == 1000
    assert not hasattr(WorkersSpec(), "ordering")  # a user is always handled in order, there is no other mode


def test_the_number_handled_at_the_same_time_has_no_upper_limit() -> None:
    assert WorkersSpec(concurrency=1).concurrency == 1
    assert WorkersSpec(concurrency=5000).concurrency == 5000
    with pytest.raises(ValidationError):
        WorkersSpec(concurrency=0)


def test_memory_defaults_and_minimums_come_from_the_config() -> None:
    assert WorkersSpec().memory_mb == config.DEFAULT_WORKER_MEM_MB
    assert ProducerSpec().memory_mb == config.DEFAULT_PRODUCER_MEM_MB
    with pytest.raises(ValidationError):
        WorkersSpec(memory_mb=config.MIN_CONTAINER_MEM_MB - 1)
    with pytest.raises(ValidationError):
        ProducerSpec(memory_mb=config.MIN_CONTAINER_MEM_MB - 1)
    WorkersSpec(memory_mb=config.MIN_CONTAINER_MEM_MB)  # the least itself is fine


def test_the_budget_cannot_be_below_the_minimum() -> None:
    with pytest.raises(ValidationError):
        SettingsUpdate(budget_mb=config.MIN_BUDGET_MB - 1)
    assert SettingsUpdate(budget_mb=config.MIN_BUDGET_MB).budget_mb == config.MIN_BUDGET_MB
