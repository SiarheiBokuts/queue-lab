"""Input validation of a run request."""

import pytest
from pydantic import ValidationError

from app.models import ScenarioSpec, SetupRequest, WorkersSpec


def test_defaults_are_valid() -> None:
    req = SetupRequest()
    assert req.scenario.type == "echo"
    assert req.workers.concurrency == 100


@pytest.mark.parametrize("field", ["partitions", "messages", "message_size"])
def test_numbers_have_limits(field: str) -> None:
    with pytest.raises(ValidationError):
        SetupRequest(**{field: 0})
    with pytest.raises(ValidationError):
        SetupRequest(**{field: 10**9})


def test_queue_names_are_restricted() -> None:
    for bad in ("", "has space", "a/b", "x" * 65):
        with pytest.raises(ValidationError):
            SetupRequest(queue=bad)


def test_only_the_listed_scenarios_exist() -> None:
    for kind in ("echo", "delay", "cpu"):
        assert ScenarioSpec(type=kind).type == kind
    with pytest.raises(ValidationError):
        ScenarioSpec(type="custom")  # running a user's own code is not part of the lab: everything is set in the form


def test_messages_are_ordered_per_user_by_default() -> None:
    assert SetupRequest().workers.ordering == "key"
    assert SetupRequest().users == 1000
    assert WorkersSpec(ordering="none").ordering == "none"
    with pytest.raises(ValidationError):
        WorkersSpec(ordering="partition")


def test_the_number_handled_at_the_same_time_is_limited() -> None:
    assert WorkersSpec(concurrency=1).concurrency == 1
    for bad in (0, 5000):
        with pytest.raises(ValidationError):
            WorkersSpec(concurrency=bad)
