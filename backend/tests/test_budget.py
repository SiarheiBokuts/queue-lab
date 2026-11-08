from app import budget, config


def containers(*limits_and_states):
    return [{"limit_mb": mb, "state": st} for mb, st in limits_and_states]


def test_used_counts_only_containers_that_hold_memory() -> None:
    cs = containers((1024, "running"), (400, "created"), (400, "exited"), (256, "restarting"))
    assert budget.used_mb(cs) == 1024 + 400 + 256


def test_five_workers_of_400_fit_into_2048() -> None:
    result = budget.check(budget_mb=2048, used_other_mb=0, available_mb=8000, requested_mb=5 * 400)
    assert result["ok"] is True
    assert result["reason"] is None
    assert result["remaining_mb"] == 2048


def test_six_workers_of_400_do_not_fit_into_2048() -> None:
    result = budget.check(2048, 0, 8000, 6 * 400)
    assert result["ok"] is False
    assert "2400" in result["reason"]
    assert "Missing 352 MB" in result["reason"]


def test_what_is_already_used_counts_against_the_budget() -> None:
    result = budget.check(2048, used_other_mb=1024, available_mb=8000, requested_mb=1500)
    assert result["ok"] is False
    assert result["remaining_mb"] == 1024


def test_the_budget_may_allow_what_the_machine_cannot() -> None:
    free = 1000
    result = budget.check(budget_mb=10_000, used_other_mb=0, available_mb=free, requested_mb=900)
    assert result["ok"] is False  # 900 > 1000 - safety reserve
    assert result["real_free_mb"] == free - config.SAFETY_RESERVE_MB
    assert "really free" in result["reason"]


def test_a_request_exactly_equal_to_the_limit_is_accepted() -> None:
    assert budget.check(1000, 0, 1000 + config.SAFETY_RESERVE_MB, 1000)["ok"] is True


def test_snapshot_flags_a_budget_above_the_available_memory() -> None:
    snap = budget.snapshot(4096, containers((1024, "running")), {"total_mb": 8000, "available_mb": 3000})
    assert snap["used_mb"] == 1024
    assert snap["remaining_mb"] == 3072
    assert snap["above_available"] is True
