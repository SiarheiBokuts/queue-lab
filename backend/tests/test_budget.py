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


def test_every_container_gets_the_same_share_of_the_machine() -> None:
    eight = budget.fair_cpu(8, 6)  # 8 cores, 1 kept for Kafka and the app, 7 shared by 6 containers
    assert eight["cpus"] == 1.15
    assert eight["compute_threads"] == 1
    four = budget.fair_cpu(4, 6)  # a smaller machine: the share is smaller, for every language alike
    assert four["cpus"] == 0.5
    assert budget.fair_cpu(16, 2)["cpus"] == config.CPU_MAX_PER_CONTAINER  # a big machine does not give one container everything


def test_the_share_never_vanishes_and_never_depends_on_the_language() -> None:
    assert budget.fair_cpu(1, 32)["cpus"] == 0.25
    assert budget.fair_cpu(8, 6) == budget.fair_cpu(8, 6)
    assert budget.fair_cpu(12, 3)["compute_threads"] == 3  # whole cores a worker may compute on


def test_the_least_budget_holds_the_light_broker_and_two_containers() -> None:
    assert budget.least_budget_mb() == config.BROKER_MEM_LIGHT_MB + 2 * config.LEAST_CONTAINER_MEM_MB == 1024
    assert budget.least_budget_mb() <= config.BUDGET_PRESETS[0]["mb"]  # the first button is not below the least that works


def test_the_first_budget_is_a_share_of_the_free_memory_rounded_down() -> None:
    assert budget.first_start_budget(10_000) == 4864  # half of 10,000 is 5,000; down to a multiple of 256
    assert budget.first_start_budget(1_000) == config.FIRST_START_BUDGET_MIN_MB  # never below the minimum
    assert budget.first_start_budget(0) == config.FIRST_START_BUDGET_MIN_MB


def test_limits_sent_to_the_screens_are_the_numbers_of_the_config() -> None:
    limits = budget.limits()
    assert limits["broker_mb"] == config.BROKER_MEM_MB
    assert limits["broker_light_mb"] == config.BROKER_MEM_LIGHT_MB
    assert limits["default_worker_mb"] == config.DEFAULT_WORKER_MEM_MB
    assert limits["default_producer_mb"] == config.DEFAULT_PRODUCER_MEM_MB
    assert limits["min_container_mb"] == config.MIN_CONTAINER_MEM_MB
    assert limits["cpu_reserve"] == config.CPU_RESERVE
    assert limits["cpu_max_per_container"] == config.CPU_MAX_PER_CONTAINER
    assert limits["budget"]["least_mb"] == budget.least_budget_mb()
    assert [p["mb"] for p in limits["budget"]["presets"]] == sorted(p["mb"] for p in config.BUDGET_PRESETS)


def test_presets_are_valid_budgets_and_the_advised_one_is_a_preset() -> None:
    mbs = [p["mb"] for p in config.BUDGET_PRESETS]
    assert all(mb >= config.MIN_BUDGET_MB for mb in mbs)
    assert config.RECOMMENDED_BUDGET_MB in mbs


def test_nothing_below_the_smallest_working_size_can_be_chosen() -> None:
    assert budget.least_budget_mb() == config.MIN_BUDGET_MB
    assert budget.least_budget_mb() == config.FIRST_START_BUDGET_MIN_MB
    assert all(p["mb"] >= config.MIN_BUDGET_MB for p in config.BUDGET_PRESETS)


def test_a_big_budget_keeps_the_usual_form_defaults() -> None:
    form = budget.form_defaults(4096, light=False)
    assert form == {
        "workers": config.DEFAULT_WORKERS,
        "worker_mb": config.DEFAULT_WORKER_MEM_MB,
        "producer_mb": config.DEFAULT_PRODUCER_MEM_MB,
    }


def test_a_smaller_budget_gets_fewer_workers_that_still_fit() -> None:
    form = budget.form_defaults(2048, light=False)  # 2048 - 1024 for Kafka = 1024 for the sender and the workers
    assert form["workers"] == 1
    assert form["producer_mb"] + form["workers"] * form["worker_mb"] <= 2048 - config.BROKER_MEM_MB
    light = budget.form_defaults(2048, light=True)  # the light broker leaves more room
    assert light["workers"] > form["workers"]


def test_the_least_budget_gets_one_worker_and_one_sender_that_fit_with_the_light_broker() -> None:
    form = budget.form_defaults(config.LEAST_BUDGET_MB, light=True)
    assert form["workers"] == 1
    assert form["producer_mb"] + form["worker_mb"] <= config.LEAST_BUDGET_MB - config.BROKER_MEM_LIGHT_MB
    assert min(form["producer_mb"], form["worker_mb"]) >= config.MIN_CONTAINER_MEM_MB


def test_a_budget_the_broker_alone_fills_still_gives_a_valid_form() -> None:
    form = budget.form_defaults(config.LEAST_BUDGET_MB, light=False)  # nothing fits: the form warns, it does not break
    assert form["workers"] == 1
    assert form["worker_mb"] >= config.MIN_CONTAINER_MEM_MB
    assert form["producer_mb"] >= config.MIN_CONTAINER_MEM_MB


def test_limits_carry_the_form_defaults_of_the_chosen_budget() -> None:
    assert budget.limits(2048, True)["form"] == budget.form_defaults(2048, True)
    assert budget.limits()["form"] == budget.form_defaults(config.DEFAULT_BUDGET_MB, False)
