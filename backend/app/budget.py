import math

from . import config


def fair_cpu(host_cpus: int, containers: int) -> dict:
    """The CPU every worker and sender gets: the same share of the machine for all, in every language.

    The cores of the machine minus a reserve for Kafka and the app, divided by the number of containers (rounded down
    to a twentieth of a core, between a quarter of a core and CPU_MAX_PER_CONTAINER). A worker may compute as many
    messages at once as it has whole cores, at least one.
    """
    usable = max(1.0, host_cpus - config.CPU_RESERVE)
    share = usable / max(1, containers)
    share = math.floor(min(config.CPU_MAX_PER_CONTAINER, max(0.25, share)) * 20) / 20
    return {"cpus": share, "compute_threads": max(1, int(share)), "host_cpus": host_cpus, "containers": containers}


def least_budget_mb() -> int:
    """The smallest budget that still holds one sender and one worker next to the light broker."""
    return config.LEAST_BUDGET_MB


def first_start_budget(available_mb: int) -> int:
    """The budget chosen at the first start: a share of the free memory, rounded down, but not below the minimum."""
    step = config.BUDGET_STEP_MB
    return max(config.FIRST_START_BUDGET_MIN_MB, int(available_mb * config.FIRST_START_BUDGET_SHARE // step * step))


def form_defaults(budget_mb: int, light: bool) -> dict:
    """What a new run form starts with, counted from the chosen budget so the first setup fits into it.

    The usual defaults when they fit. When they do not, fewer workers first (down to one); when even one worker and one
    sender do not fit, both get an equal share of what is left after the broker, but never below the minimum of a
    container. The user can still type anything: the form only warns when a setup does not fit.
    """
    broker = config.BROKER_MEM_LIGHT_MB if light else config.BROKER_MEM_MB
    room = max(0, budget_mb - broker)
    workers, worker_mb, producer_mb = config.DEFAULT_WORKERS, config.DEFAULT_WORKER_MEM_MB, config.DEFAULT_PRODUCER_MEM_MB
    while workers > 1 and producer_mb + workers * worker_mb > room:
        workers -= 1
    if producer_mb + worker_mb > room:
        share = room // 2 // 64 * 64  # a multiple of 64, the step of the memory fields
        worker_mb = min(worker_mb, max(config.MIN_CONTAINER_MEM_MB, share))
        producer_mb = min(producer_mb, max(config.MIN_CONTAINER_MEM_MB, share))
    return {"workers": workers, "worker_mb": worker_mb, "producer_mb": producer_mb}


def limits(budget_mb: int | None = None, light: bool = False) -> dict:
    """Every memory and CPU number the screens show or use as a default, in one place (sent in /api/state).

    `form` is what a new run form starts with for the chosen budget (see form_defaults).
    """
    chosen = budget_mb if budget_mb is not None else config.DEFAULT_BUDGET_MB
    return {
        "form": form_defaults(chosen, light),
        "budget": {
            "min_mb": config.MIN_BUDGET_MB,
            "default_mb": config.DEFAULT_BUDGET_MB,
            "least_mb": least_budget_mb(),
            "recommended_mb": config.RECOMMENDED_BUDGET_MB,
            "presets": [dict(p) for p in config.BUDGET_PRESETS],
        },
        "broker_mb": config.BROKER_MEM_MB,
        "broker_light_mb": config.BROKER_MEM_LIGHT_MB,
        "min_container_mb": config.MIN_CONTAINER_MEM_MB,
        "default_worker_mb": config.DEFAULT_WORKER_MEM_MB,
        "default_producer_mb": config.DEFAULT_PRODUCER_MEM_MB,
        "safety_reserve_mb": config.SAFETY_RESERVE_MB,
        "min_free_for_kafka_mb": config.MIN_FREE_FOR_KAFKA_MB,
        "cpu_reserve": config.CPU_RESERVE,
        "cpu_max_per_container": config.CPU_MAX_PER_CONTAINER,
    }


def used_mb(containers: list[dict]) -> int:
    """Memory counted in the budget: limits of sandbox containers that are (or will be) running."""
    return int(sum(c.get("limit_mb", 0) for c in containers if c.get("state") in ("running", "created", "restarting")))


def snapshot(budget_mb: int, containers: list[dict], mem: dict) -> dict:
    """Budget numbers for the UI: used and remaining against the user budget, plus the memory Docker has."""
    used = used_mb(containers)
    return {
        "budget_mb": budget_mb,
        "used_mb": used,
        "remaining_mb": max(0, budget_mb - used),
        "host_total_mb": mem["total_mb"],
        "host_available_mb": mem["available_mb"],
        "above_available": budget_mb > mem["available_mb"],
    }


def check(budget_mb: int, used_other_mb: int, available_mb: int, requested_mb: int) -> dict:
    """Admission check: the request must fit both into the user's budget and into the memory that is really free."""
    remaining = budget_mb - used_other_mb
    real_free = available_mb - config.SAFETY_RESERVE_MB
    ok = requested_mb <= remaining and requested_mb <= real_free
    reason = None
    if requested_mb > remaining:
        reason = (
            f"Needs {requested_mb} MB but only {max(0, remaining)} MB is left in your budget of {budget_mb} MB. "
            f"Missing {requested_mb - max(0, remaining)} MB."
        )
    elif requested_mb > real_free:
        reason = (
            f"Needs {requested_mb} MB but only {max(0, real_free)} MB is really free "
            f"({available_mb} MB available minus {config.SAFETY_RESERVE_MB} MB safety reserve)."
        )
    return {
        "ok": ok,
        "reason": reason,
        "requested_mb": requested_mb,
        "budget_mb": budget_mb,
        "remaining_mb": max(0, remaining),
        "real_free_mb": max(0, real_free),
    }
