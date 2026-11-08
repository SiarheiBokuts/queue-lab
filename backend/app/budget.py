from . import config


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
