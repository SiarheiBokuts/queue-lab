"""Pure analysis of a run: totals, the message window, resource usage, load distribution, verdict and highlights.

Nothing here touches Docker, Kafka or the database: every function takes the state of a run (a dict) and returns data,
so all of it is unit tested without any container. `runs.py` runs the run and calls these functions at the end.
"""

from . import config
from .models import SetupRequest

EDGES = config.HIST_EDGES


def percentile(hist: list[int], q: float):
    """Percentile (0..1) estimated from histogram counts; returns the upper edge of the bucket that contains it."""
    total = sum(hist)
    if not total:
        return None
    target, acc = total * q, 0
    for i, n in enumerate(hist):
        acc += n
        if acc >= target:
            return EDGES[i] if i < len(EDGES) else EDGES[-1]
    return EDGES[-1]


def producer_view(run) -> dict:
    """All producers of a run as one: counters summed, started when the first started, finished when the last did.

    `done` is true only when every expected producer has reported that it is done, and `finished` is set only then.
    """
    reports = list(run["producers"].values())
    if not reports:
        return {}
    done = len(reports) == run["req"].producer.count and all(p.get("done") for p in reports)
    started = [p["started"] for p in reports if p.get("started")]
    finished = [p["finished"] for p in reports if p.get("finished")]
    return {
        "sent": sum(p.get("sent", 0) for p in reports),
        "delivered": sum(p.get("delivered", 0) for p in reports),
        "errors": sum(p.get("errors", 0) for p in reports),
        "done": done,
        "started": min(started) if started else None,
        "finished": max(finished) if done and finished else None,
    }


def totals(run) -> dict:
    """Sum of the counters of the producers and all consumers: sent, received, unique, duplicates, errors."""
    ws = run["workers"].values()
    received = sum(w["received"] for w in ws)
    dups = sum(w["duplicates"] for w in ws)
    prod = producer_view(run)
    return {
        "sent": prod.get("sent", 0),
        "delivered": prod.get("delivered", 0),
        "received": received,
        "duplicates": dups,
        "gaps": sum(w["gaps"] for w in ws),
        "errors": sum(w["errors"] for w in ws) + prod.get("errors", 0),
        "unique": received - dups,
        "producer_done": bool(prod.get("done")),
    }


def window(run) -> dict:
    """Timeline of the message window, in seconds from the moment the producer was started."""
    t0, prod = run.get("t0"), producer_view(run)
    if not t0:
        return {}
    s, e = prod.get("started"), prod.get("finished")
    ws = list(run["workers"].values())
    firsts = [w["first_at"] for w in ws if w.get("first_at")]
    lasts = [w["last_at"] for w in ws if w.get("last_at")]
    out: dict = {}
    if s:
        out["send_start"] = round(s - t0, 2)
    if e:
        out["send_end"] = round(e - t0, 2)
    if firsts:
        out["first_processed"] = round(min(firsts) - t0, 2)
    if lasts:
        out["last_processed"] = round(max(lasts) - t0, 2)
    if s and e:
        out["send_s"] = round(max(0.001, e - s), 2)
    if s and lasts:
        out["total_s"] = round(max(0.001, max(lasts) - s), 2)
    if e and lasts:
        out["drain_s"] = round(max(0.0, max(lasts) - e), 2)
    return out


def resource_stats(run, win: dict, tot: dict) -> dict:
    """Per-container resource usage restricted to the message window (send start to last processed)."""
    lo = (win.get("send_start") if win.get("send_start") is not None else 0) - 0.5
    hi = (win.get("last_processed") or win.get("send_end") or 1e9) + 0.5
    per: dict[str, dict] = {}
    for cs in run["csamples"]:
        if not (lo <= cs["t"] <= hi):
            continue
        dt = cs.get("dt", 1.0)
        for c in cs["containers"]:
            d = per.setdefault(
                c["name"],
                {
                    "role": c["role"],
                    "lang": c["lang"],
                    "limit_mb": c["limit_mb"],
                    "samples": 0,
                    "secs": 0.0,
                    "cpu_peak": 0.0,
                    "cpu_peak_t": 0.0,
                    "cpu_s": 0.0,
                    "mem_peak": 0.0,
                    "mem_sum": 0.0,
                    "rx_kb": 0.0,
                    "tx_kb": 0.0,
                    "oom": False,
                },
            )
            d["samples"] += 1
            d["secs"] += dt
            if c["cpu"] >= d["cpu_peak"]:
                d["cpu_peak_t"] = cs["t"]
            d["cpu_peak"] = max(d["cpu_peak"], c["cpu"])
            d["cpu_s"] += c["cpu"] / 100 * dt
            d["mem_peak"] = max(d["mem_peak"], c["mem_mb"])
            d["mem_sum"] += c["mem_mb"] * dt
            d["rx_kb"] += c["rx_kbs"] * dt
            d["tx_kb"] += c["tx_kbs"] * dt
            d["oom"] = d["oom"] or c.get("oom", False)
    out = {}
    for name, d in per.items():
        secs = max(d["secs"], 0.001)
        handled = (
            run["workers"].get(name, {}).get("received", 0)
            if d["role"] == "consumer"
            else (tot["sent"] if d["role"] == "producer" else 0)
        )
        out[name] = {
            "role": d["role"],
            "lang": d["lang"],
            "limit_mb": d["limit_mb"],
            "samples": d["samples"],
            "cpu_avg": round(d["cpu_s"] / secs * 100, 1),
            "cpu_peak": round(d["cpu_peak"], 1),
            "cpu_seconds": round(d["cpu_s"], 2),
            "cpu_peak_at": d["cpu_peak_t"],
            "mem_avg_mb": round(d["mem_sum"] / secs, 1),
            "mem_peak_mb": round(d["mem_peak"], 1),
            "mem_headroom_mb": round(d["limit_mb"] - d["mem_peak"], 1) if d["limit_mb"] else None,
            "net_in_mb": round(d["rx_kb"] / 1024, 2),
            "net_out_mb": round(d["tx_kb"] / 1024, 2),
            "messages": handled,
            "cpu_ms_per_1k": round(d["cpu_s"] * 1000 / (handled / 1000), 1) if handled else None,
            "oom": d["oom"],
        }
    return out


def distribution(run, tot: dict) -> list[dict]:
    total = max(1, tot["received"])
    out = []
    for name, w in sorted(run["workers"].items()):
        span = (w["last_at"] - w["first_at"]) if w.get("first_at") and w.get("last_at") else 0
        out.append(
            {
                "name": name,
                "lang": w.get("lang"),
                "processed": w["received"],
                "share": round(w["received"] / total * 100, 1),
                "partitions": w.get("assigned", []),
                "avg_rate": round(w["received"] / span) if span > 0.05 else None,
                "duplicates": w["duplicates"],
                "errors": w["errors"],
            }
        )
    return out


def network(run, tot, win, resources: dict) -> dict:
    """What the data needed on the network and what the containers really moved, in MiB (1 MiB = 1,048,576 bytes).

    `needed` is messages x size: the raw payload. The producers' real traffic is smaller with compression, and every
    number also includes Kafka protocol overhead and the small progress reports.
    """
    req = run["req"]
    mib = 1024 * 1024

    def total(role: str, key: str) -> float:
        return round(sum(r[key] for r in resources.values() if r["role"] == role), 2)

    needed = round(tot["sent"] * req.message_size / mib, 2)
    producers_sent = total(config.ROLE_PRODUCER, "net_out_mb")
    send_s = win.get("send_s")
    return {
        "message_size": req.message_size,
        "messages": tot["sent"],
        "compression": req.compression,
        "payload": req.payload,
        "needed_mb": needed,
        "producers_sent_mb": producers_sent,
        "broker_received_mb": total(config.ROLE_BROKER, "net_in_mb"),
        "broker_sent_mb": total(config.ROLE_BROKER, "net_out_mb"),
        "workers_received_mb": total(config.ROLE_CONSUMER, "net_in_mb"),
        "compression_ratio": round(needed / producers_sent, 2) if producers_sent > 0.05 else None,
        "needed_mbit_s": round(needed * 8 / send_s, 1) if send_s else None,
        "actual_mbit_s": round(producers_sent * 8 / send_s, 1) if send_s else None,
    }


def insights(run, tot, win, summary) -> list[dict]:
    req: SetupRequest = run["req"]
    res = summary["resources"]
    out: list[dict] = []

    def add(level, text):
        out.append({"level": level, "text": text})

    if run["status"] == "cancelled":
        add("warn", "The run was cancelled, so these numbers are partial.")
    if run["status"] == "failed":
        add("bad", f"The run failed: {run['error']}")
    if tot["sent"]:
        if summary["lost"] == 0 and tot["duplicates"] == 0:
            add("ok", f"All {tot['sent']:,} messages were delivered exactly once: nothing lost, nothing duplicated.")
        else:
            add("bad", f"{summary['lost']:,} messages were lost and {tot['duplicates']:,} were duplicated.")
    if win.get("send_s") and win.get("total_s"):
        add(
            "info",
            f"Sending took {win['send_s']} s ({round(tot['sent'] / win['send_s']):,} msg/s). "
            f"Everything was processed {win['total_s']} s after the first message was sent.",
        )
    peak_lag = summary["peak_lag"]
    drain = win.get("drain_s")
    if drain is not None and peak_lag:
        if drain > 1.5:
            add(
                "warn",
                f"Consumers were slower than the producer: lag peaked at {peak_lag:,} and it took "
                f"{drain} s after sending ended to drain it.",
            )
        else:
            add(
                "ok",
                f"Consumers kept up with the producer: lag peaked at {peak_lag:,} and drained within {drain} s.",
            )
    workers = [r for r in res.values() if r["role"] == "consumer"]
    if req.scenario.type == "delay" and req.scenario.param > 0:
        active = min(req.workers.count, req.partitions)
        limit = int(active * 1000 / req.scenario.param)
        add(
            "info",
            f"With a {req.scenario.param} ms delay per message, {active} active worker(s) can handle at most "
            f"about {limit:,} msg/s. That ceiling, not Kafka, limits this run.",
        )
    if req.workers.count > req.partitions:
        add(
            "warn",
            f"{req.workers.count - req.partitions} worker(s) were idle: there were more workers than partitions. "
            "Add partitions or use fewer workers.",
        )
    busy = [r for r in workers if r["cpu_avg"] >= 60]
    if busy:
        add(
            "warn",
            f"Workers were CPU-bound: they averaged {max(r['cpu_avg'] for r in busy):.0f}% of one core while "
            "processing. More workers (up to the partition count) or a lighter scenario would help.",
        )
    elif workers and summary["peak_recv_rate"]:
        avg_cpu = sum(r["cpu_avg"] for r in workers) / len(workers)
        if avg_cpu < 35:
            add(
                "info",
                f"Workers were mostly waiting, not computing: {avg_cpu:.0f}% of one core on average, "
                "so the limit was something else, such as the delay, Kafka or the producer.",
            )
    shares = [d["share"] for d in summary["distribution"] if d["partitions"]]
    if len(shares) > 1 and min(shares) > 0 and max(shares) / min(shares) > 1.4:
        add(
            "warn",
            f"The load was uneven: one worker handled {max(shares)}% of the messages and another {min(shares)}%. "
            "This happens when partitions do not divide evenly between workers.",
        )
    for name, r in res.items():
        if r["limit_mb"] and r["mem_peak_mb"] / r["limit_mb"] > 0.9:
            add(
                "warn",
                f"{config.short_name(name)} used {r['mem_peak_mb']:.0f} of {r['limit_mb']} MB "
                "(over 90%), so it is close to being killed for memory.",
            )
    if workers:
        peak = max(r["mem_peak_mb"] for r in workers)
        lim = workers[0]["limit_mb"]
        if lim and peak < lim * 0.4:
            add(
                "info",
                f"Workers needed at most {peak:.0f} MB of their {lim} MB limit; "
                f"{max(128, int(peak * 1.6 // 32 * 32 + 32))} MB would be enough for this load.",
            )
    if summary["p99"] and summary["p99"] >= 1000:
        add(
            "info",
            f"p99 latency was {summary['p99']} ms. Most of it is time spent waiting in the queue behind the backlog, "
            "not processing time.",
        )
    req = run["req"]
    if win.get("total_s") and win["total_s"] < 2:
        add(
            "info",
            f"This run was very short ({win['total_s']} s), so the CPU, memory and network numbers are rough. "
            "Send more messages or set a send speed for steadier numbers.",
        )
    if req.rate and win.get("send_s") and tot["sent"]:
        achieved = round(tot["sent"] / win["send_s"])
        if achieved < req.rate * 0.9:
            add(
                "warn",
                f"The producers could not reach the requested {req.rate:,} msg/s: they sent {achieved:,} msg/s. "
                "Add producers or give them more memory.",
            )
        else:
            add("ok", f"The producers held the requested rate: {achieved:,} msg/s (target {req.rate:,}).")
    net = summary.get("network")
    if net and net["needed_mb"] and net["producers_sent_mb"] > 0.05:
        ratio = net["compression_ratio"]
        how = (
            f"{req.compression} made it {ratio}x smaller on the wire"
            if req.compression != "none" and ratio and ratio > 1.15
            else "about the raw size plus protocol overhead"
        )
        add(
            "info",
            f"Network: the messages carry {net['needed_mb']} MiB of data ({net['messages']:,} x {net['message_size']} B). "
            f"The producers sent {net['producers_sent_mb']} MiB over the network ({how}).",
        )
    return out


def highlights(run, tot, win, summary) -> list[dict]:
    """The most interesting facts of a finished run, shown as cards.

    Each card names an `anchor` (a section of the report page) and optionally a `container`, so the UI can link
    straight to the statistics behind the number.
    """
    out: list[dict] = []

    def add(kind: str, title: str, text: str, anchor: str, container: str | None = None) -> None:
        out.append({"kind": kind, "title": title, "text": text, "anchor": anchor, "container": container})

    samples = run["samples"]
    best = max(samples, key=lambda s: s["recv_rate"], default=None)
    if best and best["recv_rate"]:
        add(
            "speed",
            f"{best['recv_rate']:,} msg/s",
            f"Fastest processing, reached {best['t']} s after the producer started. "
            f"The average over the active time was {summary['avg_recv_rate']:,} msg/s.",
            "throughput",
        )
    if win.get("send_s"):
        add(
            "send",
            f"{round(tot['sent'] / win['send_s']):,} msg/s",
            f"Producer speed: {tot['sent']:,} messages sent in {win['send_s']} s.",
            "throughput",
        )
    if summary.get("latency_max_ms"):
        add(
            "latency",
            f"{summary['latency_max_ms']:,.0f} ms",
            f"The slowest message waited this long between being sent and being processed. "
            f"Typical (p50) was {summary['p50']} ms and p99 was {summary['p99']} ms.",
            "latency",
        )
    res = summary["resources"]
    if res:
        name, r = max(res.items(), key=lambda kv: kv[1]["cpu_peak"])
        add(
            "cpu",
            f"{r['cpu_peak']:.0f}% CPU",
            f"{config.short_name(name)} ({r['role']}) had the highest CPU, at {r['cpu_peak_at']} s. "
            f"It averaged {r['cpu_avg']:.0f}% of one core while the messages were moving.",
            "resources",
            name,
        )
        limited = {n: v for n, v in res.items() if v["limit_mb"]}
        if limited:
            name, r = max(limited.items(), key=lambda kv: kv[1]["mem_peak_mb"] / kv[1]["limit_mb"])
            add(
                "memory",
                f"{r['mem_peak_mb']:.0f} of {r['limit_mb']} MB",
                f"{config.short_name(name)} came closest to its memory limit "
                f"({r['mem_peak_mb'] / r['limit_mb'] * 100:.0f}%).",
                "resources",
                name,
            )
    if summary["peak_lag"]:
        add(
            "lag",
            f"{summary['peak_lag']:,} waiting",
            f"Lag peaked {summary['peak_lag_at']} s into the run and was drained "
            f"{win.get('drain_s', 0)} s after sending ended.",
            "lag",
        )
    dist = [d for d in summary["distribution"] if d["partitions"]]
    if len(dist) > 1:
        top = max(dist, key=lambda d: d["processed"])
        add(
            "workers",
            f"{top['share']}% by one worker",
            f"{config.short_name(top['name'])} processed the most messages. See how the work was split.",
            "distribution",
            top["name"],
        )
    net = summary.get("network")
    if net and net["producers_sent_mb"] > 0.05:
        add(
            "network",
            f"{net['producers_sent_mb']} MiB sent",
            f"The data itself is {net['needed_mb']} MiB. "
            + (
                f"{net['actual_mbit_s']} Mbit/s went over the wire while sending (the data alone would need {net['needed_mbit_s']})."
                if net["actual_mbit_s"]
                else ""
            ),
            "network",
        )
    return out


# ----------------------------------------------------------------------------------- comparing runs
# (key, label, unit, lower_is_better, hint). Every run has all of these numbers, so any runs can be compared.
COMPARE_ROWS = [
    ("e2e_rate", "Processing speed, end to end", "msg/s", False, "Messages processed per second from the first send to the last message processed."),
    ("peak_rate", "Peak processing speed", "msg/s", False, "The fastest second of processing."),
    ("send_rate", "Producer speed", "msg/s", False, "How fast the producers sent the messages."),
    ("total_s", "Total time", "s", True, "From the first message sent to the last one processed."),
    ("drain_s", "Tail after sending ended", "s", True, "How long the consumers kept working after the producers were done."),
    ("p50", "Latency p50", "ms", True, "Half of the messages were processed faster than this."),
    ("p99", "Latency p99", "ms", True, "99% of the messages were processed faster than this."),
    ("max_latency", "Slowest message", "ms", True, "The longest a single message waited."),
    ("peak_lag", "Peak lag", "messages", True, "The largest backlog that built up in Kafka."),
    ("consumer_cpu_avg", "Worker CPU, average", "% of a core", True, "Average CPU of the workers while messages were moving."),
    ("consumer_cpu_peak", "Worker CPU, peak", "% of a core", True, "Highest CPU of any worker."),
    ("consumer_cpu_ms_per_1k", "CPU per 1,000 messages (workers)", "ms", True, "CPU time a worker needs to process 1,000 messages: the efficiency of the code and the client."),
    ("consumer_mem_peak", "Worker memory, peak", "MB", True, "Highest memory use of any worker."),
    ("producer_cpu_ms_per_1k", "CPU per 1,000 messages (producer)", "ms", True, "CPU time a producer needs to send 1,000 messages."),
    ("producer_mem_peak", "Producer memory, peak", "MB", True, "Highest memory use of a producer."),
    ("network_mb", "Network into Kafka", "MiB", True, "Data Kafka received from the senders, after compression. Measured at the broker, which runs the whole time, so it is reliable even when a sender finishes in a second."),
    ("lost", "Lost messages", "messages", True, "Sent but never processed."),
    ("duplicates", "Duplicates", "messages", True, "Processed more than once."),
]  # fmt: skip

# Setup fields that must be equal for a fair comparison, with a readable name. Language fields are the point of the
# comparison, so a difference there is expected.
COMPARED_FIELDS = [
    (("partitions",), "partitions", False),
    (("workers", "lang"), "worker language", True),
    (("workers", "count"), "worker count", False),
    (("workers", "memory_mb"), "worker memory", False),
    (("producer", "lang"), "producer language", True),
    (("producer", "memory_mb"), "producer memory", False),
    (("producer", "count"), "producer count", False),
    (("rate",), "target rate", False),
    (("compression",), "compression", False),
    (("payload",), "payload type", False),
    (("messages",), "message count", False),
    (("message_size",), "message size", False),
    (("scenario", "type"), "scenario", False),
    (("scenario", "param"), "scenario parameter", False),
]  # fmt: skip

TIE_BAND = 0.05  # values within 5% of each other count as equal
LATENCY_KEYS = ("p50", "p99", "max_latency")  # reported by a histogram that stops at config.HIST_EDGES[-1]


def _dig(config_dict: dict, path: tuple[str, ...]):
    for key in path:
        config_dict = config_dict.get(key) if isinstance(config_dict, dict) else None
    return config_dict


def run_metrics(run: dict) -> dict:
    """The standard set of numbers of a finished run (see COMPARE_ROWS). Missing values are None."""
    s = run["summary"]
    win = s.get("window") or {}
    res = (s.get("resources") or {}).values()
    consumers = [r for r in res if r["role"] == config.ROLE_CONSUMER]
    producers = [r for r in res if r["role"] == config.ROLE_PRODUCER]

    def mean(values):
        values = [v for v in values if v is not None]
        return round(sum(values) / len(values), 1) if values else None

    def top(values):
        values = [v for v in values if v is not None]
        return max(values) if values else None

    unique = s["received"] - s["duplicates"]
    return {
        "e2e_rate": round(unique / win["total_s"]) if win.get("total_s") else None,
        "peak_rate": s.get("peak_recv_rate"),
        "send_rate": round(s["sent"] / win["send_s"]) if win.get("send_s") else None,
        "total_s": win.get("total_s"),
        "drain_s": win.get("drain_s"),
        "p50": s.get("p50"),
        "p99": s.get("p99"),
        "max_latency": round(s["latency_max_ms"]) if s.get("latency_max_ms") else None,
        "peak_lag": s.get("peak_lag"),
        "consumer_cpu_avg": mean(r["cpu_avg"] for r in consumers),
        "consumer_cpu_peak": top(r["cpu_peak"] for r in consumers),
        # A zero here means "no measurement": a container that lived for less than one sample has no CPU delta yet.
        "consumer_cpu_ms_per_1k": mean(r.get("cpu_ms_per_1k") or None for r in consumers),
        "consumer_mem_peak": top(r["mem_peak_mb"] for r in consumers),
        "producer_cpu_ms_per_1k": mean(r.get("cpu_ms_per_1k") or None for r in producers),
        "producer_mem_peak": top(r["mem_peak_mb"] for r in producers),
        "network_mb": ((s.get("network") or {}).get("broker_received_mb") or (s.get("network") or {}).get("producers_sent_mb")) or None,
        "lost": s.get("lost"),
        "duplicates": s.get("duplicates"),
    }


def run_label(run: dict) -> str:
    """Short name of a run for the comparison, by its worker language."""
    return config.LANG_NAMES.get(_dig(run["config"], ("workers", "lang")), f"Run {run['id']}")


def setup_differences(runs: list[dict]) -> list[dict]:
    """The setup fields that are not the same in all runs. `expected` marks the language fields, the point of it."""
    out = []
    for path, name, expected in COMPARED_FIELDS:
        values = [_dig(r["config"], path) for r in runs]
        if any(v != values[0] for v in values):
            out.append({"field": name, "values": values, "expected": expected})
    return out


def compare_row(key: str, label: str, unit: str, lower_is_better: bool, hint: str, values: list) -> dict:
    """One metric across all runs: the best run (None if unknown or all within 5%) and how much worse each is.

    `winners` lists every run within the tie band of the best, so equal values are all marked best instead of the first
    one "winning" and the others being "1x worse". `capped` marks latencies at the measurement limit: their ratio is a
    lower bound ("at least 50x worse").
    """
    best, ratios, winners = None, [None] * len(values), []
    cap = config.HIST_EDGES[-1] if key in LATENCY_KEYS else None
    capped = [cap is not None and v is not None and v >= cap for v in values]
    known = [(i, v) for i, v in enumerate(values) if v is not None]
    if len(known) >= 2:
        pick = min if lower_is_better else max
        best_i, best_v = pick(known, key=lambda iv: iv[1])
        worst_v = (max if lower_is_better else min)(v for _, v in known)
        spread = abs(worst_v - best_v) / max(abs(worst_v), abs(best_v), 1e-9)
        if spread > TIE_BAND:
            best = best_i
            for i, v in known:
                close = abs(v - best_v) <= TIE_BAND * max(abs(v), abs(best_v), 1e-9)
                if close:
                    winners.append(i)
                if best_v > 0 and v > 0:
                    ratios[i] = round(v / best_v if lower_is_better else best_v / v, 2)
    return {"key": key, "label": label, "unit": unit, "values": values, "best": best, "winners": winners, "ratios": ratios, "capped": capped, "hint": hint}


# The winner is decided by categories, not by counting rows (many rows say the same thing, like speed and total time).
# Each category averages how close a run is to the best one on its metrics; the weights say what matters most.
SCORE_CATEGORIES = [
    ("speed", "Speed", 0.4, ("e2e_rate", "total_s", "send_rate")),
    ("latency", "Latency", 0.3, ("p50", "p99", "max_latency")),
    ("efficiency", "Efficiency", 0.3, ("consumer_cpu_ms_per_1k", "consumer_cpu_avg", "consumer_mem_peak", "producer_cpu_ms_per_1k", "producer_mem_peak")),
]  # fmt: skip
CLEAR_LEAD = 5.0  # points of 100 the leader needs over the second place to be called the winner


def _closeness(rows: list[dict], key: str, i: int) -> float | None:
    """0..1: how close run i is to the best value of a metric (1 = best or within the tie band, None = unknown)."""
    row = next(r for r in rows if r["key"] == key)
    lower = next(c[3] for c in COMPARE_ROWS if c[0] == key)
    known = [v for v in row["values"] if v is not None]
    v = row["values"][i]
    if v is None or not known:
        return None
    best = min(known) if lower else max(known)
    if lower:
        return 1.0 if v <= best * (1 + TIE_BAND) else best / v
    return 1.0 if v >= best * (1 - TIE_BAND) else v / best


def scores(rows: list[dict], count: int) -> dict:
    """Score every run from 0 to 100 per category and overall, and name a winner when the lead is clear.

    A run that lost or duplicated messages is disqualified: speed means nothing if messages are missing.
    """
    categories = []
    for key, label, weight, metrics in SCORE_CATEGORIES:
        values = []
        for i in range(count):
            parts = [c for c in (_closeness(rows, m, i) for m in metrics) if c is not None]
            values.append(round(sum(parts) / len(parts) * 100) if parts else None)
        categories.append({"key": key, "label": label, "weight": weight, "values": values})
    overall: list[int | None] = []
    for i in range(count):
        got = [(c["weight"], c["values"][i]) for c in categories if c["values"][i] is not None]
        total = sum(w for w, _ in got)
        overall.append(round(sum(w * v for w, v in got) / total) if total else None)
    problems = {r["key"]: r["values"] for r in rows if r["key"] in ("lost", "duplicates")}
    disqualified = [bool((problems["lost"][i] or 0) + (problems["duplicates"][i] or 0)) for i in range(count)]
    ranked = sorted((i for i in range(count) if overall[i] is not None and not disqualified[i]), key=lambda i: -(overall[i] or 0))
    winner = None
    if ranked and (len(ranked) == 1 or (overall[ranked[0]] or 0) - (overall[ranked[1]] or 0) >= CLEAR_LEAD):
        winner = ranked[0]
    return {"categories": categories, "overall": overall, "disqualified": disqualified, "winner": winner}


def compare(runs: list[dict]) -> dict:
    """Compare finished runs (two or more): standard metrics side by side, setup differences and a verdict."""
    metrics = [run_metrics(r) for r in runs]
    names = [run_label(r) for r in runs]
    if len(set(names)) != len(names):
        names = [f"Run {r['id']}" for r in runs]
    rows = [
        compare_row(k, label, unit, low, hint, [m[k] for m in metrics]) for k, label, unit, low, hint in COMPARE_ROWS
    ]
    diffs = setup_differences(runs)
    unexpected = [d for d in diffs if not d["expected"]]
    by_key = {r["key"]: r for r in rows}
    verdict: list[dict] = []

    def add(level: str, text: str) -> None:
        verdict.append({"level": level, "text": text})

    if unexpected:
        fields = ", ".join(d["field"] for d in unexpected)
        add(
            "warn",
            f"The setups differ in more than the language ({fields}), so the numbers are not directly comparable.",
        )
    else:
        add(
            "ok",
            "Only the language differed. Everything else, including the Kafka client settings, was identical, so the numbers are directly comparable.",
        )

    def leader(key: str, template: str) -> None:
        row = by_key[key]
        if row["best"] is None:
            return
        best = row["best"]
        worst = max((i for i, r in enumerate(row["ratios"]) if r is not None), key=lambda i: row["ratios"][i])
        add(
            "info",
            template.format(
                w=names[best],
                l=names[worst],
                x=row["ratios"][worst] or "much",
                wv=row["values"][best],
                lv=row["values"][worst],
                u=row["unit"],
            ),
        )

    leader("e2e_rate", "{w} processed messages fastest end to end ({wv:,} {u}), {x}x the speed of {l} ({lv:,} {u}).")
    leader("send_rate", "{w}'s producer sent fastest ({wv:,} {u}), {x}x the speed of {l} ({lv:,} {u}).")
    leader(
        "consumer_cpu_ms_per_1k",
        "{w} needed the least CPU per 1,000 messages in the workers ({wv} {u}), {x}x less than {l} ({lv} {u}).",
    )
    leader("p99", "{w} had the lowest p99 latency ({wv} {u}) against {lv} {u} for {l}.")
    leader("consumer_mem_peak", "{w}'s workers used the least memory at peak ({wv} {u}) against {lv} {u} for {l}.")
    if not any(v["level"] == "info" for v in verdict):
        add("info", "There is no clear winner: the results are within 5% of each other.")
    problems = sum((m["lost"] or 0) + (m["duplicates"] or 0) for m in metrics)
    if problems == 0:
        add("ok", "Every run delivered every message exactly once.")
    else:
        add("bad", "At least one run lost or duplicated messages. Check the run reports.")
    wins = [sum(1 for r in rows if i in r["winners"]) for i in range(len(runs))]
    return {
        "names": names,
        "runs": [{"id": r["id"], "config": r["config"]} for r in runs],
        "rows": rows,
        "differences": diffs,
        "fair": not unexpected,
        "verdict": verdict,
        "wins": wins,
        "scores": scores(rows, len(runs)),
    }
