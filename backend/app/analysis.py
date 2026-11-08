"""Pure analysis of a run: totals, the message window, resource usage, load distribution and notes on the data.

Nothing here touches Docker, Kafka or the database: every function takes the state of a run (a dict) and returns data,
so all of it is unit tested without any container. `runs.py` runs the run and calls these functions at the end.
"""

from . import config

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


def merged_size(ranges: list[list[int]]) -> int:
    """How many different numbers a list of [start, end] ranges covers (ranges may overlap or touch)."""
    total, end = 0, -1
    for first, last in sorted(ranges):
        if first > end:
            total += last - first + 1
            end = last
        elif last > end:
            total += last - end
            end = last
    return total


def totals(run) -> dict:
    """Sum of the counters of the producers and all consumers: sent, received, unique, duplicates, errors.

    Every worker reports which message numbers it received, as ranges per stream (sender and partition). The ranges
    of all workers are merged here, so a message counts once however many workers or restarts it went through:
    unique is the number of different messages, duplicates is everything received beyond that.
    """
    ws = list(run["workers"].values())
    received = sum(w["received"] for w in ws)
    streams: dict[str, list[list[int]]] = {}
    for w in ws:
        for stream, ranges in (w.get("ranges") or {}).items():
            streams.setdefault(stream, []).extend(ranges)
    unlisted = sum(w.get("overflow", 0) for w in ws)  # numbers a worker counted but did not list: taken as unique
    unique = min(received, sum(merged_size(r) for r in streams.values()) + unlisted)
    prod = producer_view(run)
    return {
        "sent": prod.get("sent", 0),
        "delivered": prod.get("delivered", 0),
        "received": received,
        "duplicates": received - unique,
        "errors": sum(w["errors"] for w in ws) + prod.get("errors", 0),
        "unique": unique,
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


def notes(run, tot, summary) -> list[dict]:
    """Facts about the data of a run, not judgements: did it finish, and was anything lost or received twice."""
    out: list[dict] = []
    if run["status"] == "cancelled":
        out.append({"level": "warn", "text": "The run was cancelled, so these numbers are partial."})
    if run["status"] == "failed":
        out.append({"level": "bad", "text": f"The run failed: {run['error']}"})
    if tot["sent"]:
        if summary["lost"] == 0 and tot["duplicates"] == 0:
            out.append({"level": "ok", "text": f"All {tot['sent']:,} messages were received exactly once."})
        else:
            out.append({
                "level": "bad",
                "text": f"{summary['lost']:,} messages were lost and {tot['duplicates']:,} were received more than once. "
                "A repeat is expected after a worker restarts: the batch it was handling is read again.",
            })  # fmt: skip
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


def compare(runs: list[dict]) -> dict:
    """Compare finished runs (two or more): every metric side by side and what differs between the setups.

    There is no score and no winner: the numbers depend on the scenario, the message size, the batch size and the
    machine, so the page shows them next to the setup that produced them and leaves the reading to the person.
    """
    metrics = [run_metrics(r) for r in runs]
    names = [run_label(r) for r in runs]
    if len(set(names)) != len(names):
        names = [f"Run {r['id']}" for r in runs]
    rows = [
        compare_row(k, label, unit, low, hint, [m[k] for m in metrics]) for k, label, unit, low, hint in COMPARE_ROWS
    ]
    diffs = setup_differences(runs)
    unexpected = [d for d in diffs if not d["expected"]]
    notes_out: list[dict] = []
    if unexpected:
        fields = ", ".join(d["field"] for d in unexpected)
        notes_out.append({"level": "warn", "text": f"The setups differ in more than the language ({fields}), so the numbers are not directly comparable."})
    else:
        notes_out.append({"level": "ok", "text": "Only the language differed. Everything else, including the Kafka client settings, was identical."})
    if sum((m["lost"] or 0) + (m["duplicates"] or 0) for m in metrics):
        notes_out.append({"level": "bad", "text": "At least one run lost messages or received some twice. Check the run reports."})
    return {
        "names": names,
        "runs": [{"id": r["id"], "config": r["config"]} for r in runs],
        "rows": rows,
        "differences": diffs,
        "fair": not unexpected,
        "notes": notes_out,
    }
