"""Run lifecycle: set up a queue, workers and producer, drive the run, aggregate reports, store results."""

import asyncio
import random
import time
import uuid

from fastapi import HTTPException

from . import analysis, budget, config, contract
from .docker_service import DockerService
from .hub import Hub
from .kafka_service import KafkaService, OffsetReader
from .models import ComparisonRequest, SetupRequest
from .store import Store

ACTIVE = ("pending", "running")
STEPS = [
    ("images", "Prepare worker images"),
    ("broker", "Start Kafka broker"),
    ("topic", "Create queue"),
    ("workers", "Start workers"),
    ("join", "Wait for partition assignment"),
    ("producer", "Start producer"),
    ("run", "Run and process messages"),
]
EDGES = config.HIST_EDGES


def comparison_order(langs: list[str], random_order: bool, rng: random.Random | None = None) -> list[str]:
    """The order the languages of a comparison run in: as given, or drawn at random (the default)."""
    order = list(langs)
    if random_order:
        (rng or random).shuffle(order)
    return order


class RunManager:
    """Owns the single active run: plans it, starts the containers, aggregates worker reports, stores the result."""

    def __init__(self, docker: DockerService, kafka: KafkaService, store: Store, hub: Hub):
        self.docker, self.kafka, self.store, self.hub = docker, kafka, store, hub
        self.current: dict | None = None
        self.task: asyncio.Task | None = None
        self.containers: list[dict] = []
        self._last_tick = 0.0
        self.comparison: dict | None = None  # the comparison in progress or the last finished one
        self.cmp_task: asyncio.Task | None = None

    # ---------- settings helpers ----------
    def budget_mb(self) -> int:
        return int(self.store.get_setting("budget_mb", 2048))

    def light(self) -> bool:
        return bool(self.store.get_setting("light_mode", False))

    # ---------- planning ----------
    async def plan(self, req: SetupRequest) -> dict:
        """Check whether a setup fits the memory budget and the really free memory, without creating anything."""
        containers = await asyncio.to_thread(self.docker.collect)
        mem = await asyncio.to_thread(self.docker.meminfo)
        replaced = sum(
            c["limit_mb"]
            for c in containers
            if c["queue"] == req.queue and c["role"] in ("consumer", "producer") and c["state"] == "running"
        )
        broker_up = self.kafka.broker_state() == "running"
        broker_mb = 0 if broker_up else self.kafka.broker_memory(self.light())
        requested = req.workers.count * req.workers.memory_mb + req.producer.count * req.producer.memory_mb + broker_mb
        chk = budget.check(
            self.budget_mb(), budget.used_mb(containers) - replaced, mem["available_mb"] + replaced, requested
        )
        chk["breakdown"] = {
            "broker_mb": broker_mb,
            "workers_mb": req.workers.count * req.workers.memory_mb,
            "producer_mb": req.producer.count * req.producer.memory_mb,
        }
        chk["idle_workers"] = max(0, req.workers.count - req.partitions)
        return chk

    # ---------- start / cancel ----------
    async def start(self, req: SetupRequest, group: str | None = None, label: str | None = None) -> dict:
        """Validate and start a run in the background. Returns immediately; progress arrives over the WebSocket."""
        if self.docker.client is None:
            raise HTTPException(503, "Docker is not reachable. Start Docker Desktop and reload the page.")
        if self.current and self.current["status"] in ACTIVE:
            raise HTTPException(409, "A run is already in progress. Cancel it or wait until it finishes.")
        chk = await self.plan(req)
        if not chk["ok"]:
            raise HTTPException(422, chk["reason"])
        run = {
            "id": uuid.uuid4().hex[:8],
            "created": time.time(),
            "status": "pending",
            "req": req,
            "steps": [{"key": k, "label": label, "state": "pending", "detail": ""} for k, label in STEPS],
            "log": [],
            "cancel": False,
            "workers": {},
            "producers": {},  # latest report of each producer, by container name
            "baseline": [],
            "hist_sec": [0] * (len(EDGES) + 1),
            "hist_total": [0] * (len(EDGES) + 1),
            "samples": [],
            "csamples": [],
            "sample": None,
            "error": None,
            "summary": None,
            "started_at": None,
            "t0": None,
            "group": group,  # id of the comparison this run belongs to, if any
            "label": label,
        }
        self.current = run
        self.task = asyncio.create_task(self._execute(run))
        return self.public(run)

    async def cancel(self):
        """Ask the running run (and a comparison it belongs to) to stop. The loop notices within one sample."""
        if self.comparison and self.comparison["status"] == "running":
            self.comparison["cancel"] = True
        if self.current and self.current["status"] in ACTIVE:
            self.current["cancel"] = True

    # ---------- comparison: the same setup once per language ----------
    async def start_comparison(self, creq: ComparisonRequest) -> dict:
        """Run the same setup once per language, one after the other, with a pause in between."""
        if self.docker.client is None:
            raise HTTPException(503, "Docker is not reachable. Start Docker Desktop and reload the page.")
        if (self.comparison and self.comparison["status"] == "running") or (
            self.current and self.current["status"] in ACTIVE
        ):
            raise HTTPException(409, "A run is already in progress. Cancel it or wait until it finishes.")
        chk = await self.plan(creq.setup)
        if not chk["ok"]:
            raise HTTPException(422, chk["reason"])
        order = comparison_order(creq.langs, creq.random_order)
        cmp = {
            "id": uuid.uuid4().hex[:8], "status": "running", "step": 0, "total": len(order), "langs": order,
            "run_ids": [], "cooldown_left": 0, "cooldown_s": creq.cooldown_s, "cancel": False, "error": None,
        }  # fmt: skip
        self.comparison = cmp
        self.cmp_task = asyncio.create_task(self._run_comparison(cmp, creq))
        return self.comparison_view()

    async def _run_comparison(self, cmp: dict, creq: ComparisonRequest) -> None:
        """One run per language in cmp["langs"], with a cooldown between them. Ends with a status on `cmp`."""
        try:
            for index, lang in enumerate(cmp["langs"]):
                cmp["step"] = index + 1
                req = creq.setup.model_copy(deep=True)
                req.workers.lang = lang
                if creq.producer_follows:
                    req.producer.lang = lang
                run = await self.start(req, group=cmp["id"], label=lang)
                cmp["run_ids"].append(run["id"])
                await self.task  # wait until this run is finished
                if cmp["cancel"] or self.current["status"] != "completed":
                    cmp["status"] = "cancelled" if cmp["cancel"] else "failed"
                    cmp["error"] = self.current.get("error")
                    return
                if index < len(cmp["langs"]) - 1:
                    for left in range(cmp["cooldown_s"], 0, -1):
                        cmp["cooldown_left"] = left
                        if cmp["cancel"]:
                            cmp["status"] = "cancelled"
                            return
                        await asyncio.sleep(1)
                    cmp["cooldown_left"] = 0
            cmp["status"] = "completed"
        except HTTPException as exc:
            cmp["status"], cmp["error"] = "failed", str(exc.detail)
        except Exception as exc:  # noqa: BLE001
            cmp["status"], cmp["error"] = "failed", str(exc)

    def comparison_view(self) -> dict | None:
        """The comparison as the UI needs it, or None."""
        c = self.comparison
        if not c:
            return None
        keys = ("id", "status", "step", "total", "langs", "run_ids", "cooldown_left", "cooldown_s", "error")
        return {k: c[k] for k in keys}

    # ---------- reports from workers ----------
    def handle_report(self, p: dict):
        """Merge one progress report from a worker or producer (POST /internal/report) into the run state."""
        run = self.current
        if not run or p.get("runId") != run["id"]:
            return
        if p.get("role") == "producer":
            run["producers"][p["worker"]] = p
            return
        w = run["workers"].setdefault(p["worker"], {"received": 0, "ts": None, "rate": 0})
        rate = 0
        if w["ts"] and p["ts"] > w["ts"]:
            rate = (p["received"] - w["received"]) / (p["ts"] - w["ts"])
        w.update(
            received=p["received"],
            duplicates=p["duplicates"],
            gaps=p["gaps"],
            errors=p["errors"],
            offsets=p["offsets"],
            assigned=p["assigned"],
            ready=p["ready"],
            rate=round(rate),
            ts=p["ts"],
            lang=p["lang"],
            first_at=p.get("firstAt"),
            last_at=p.get("lastAt"),
            max_latency=p.get("maxLatencyMs"),
        )
        for i, n in enumerate(p["hist"]):
            run["hist_sec"][i] += n
            run["hist_total"][i] += n

    # ---------- helpers ----------
    def _step(self, run, key, state, detail=""):
        """Update the state and detail of one setup step shown in the UI."""
        for s in run["steps"]:
            if s["key"] == key:
                s["state"], s["detail"] = state, detail

    def _log(self, run, line: str):
        """Append a line to the run log shown during setup (kept short)."""
        run["log"].append(line)
        del run["log"][:-40]

    def public(self, run) -> dict:
        """The run as the UI needs it (no internal fields)."""
        req: SetupRequest = run["req"]
        return {
            "id": run["id"],
            "status": run["status"],
            "created": run["created"],
            "config": req.model_dump(),
            "steps": run["steps"],
            "log": run["log"][-25:],
            "error": run["error"],
            "sample": run["sample"],
            "tail": run["samples"][-4:],
            "window": analysis.window(run),
            "totals": analysis.totals(run),
            "summary": run["summary"],
            "assignment": {n: w.get("assigned", []) for n, w in run["workers"].items()},
        }

    def run_view(self):
        """Public view of the current run, or None."""
        return self.public(self.current) if self.current else None

    # ---------- main flow ----------
    # A run is a fixed list of steps (see STEPS in start()). Each step is its own method below, so the whole life of a
    # run reads top to bottom in _execute and a step can be understood, and tested, on its own.
    async def _execute(self, run):
        """The whole life of a run: images, broker, queue, workers, partitions, producers, drive, finalize."""
        req: SetupRequest = run["req"]
        reader: OffsetReader | None = None
        producer_names = [config.container_name(req.queue, f"producer{i + 1}") for i in range(req.producer.count)]
        try:
            run["status"] = "pending"
            await self._step_images(run)
            await self._step_broker(run)
            await self._step_queue(run)
            await self._step_workers(run)
            reader = await self._step_join(run)
            await self._step_producers(run, producer_names)

            self._step(run, "run", "running")
            run["status"] = "running"
            run["started_at"] = time.time()
            await self._drive(run, reader, producer_names)
            if run["status"] == "completed":
                self._step(run, "run", "done")
        except asyncio.CancelledError:
            run["status"] = "cancelled"
        except Exception as exc:  # noqa: BLE001
            self._fail(run, exc)
        finally:
            if reader:
                await reader.stop()
            for producer_name in producer_names:
                await asyncio.to_thread(self.docker.remove, producer_name)
            await self._finalize(run)

    def _fail(self, run, exc: Exception):
        """Mark the run failed and show the error on the step that was running."""
        run["status"] = "failed"
        run["error"] = str(exc)
        for s in run["steps"]:
            if s["state"] == "running":
                s["state"], s["detail"] = "error", str(exc)

    async def _step_images(self, run):
        """Build the worker image of every language the run uses, unless it is already up to date."""
        req: SetupRequest = run["req"]
        loop = asyncio.get_running_loop()
        self._step(run, "images", "running")
        for lang in sorted({req.workers.lang, req.producer.lang}):
            if not await asyncio.to_thread(self.docker.worker_image_current, lang):
                self._log(run, f"Building the {lang} worker image (first use, can take a few minutes)")
                await asyncio.to_thread(
                    self.docker.build_worker_image,
                    lang,
                    lambda line: loop.call_soon_threadsafe(self._log, run, line),
                )
        self._step(run, "images", "done")

    async def _step_broker(self, run):
        """Start the Kafka broker, or reuse the running one."""
        self._step(run, "broker", "running")
        await self.kafka.ensure_broker(self.light(), lambda t: self._log(run, t))
        self._step(run, "broker", "done")

    async def _step_queue(self, run):
        """Create the queue (topic). An existing queue is reused only if it has the requested partition count."""
        req: SetupRequest = run["req"]
        self._step(run, "topic", "running")
        existing = {t["name"]: t["partitions"] for t in await self.kafka.topics()}
        if req.queue in existing and existing[req.queue] != req.partitions:
            raise RuntimeError(
                f"Queue '{req.queue}' already exists with {existing[req.queue]} partitions. Use that number or another name."
            )
        if req.queue not in existing:
            await self.kafka.create_topic(req.queue, req.partitions)
        self._step(run, "topic", "done", f"{req.partitions} partitions")

    async def _start_container(self, run, name: str, role: str, spec, env: dict):
        """Start one worker or producer container (spec is the WorkersSpec or ProducerSpec of the request)."""
        req: SetupRequest = run["req"]
        await asyncio.to_thread(
            self.docker.run_container,
            name,
            config.worker_image(spec.lang),
            role,
            req.queue,
            run["id"],
            spec.lang,
            env,
            spec.memory_mb,
        )

    async def _step_workers(self, run):
        """Remove leftovers of an earlier run of this queue, then start the workers."""
        req: SetupRequest = run["req"]
        self._step(run, "workers", "running")
        await asyncio.to_thread(
            self.docker.remove_where,
            [f"{config.LABEL_QUEUE}={req.queue}"],
            [config.ROLE_CONSUMER, config.ROLE_PRODUCER],
        )
        for i in range(req.workers.count):
            name = config.container_name(req.queue, f"w{i + 1}")
            env = contract.consumer_env(req, run["id"], name)
            await self._start_container(run, name, "consumer", req.workers, env)
        self._step(run, "workers", "done", f"{req.workers.count} x {req.workers.lang}")

    async def _step_join(self, run) -> OffsetReader:
        """Wait until every worker is ready and all partitions are assigned, then note where each partition ends."""
        req: SetupRequest = run["req"]
        self._step(run, "join", "running")
        deadline = time.time() + config.JOIN_TIMEOUT_S
        while True:
            ws = list(run["workers"].values())
            ready = len(ws) == req.workers.count and all(w.get("ready") for w in ws)
            if ready and sum(len(w["assigned"]) for w in ws) == req.partitions:
                break
            if run["cancel"]:
                raise asyncio.CancelledError
            if time.time() > deadline:
                raise RuntimeError("Workers did not join the consumer group in time. Check their logs.")
            await asyncio.sleep(0.5)
        await asyncio.sleep(2)
        reader = OffsetReader(req.queue, req.partitions)
        await reader.start()
        run["baseline"] = await reader.end_offsets()  # messages already in the queue do not count as lag
        self._step(run, "join", "done")
        return reader

    async def _step_producers(self, run, producer_names: list[str]):
        """Start the producers: from here on messages are flowing."""
        req: SetupRequest = run["req"]
        self._step(run, "producer", "running")
        run["t0"] = time.time()
        for index, name in enumerate(producer_names):
            env = contract.producer_env(req, run["id"], name, index)
            await self._start_container(run, name, config.ROLE_PRODUCER, req.producer, env)
        self._step(run, "producer", "done")

    @staticmethod
    def _lag(run, ends: list[int]) -> list[int]:
        """Messages waiting per partition: where the partition ends minus how far the workers have read."""
        ws = list(run["workers"].values())
        lag = []
        for p in range(run["req"].partitions):
            read = max((w.get("offsets", {}).get(str(p), 0) for w in ws), default=0)
            lag.append(max(0, ends[p] - max(read, run["baseline"][p])))
        return lag

    async def _check_producers(self, run, producer_names: list[str]):
        """Fail the run if a container ran out of memory, or a producer died before it reported that it was done."""
        for name in [*producer_names, *run["workers"]]:
            container = next((c for c in self.containers if c["name"] == name), None)
            if container and container.get("oom"):
                who, setting = ("A sender", "Senders") if name in producer_names else ("A worker", "Workers")
                raise RuntimeError(
                    f"{who} ran out of memory and was stopped by Docker (limit {container.get('limit_mb')} MB). "
                    f"Give {setting.lower()} more memory in the form (step {4 if setting == 'Senders' else 3}) and run again."
                )
        for name in producer_names:
            container = next((c for c in self.containers if c["name"] == name), None)
            if container and container["state"] == "exited" and not run["producers"].get(name, {}).get("done"):
                logs = await asyncio.to_thread(self.docker.logs, name, 15)
                raise RuntimeError(f"A producer stopped unexpectedly. Last log lines: {logs.strip()[-400:]}")

    async def _drive(self, run, reader: OffsetReader, producer_names: list[str]):
        """Sample the run every SAMPLE_INTERVAL_S (rates, lag, latency percentiles) until it ends."""
        t0 = run["t0"]
        prev_sent = prev_recv = 0
        prev_t = t0
        last_progress = time.time()
        last_pct = (None, None, None)
        while True:
            await asyncio.sleep(config.SAMPLE_INTERVAL_S)
            now = time.time()
            dt = max(0.05, now - prev_t)
            prev_t = now
            if run["cancel"]:
                run["status"] = "cancelled"
                return
            tot = analysis.totals(run)
            lag = self._lag(run, await reader.end_offsets())
            hist, run["hist_sec"] = run["hist_sec"], [0] * (len(EDGES) + 1)
            pcts = (analysis.percentile(hist, 0.5), analysis.percentile(hist, 0.95), analysis.percentile(hist, 0.99))
            if pcts[0] is None:  # nothing was processed this second: keep showing the last known values
                pcts = last_pct
            last_pct = pcts
            sample = {
                "t": round(now - t0, 2),
                "dt": round(dt, 2),
                "sent": tot["sent"],
                "received": tot["received"],
                "send_rate": round((tot["sent"] - prev_sent) / dt),
                "recv_rate": round((tot["received"] - prev_recv) / dt),
                "lag_total": sum(lag),
                "lag": lag,
                "p50": pcts[0],
                "p95": pcts[1],
                "p99": pcts[2],
            }
            if tot["received"] != prev_recv:
                last_progress = time.time()
            prev_sent, prev_recv = tot["sent"], tot["received"]
            run["samples"].append(sample)
            run["sample"] = sample

            producers_done = tot["producer_done"]
            if producers_done and (tot["unique"] >= tot["sent"] or time.time() - last_progress > config.STALL_TIMEOUT_S):
                run["status"] = "completed"  # everything arrived, or the rest is lost (no progress for a while)
                return
            await self._check_producers(run, producer_names)
            if time.time() - t0 > config.RUN_TIMEOUT_S:
                raise RuntimeError(f"The run took longer than {config.RUN_TIMEOUT_S // 60} minutes and was stopped.")

    async def _finalize(self, run):
        """Build the summary (window, resources, distribution, verdict, highlights) and store the run."""
        req: SetupRequest = run["req"]
        tot = analysis.totals(run)
        samples = run["samples"]
        active = [s for s in samples if s["send_rate"] > 0 or s["recv_rate"] > 0]
        duration = round(samples[-1]["t"], 1) if samples else 0
        active_time = sum(s.get("dt", 1) for s in active)
        peaks: dict[str, dict] = {}
        for cs in run["csamples"]:
            for c in cs["containers"]:
                pk = peaks.setdefault(
                    c["name"],
                    {
                        "role": c["role"],
                        "lang": c["lang"],
                        "cpu": 0,
                        "mem_mb": 0,
                        "limit_mb": c["limit_mb"],
                        "oom": False,
                    },
                )
                pk["cpu"] = max(pk["cpu"], c["cpu"])
                pk["mem_mb"] = max(pk["mem_mb"], c["mem_mb"])
                pk["oom"] = pk["oom"] or c.get("oom", False)
        win = analysis.window(run)
        lag_peaks = [max((s["lag"][p] for s in samples if len(s["lag"]) > p), default=0) for p in range(req.partitions)]
        run["summary"] = {
            "duration_s": duration,
            "sent": tot["sent"],
            "received": tot["received"],
            "duplicates": tot["duplicates"],
            "lost": max(0, tot["sent"] - tot["unique"]),
            "errors": tot["errors"],
            "avg_recv_rate": round(tot["unique"] / max(1.0, active_time)),
            "peak_recv_rate": max((s["recv_rate"] for s in samples), default=0),
            "peak_send_rate": max((s["send_rate"] for s in samples), default=0),
            "p50": analysis.percentile(run["hist_total"], 0.5),
            "p90": analysis.percentile(run["hist_total"], 0.9),
            "p95": analysis.percentile(run["hist_total"], 0.95),
            "p99": analysis.percentile(run["hist_total"], 0.99),
            "peak_lag": max((s["lag_total"] for s in samples), default=0),
            "containers": peaks,
            "window": win,
            "latency_max_ms": max((w.get("max_latency") or 0 for w in run["workers"].values()), default=0) or None,
            "peak_recv_at": max(samples, key=lambda s: s["recv_rate"])["t"] if samples else None,
            "peak_lag_at": max(samples, key=lambda s: s["lag_total"])["t"] if samples else None,
            "latency_hist": {"edges": EDGES, "counts": run["hist_total"]},
            "partition_lag_peaks": lag_peaks,
            "distribution": analysis.distribution(run, tot),
            "resources": analysis.resource_stats(run, win, tot),
        }
        run["summary"]["network"] = analysis.network(run, tot, win, run["summary"]["resources"])
        run["summary"]["insights"] = analysis.insights(run, tot, win, run["summary"])
        # Highlights describe a run that actually moved messages; a failed or empty run only gets the verdict.
        moved = run["status"] in ("completed", "cancelled") and tot["received"] > 0
        run["summary"]["highlights"] = analysis.highlights(run, tot, win, run["summary"]) if moved else []
        self.store.save_run(
            run["id"],
            run["created"],
            run["status"],
            req.model_dump() | {"group": run.get("group"), "label": run.get("label")},
            run["summary"],
            samples,
            run["csamples"],
        )

    # ---------- per-second tick for the UI ----------
    async def tick(self) -> dict:
        """One snapshot for the UI: containers, budget and the current run. Also records container samples during a run."""
        if self.docker.client is None:
            return {"type": "tick", "t": time.time(), "docker_ok": False, "error": self.docker.error}
        containers = await asyncio.to_thread(self.docker.collect)
        mem = await asyncio.to_thread(self.docker.meminfo)
        run = self.current
        for c in containers:
            c["msgs_s"], c["partitions"] = 0, []
            if run and c["role"] == "consumer":
                w = run["workers"].get(c["name"])
                if w:
                    c["msgs_s"], c["partitions"] = w.get("rate", 0), w.get("assigned", [])
            elif run and c["role"] == "producer" and run["sample"]:
                c["msgs_s"] = run["sample"]["send_rate"]
        self.containers = [c for c in containers if c]
        now = time.time()
        dt = now - self._last_tick if self._last_tick else 1.0
        self._last_tick = now
        if run and run["status"] == "running" and run["t0"]:
            run["csamples"].append(
                {
                    "t": round(now - run["t0"], 2),
                    "dt": round(dt, 2),
                    "containers": [
                        {
                            k: c[k]
                            for k in (
                                "name",
                                "role",
                                "lang",
                                "cpu",
                                "mem_mb",
                                "limit_mb",
                                "oom",
                                "msgs_s",
                                "rx_kbs",
                                "tx_kbs",
                            )
                        }
                        for c in self.containers
                    ],
                }
            )
        return {
            "type": "tick",
            "comparison": self.comparison_view(),
            "t": time.time(),
            "docker_ok": True,
            "containers": self.containers,
            "budget": budget.snapshot(self.budget_mb(), self.containers, mem),
            "run": self.run_view(),
        }
