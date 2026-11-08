import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from . import analysis, catalog, config, security
from .docker_service import DockerService
from .hub import Hub
from .kafka_service import KafkaService
from .models import ComparisonRequest, SettingsUpdate, SetupRequest
from .runs import RunManager
from .store import Store

log = logging.getLogger(config.PREFIX)
logging.basicConfig(level=logging.INFO)


class QuietAccessLog(logging.Filter):
    """Drop the access-log lines of the requests that repeat every second (see config.QUIET_PATHS)."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not any(path in record.getMessage() for path in config.QUIET_PATHS)


logging.getLogger("uvicorn.access").addFilter(QuietAccessLog())

docker_svc = DockerService()
store = Store(config.DATA_DIR / f"{config.PREFIX}.db")
hub = Hub()
kafka = KafkaService(docker_svc)
runs = RunManager(docker_svc, kafka, store, hub)
state = {"tick": None}


def connect_docker() -> bool:
    """Connect to Docker, make sure the sandbox network exists and pick a first-start budget (50% of free memory)."""
    ok = docker_svc.connect()
    if ok:
        docker_svc.ensure_network()
        if store.get_setting("budget_mb") is None:
            avail = docker_svc.meminfo()["available_mb"]
            store.set_setting("budget_mb", max(1024, int(avail * 0.5 // 256 * 256)))
    return ok


async def tick_loop():
    """Every second (twice per second during a run) build a snapshot and push it to all browsers."""
    while True:
        started = time.time()
        try:
            if docker_svc.client is None:
                await asyncio.to_thread(connect_docker)
            payload = await runs.tick()
            state["tick"] = payload
            await hub.broadcast(payload)
        except Exception:
            log.exception("tick failed")
        active = runs.current is not None and runs.current["status"] in ("pending", "running")
        await asyncio.sleep(max(0.15, (0.5 if active else 1.0) - (time.time() - started)))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Startup: connect to Docker and start the tick loop. Shutdown: remove the sandbox unless the user turned that off."""
    await asyncio.to_thread(connect_docker)
    task = asyncio.create_task(tick_loop())
    yield
    task.cancel()
    if runs.task and not runs.task.done():
        runs.current["cancel"] = True
    if docker_svc.client is not None and store.get_setting("remove_on_stop", True):
        log.info("Shutting down: removing sandbox containers")
        await asyncio.to_thread(docker_svc.remove_sandbox, True)


app = FastAPI(title="queue-lab", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.middleware("http")
async def guard(request: Request, call_next):
    """Refuse foreign hosts and cross-site changes, limit worker reports, add the security headers."""
    if not security.host_allowed(request.headers.get("host")):
        return PlainTextResponse("Unknown host.", status_code=400)
    if request.method not in ("GET", "HEAD", "OPTIONS") and not security.origin_allowed(request.headers.get("origin")):
        return PlainTextResponse("Cross-site request refused.", status_code=403)
    if request.url.path == "/internal/report" and int(request.headers.get("content-length") or 0) > security.MAX_REPORT_BYTES:
        return PlainTextResponse("Report too large.", status_code=413)
    response = await call_next(request)
    for name, value in security.SECURITY_HEADERS.items():
        response.headers.setdefault(name, value)
    return response


def settings_view() -> dict:
    """Settings with defaults applied."""
    return {
        "budget_mb": int(store.get_setting("budget_mb", 2048)),
        "light_mode": bool(store.get_setting("light_mode", False)),
        "remove_on_stop": bool(store.get_setting("remove_on_stop", True)),
    }


# ---------------- API ----------------
@app.get("/api/state")
async def get_state():
    """Everything the UI needs on load: Docker status, warnings, settings, queues, runs and the last tick."""
    queues = []
    try:
        queues = await kafka.topics() if docker_svc.client else []
    except Exception:  # noqa: BLE001
        queues = []
    mem = await asyncio.to_thread(docker_svc.meminfo) if docker_svc.client else {"total_mb": 0, "available_mb": 0, "cpus": 0}
    warnings = []
    if docker_svc.client is None:
        warnings.append("Docker is not reachable. Start Docker Desktop and reload the page.")
    elif mem["available_mb"] < config.MIN_FREE_FOR_KAFKA_MB:
        warnings.append(
            f"Only {mem['available_mb']} MB of memory is available to Docker. Kafka needs about "
            f"{config.MIN_FREE_FOR_KAFKA_MB} MB. Raise the Docker memory limit (WSL: .wslconfig)."
        )
    return {
        "docker_ok": docker_svc.client is not None,
        "docker_error": docker_svc.error,
        "warnings": warnings,
        "settings": settings_view(),
        "tick": state["tick"],
        "queues": queues,
        "runs": store.list_runs(),
        "host": mem,
        "version": config.VERSION,
        "project": docker_svc.project,
        "sandbox_project": docker_svc.sandbox_project,
    }


@app.get("/api/scenarios")
async def scenarios():
    """Descriptions of the processing scenarios shown in the run form."""
    return catalog.SCENARIOS


@app.get("/api/code")
async def code():
    """Real worker source code and its regions for the Code page."""
    return catalog.code()


@app.post("/api/plan")
async def plan(req: SetupRequest):
    """Memory plan of a setup without creating anything."""
    if docker_svc.client is None:
        raise HTTPException(503, "Docker is not reachable.")
    return await runs.plan(req)


@app.post("/api/setups")
async def setups(req: SetupRequest):
    """Create the queue, workers and producer and start the run."""
    return await runs.start(req)


@app.post("/api/comparisons")
async def start_comparison(req: ComparisonRequest):
    """Run the same setup with Python and with Node.js, one after the other, then compare the two runs."""
    return await runs.start_comparison(req)


@app.get("/api/compare")
async def compare_runs(ids: str):
    """Standard metrics of two or more stored runs side by side, what differs between their setups and a verdict."""
    wanted = [i for i in ids.split(",") if i]
    if not 2 <= len(wanted) <= 8:
        raise HTTPException(400, "Pick between 2 and 8 runs to compare.")
    runs_found = [store.get_run(i) for i in wanted]
    if any(r is None for r in runs_found):
        raise HTTPException(404, "One of the runs was not found. It may have been deleted.")
    return analysis.compare(runs_found)


@app.post("/api/runs/current/cancel")
async def cancel_run():
    """Cancel the running run."""
    await runs.cancel()
    return {"ok": True}


@app.get("/api/runs")
async def list_runs():
    """Run history, newest first."""
    return store.list_runs()


@app.get("/api/runs/current/samples")
async def current_samples():
    """All samples of the current run (used by a page that opens mid-run)."""
    return runs.current["samples"] if runs.current else []


@app.get("/api/runs/current/full")
async def current_full():
    """The current run with its container samples, for the container page."""
    r = runs.current
    if not r:
        raise HTTPException(404, "No run yet.")
    view = runs.public(r)
    return {
        "id": r["id"],
        "status": r["status"],
        "config": view["config"],
        "summary": r["summary"],
        "window": view["window"],
        "samples": r["samples"],
        "container_samples": r["csamples"],
    }


@app.get("/api/runs/{run_id}")
async def get_run(run_id: str):
    """A stored run with samples and container samples."""
    r = store.get_run(run_id)
    if not r:
        raise HTTPException(404, "Run not found.")
    return r


@app.delete("/api/runs/{run_id}")
async def delete_run(run_id: str):
    """Delete a run from the history."""
    store.delete_run(run_id)
    return {"ok": True}


@app.delete("/api/runs")
async def delete_all_runs():
    """Delete the whole run history."""
    store.delete_all_runs()
    return {"ok": True}


@app.get("/api/settings")
async def get_settings():
    return settings_view()


@app.put("/api/settings")
async def put_settings(body: SettingsUpdate):
    for key, value in body.model_dump(exclude_none=True).items():
        store.set_setting(key, value)
    return settings_view()


@app.get("/api/queues")
async def queues():
    return await kafka.topics() if docker_svc.client else []


@app.delete("/api/queues/{name}")
async def delete_queue(name: str):
    if runs.current and runs.current["status"] in ("pending", "running"):
        raise HTTPException(409, "A run is in progress.")
    await asyncio.to_thread(
        docker_svc.remove_where, [f"{config.LABEL_QUEUE}={name}"], [config.ROLE_CONSUMER, config.ROLE_PRODUCER]
    )
    try:
        await kafka.delete_topic(name)
    except Exception as exc:
        raise HTTPException(400, f"Could not delete the queue: {exc}") from exc
    return {"ok": True}


@app.get("/api/containers/{name}/logs")
async def container_logs(name: str, tail: int = 200):
    if not name.startswith(f"{config.PREFIX}-"):
        raise HTTPException(404, "Unknown container.")
    return {"logs": await asyncio.to_thread(docker_svc.logs, name, min(max(tail, 1), 1000))}


@app.post("/api/reset")
async def reset():
    """Cancel the run if needed, remove all lab containers and clear the dashboard. History is kept."""
    if runs.task and not runs.task.done():
        runs.current["cancel"] = True
        await asyncio.sleep(1.5)
    await asyncio.to_thread(docker_svc.remove_sandbox, False)
    runs.current = None
    runs.comparison = None
    return {"ok": True}


@app.get("/healthz")
async def healthz():
    """For the container health check: the app is up."""
    return {"ok": True, "version": config.VERSION}


@app.post("/internal/report")
async def report(request: Request):
    """Receive the once-per-second progress report of a worker. Internal: only the sandbox network can reach it."""
    runs.handle_report(await request.json())
    return {}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    """WebSocket that streams ticks to the browser. Only this app's own page may open it."""
    if not (security.host_allowed(ws.headers.get("host")) and security.origin_allowed(ws.headers.get("origin"))):
        await ws.close(code=1008)
        return
    await hub.add(ws)
    if state["tick"]:
        await ws.send_text(json.dumps(state["tick"]))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        hub.remove(ws)
    except Exception:  # noqa: BLE001
        hub.remove(ws)


# ---------------- static frontend ----------------
if (config.STATIC_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=config.STATIC_DIR / "assets"), name="assets")


@app.get("/{path:path}")
async def spa(path: str):
    """Serve the built frontend; unknown paths return index.html so client-side routes work."""
    if path.startswith(("api/", "internal/")):
        raise HTTPException(404)
    candidate = (config.STATIC_DIR / path).resolve()
    if path and candidate.is_file() and config.STATIC_DIR.resolve() in candidate.parents:
        return FileResponse(candidate)
    index = config.STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    raise HTTPException(404, "Frontend is not built.")
