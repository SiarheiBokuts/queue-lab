import asyncio
import json
import logging
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from . import analysis, budget, catalog, config, reports, security
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
# The newest live snapshot (containers, budget, current run). The loop below refreshes it every second, pushes it to
# every open page over the WebSocket, and /api/state hands it to a page that has just loaded.
state = {"snapshot": None}


def connect_docker() -> bool:
    """Connect to Docker, make sure the sandbox network exists and pick a first-start budget (50% of free memory)."""
    ok = docker_svc.connect()
    if ok:
        docker_svc.ensure_network()
        if store.get_setting("budget_mb") is None:
            avail = docker_svc.meminfo()["available_mb"]
            store.set_setting("budget_mb", budget.first_start_budget(avail))
    return ok


async def snapshot_loop():
    """Every second (twice per second during a run) build a snapshot and push it to all browsers."""
    while True:
        started = time.time()
        try:
            if docker_svc.client is None:
                await asyncio.to_thread(connect_docker)
            payload = await runs.snapshot()
            state["snapshot"] = payload
            await hub.broadcast(payload)
        except Exception:
            log.exception("snapshot failed")
        active = runs.current is not None and runs.current["status"] in ("pending", "running")
        await asyncio.sleep(max(0.15, (0.5 if active else 1.0) - (time.time() - started)))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Startup: connect to Docker and start the snapshot loop. Shutdown: remove the sandbox unless the user turned that off."""
    await asyncio.to_thread(connect_docker)
    task = asyncio.create_task(snapshot_loop())
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
    if request.method not in ("GET", "HEAD", "OPTIONS") and not security.origin_allowed(
        request.headers.get("origin"), request.headers.get("host")
    ):
        return PlainTextResponse("Cross-site request refused.", status_code=403)
    if request.url.path == "/internal/report":
        try:
            declared = int(request.headers.get("content-length") or 0)
        except ValueError:
            return PlainTextResponse("Bad Content-Length.", status_code=400)
        if declared > security.MAX_REPORT_BYTES:
            return PlainTextResponse("Report too large.", status_code=413)
    response = await call_next(request)
    for name, value in security.SECURITY_HEADERS.items():
        response.headers.setdefault(name, value)
    return response


def settings_view() -> dict:
    """Settings with defaults applied."""
    return {
        "budget_mb": int(store.get_setting("budget_mb", config.DEFAULT_BUDGET_MB)),
        "light_mode": bool(store.get_setting("light_mode", False)),
        "remove_on_stop": bool(store.get_setting("remove_on_stop", True)),
        "welcomed": bool(store.get_setting("welcomed", False)),
    }


# ---------------- API ----------------
@app.get("/api/state")
async def get_state():
    """Everything the UI needs on load: Docker status, warnings, settings, queues, runs and the newest live snapshot."""
    queues = []
    try:
        queues = await kafka.topics() if docker_svc.client else []
    except Exception:  # noqa: BLE001
        queues = []
    mem = await asyncio.to_thread(docker_svc.meminfo) if docker_svc.client else {"total_mb": 0, "available_mb": 0, "cpus": 0}
    warnings = []
    current = settings_view()
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
        "settings": current,
        "snapshot": state["snapshot"],
        "queues": queues,
        "runs": store.list_runs(),
        "host": mem,
        "limits": budget.limits(current["budget_mb"], current["light_mode"]),
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
    """Standard metrics of two or more stored runs side by side, and what differs between their setups. No score and no winner."""
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
    if not security.QUEUE_NAME.fullmatch(name):  # the name goes into a Docker label filter and a Kafka request
        raise HTTPException(422, "A queue name has letters, digits, dots, dashes and underscores, up to 64 characters.")
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
    if not security.container_name_ok(name):
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
    """Receive the once-per-second progress report of a worker. Internal: only the sandbox network can reach it.

    Whatever arrives is checked first (`reports.parse_report`): a body that is too big, is not JSON or does not have the
    fields of a report is answered with a 4xx and the run is left as it was.
    """
    body = bytearray()
    async for chunk in request.stream():  # the size is counted as it arrives: a missing or false Content-Length changes nothing
        body += chunk
        if len(body) > security.MAX_REPORT_BYTES:
            raise HTTPException(413, "The report is too large.")
    try:
        parsed = reports.parse_report(bytes(body))
    except reports.ReportError as exc:
        raise HTTPException(exc.status, str(exc)) from exc
    runs.handle_report(parsed)
    return {}


@app.websocket("/ws/logs/{name}")
async def ws_logs(ws: WebSocket, name: str):
    """Stream the log of one lab container to the browser as it grows."""
    allowed = security.host_allowed(ws.headers.get("host")) and security.origin_allowed(
        ws.headers.get("origin"), ws.headers.get("host")
    )
    if not allowed or not security.container_name_ok(name) or docker_svc.client is None:
        await ws.close(code=1008)
        return
    await ws.accept()
    loop = asyncio.get_running_loop()
    lines: asyncio.Queue[str | None] = asyncio.Queue()

    def pump() -> None:  # the Docker SDK blocks, so the stream is read in a thread
        try:
            for chunk in docker_svc.log_stream(name):
                loop.call_soon_threadsafe(lines.put_nowait, chunk)
        except Exception as exc:  # noqa: BLE001
            loop.call_soon_threadsafe(lines.put_nowait, f"[log stream ended: {exc}]\n")
        finally:
            loop.call_soon_threadsafe(lines.put_nowait, None)

    threading.Thread(target=pump, daemon=True).start()
    closed = asyncio.create_task(ws.receive_text())  # finishes when the browser leaves
    try:
        while True:
            next_line = asyncio.create_task(lines.get())
            await asyncio.wait({next_line, closed}, return_when=asyncio.FIRST_COMPLETED)
            if closed.done():
                next_line.cancel()
                break
            chunk = next_line.result()
            if chunk is None:
                break
            await ws.send_text(chunk)
    except WebSocketDisconnect:
        pass
    finally:
        closed.cancel()


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    """WebSocket that streams live snapshots to the browser. Only this app's own page may open it."""
    if not (
        security.host_allowed(ws.headers.get("host"))
        and security.origin_allowed(ws.headers.get("origin"), ws.headers.get("host"))
    ):
        await ws.close(code=1008)
        return
    await hub.add(ws)
    if state["snapshot"]:
        await ws.send_text(json.dumps(state["snapshot"]))
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
    try:
        candidate = (config.STATIC_DIR / path).resolve()
        is_static_file = bool(path) and candidate.is_file() and config.STATIC_DIR.resolve() in candidate.parents
    except (OSError, ValueError):  # a path with a null byte or one the system cannot resolve is not a file of ours
        is_static_file = False
    if is_static_file:
        return FileResponse(candidate)
    index = config.STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    raise HTTPException(404, "Frontend is not built.")
