"""Everything that talks to the Docker Engine API. Synchronous docker-py, called through threads."""

import concurrent.futures as cf
import contextlib
import hashlib
import os
import socket
import time

import docker
from docker.errors import APIError, ImageNotFound, NotFound

from . import config

MB = 1024 * 1024

# Folders and files that exist only for development (tests, build output). They never go into a worker image, so
# changing them must not trigger an image rebuild.
DEV_PARTS = {"tests", "test", "node_modules", "dist", "dist-test", "__pycache__", ".pytest_cache", "target"}


def is_dev_file(relative) -> bool:
    return bool(DEV_PARTS & set(relative.parts)) or relative.name.endswith((".test", "_test.go"))


def compose_labels(project: str, service: str, extra: dict) -> dict:
    """Labels that make a dynamically created container show up as part of a Compose project.

    Verified in the spike: all of these are needed for `docker compose ls/ps/down --remove-orphans` to see it.
    """
    return {
        "com.docker.compose.project": project,
        "com.docker.compose.service": service,
        "com.docker.compose.oneoff": "False",
        "com.docker.compose.container-number": "1",
        "com.docker.compose.version": "2.0.0",
        "com.docker.compose.config-hash": config.PREFIX,
        "com.docker.compose.project.working_dir": "/queuelab",
        **extra,
    }


class DockerService:
    """Talks to the Docker Engine API. The app never shells out to the docker CLI. Blocking calls run in threads."""

    def __init__(self):
        self.client: docker.DockerClient | None = None
        self.error: str | None = None
        self.project = config.DEFAULT_PROJECT
        self.sandbox_project = config.sandbox_project(self.project)
        self.network = self.sandbox_project
        self.me = None
        self._prev: dict[str, dict] = {}
        self._pool = cf.ThreadPoolExecutor(max_workers=16)

    # ---------- connection and identity ----------
    def connect(self) -> bool:
        """Connect to the Docker socket and learn which Compose project the app itself runs in. Returns False if Docker is down."""
        try:
            self.client = docker.from_env(version="auto")
            self.client.ping()
            self.error = None
        except Exception as exc:  # noqa: BLE001
            self.client = None
            self.error = str(exc)
            return False
        try:
            self.me = self.client.containers.get(socket.gethostname())
            self.project = self.me.labels.get(config.COMPOSE_PROJECT_LABEL) or self.project
        except Exception:  # noqa: BLE001
            self.me = None
        self.sandbox_project = config.sandbox_project(self.project)
        self.network = self.sandbox_project
        return True

    def ensure_network(self):
        """Create the sandbox network if missing and attach the app to it, so workers can reach the app by its alias."""
        nets = [n for n in self.client.networks.list(names=[self.network]) if n.name == self.network]
        net = (
            nets[0]
            if nets
            else self.client.networks.create(
                self.network, driver="bridge", labels={config.LABEL_LEVEL: config.LEVEL_SANDBOX}
            )
        )
        if self.me is not None:
            self.me.reload()
            if self.network not in self.me.attrs["NetworkSettings"]["Networks"]:
                net.connect(self.me, aliases=[config.APP_ALIAS])
        return net

    # ---------- memory ----------
    def meminfo(self) -> dict:
        """Total and available memory as Docker sees it (the Docker VM on Windows and macOS), in MB."""
        total = avail = None
        try:
            with open("/proc/meminfo") as fh:
                for line in fh:
                    if line.startswith("MemTotal:"):
                        total = int(line.split()[1]) // 1024
                    elif line.startswith("MemAvailable:"):
                        avail = int(line.split()[1]) // 1024
        except OSError:
            pass
        if total is None and self.client is not None:
            total = int(self.client.info()["MemTotal"]) // MB
            avail = total
        return {"total_mb": total or 0, "available_mb": avail or 0, "cpus": self.cpu_count()}

    def cpu_count(self) -> int:
        """How many CPU cores Docker can use (the Docker VM on Windows and macOS). Read once, it does not change."""
        if getattr(self, "_ncpu", None) is None:
            try:
                self._ncpu = int(self.client.info()["NCPU"])
            except Exception:  # noqa: BLE001
                self._ncpu = os.cpu_count() or 1
        return self._ncpu

    # ---------- containers ----------
    def sandbox_summaries(self, all_=True) -> list[dict]:
        """Lightweight list of every container of the sandbox level (the only containers the app ever touches)."""
        return self.client.api.containers(all=all_, filters={"label": [config.SANDBOX_FILTER]})

    def get_state(self, name: str) -> dict | None:
        """State of one container by name, or None if it does not exist."""
        try:
            info = self.client.api.inspect_container(name)
        except NotFound:
            return None
        return {"state": info["State"]["Status"], "id": info["Id"]}

    def run_container(self, name, image, role, queue, run_id, lang, env, mem_mb, command=None):
        """Create and start a lab container with a hard memory limit, our labels and the Compose project labels."""
        labels = compose_labels(
            self.sandbox_project,
            name,
            {
                config.LABEL_LAB: "true",
                config.LABEL_LEVEL: config.LEVEL_SANDBOX,
                config.LABEL_ROLE: role,
                config.LABEL_QUEUE: queue or "",
                config.LABEL_RUN: run_id or "",
                config.LABEL_LANG: lang or "",
            },
        )
        self.remove(name)
        return self.client.containers.run(
            image,
            command=command,
            name=name,
            detach=True,
            environment=env,
            labels=labels,
            network=self.network,
            mem_limit=f"{mem_mb}m",
            memswap_limit=f"{mem_mb}m",
        )

    def remove(self, name: str):
        """Force-remove a container (and its anonymous volumes); a missing container is not an error."""
        try:
            self.client.api.remove_container(name, force=True, v=True)
        except NotFound:
            pass
        except APIError:
            pass

    def remove_where(self, extra_labels: list[str] | None = None, roles: list[str] | None = None) -> int:
        """Remove sandbox containers filtered by "label=value" strings and/or roles. Returns how many were removed."""
        n = 0
        for c in self.sandbox_summaries():
            lab = c.get("Labels", {})
            if roles and lab.get(config.LABEL_ROLE) not in roles:
                continue
            if extra_labels and not all(lab.get(k.split("=")[0]) == k.split("=", 1)[1] for k in extra_labels):
                continue
            self.remove(c["Id"])
            n += 1
        return n

    def remove_sandbox(self, remove_network: bool = False):
        """Remove every sandbox container and optionally the sandbox network. Used by Reset and on shutdown."""
        self.remove_where()
        if not remove_network:
            return
        # Best effort: the network may already be gone or still be in use. Neither is worth failing a shutdown for.
        with contextlib.suppress(Exception):
            nets = [n for n in self.client.networks.list(names=[self.network]) if n.name == self.network]
            for net in nets:
                if self.me is not None:
                    with contextlib.suppress(Exception):
                        net.disconnect(self.me, force=True)
                net.remove()

    def logs(self, name: str, tail: int = 200) -> str:
        """Last `tail` log lines of a container as text."""
        try:
            return self.client.api.logs(name, tail=tail, timestamps=False).decode("utf-8", "replace")
        except NotFound:
            return ""

    # ---------- images ----------
    def image_exists(self, tag: str) -> bool:
        """True if an image with this tag exists locally."""
        try:
            self.client.images.get(tag)
            return True
        except ImageNotFound:
            return False

    @staticmethod
    def source_hash(lang: str) -> str:
        h = hashlib.sha1()
        base = config.WORKERS_DIR / lang
        for f in sorted(p for p in base.rglob("*") if p.is_file() and not is_dev_file(p.relative_to(base))):
            h.update(str(f.relative_to(base)).encode())
            h.update(f.read_bytes())
        return h.hexdigest()[:12]

    def worker_image_current(self, lang: str) -> bool:
        """True when the cached worker image was built from the current worker sources."""
        try:
            img = self.client.images.get(config.worker_image(lang))
        except ImageNotFound:
            return False
        return (img.labels or {}).get(config.LABEL_SRC) == self.source_hash(lang)

    def build_worker_image(self, lang: str, on_line):
        tag = config.worker_image(lang)
        path = str(config.WORKERS_DIR / lang)
        for chunk in self.client.api.build(
            path=path, tag=tag, rm=True, forcerm=True, decode=True, labels={config.LABEL_SRC: self.source_hash(lang)}
        ):
            if "error" in chunk:
                raise RuntimeError(chunk["error"].strip())
            line = (chunk.get("stream") or chunk.get("status") or "").strip()
            if line:
                on_line(line)
        return tag

    def pull_image(self, image: str, on_line):
        """Pull an image if it is not present, reporting progress lines through on_line."""
        if self.image_exists(image):
            return
        for chunk in self.client.api.pull(image, stream=True, decode=True):
            line = chunk.get("status", "")
            if chunk.get("id"):
                line = f"{chunk['id']}: {line}"
            on_line(line)

    # ---------- stats ----------
    def collect(self) -> list[dict]:
        """Inspect and measure every sandbox container in parallel: state, limits, CPU, memory, network and disk rates."""
        summaries = self.sandbox_summaries()
        futures = [self._pool.submit(self._one, s) for s in summaries]
        out = [m for m in (f.result() for f in futures) if m]
        live = {s["Id"] for s in summaries}
        for k in list(self._prev):
            if k not in live:
                self._prev.pop(k, None)
        return sorted(out, key=lambda m: (m["role"] != "broker", m["role"] != "producer", m["name"]))

    def _one(self, s: dict) -> dict:
        """Metrics of one container. CPU and I/O rates are computed from the difference to the previous sample."""
        cid = s["Id"]
        lab = s.get("Labels", {})
        try:
            info = self.client.api.inspect_container(cid)
        except NotFound:
            return {}
        st = info["State"]
        limit_mb = int(info["HostConfig"].get("Memory", 0) / MB)
        m = {
            "name": info["Name"].lstrip("/"),
            "id": cid[:12],
            "role": lab.get(config.LABEL_ROLE, ""),
            "queue": lab.get(config.LABEL_QUEUE, ""),
            "lang": lab.get(config.LABEL_LANG, ""),
            "run": lab.get(config.LABEL_RUN, ""),
            "state": st["Status"],
            "image": info["Config"]["Image"],
            "oom": bool(st.get("OOMKilled")),
            "restarts": info.get("RestartCount", 0),
            "exit_code": st.get("ExitCode"),
            "started": st.get("StartedAt"),
            "limit_mb": limit_mb,
            "cpu": 0.0,
            "mem_mb": 0.0,
            "mem_pct": 0.0,
            "rx_kbs": 0.0,
            "tx_kbs": 0.0,
            "blk_r_kbs": 0.0,
            "blk_w_kbs": 0.0,
        }
        if st["Status"] != "running":
            return m
        try:
            raw = self.client.api.stats(cid, stream=False, one_shot=True)
        except Exception:  # noqa: BLE001
            return m
        now = time.time()
        mem = raw.get("memory_stats", {})
        cache = mem.get("stats", {}).get("inactive_file", mem.get("stats", {}).get("total_inactive_file", 0))
        used = max(0, mem.get("usage", 0) - cache)
        lim = limit_mb * MB or mem.get("limit", 0)
        m["mem_mb"] = round(used / MB, 1)
        m["limit_mb"] = int(lim / MB) if lim else 0
        m["mem_pct"] = round(used / lim * 100, 1) if lim else 0.0
        cpu_total = raw.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
        nets = raw.get("networks", {}) or {}
        rx = sum(v.get("rx_bytes", 0) for v in nets.values())
        tx = sum(v.get("tx_bytes", 0) for v in nets.values())
        blk_r = blk_w = 0
        for e in raw.get("blkio_stats", {}).get("io_service_bytes_recursive") or []:
            op = str(e.get("op", "")).lower()
            if op == "read":
                blk_r += e.get("value", 0)
            elif op == "write":
                blk_w += e.get("value", 0)
        prev = self._prev.get(cid)
        if prev and now > prev["t"]:
            dt = now - prev["t"]
            m["cpu"] = round(max(0.0, (cpu_total - prev["cpu"]) / 1e9 / dt * 100), 1)
            m["rx_kbs"] = round(max(0, rx - prev["rx"]) / 1024 / dt, 1)
            m["tx_kbs"] = round(max(0, tx - prev["tx"]) / 1024 / dt, 1)
            m["blk_r_kbs"] = round(max(0, blk_r - prev["blk_r"]) / 1024 / dt, 1)
            m["blk_w_kbs"] = round(max(0, blk_w - prev["blk_w"]) / 1024 / dt, 1)
        self._prev[cid] = {"t": now, "cpu": cpu_total, "rx": rx, "tx": tx, "blk_r": blk_r, "blk_w": blk_w}
        return m
