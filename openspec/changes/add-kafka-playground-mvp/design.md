# Design: Kafka playground MVP

## Context
Goal: an interactive site that really starts resources in Docker, shows what they cost on the current machine,
and works for almost anyone with a single command. Principle: **no complications**.

## Architecture

```
Browser (Vue)  ──HTTP/WebSocket──►  queue-lab (FastAPI, in a container)  ──Docker socket──►  Docker Engine
                                          │                                                     │
                                          │  aiokafka (admin, offsets, lag)                     ├─ queuelab-kafka       (apache/kafka, KRaft)
                                          └──────────────────────────────────────────────────►  ├─ queuelab-producer    (Python or Node.js)
                                                                                                └─ queuelab-worker-N    (Python or Node.js consumers)
         App level:     Compose project `queue-lab`          (app container, network `queue-lab_default`)
         Sandbox level: Compose project `queue-lab-sandbox`  (Kafka, producer, workers; network `queue-lab-sandbox`)
```

- **One app container** serves both the API and the built Vue static files. No separate frontend server in production.
- Kafka and workers are **not** in `compose.yaml`; the backend creates them dynamically through the Docker Engine API
  into the separate Sandbox project and network.
- Compose only defines the app container, its network and a volume for SQLite.

## Decisions

### 1. Portability: only Docker is required
- Start command: `docker compose up --build` (later: prebuilt image from GHCR so no build is needed).
- The compose file mounts `/var/run/docker.sock`. This path also works for Linux containers on Docker Desktop
  (Windows WSL2 / macOS) and on Linux.
- Port defaults to `8080`, configurable through `QUEUELAB_PORT`; bound to `127.0.0.1` only.
- A **pre-flight check** at startup verifies: Docker reachable, Docker memory available above a minimum, images
  present or pullable. Problems are shown as a clear message in the UI instead of a stack trace.
- Docker itself must be installed: the one command cannot install Docker (it needs admin rights and differs per OS).
  The README links to the installers and the pre-flight check explains what is missing.
- **Docker is the only prerequisite, by design.** The goal is real containers with hard memory limits and isolation, and one
  installer provides the runtime, the limits and the Kafka image. Running everything as plain OS processes would need Java,
  Node.js and Python installed by the user and has no uniform memory limits across Windows, macOS and Linux, so it is rejected.
  Podman exposes a Docker-compatible API socket, so it may work later with the same code; this is not a goal of the MVP.
- **Two levels, two Compose projects, two networks.**

  | Level | Compose project | Network | Contains |
  |---|---|---|---|
  | App | `queue-lab` | `queue-lab_default` | the app container, the SQLite volume |
  | Sandbox | `queue-lab-sandbox` | `queue-lab-sandbox` | Kafka, producers, workers (created at runtime) |

  `docker compose up` creates only the App level. Containers of the Sandbox level are created later by the backend
  through the Docker API with the labels `com.docker.compose.project=queue-lab-sandbox`,
  `com.docker.compose.service=<name>` and `com.docker.compose.oneoff=False`, and attach to the sandbox network.
  In Docker Desktop they appear as a separate group from the app.
- The app container is attached to both networks. Workers send reports to it by the alias `queuelab-app` on the sandbox
  network. Sandbox containers cannot reach the app's volume or the Docker socket.
- Sandbox containers carry extra labels for grouping and cleanup: `queuelab.level=sandbox`,
  `queuelab.role=broker|producer|consumer`, `queuelab.queue=<name>`, `queuelab.run=<id>`, `queuelab.lang=python|node`.
- The backend reads its own container labels at startup to learn the App project name and derives the Sandbox name from it
  (`<project>-sandbox`), so a custom `docker compose -p` still works.
- Workers reach Kafka via `queuelab-kafka:9092` on the sandbox network. Kafka has only the internal listener
  unless the user asks to expose one.
- (To be verified in the first implementation spike on Docker Desktop and Linux.)

### 2. Real Kafka
`apache/kafka` in KRaft mode, one combined broker/controller node, created on first queue creation.
Default memory limit 1024 MB (JVM heap 512 MB). It is counted inside the resource budget like any other container.
One shared broker serves all queues in the MVP.

### 3. Resource budget is set by the user
The user enters a memory budget (MB). The backend shows the memory that Docker actually has available and:
- refuses to create containers that exceed the remaining budget;
- refuses to create containers when the real available memory (minus a safety reserve) is lower than requested,
  even if the budget would allow it;
- warns when the entered budget is larger than the memory currently available.

Note: on Docker Desktop, "available memory" is the memory of the Docker VM (WSL2 / HyperKit), which is
what containers can really use. The UI labels it accordingly and also shows host totals where the platform allows.

### 4. Worker image built through the Docker API
There is one worker image per language. Their `Dockerfile`s and code are bundled in the app image
(`workers/python/` and `workers/node/`). On first use of a language the backend builds `queuelab-worker-python` or
`queuelab-worker-node` through the Docker API and caches it. Build progress is streamed to the UI.
The user picks the language per worker group, and separately for the producer. Both languages are simple test processors
behind the same contract, so the same run can be repeated with Python and with Node.js and the results compared.
The contract between backend and worker (so more languages can follow):

| Direction | Contract |
|---|---|
| In (env) | `ROLE` (`producer` or `consumer`), `KAFKA_BOOTSTRAP`, `TOPIC`, `GROUP_ID`, `RUN_ID`, `SCENARIO`, `SCENARIO_PARAMS` (JSON), `MESSAGES`, `MSG_SIZE`, `REPORT_URL` |
| Out | `POST REPORT_URL` every second with processed count, per-partition last seq, error count, latency histogram |

### 5. Message format and correctness
The producer runs as **its own container** (the worker image of the chosen language with `ROLE=producer`), so its resource cost is
visible and limited like the workers'. Each message carries `run_id`, `partition`, a per-partition
monotonically increasing `seq`, the send timestamp and padding to reach the requested size.
Workers track the last `seq` per partition, which makes lost messages (gaps), duplicates and out-of-order
delivery detectable without storing every id. Latency = worker receive time minus send timestamp (same machine clock).

### 6. Processing scenarios
Chosen per run in the UI: `echo` (count only), `delay` (sleep N ms per message), `cpu` (hash loop of N iterations).
New scenarios are added by registering a name and parameters in both backend schema and worker.

### 7. Live monitoring
- Container stats from the Docker stats stream (CPU, memory, network, block I/O), aggregated per second.
- Host/Docker memory: total, available, used by queue-lab containers, budget.
- Kafka: messages/s, per-partition end offset, committed offset, **lag**, consumer-to-partition assignment, rebalance events.
- Logs: live container log streaming.
- Delivered to the browser over WebSocket (one multiplexed connection); charts are rendered in the frontend.

### 8. Storage
SQLite file on a named volume: runs, per-second samples, settings. Survives restarts.

### 9. Safe cleanup
All dynamically created resources carry `queuelab=true`, `queuelab.level=sandbox`, the Sandbox Compose project labels
and the name prefix `queuelab-`.
- A "Reset" action in the UI removes only containers with `queuelab.level=sandbox`.
- `docker compose down` stops the app; because the Sandbox is a different project, the backend removes the Sandbox itself on
  graceful shutdown (SIGTERM). The compose file sets `stop_grace_period: 30s` to leave time for this. The behavior is controlled by a
  setting "remove lab containers when the app stops" (on by default).
- Fallback when the app was killed: a `cleanup` script runs
  `docker rm -f $(docker ps -aq --filter label=queuelab.level=sandbox)` and removes the sandbox network.
- On startup the backend reconciles: containers with `queuelab.level=sandbox` are adopted and shown in the UI.

## Technology Choices
Verified on 2026-11-08 against PyPI, the npm registry, Docker Hub and nodejs.org. All come from the official
or de-facto maintainers' repositories. Pin exact versions in lockfiles and image tags.

### Backend (Python)
| Part | Choice | Version | Source |
|---|---|---|---|
| Runtime | Python, image `python:3.14-slim-trixie` | 3.14.x | Docker Official Image |
| Package manager | `uv` | 0.13.x | astral-sh/uv |
| Web framework | FastAPI | 0.162.x | github.com/fastapi/fastapi |
| Server | uvicorn | 0.54.x | Kludex/uvicorn |
| Validation / settings | pydantic, pydantic-settings | 2.14.x / 2.16.x | pydantic/pydantic |
| Docker Engine API | `docker` (official Docker SDK for Python) | 7.2.0 | github.com/docker/docker-py |
| Kafka admin, offsets, lag | `aiokafka` | 0.15.0 (Python 3.11–3.15) | github.com/aio-libs/aiokafka |
| Database | SQLAlchemy (async) + `aiosqlite` | 2.1.x / 0.22.x | sqlalchemy/sqlalchemy |
| Lint, format | `ruff` | 0.17.x | astral-sh/ruff |
| Tests | `pytest`, `pytest-asyncio`, `httpx` | 9.2 / 1.4 / 0.28 | pytest-dev |

Notes: `docker` is synchronous, so calls run in worker threads (`asyncio.to_thread`) and streams are bridged to async.
`aiodocker` is an async alternative from aio-libs (0.27.0) and is evaluated in the spike (task 0.6).

### Queue
- `apache/kafka:4.4.0` (official Apache image, KRaft only; ZooKeeper no longer exists in 4.x).
- **Always pin the tag.** The `latest` tag was last published in June 2026 (4.3.1) and is older than 4.4.0 and 4.3.2.
- `apache/kafka-native` (GraalVM native build, same versions) starts faster and uses less memory but is documented
  by Apache as intended for local development and testing. It is offered as an **optional "light mode"** for small machines;
  the default is the regular JVM image.

### Workers (Python)
- Runtime: `python:3.14-slim-trixie`.
- Kafka client: **`confluent-kafka` 2.16.x** (github.com/confluentinc/confluent-kafka-python), Confluent's official client on librdkafka.
  Prebuilt wheels are published, so no compiler is needed in the image (confirm in spike 0.5).

### Workers (Node.js)
- Runtime: Node.js **26** (entered LTS "Lithium" on 2026-10-28) or **24** "Krypton" LTS; image `node:26-slim` / `node:24-slim`.
- Kafka client (chosen): **`@confluentinc/kafka-javascript` 1.11.x** (github.com/confluentinc/confluent-kafka-javascript),
  Confluent's official client built on librdkafka. Highest throughput, KafkaJS-compatible API. It uses native bindings, so the spike
  (task 0.5) must confirm that prebuilt binaries load in the worker image on both amd64 and arm64 (Apple Silicon) with no compiler.
- Fallback if the native binary is a problem on some platform: `@platformatic/kafka` 2.15.x (github.com/platformatic/kafka),
  pure JavaScript, requires Node >= 22.22 or >= 24.6.
- **Not `kafkajs`**: its last release was in February 2023 and it is no longer maintained.

### Frontend (Vue)
| Part | Choice | Version |
|---|---|---|
| Framework | Vue (stable `latest` tag) | 3.5.45 |
| Next major (optional experiment) | Vue 3.6 release candidate with **Vapor Mode**, signals-based reactivity | 3.6.0-rc.14 |
| Build tool | Vite (Rolldown-based) + `@vitejs/plugin-vue` | 8.3.x / 6.0.x |
| Language | TypeScript + `vue-tsc` | 7.0.x (native compiler) / 3.3.x, see spike 0.4 |
| State, routing | Pinia, Vue Router | 4.1.x, 5.4.x |
| Utilities | VueUse, TanStack Vue Query | 15.0.x, 5.107.x |
| Tests | Vitest | 5.1.x |
| Lint | ESLint, Oxlint | 10.14.x, 1.92.x |

### Visual layer
- **Styling:** Tailwind CSS 4.3 (`@tailwindcss/vite`), dark mode first.
- **Components:** shadcn-vue 2.9 on Reka UI 2.11 (components are copied into the repo, no heavy UI runtime).
- **Icons:** `@lucide/vue` (the older `lucide-vue-next` is deprecated).
- **Animation:** `motion-v` 2.x for smooth transitions and number counters.
- **Charts:** Apache ECharts 6.1 through `vue-echarts` 8.3 (tree-shaken): live line/area charts, gauges, and a **heatmap of lag per partition**.
  `uPlot` 1.6 is an optional ultra-light renderer for tiny sparklines (its last release was March 2025).
- **Live topology canvas:** Vue Flow (`@vue-flow/core` 1.48) drawing Producer → Partitions → Consumer workers with animated edges;
  edge animation speed follows real throughput, node borders follow memory pressure.

## Risks / Trade-offs
- **Docker socket = root.** Mitigation: bind to localhost only, document clearly, no auth in MVP.
- **Kafka cold start** takes 10–30 s. Mitigation: readiness probe and visible progress in the UI.
- **Image pull/build on first run** is slow. Mitigation: progress streaming; prebuilt images later.
- **Docker Desktop memory is a VM limit**, not the whole PC. Mitigation: label clearly, link to how to raise it.
- **A memory-limited producer container** can itself become the bottleneck at high rates. This is intentional
  and visible in the monitoring, but the UI explains it so it is not mistaken for a broker limit.

## Open Questions
Resolved with the author:
- Memory budget is entered by the user (not a fixed percentage); the app warns when it exceeds available memory.
- Real Apache Kafka (not Redpanda).
- Processing scenario is chosen per run so scenarios can be compared.
- Monitor everything that is observable.
- The producer is its own container.
- Limits: at most 64 partitions per queue and 1,000,000 messages per run.
- Kafka is not exposed to the host in the MVP.
- The UI is English-only and dark-only (no language switcher, no light theme) in the MVP.
- Images are built locally from the sources on first start (`docker compose up --build`); no registry publishing in the MVP.
- Technology choices: Vue 3.5 stable (Vapor Mode later), the official `docker` SDK, `@confluentinc/kafka-javascript` in workers,
  and an optional `apache/kafka-native` light mode.

Still open:
1. Should the budget field show a suggested value (for example 50% of available memory) or start empty?
2. Which additional scenarios come next (random failures, memory growth, slow partition)?
3. Prebuilt images on GHCR can be added later to shorten the first start; the MVP builds everything locally.
