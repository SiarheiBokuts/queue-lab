# Project Context

## Purpose
**queue-lab** is a local web playground for experimenting with message queues.
Anyone clones the repository, runs **one command**, opens `http://localhost:<port>`
and from the browser can:

1. create a queue (Kafka first) with a chosen number of partitions;
2. spin up one or more real Docker containers (Python, Node.js, Java, Go or Rust workers) with chosen memory limits;
3. send a chosen number of test messages through the queue;
4. watch the simple test processing and see statistics and results.

The guiding principle is **no complications**: one command to start, zero manual setup,
everything else is created dynamically from the UI.

## Tech Stack
Exact versions are pinned in `backend/requirements.txt`, `frontend/package.json`, `workers/*/` and the Dockerfiles; the
choices and their sources are in `openspec/changes/add-kafka-playground-mvp/design.md` (section "Technology Choices").
- **Backend:** Python (the `python:3.15-slim-trixie` image), FastAPI with uvicorn, the official Docker SDK for Python
  (`docker`) for the Docker Engine API, `aiokafka` for Kafka admin, offsets and lag, the standard `sqlite3` module for
  run history. Dependencies are installed with `pip` from `backend/requirements.txt`.
- **Frontend:** Vue 3.5 + Vite 8 + TypeScript, Pinia, Vue Router, Tailwind CSS 4; Apache ECharts for charts, Vue Flow for
  the live topology canvas, floating-vue for tooltips, Shiki for the code (the colours of VS Code). It is built into
  static files and served by the backend.
- **Queue:** Apache Kafka 4.x (official `apache/kafka` image, pinned tag; `apache/kafka-native` in the light mode) in
  KRaft mode, single broker, started on demand.
- **Workers:** the user picks the language per worker group: Python (`confluent-kafka`), Node.js LTS
  (`@confluentinc/kafka-javascript`), Java (Apache Kafka client), Go and Rust (librdkafka). All are plain test
  processors behind the same worker contract. Workers are started on demand.
- **Packaging:** Docker + `docker compose`. The only host requirement is Docker.

## Project Conventions

### Code Style
- Python (backend and worker): `ruff`, type hints, `pydantic` models for API schemas.
- Frontend: TypeScript strict mode, `<script setup lang="ts">`, Prettier (`frontend/.prettierrc.json`), `vue-tsc`.
- Other workers: Biome (Node.js), gofmt and go vet (Go), rustfmt and clippy (Rust), google-java-format (Java).
- UI: English only, dark or light theme. No i18n layer yet.
- Names: snake_case in Python, camelCase in TypeScript, kebab-case for files and Docker resources.

### Architecture Patterns
- The backend is the only component that talks to the Docker Engine API.
- Two levels: the **App** (Compose project `queue-lab`) and the **Sandbox** (Compose project `queue-lab-sandbox`,
  its own network). All containers created at runtime belong to the Sandbox, carry `queuelab.level=sandbox` plus
  `queuelab.role`, `queuelab.queue`, `queuelab.run`, `queuelab.lang`, and are named with the `queuelab-` prefix.
  This makes cleanup safe and never touches foreign containers.
- Queue types are behind an adapter interface so RabbitMQ, NATS and others can be added later
  without changing the API or UI contracts.
- Worker languages are behind a worker-image contract (environment variables in, a JSON report out)
  so other languages can be added later.
- Every constant lives in `backend/app/config.py`; Kafka client settings are sent to the workers and never written in one.

### Testing Strategy
- Backend: `pytest` with the Docker client mocked; the calculations (memory budget, comparison, counting of lost
  messages) are pure functions with unit tests, because the budget protects the user's machine.
- Frontend: `vitest` for the pure helpers (formats, capacity, theme, ...).
- Workers: unit tests of the pure helpers in every language, and a contract test in the backend that checks that all five
  read the same environment variables and send the same report fields.
- End to end: Playwright tests (`e2e/`) against the running app: pages, the form, the security checks, and one real run
  in two languages.
- CI also builds every image, scans for secrets (gitleaks), known vulnerabilities (npm audit, pip-audit), Dockerfiles
  (hadolint) and workflow files (actionlint).

### Git Workflow
- Short-lived branches (`feature/...`, `fix/...`), small pull requests into `main`.
- A commit message says what changed and why.
- Spec-driven: behavior changes start as an OpenSpec change under `openspec/changes/`.

## Domain Context
- **Queue:** a logical queue the user creates in the UI. For Kafka this is a topic with N partitions
  plus the broker that serves it.
- **Worker:** a container that consumes messages from a queue and performs the simple test processing.
- **Run:** one execution of a load test: N messages sent through a queue, processed by the workers,
  with statistics recorded.
- **Resource budget:** the amount of memory the app is allowed to allocate to containers. It is chosen on the first
  visit (the default is half of the memory free in Docker) and can be changed in Settings.

## Important Constraints
- The app must never exhaust the user's machine. Container memory is limited and checked
  against a budget **before** any container is created. The numbers of a run are not capped; a form warning says what to
  expect from a big one (only the largest numbers a worker can read are refused).
- Every sender and worker gets the same share of the cores (cores minus one, divided equally, at most four each).
- A run is stopped after 30 minutes.
- The backend needs access to the Docker socket, which is equivalent to root on the host.
  The app is therefore **local-only**: it binds to `localhost` and has no authentication.
- Must work on Windows, macOS and Linux through Docker Desktop / Docker Engine.
- Starting the whole system takes one command and no prior configuration.

## External Dependencies
- Docker Engine API (via the mounted Docker socket).
- Docker Hub images: `apache/kafka`, `python` (backend image and Python worker image), `node` (frontend build and Node.js
  worker image), `golang`, `rust`, `maven` and `eclipse-temurin` (the Go, Rust and Java workers).
