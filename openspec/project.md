# Project Context

## Purpose
**queue-lab** is a local web playground for experimenting with message queues.
Anyone clones the repository, runs **one command**, opens `http://localhost:<port>`
and from the browser can:

1. create a queue (Kafka first) with a chosen number of partitions;
2. spin up one or more real Docker containers (Python and Node.js workers in the MVP) with chosen memory limits;
3. send a chosen number of test messages through the queue;
4. watch the simple test processing and see statistics and results.

The guiding principle is **no complications**: one command to start, zero manual setup,
everything else is created dynamically from the UI.

## Tech Stack
Exact versions and their sources are listed in `openspec/changes/add-kafka-playground-mvp/design.md`
(section "Technology Choices", verified 2026-11-08).
- **Backend:** Python 3.14, FastAPI, the official Docker SDK for Python (`docker`) for the Docker Engine API,
  `aiokafka` for Kafka admin, offsets and lag, SQLAlchemy + SQLite for run history, managed with `uv`.
- **Frontend:** Vue 3.5 + Vite 8 + TypeScript, Pinia, Tailwind CSS 4, shadcn-vue; Apache ECharts for charts and
  Vue Flow for the live topology canvas (built into static files and served by the backend).
- **Queue (MVP):** Apache Kafka 4.x (official `apache/kafka` image, pinned tag) in KRaft mode, single broker, started on demand.
- **Workers (MVP):** the user picks the language per worker group; Python (`confluent-kafka`) and Node.js LTS
  (`@confluentinc/kafka-javascript`) are both available. Both are plain test processors behind the same worker contract.
  Workers are started on demand.
- **Packaging:** Docker + `docker compose`. The only host requirement is Docker.

## Project Conventions

### Code Style
- Python: `ruff` for lint and format, type hints everywhere, `pydantic` models for API schemas.
- TypeScript: strict mode, `<script setup lang="ts">`, ESLint + Prettier.
- UI: English only, dark theme only (MVP). No i18n layer yet.
- Names: snake_case in Python, camelCase in TypeScript, kebab-case for files and Docker resources.

### Architecture Patterns
- The backend is the only component that talks to the Docker Engine API.
- Two levels: the **App** (Compose project `queue-lab`) and the **Sandbox** (Compose project `queue-lab-sandbox`,
  its own network). All containers created at runtime belong to the Sandbox, carry `queuelab.level=sandbox` plus
  `queuelab.role`, `queuelab.queue`, `queuelab.run`, `queuelab.lang`, and are named with the `queuelab-` prefix.
  This makes cleanup safe and never touches foreign containers.
- Queue types are behind an adapter interface so RabbitMQ, NATS and others can be added later
  without changing the API or UI contracts.
- Worker languages are behind a worker-image contract (environment variables in, metrics out)
  so Go, Python, Java and others can be added later.

### Testing Strategy
- Backend: `pytest` with the Docker client mocked for unit tests; a small number of integration
  tests against a real Docker daemon, marked `integration`.
- Frontend: `vitest` for stores and components.
- Resource-budget logic must be covered by unit tests, because it protects the user's machine.

### Git Workflow
- Trunk-based: short-lived branches, small pull requests into `main`.
- Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`).
- Spec-driven: behavior changes start as an OpenSpec change under `openspec/changes/`.

## Domain Context
- **Queue:** a logical queue the user creates in the UI. For Kafka this is a topic with N partitions
  plus the broker that serves it.
- **Worker:** a container that consumes messages from a queue and performs the simple test processing.
- **Run:** one execution of a load test: N messages sent through a queue, processed by the workers,
  with statistics recorded.
- **Resource budget:** the amount of memory the app is allowed to allocate to containers,
  computed from the memory actually free on the machine.

## Important Constraints
- The app must never exhaust the user's machine. Container memory is limited and checked
  against a budget **before** any container is created.
- The backend needs access to the Docker socket, which is equivalent to root on the host.
  The app is therefore **local-only**: it binds to `localhost` and has no authentication in the MVP.
- Must work on Windows, macOS and Linux through Docker Desktop / Docker Engine.
- Starting the whole system takes one command and no prior configuration.

## External Dependencies
- Docker Engine API (via the mounted Docker socket).
- Docker Hub images: `apache/kafka`, `python` (backend image and Python worker image), `node` (Node.js worker image).
