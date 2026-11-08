# Change: Kafka playground MVP

## Why
Learning how message queues behave (partitions, consumers, throughput, lag, resource cost) normally requires
hand-written Docker Compose files, scripts and several tools. We want an **interactive lab**: one command to
start, a web page on `localhost`, and real containers created dynamically from the UI, with the real load
they put on the machine visible live.

## What Changes
- Add a Vue web UI (English only, dark theme only) and a FastAPI backend that runs in Docker and controls the local Docker Engine.
- Add **two levels** of Docker resources: the **App** (Compose project `queue-lab`) and the **Sandbox**
  (Compose project `queue-lab-sandbox`, own network) for everything the user creates from the UI.
- Add **queue management**: create a Kafka queue (topic with a chosen number of partitions); the real Apache Kafka broker
  container is started on demand, with an optional light mode.
- Add a user-defined **resource budget** (memory) and an admission check that stops the app from taking over
  the machine.
- Add **worker containers**: create one or more consumer containers (Python or Node.js, chosen per group) with a chosen count and
  memory limit per container, built through the Docker API.
- Add **load runs**: choose how many messages to send, the message size and a processing scenario
  (echo, delay, CPU load), then run. The producer runs in its own container.
- Add **live monitoring** of everything observable: per-container CPU/memory/network/disk, host memory vs.
  budget, throughput, lag per partition, latency, partition assignment, rebalances, container logs, a live topology
  canvas and a per-container detail page.
- Add **run results**: statistics saved per run and per container in SQLite, history and side-by-side comparison.
- Add **one-command deployment** with `docker compose up --build`, with pre-flight checks and safe cleanup.

## Impact
- Affected specs (all new): `local-deployment`, `queue-management`, `resource-budget`, `worker-containers`,
  `load-runs`, `live-monitoring`, `run-results`, `web-ui`.
- Affected code: new repository layout (`backend/`, `frontend/`, `workers/python/`, `workers/node/`, `compose.yaml`, `scripts/cleanup`).
- Security: the backend mounts the Docker socket (root-equivalent). The app is local-only and binds to `127.0.0.1`.
  Sandbox containers cannot reach the app's volume or the Docker socket.

## Out of Scope (future changes)
- Other brokers (RabbitMQ, NATS, Redis Streams, Redpanda) and multi-broker Kafka clusters.
- Worker languages other than Python and Node.js (Go, Java, .NET); user-supplied worker code.
- Chaos testing (kill or pause containers, network faults).
- Authentication, multi-user use, remote deployment.
- Prebuilt images in a registry, light theme, UI translations.

## Status
Implemented and released as version 1.0.0 (see CHANGELOG.md). Beyond the first idea, the lab now covers five worker languages, a scored comparison of languages, production-style handling of messages (lanes, ordering per user), hard sustained load, compression and a network view. The list of what is done is in tasks.md.
