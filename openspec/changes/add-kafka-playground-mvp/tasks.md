# Tasks

> Prototype status (2026-11-08): checked items are implemented and were exercised end to end on Windows with Docker Desktop (WSL 2). Not done yet: 0.1 lockfiles, 0.4 (vue-tsc is not used), 0.7 (kafka-native untested), 1.4 full pre-flight, 4.4 light mode verification, rebalance events (7.3), unit tests (3.3, 9.2), CI (9.3). Stats code is planned to be split into a pure module (see 9.5).
## 0. Technology spike (before building anything else)
- [ ] 0.1 Pin versions from design.md "Technology Choices"; commit lockfiles (`uv.lock`, `package-lock.json`)
- [x] 0.2 Verify a dynamically created container shows up as a separate `queue-lab-sandbox` group next to `queue-lab` (labels + network) on Docker Desktop and Linux
- [x] 0.3 Verify `apache/kafka:4.4.0` starts in KRaft mode with a memory limit and the internal listener only
- [ ] 0.4 Verify `vue-tsc` works with the chosen TypeScript major version; otherwise pin the previous major
- [x] 0.5 Verify `@confluentinc/kafka-javascript` loads its prebuilt binary on the chosen Node.js LTS in the worker image (amd64 and arm64, no compiler); fall back to `@platformatic/kafka` if not
- [x] 0.6 Verify the official sync `docker` SDK handles stats, logs and image-build streaming through threads; `aiodocker` is the fallback
- [ ] 0.7 Verify `apache/kafka-native` starts with the same configuration for the optional light mode

## 1. Repository and deployment
- [x] 1.1 Create layout: `backend/`, `frontend/`, `workers/python/`, `workers/node/`, `scripts/`, `compose.yaml`, `README.md`
- [x] 1.2 Multi-stage `Dockerfile`: build Vue, then FastAPI image that serves the static files
- [x] 1.3 `compose.yaml`: app service, its network, Docker socket mount, SQLite volume, localhost-only port, `stop_grace_period: 30s`
- [ ] 1.4 Pre-flight check endpoint and UI banner (Docker reachable, memory, images)
- [x] 1.5 README: one-command quick start, Windows/macOS/Linux notes, security note about the Docker socket

## 2. Backend foundation
- [x] 2.1 FastAPI app, settings (`QUEUELAB_PORT`, safety reserve), health endpoint
- [x] 2.2 Docker service layer: containers, network, image build, stats stream, logs, labels
- [x] 2.3 SQLite storage layer (runs, samples, settings)
- [x] 2.4 WebSocket hub for live events

## 3. Resource budget
- [x] 3.1 Read available/total memory from Docker and compute used-by-queue-lab
- [x] 3.2 Budget setting (MB) with validation and warning when above available memory
- [x] 3.3 Admission check used by every container-creating operation (unit tested)

## 4. Queue management
- [x] 4.1 Start Kafka (`apache/kafka`, KRaft) on demand with readiness probe
- [x] 4.2 Create / list / delete queue (topic + partitions) via aiokafka admin
- [x] 4.3 Queue API and UI form
- [ ] 4.4 Optional "light mode" switch that starts `apache/kafka-native` instead of the JVM image

## 5. Worker containers
- [x] 5.1 Python worker image (`confluent-kafka`) with `ROLE=consumer|producer` implementing the worker contract and scenarios `echo`, `delay`, `cpu`
- [x] 5.2 Node.js worker image (`@confluentinc/kafka-javascript`) implementing the same contract and scenarios
- [x] 5.3 Build worker images through the Docker API with streamed progress, one image per language, built on first use
- [x] 5.4 Create N workers with per-container memory limit; stop and remove workers
- [x] 5.5 Warning when consumers exceed partitions (idle workers)
- [x] 5.6 Language selector in the UI (Python, Node.js) per worker group and for the producer; same results format for both

## 6. Load runs
- [x] 6.1 Producer container (budgeted, removed after the run): send N messages of size S with per-partition sequence numbers
- [x] 6.2 Run lifecycle: pending, running, completed, cancelled, failed
- [x] 6.3 Worker report endpoint and aggregation (processed, lost, duplicates, latency percentiles)
- [x] 6.4 Run configuration UI and one-button "create everything and run" flow

## 7. Live monitoring
- [x] 7.1 Container CPU/memory/network/disk charts
- [x] 7.2 Host memory vs. budget gauge
- [x] 7.3 Throughput, lag per partition, partition assignment, rebalance events
- [x] 7.4 Live container logs panel
- [x] 7.5 Container detail page (charts, partitions, limits, restarts, OOM events, logs)

## 8. Results
- [x] 8.1 Persist run summary and per-second samples
- [x] 8.2 Run history list, detail view, side-by-side comparison of two runs
- [x] 8.3 Per-container samples and a per-container view of finished runs, including peaks and OOM events

## 8b. Web UI shell
- [x] 8b.1 Vue app skeleton: Vite, Pinia, Vue Router, Tailwind, shadcn-vue, dark theme only, English texts
- [x] 8b.2 Screens: Dashboard, Create setup, Container detail, Runs, Settings
- [x] 8b.3 Live data client (single WebSocket with automatic reconnect) and error banners

## 9. Cleanup and quality
- [x] 9.1 Reset action removing only `queuelab.level=sandbox` resources; reconcile on startup
- [x] 9.4 Remove the sandbox on graceful shutdown (setting, `stop_grace_period: 30s`) and a `cleanup` script as fallback
- [ ] 9.5 Split container stats collection (docker_service) into a pure, unit-tested stats module
- [ ] 9.2 Unit tests (budget, run aggregation), integration test against real Docker
- [ ] 9.3 CI: lint, tests, image build

## 10. Run report and code visibility (added after the first prototype review)
- [x] 10.1 Message window timeline, verdict, load distribution and per-partition peak lag
- [x] 10.2 Resource analysis restricted to the message window, per-container zoom and cost per 1k messages
- [x] 10.3 Scenario descriptions in the run form and a Code page with the real worker source
- [x] 10.4 Worker images are rebuilt when their sources change (source hash label)
- [ ] 10.5 Unit tests for window and insight calculations
- [x] 10.6 Worker code split by role (producer, consumer/handler, settings, reporting, pure helpers) in both languages; run analysis split into analysis.py
- [x] 10.7 Identical Kafka client settings for every language from one place (config.py), enforced by tests
- [x] 10.8 Constants in one place (config.py); cross-platform cleanup command instead of Windows and shell scripts
- [x] 10.9 Unit tests for backend, workers (Python, Node.js) and frontend; type checks and lint

## Version 1.0 (implemented)

- [x] Five worker languages (Python, Node.js, Java, Go, Rust) behind one contract with identical Kafka settings
- [x] Comparison of languages: all by default, random order, pause, scored winner, every metric side by side
- [x] Hard load: up to 50M messages, target rate, several senders, message size, compression, random or compressible content
- [x] Production-style workers: one loop, N lanes, ordering per user, computing on all cores
- [x] Messages with a key (user); a user always lands in one partition
- [x] Network needed against actual, approximate size before the run, what compression saved
- [x] Scenarios echo, delay, CPU; no custom user code
- [x] Live dashboard: topology, progress, time to catch up, bottleneck, messages waiting per partition, stable tables
- [x] Report: window, highlights, verdict, resources, latency in readable units, network
- [x] Code page: main code, per-message work, how each language reads and computes, shared Kafka settings explained
- [x] Settings: memory budget, CPU of Docker, queues, cleanup
- [x] Clear states of containers, clear message when a container runs out of memory
- [x] Interface: icons, tooltips, grouped numbers, spinners and loading states, animations, sticky menu, logo, glossary
- [x] Tests for every layer; fairness tests (variables, report fields, settings, compilation of Python workers, version)
- [x] Documentation, changelog, screenshots