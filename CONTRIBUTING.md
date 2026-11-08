# Contributing

Thank you for helping. The lab is small on purpose: one command to start, everything in Docker, nothing to install.

## Run it

```bash
docker compose up --build        # http://localhost:8080
```

## Test it (all through Docker, nothing to install)

```bash
docker build --target backend-test .                              # ruff + pytest
docker build --target web-test .                                  # vue-tsc + vitest
docker build -f workers/python/Dockerfile.test workers/python
docker build -f workers/node/Dockerfile.test workers/node
docker build -f workers/go/Dockerfile.test workers/go
docker build -f workers/rust/Dockerfile.test workers/rust
docker build -f workers/java/Dockerfile.test workers/java
```

## Rules that keep the comparison fair

- **Every constant lives in one place**: `backend/app/config.py`. Kafka client settings are never written in a worker.
- **Every language has the same inputs and outputs**: the environment variables of `backend/app/contract.py` and the report
  fields in it. A test checks all five languages.
- **Every worker does the same steps per message** (see the Code page). A language may implement them in the way that is
  fastest for it, but not skip one.
- Prefer a pure function with a test over logic inside a loop.

## Where to change things

| You want to add | Change these |
|---|---|
| A **language** | `workers/<lang>/` (Dockerfile, Dockerfile.test, consumer, producer, tests), `config.LANGS`, `models.Lang`, `catalog.REGION_FILES` and `READING`, `frontend/src/langs.ts` |
| A **scenario** (what a worker does with a message) | `models.ScenarioSpec`, `catalog.SCENARIOS` (the text), the `@scenario` part of every worker's consumer, `SCENARIO_LOOK` in `SetupPage.vue` |
| An **option of the run form** | `backend/app/models.py` (the field and its limits), `contract.py` (the environment variable), every worker's settings file, `frontend/src/pages/SetupPage.vue`. The contract tests fail until all five languages read it |
| A **Kafka client setting** | `config.PRODUCER_CONF` or `CONSUMER_CONF`, its note in `catalog.CONF_NOTES`; if Java names it differently, `workers/java/.../KafkaConf.java` |
| A **metric** | `backend/app/analysis.py` (`resource_stats` or `run_metrics`, then `COMPARE_ROWS`), then show it in `frontend/src/pages/RunDetailPage.vue` |
| A **page** | `frontend/src/pages/`, the route in `frontend/src/router.ts`, the menu in `App.vue` |
| A **constant** (limit, timeout, name) | `backend/app/config.py`, nowhere else |

Where things live: `backend/app/` is the app (one file per job, see the README), `workers/` has one folder per language with the same file roles, `frontend/src/` has `pages/`, `components/`, `stores/` and small pure helpers with a `.test.ts` next to each.
## Adding a language

Create `workers/<lang>/` with a `Dockerfile`, a `Dockerfile.test`, a consumer, a producer and unit tests, add the language to
`config.LANGS`, `models.Lang`, `catalog.REGION_FILES`/`READING` and `frontend/src/langs.ts`. The contract tests tell you
what is missing.

## Style

English in code, comments and the UI. Comments say why, not what. `ruff` for Python, `vue-tsc` for the frontend.