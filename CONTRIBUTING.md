# Contributing

Thank you for helping. The lab is small on purpose: one command to start, everything in Docker, nothing to install.

## Run it

```bash
docker compose up --build        # http://localhost:8080
```

## Test it (all through Docker, nothing to install)

```bash
docker build --target backend-test .                              # mistakes and style, unit tests
docker build --target web-test .                                  # type check, unit tests
docker build -f workers/python/Dockerfile.test workers/python
docker build -f workers/node/Dockerfile.test workers/node
docker build -f workers/go/Dockerfile.test workers/go
docker build -f workers/rust/Dockerfile.test workers/rust
docker build -f workers/java/Dockerfile.test workers/java
```

Each of these builds does several checks one after another; the build fails at the first one that finds a problem. What
every check is for, and the tool that does it:

| Part | Formatting and style | Mistakes and suspicious code | Types | Unit tests |
|---|---|---|---|---|
| Backend (Python) | style and mistakes (ruff) | (ruff) | (not checked) | pytest |
| Frontend (Vue, TypeScript) | formatting (Prettier, run by hand: `npx prettier --write`) | (not checked) | vue-tsc | vitest |
| Python worker | style and mistakes (ruff) | (ruff) | (not checked) | pytest |
| Node.js worker | formatting and style (Biome) | (Biome) | tsc | node --test |
| Go worker | formatting (gofmt) | go vet | the compiler | go test |
| Rust worker | formatting (rustfmt) | clippy | the compiler | cargo test |
| Java worker | formatting (google-java-format, through Spotless) | compiler warnings as errors (javac -Xlint) | the compiler | JUnit, run by Maven |
| Whole app in a browser (`e2e/`) | (not checked) | (not checked) | (not checked) | Playwright, against the running lab |

To fix the formatting yourself: `ruff check --fix` (Python), `gofmt -w .` (Go), `cargo fmt` (Rust), `npm run format` in
`workers/node` (Biome), `mvn spotless:apply` in `workers/java` (google-java-format).

## Checks before a pull request

CI runs the checks above, builds every image, and runs these, which you can run the same way (the end-to-end tests
are described in the next section):

```bash
# leaked secrets in the whole history (gitleaks)
docker run --rm -v "$PWD:/repo" zricethezav/gitleaks:v8.30.1 detect --source /repo --no-banner --redact
# known vulnerabilities in the dependencies (npm audit, pip-audit)
docker run --rm -v "$PWD/frontend:/app" -w /app node:24-slim npm audit --omit=dev --audit-level=high
docker run --rm -v "$PWD/backend:/app" -w /app python:3.14-slim-trixie sh -c "pip install -q pip-audit && pip-audit -r requirements.txt"
# mistakes in the Dockerfiles (hadolint)
docker run --rm -i hadolint/hadolint:v2.15.1 hadolint --failure-threshold warning - < Dockerfile
# mistakes in the workflow files (actionlint)
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.12
```

## End-to-end tests and screenshots

The tests in `e2e/` (Playwright) open the real app in a browser. They need the lab running, and the browser itself runs in
a container, so nothing is installed on your machine:

```bash
docker compose up --build -d
docker compose -f compose.e2e.yaml run --rm e2e                      # fast tests: every page, the form, security (about a minute)
docker compose -f compose.e2e.yaml run --rm e2e npm run test:slow    # slow test: a real run in Go and Python (a few minutes)
docker compose -f compose.e2e.yaml run --rm e2e npm run screenshots  # new screenshots into docs/screenshots
```

- Fast tests are the files `*.fast.spec.ts`, slow ones `*.slow.spec.ts`; the helpers are in `e2e/tests/lab.ts`.
- The test container joins the sandbox network of the lab because the app only answers to `localhost` and to its own
  network name. That is also why the security tests can check that a foreign `Host` or `Origin` is refused.
- A test checks what a person sees or what the app promises, not how it is built. Do not weaken a test to make it pass.
- After a change to a screen, take the screenshots again so the README does not show an old UI.

## How to work

Work on a branch (`feature/...`, `fix/...`), commit in small steps with a message that says what changed and why, and
open a pull request into `main`. Do not commit secrets, `.env` files or run history (`*.db`).

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
| A **scenario** (what a worker does with a message) | `models.ScenarioSpec`, `catalog.SCENARIOS` (the text), the `@scenario` part of every worker's consumer, `LOOK` in `frontend/src/components/setup/ScenarioCard.vue` |
| An **option of the run form** | `backend/app/models.py` (the field and its limits), `contract.py` (the environment variable), every worker's settings file, the matching section in `frontend/src/components/setup/` (the form state is in `composables/useRunForm.ts`). The contract tests fail until all five languages read it |
| A **Kafka client setting** | `config.PRODUCER_CONF` or `CONSUMER_CONF`, its note in `catalog.CONF_NOTES`; if Java names it differently, `workers/java/.../KafkaConf.java` |
| A **metric** | `backend/app/analysis.py` (`resource_stats` or `run_metrics`, then `COMPARE_ROWS`), then show it in `frontend/src/pages/RunDetailPage.vue` |
| A **page** | `frontend/src/pages/`, the route in `frontend/src/router.ts`, the menu in `App.vue` |
| A **constant** (limit, timeout, name) | `backend/app/config.py`, nowhere else |

Where things live: `backend/app/` is the app (one file per job, see the README), `workers/` has one folder per language with the same file roles, `frontend/src/` has `pages/`, `components/`, `stores/` and small pure helpers with a `.test.ts` next to each; `e2e/` has the browser tests.

## Adding a language

Create `workers/<lang>/` with a `Dockerfile`, a `Dockerfile.test`, a consumer, a producer and unit tests, add the language to
`config.LANGS`, `models.Lang`, `catalog.REGION_FILES`/`READING` and `frontend/src/langs.ts`. The contract tests tell you
what is missing.

## Style

English in code, comments and the UI. Comments say why, not what. The tool of every language is in the table of checks
above: `ruff` for Python, Prettier and `vue-tsc` for the frontend (`npx prettier --write src`), Biome, gofmt, rustfmt and
google-java-format for the other workers.
