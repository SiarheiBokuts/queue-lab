# Changelog

## 1.0.0 (2026-11-08)

First release.

**Lab**
- Workers and senders in Python, Node.js, Java, Go and Rust behind one contract, with identical Kafka client settings.
- Run a test from the web page: queue, partitions, workers, senders, load (messages, target rate, size, compression),
  scenario (echo, delay, JSON).
- Every worker runs the same loop: read a batch (up to N messages), group it by user, handle the groups at the same
  time (the messages of one user one after another), commit the offsets, read the next batch.
- Every worker and sender gets the same share of the cores and the same memory limit.
- No caps on the numbers of a run: the form warns about what to expect instead (data volume, the time limit, a message
  above the broker limit, ...). Only the memory budget and the largest number a worker can read are refused.

**Results**
- Compare languages: the same test per language in random order, every number side by side, no score and no winner.
- Lost and repeated messages are counted exactly: workers report the message numbers they received as ranges and the
  backend merges them.
- The report starts with three answers: did it finish, was anything lost, how fast. It also counts messages handled out
  of order and the time spent waiting for the slowest user in a batch.
- Live dashboard, run report, comparison, code page (shown with the colours of VS Code), settings, light and dark theme.
- First-visit welcome page: Docker found, its cores and memory, choice of the memory budget.

**Quality and security**
- Tests for the backend, the frontend and every worker language, and end-to-end tests in a browser (Playwright) with a
  command that takes the README screenshots.
- Every language is checked for style and suspicious code (ruff, Prettier, Biome, gofmt, go vet, rustfmt, clippy,
  google-java-format) together with its tests.
- CI builds every image and scans for leaked secrets, vulnerable dependencies, Dockerfile and workflow mistakes.
- The app checks the Host and Origin of requests, limits worker reports to 1 MB and sends security headers.
