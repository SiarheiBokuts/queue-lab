# Changelog

## Unreleased

- Every sender is held to the same limit of unconfirmed data (300,000 messages or 32 MB, `queue.buffering.max.*` in
  `config.py`). Node.js turns it into the number of batches it may have waiting; the fixed 8 batches are gone. Before,
  Node.js stopped at 8,000 messages while the others held 300,000, which made the sender memory unequal.
- Fixed: the Java sender stalled with big messages (a batch of up to 16 MB per partition filled the 64 MB buffer). A batch
  is now at most 1 MB, like in the other clients.
- Fixed: the Java worker never sent its final report (the line was inside a comment).
- Every memory and CPU number (defaults of a worker and a sender, the least a container may have, the budget buttons,
  the size of the broker, the cores kept for Kafka) is defined once in `config.py` and sent to the screens in
  `/api/state` (`limits`). The first-visit page gets a "Minimal" button (1 GB) next to the old ones, and 1 GB is now the
  smallest budget. A new run form starts with a number of workers and a memory size counted from the chosen budget (and
  Light Kafka mode), so its first setup fits; the screens only warn when it does not. Checked with a real run of all
  five languages in that size: one light Kafka, one sender and one worker of 256 MB, 100,000 messages, nothing lost; the
  peaks were 7 to 142 MB per container.
- Fixed: the Rust worker started a new operating system thread for every user group of every batch, which used almost
  all of its processor (105% of a 115% share) and held it to about 36,000 messages per second in the echo scenario. It
  now keeps its threads between batches (a pool that grows to the largest batch): about 2.5 times faster with a tenth
  of the processor. The messages, their order and the numbers are the same, only the cost changed. Its speed in echo still
  changes from run to run (55,000, 89,000 and 235,000 messages per second in three identical runs, Go 234,000 each time):
  in a slow run about 90% of the time is the wait for the first message of a batch, the Kafka client's queue runs dry
  about 17 times a second for 50 ms. The cause is not found yet.
- New documentation of the workers (`docs/workers/`): what is the same in all five, what may differ and why, and one page
  per language (how it works, what is special, why we think it is fair, what is still open). The README has a table of
  the five workers with links.
- The README shows how much memory Docker needs, the top of it is easier to scan, and the screenshots show the echo
  scenario instead of the JSON one.
- The frontend audit (`.github/scripts/npm-audit.mjs`) accepts the picomatch ReDoS advisory GHSA-fc5q-39xc-vjpf: it has
  no patched version yet and only the build uses it. Remove the entry when a fix is released.

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
