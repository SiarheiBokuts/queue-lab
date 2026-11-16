# Security

## What the app is allowed to do

queue-lab controls the Docker Engine of your computer through the Docker socket. That is the whole idea of the lab, and it
means the app is as powerful as root on your machine. So:

- It listens on `127.0.0.1` only and has **no login**. Never expose its port to a network and never run it on a shared server.
- It refuses requests whose `Host` is not this machine and requests that change something from another web site (also the
  WebSocket), which stops a web page you visit from driving Docker through your browser.
- It sends strict security headers (a Content-Security-Policy that allows scripts and styles from the app only, no framing).
- The containers it creates carry memory limits and live in their own Compose project and network. They cannot reach the
  app's data or the Docker socket.
- The lab never runs code written by a user. Everything is chosen in the form.
- The app container drops all capabilities, cannot gain new privileges and has a memory and process limit.
- The lab's containers send their progress to the app (`/internal/report`). Anything on the sandbox network can reach that
  endpoint, so a report is not trusted: a body above 1 MB is refused, and wrong types or missing fields end in an error
  without changing the run.
- The log stream (`/ws/logs/...`) serves only containers that belong to the lab (names with the `queuelab-` prefix),
  never other containers on your machine.
- The numbers of a run are not capped (the form only warns), so a large run uses as much time, disk and processor as it
  asks for. Memory is the exception: it is checked against the budget you chose before any container starts, and a run
  is stopped after 30 minutes.

## How it is checked

- **Tests of the app** cover the Host and Origin checks, the report limit and the budget.
- **End-to-end tests** (`e2e/tests/security.fast.spec.ts`) try them against the running app: a foreign `Host`, a change
  from another site, the security headers, a report that is too big and a log stream of a foreign container.
- **CI** looks for leaked secrets in the whole history (gitleaks), for known vulnerabilities in the dependencies
  (npm audit, pip-audit), and checks the Dockerfiles (hadolint) and the workflow files (actionlint). Dependabot proposes
  updates.

## Reporting a problem

Please do not open a public issue for a security problem. Use GitHub's "Report a vulnerability" on the Security tab of the
repository, or write to the maintainer. You will get an answer within a week.

## Supported versions

Only the latest release (1.x).