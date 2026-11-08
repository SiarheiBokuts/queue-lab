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

## Reporting a problem

Please do not open a public issue for a security problem. Use GitHub's "Report a vulnerability" on the Security tab of the
repository, or write to the maintainer. You will get an answer within a week.

## Supported versions

Only the latest release (1.x).