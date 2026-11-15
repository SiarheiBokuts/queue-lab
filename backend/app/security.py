"""Guards for an app that controls the local Docker Engine and listens on localhost.

A web page you visit could try to talk to `localhost:8080` (cross-site requests) or point its own domain at 127.0.0.1
(DNS rebinding) and then drive Docker. Two checks stop both: the Host header must name this machine or the app, and a
request that changes something (or opens the WebSocket) must come from this app's own page or from no browser at all.
"""

import re
from urllib.parse import urlsplit

from . import config

ALLOWED_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", config.APP_ALIAS})
CONTAINER_NAME_PATTERN = rf"^{config.PREFIX}-[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}$"
CONTAINER_NAME = re.compile(CONTAINER_NAME_PATTERN)
QUEUE_NAME = re.compile(r"^[a-zA-Z0-9._-]{1,64}$")  # the same rule as SetupRequest.queue
MAX_REPORT_BYTES = 1_000_000  # a worker report is a few kilobytes; anything near a megabyte is not one

# The page is served by this app only: scripts and styles from itself, no frames, no other sites to talk to.
SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; "
        "connect-src 'self' ws: wss:; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Cross-Origin-Opener-Policy": "same-origin",
}


def _hostname(value: str) -> str:
    """The host name of a Host header value (`localhost:8080`, `[::1]:8080`), lower case, without the port."""
    value = value.strip().lower()
    if value.startswith("["):  # IPv6 literal
        return value[1:].split("]", 1)[0]
    return value.rsplit(":", 1)[0] if value.count(":") == 1 else value


def _port(value: str) -> str | None:
    """The port of a Host header value, or None when it names none."""
    value = value.strip().lower()
    if value.startswith("["):
        rest = value.split("]", 1)[1] if "]" in value else ""
        return rest[1:] if rest.startswith(":") else None
    return value.rsplit(":", 1)[1] if value.count(":") == 1 else None


def host_allowed(host_header: str | None) -> bool:
    """True if the Host header names this machine or the app (a worker reaches the app by its alias)."""
    return bool(host_header) and _hostname(host_header or "") in ALLOWED_HOSTS


def origin_allowed(origin: str | None, host_header: str | None) -> bool:
    """True for a request without an Origin (curl, a worker) or from a page served by this very app.

    The Origin must be the address the request was sent to: the same host *and port* as the Host header, which
    `host_allowed` has already limited to this machine or the app. Checking the host name alone would let any other web
    app on `localhost` (another port, a dev server of a page you visit) change settings or open the WebSocket.
    """
    if not origin:
        return True
    try:
        parts = urlsplit(origin)
        port = parts.port  # raises on a port that is not a number
    except ValueError:
        return False
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.path not in ("", "/") or parts.username:
        return False
    if (parts.hostname or "").lower() not in ALLOWED_HOSTS:
        return False
    default_port = "443" if parts.scheme == "https" else "80"
    return (parts.hostname or "").lower() == _hostname(host_header or "") and str(port or default_port) == (
        _port(host_header or "") or default_port
    )


def container_name_ok(name: str) -> bool:
    """True for a name the app gives its own containers (`queuelab-<queue>-w1`), and for nothing else.

    The name goes into a Docker Engine API path, so it must not hold a slash, a dot-dot path, a query or a space.
    """
    return CONTAINER_NAME.fullmatch(name) is not None
