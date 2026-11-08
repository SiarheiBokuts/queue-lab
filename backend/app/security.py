"""Guards for an app that controls the local Docker Engine and listens on localhost.

A web page you visit could try to talk to `localhost:8080` (cross-site requests) or point its own domain at 127.0.0.1
(DNS rebinding) and then drive Docker. Two checks stop both: the Host header must name this machine or the app, and a
request that changes something (or opens the WebSocket) must come from this app's own page or from no browser at all.
"""

from urllib.parse import urlsplit

from . import config

ALLOWED_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", config.APP_ALIAS})
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


def host_allowed(host_header: str | None) -> bool:
    """True if the Host header names this machine or the app (a worker reaches the app by its alias)."""
    return bool(host_header) and _hostname(host_header or "") in ALLOWED_HOSTS


def origin_allowed(origin: str | None) -> bool:
    """True for a request without an Origin (curl, a worker) or from a page served by this app."""
    if not origin:
        return True
    return (urlsplit(origin).hostname or "").lower() in ALLOWED_HOSTS
