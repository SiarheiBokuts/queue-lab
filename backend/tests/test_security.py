"""The guards that keep other web pages from driving the Docker Engine through this app."""

from app import config, security


def test_only_this_machine_and_the_app_are_known_hosts() -> None:
    for host in ("localhost:8080", "127.0.0.1:8080", "[::1]:8080", "localhost", f"{config.APP_ALIAS}:8000"):
        assert security.host_allowed(host), host
    for host in ("evil.example", "evil.example:8080", "localhost.evil.example", "", None):
        assert not security.host_allowed(host), host  # DNS rebinding sends the attacker's own name


def test_a_request_without_origin_is_allowed_and_a_foreign_origin_is_not() -> None:
    assert security.origin_allowed(None)  # curl, workers
    assert security.origin_allowed("http://localhost:8080")
    assert security.origin_allowed("http://127.0.0.1:8080")
    assert not security.origin_allowed("https://evil.example")
    assert not security.origin_allowed("http://localhost.evil.example")


def test_the_security_headers_forbid_framing_and_foreign_scripts() -> None:
    h = security.SECURITY_HEADERS
    assert "frame-ancestors 'none'" in h["Content-Security-Policy"]
    assert "script-src 'self'" in h["Content-Security-Policy"]
    assert h["X-Content-Type-Options"] == "nosniff"
    assert h["X-Frame-Options"] == "DENY"
