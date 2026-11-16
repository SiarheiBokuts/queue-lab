"""The guards that keep other web pages from driving the Docker Engine through this app."""

import asyncio
import inspect

import pytest
from fastapi import HTTPException
from starlette.requests import Request
from starlette.responses import Response

from app import config, main, security


def test_only_this_machine_and_the_app_are_known_hosts() -> None:
    for host in ("localhost:8080", "127.0.0.1:8080", "[::1]:8080", "localhost", f"{config.APP_ALIAS}:8000"):
        assert security.host_allowed(host), host
    for host in ("evil.example", "evil.example:8080", "localhost.evil.example", "", None):
        assert not security.host_allowed(host), host  # DNS rebinding sends the attacker's own name


def test_a_request_without_origin_is_allowed_and_a_foreign_origin_is_not() -> None:
    assert security.origin_allowed(None, "localhost:8080")  # curl, workers
    assert security.origin_allowed("http://localhost:8080", "localhost:8080")
    assert security.origin_allowed("http://127.0.0.1:8080", "127.0.0.1:8080")
    assert not security.origin_allowed("https://evil.example", "localhost:8080")
    assert not security.origin_allowed("http://localhost.evil.example", "localhost:8080")


def test_the_origin_must_be_the_address_the_request_went_to_port_included() -> None:
    """Any other web app on localhost (a dev server on :3000, another tool on :9000) is still another origin."""
    assert security.origin_allowed(f"http://{config.APP_ALIAS}:8000", f"{config.APP_ALIAS}:8000")
    assert security.origin_allowed("http://[::1]:8080", "[::1]:8080")
    assert security.origin_allowed("http://localhost", "localhost")  # the default port on both sides
    assert security.origin_allowed("https://localhost", "localhost:443")
    for origin, host in (
        ("http://localhost:3000", "localhost:8080"),  # another app on the same machine
        ("http://localhost:8080", "localhost:3000"),
        ("http://127.0.0.1:8080", "localhost:8080"),  # same port, another name: not the page the user opened
        ("http://localhost", "localhost:8080"),
        ("http://localhost:8080", "localhost"),
        ("https://localhost:8080", "localhost:80"),
        ("http://[::1]:3000", "[::1]:8080"),
    ):
        assert not security.origin_allowed(origin, host), (origin, host)


@pytest.mark.parametrize(
    "origin",
    ["null", "file://", "javascript:alert(1)", "localhost:8080", "http://localhost:notaport", "http://localhost:99999",
     "http://user@localhost:8080", "http://localhost:8080/path", "ftp://localhost:8080", "http://", "//localhost:8080"],
)  # fmt: skip
def test_an_origin_that_is_not_a_plain_web_origin_is_refused(origin: str) -> None:
    assert not security.origin_allowed(origin, "localhost:8080")


def test_the_security_headers_forbid_framing_and_foreign_scripts() -> None:
    h = security.SECURITY_HEADERS
    assert "frame-ancestors 'none'" in h["Content-Security-Policy"]
    assert "script-src 'self'" in h["Content-Security-Policy"]
    assert h["X-Content-Type-Options"] == "nosniff"
    assert h["X-Frame-Options"] == "DENY"


# ---------------------------------------------------------------------------------------- container names
@pytest.mark.parametrize(
    "name", ["queuelab-orders-w1", "queuelab-q-producer12", "queuelab-kafka", f"{config.PREFIX}-a.b_c-d", "queuelab-app"]
)
def test_the_names_the_lab_gives_its_containers_are_accepted(name: str) -> None:
    assert security.container_name_ok(name)


@pytest.mark.parametrize(
    "name",
    ["", "queuelab-", "queuelab", "other", "nginx", "queuelab-a/../../x", "queuelab-../x", "queuelab-a/b", "queuelab-a?all=1",
     "queuelab-a b", "queuelab-a\n", "queuelab-a\x00", "queuelab-%2e%2e", "queuelab-a%2Fb", "queuelab-" + "a" * 200, "QUEUELAB-a",
     "queuelab--a", "queuelab-é"],
)  # fmt: skip
def test_any_other_container_name_is_refused(name: str) -> None:
    assert not security.container_name_ok(name)


def test_the_log_endpoints_only_take_lab_container_names() -> None:
    for name in ("nginx", "queuelab-a/../../x", "queuelab-a?x=1", "queuelab-%2e%2e"):
        with pytest.raises(HTTPException) as err:
            asyncio.run(main.container_logs(name))
        assert err.value.status_code == 404, name


def test_the_log_stream_is_only_for_lab_containers() -> None:
    source = inspect.getsource(main.ws_logs)
    assert "security.container_name_ok(name)" in source  # nothing else on the machine can be read through the app
    assert "origin_allowed" in source and "host_allowed" in source


class FakeSocket:
    """Just enough of a WebSocket to see that the handshake is refused before anything is accepted."""

    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = headers
        self.closed: int | None = None
        self.accepted = False

    async def close(self, code: int = 1000) -> None:
        self.closed = code

    async def accept(self) -> None:
        self.accepted = True


@pytest.mark.parametrize(
    "headers",
    [
        {"host": "evil.example", "origin": "http://evil.example"},
        {"host": "localhost:8080", "origin": "http://evil.example"},
        {"host": "localhost:8080", "origin": "http://localhost:3000"},  # another local web app
        {"host": "localhost:8080", "origin": "null"},
    ],
)
def test_both_websockets_refuse_a_foreign_page_before_accepting(headers: dict[str, str]) -> None:
    logs = FakeSocket(headers)
    asyncio.run(main.ws_logs(logs, "queuelab-q-w1"))
    assert logs.closed == 1008 and not logs.accepted
    live = FakeSocket(headers)
    asyncio.run(main.ws_endpoint(live))
    assert live.closed == 1008 and not live.accepted


@pytest.mark.parametrize("name", ["nginx", "queuelab-a/../../x", "queuelab-a?x=1"])
def test_the_log_websocket_refuses_a_name_that_is_not_a_lab_container(name: str) -> None:
    ws = FakeSocket({"host": "localhost:8080", "origin": "http://localhost:8080"})
    asyncio.run(main.ws_logs(ws, name))
    assert ws.closed == 1008 and not ws.accepted


# --------------------------------------------------------------------------------------------- middleware
def call_guard(method: str, path: str, headers: dict[str, str]) -> Response:
    scope = {
        "type": "http", "method": method, "path": path, "raw_path": path.encode(), "query_string": b"",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()], "scheme": "http",
        "server": ("localhost", 8080), "client": ("127.0.0.1", 1), "http_version": "1.1", "root_path": "",
    }  # fmt: skip

    async def call_next(_request: Request) -> Response:
        return Response("passed")

    return asyncio.run(main.guard(Request(scope), call_next))


def test_a_cross_port_origin_cannot_change_anything() -> None:
    mine = {"host": "localhost:8080", "origin": "http://localhost:8080"}
    assert call_guard("PUT", "/api/settings", mine).status_code == 200
    assert call_guard("PUT", "/api/settings", mine | {"origin": "http://localhost:3000"}).status_code == 403
    assert call_guard("POST", "/api/reset", mine | {"origin": "https://evil.example"}).status_code == 403
    assert call_guard("DELETE", "/api/runs", mine | {"origin": "null"}).status_code == 403


def test_a_foreign_host_is_refused_and_the_answer_carries_the_headers_when_it_passes() -> None:
    assert call_guard("GET", "/api/state", {"host": "evil.example"}).status_code == 400
    ok = call_guard("GET", "/api/state", {"host": "localhost:8080"})
    assert ok.headers["x-content-type-options"] == "nosniff"
    assert "default-src 'self'" in ok.headers["content-security-policy"]


@pytest.mark.parametrize("length", ["abc", "-", "1e3", "1 2", "0x10", "12.5"])
def test_a_content_length_that_is_not_a_number_is_a_400_not_a_crash(length: str) -> None:
    res = call_guard("POST", "/internal/report", {"host": "localhost:8080", "content-length": length})
    assert res.status_code == 400


def test_a_report_that_declares_too_much_is_refused_before_it_is_read() -> None:
    res = call_guard(
        "POST", "/internal/report", {"host": "localhost:8080", "content-length": str(security.MAX_REPORT_BYTES + 1)}
    )
    assert res.status_code == 413


# ------------------------------------------------------------------------------------------ other inputs
@pytest.mark.parametrize("name", ["a/b", "a b", "a;b", "x" * 65, "", "q\x00", "q\n", "é", "a%2Fb", "a*"])
def test_deleting_a_queue_needs_a_valid_queue_name(name: str) -> None:
    """The name goes into a label filter and a Kafka request: it follows the same rule as the one in the setup form."""
    with pytest.raises(HTTPException) as err:
        asyncio.run(main.delete_queue(name))
    assert err.value.status_code == 422


def test_the_queue_name_rule_is_the_one_of_the_form() -> None:
    from pydantic import ValidationError

    from app.models import SetupRequest

    for name in ("orders", "a.b_c-d", "x" * 64, "x" * 65, "", "a/b", "a b", "q\n", "é", "a;b"):
        try:
            SetupRequest(queue=name)
            in_form = True
        except ValidationError:
            in_form = False
        assert in_form == bool(security.QUEUE_NAME.fullmatch(name)), name


@pytest.mark.parametrize("path", ["a\x00b", "..%2f..%2fetc/passwd", "../../etc/passwd", "assets/\x00", "\x00"])
def test_a_path_the_system_cannot_resolve_gives_the_front_page_or_404_never_a_crash(path: str) -> None:
    try:
        res = asyncio.run(main.spa(path))
    except HTTPException as exc:
        assert exc.status_code == 404
    else:
        assert getattr(res, "path", None) is None or str(res.path).endswith("index.html")  # never a file outside the site


def test_the_workers_run_without_capabilities_and_cannot_gain_privileges() -> None:
    from app.docker_service import DockerService

    for role in (config.ROLE_CONSUMER, config.ROLE_PRODUCER):
        assert DockerService.hardening(role) == {"cap_drop": ["ALL"], "security_opt": ["no-new-privileges"]}
    assert DockerService.hardening(config.ROLE_BROKER) == {}  # a third-party image: Docker's defaults
