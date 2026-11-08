"""The constants in config.py are the single source of truth; these tests keep them consistent."""

import typing

from app import config, models


def test_naming_helpers() -> None:
    assert config.container_name("orders", "w1") == "queuelab-orders-w1"
    assert config.worker_image("node") == "queuelab-worker-node"
    assert config.sandbox_project("queue-lab") == "queue-lab-sandbox"
    assert config.short_name("queuelab-orders-w1") == "orders-w1"
    assert config.short_name("not-ours") == "not-ours"


def test_sandbox_filter_matches_the_label_constants() -> None:
    assert f"{config.LABEL_LEVEL}={config.LEVEL_SANDBOX}" == config.SANDBOX_FILTER


def test_labels_are_unique() -> None:
    labels = [
        config.LABEL_LAB,
        config.LABEL_LEVEL,
        config.LABEL_ROLE,
        config.LABEL_QUEUE,
        config.LABEL_RUN,
        config.LABEL_LANG,
        config.LABEL_SRC,
    ]
    assert len(labels) == len(set(labels))


def test_languages_in_config_and_models_agree() -> None:
    assert set(typing.get_args(models.Lang)) == set(config.LANGS)


def test_histogram_edges_are_positive_and_increasing() -> None:
    edges = config.HIST_EDGES
    assert all(e > 0 for e in edges)
    assert edges == sorted(set(edges))


def test_the_report_url_points_at_the_app_alias() -> None:
    assert config.REPORT_URL.startswith(f"http://{config.APP_ALIAS}:")


def test_the_repeating_requests_are_left_out_of_the_access_log() -> None:
    import logging

    from app.main import QuietAccessLog

    def record(text: str) -> logging.LogRecord:
        return logging.LogRecord("uvicorn.access", logging.INFO, "", 0, text, None, None)

    quiet = QuietAccessLog()
    assert not quiet.filter(record('172.19.0.5:60264 - "POST /internal/report HTTP/1.1" 200 OK'))
    assert not quiet.filter(record('172.18.0.1:37090 - "GET /api/state HTTP/1.1" 200 OK'))
    assert quiet.filter(record('172.18.0.1:37090 - "POST /api/setups HTTP/1.1" 200 OK'))


def test_the_version_is_the_same_everywhere() -> None:
    """One number: the app, the UI package and the changelog must agree."""
    import json

    root = config.WORKERS_DIR.parent
    ui = json.loads((root / "frontend" / "package.json").read_text(encoding="utf-8"))["version"]
    assert ui == config.VERSION
    changelog = root / "CHANGELOG.md"
    if changelog.exists():  # the image of the app carries the backend only, the repository carries the changelog too
        assert f"## {config.VERSION}" in changelog.read_text(encoding="utf-8")
