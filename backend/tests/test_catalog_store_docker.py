"""Tests of the code catalog, the SQLite store and the pure parts of the Docker service."""

from pathlib import Path

from app import catalog, config
from app.docker_service import DockerService, compose_labels, is_dev_file
from app.store import Store


# --------------------------------------------------------------------------------------------------- catalog
def test_region_cuts_the_marked_block_and_dedents_it() -> None:
    text = "a\n    # @x-start\n    if y:\n        z()\n    # @x-end\nb\n"
    assert catalog.region(text, "x") == "if y:\n    z()"


def test_region_of_a_missing_marker_is_empty() -> None:
    assert catalog.region("nothing here", "x") == ""


def test_the_code_page_shows_the_main_parts_and_a_template_for_both_languages() -> None:
    code = catalog.code()
    assert set(code) == {*config.LANGS, "client_conf", "client_notes", "reading", "common_work"}
    for lang in config.LANGS:
        assert code[lang]["producer"], lang
        assert "delay" in code[lang]["scenario"], lang
        assert set(code[lang]) == {"producer", "scenario"}


def test_the_shared_client_settings_are_exposed() -> None:
    conf = catalog.code()["client_conf"]
    assert conf["producer"] == config.PRODUCER_CONF
    assert conf["consumer"] == config.CONSUMER_CONF


def test_every_shared_setting_has_a_note_and_every_language_says_how_it_reads() -> None:
    code = catalog.code()
    for values in code["client_conf"].values():
        for key in values:
            assert key in code["client_notes"], key
    assert set(code["reading"]) == set(config.LANGS)
    assert all(r["read"] and r["handle"] and r["file"] and r["cpu"] for r in code["reading"].values())


def test_every_scenario_is_described() -> None:
    assert [s["id"] for s in catalog.SCENARIOS] == ["echo", "delay", "cpu"]
    for s in catalog.SCENARIOS:
        assert s["summary"] and s["simulates"] and s["does"] and s["watch"]


# ----------------------------------------------------------------------------------------------------- store
def test_settings_round_trip(tmp_path: Path) -> None:
    store = Store(tmp_path / "t.db")
    assert store.get_setting("budget_mb", 123) == 123
    store.set_setting("budget_mb", 4096)
    store.set_setting("budget_mb", 2048)
    assert store.get_setting("budget_mb") == 2048


def test_a_run_can_be_saved_listed_read_and_deleted(tmp_path: Path) -> None:
    store = Store(tmp_path / "t.db")
    store.save_run(
        "r1", 1.0, "completed", {"queue": "q"}, {"lost": 0}, [{"t": 1}, {"t": 2}], [{"t": 1, "containers": []}]
    )
    store.save_run("r2", 2.0, "completed", {"queue": "q"}, {"lost": 1}, [], [])
    assert [r["id"] for r in store.list_runs()] == ["r2", "r1"]  # newest first
    run = store.get_run("r1")
    assert run is not None
    assert len(run["samples"]) == 2
    assert len(run["container_samples"]) == 1
    store.delete_run("r1")
    assert store.get_run("r1") is None
    assert [r["id"] for r in store.list_runs()] == ["r2"]
    store.delete_all_runs()
    assert store.list_runs() == []


def test_saving_a_run_again_replaces_its_samples(tmp_path: Path) -> None:
    store = Store(tmp_path / "t.db")
    store.save_run("r1", 1.0, "completed", {}, {}, [{"t": 1}, {"t": 2}], [])
    store.save_run("r1", 1.0, "completed", {}, {}, [{"t": 1}], [])
    assert len(store.get_run("r1")["samples"]) == 1


# ---------------------------------------------------------------------------------------- docker service
def test_compose_labels_group_the_container_into_the_project() -> None:
    labels = compose_labels("queue-lab-sandbox", "queuelab-q-w1", {config.LABEL_ROLE: "consumer"})
    assert labels[config.COMPOSE_PROJECT_LABEL] == "queue-lab-sandbox"
    assert labels["com.docker.compose.service"] == "queuelab-q-w1"
    assert labels["com.docker.compose.oneoff"] == "False"
    assert labels[config.LABEL_ROLE] == "consumer"


def test_development_files_are_not_part_of_a_worker_image() -> None:
    assert is_dev_file(Path("tests/test_lib.py"))
    assert is_dev_file(Path("node_modules/x/index.js"))
    assert is_dev_file(Path("Dockerfile.test"))
    assert not is_dev_file(Path("src/worker.ts"))
    assert not is_dev_file(Path("Dockerfile"))


def test_the_source_hash_is_stable_and_differs_between_languages() -> None:
    h = DockerService.source_hash
    assert h("python") == h("python")
    assert h("python") != h("node")
    assert len(h("node")) == 12
