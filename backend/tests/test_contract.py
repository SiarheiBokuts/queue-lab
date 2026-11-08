"""The worker contract must match what the worker sources actually read from their environment."""

import json
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from app import analysis, config, contract, runs
from app.models import ScenarioSpec, SetupRequest

REQ = SetupRequest(
    queue="orders", partitions=4, messages=500, message_size=128, scenario=ScenarioSpec(type="delay", param=7)
)


def test_consumer_gets_everything_it_needs() -> None:
    env = contract.consumer_env(REQ, "run1", "queuelab-orders-w1")
    assert set(env) == set(contract.COMMON + contract.CONSUMER_ONLY)
    assert env["ROLE"] == "consumer"
    assert env["GROUP_ID"] == "g-run1"
    assert env["SCENARIO"] == "delay"
    assert env["SCENARIO_PARAM"] == "7"


def test_producer_gets_everything_it_needs() -> None:
    env = contract.producer_env(REQ, "run1", "queuelab-orders-producer")
    assert set(env) == set(contract.COMMON + contract.PRODUCER_ONLY)
    assert env["ROLE"] == "producer"
    assert (env["MESSAGES"], env["MSG_SIZE"], env["PARTITIONS"]) == ("500", "128", "4")


def test_histogram_edges_reach_every_role_in_the_defined_format() -> None:
    for env in (contract.consumer_env(REQ, "r", "w"), contract.producer_env(REQ, "r", "p")):
        assert [float(e) for e in env["HIST_EDGES"].split(",")] == [float(e) for e in config.HIST_EDGES]


SOURCE_EXTENSIONS = {"python": "*.py", "node": "*.ts", "java": "*.java", "go": "*.go", "rust": "*.rs"}
# The file where each worker reads the variables every role needs, and how a required variable looks in it.
SETTINGS_FILES = {
    "python": ("settings.py", r'os\.environ\["(\w+)"\]'),
    "node": ("src/settings.ts", r"required\('(\w+)'\)"),
    "go": ("settings.go", r'required\("(\w+)"\)'),
    "rust": ("src/settings.rs", r'required\("(\w+)"\)'),
    "java": ("src/main/java/lab/Settings.java", r'required\("(\w+)"\)'),
}  # fmt: skip
# Worker files that may name librdkafka properties, because they only translate names for another client.
TRANSLATION_FILES = {"KafkaConf.java"}


def _sources(lang: str) -> dict[str, str]:
    """Every source file of a worker (no tests, no build output), by relative name."""
    base = config.WORKERS_DIR / lang
    skip = {"tests", "test", "node_modules", "target", "dist", "dist-test"}
    return {
        str(f.relative_to(base)).replace("\\", "/"): f.read_text(encoding="utf-8")
        for f in base.rglob(SOURCE_EXTENSIONS[lang])
        if not skip & set(f.relative_to(base).parts) and not f.name.endswith("_test.go")
    }


def _required_by_workers() -> dict[str, set[str]]:
    """Variables each worker reads at start-up (in its settings file, so needed by both roles)."""
    out = {}
    for lang, (file, pattern) in SETTINGS_FILES.items():
        out[lang] = set(re.findall(pattern, _sources(lang)[file]))
    return out


def test_variables_read_by_both_workers_at_startup_are_provided_to_every_role() -> None:
    """This is the test that would have caught a producer started without HIST_EDGES."""
    given_to_all = set(contract.consumer_env(REQ, "r", "w")) & set(contract.producer_env(REQ, "r", "p"))
    for lang, names in _required_by_workers().items():
        assert names, f"no required variables found in the {lang} worker: the pattern in this test is out of date"
        assert names <= given_to_all, f"{lang} worker needs {names - given_to_all} but not every role gets it"


def test_role_specific_variables_are_read_by_the_workers() -> None:
    for lang in config.LANGS:
        source = "\n".join(_sources(lang).values())
        for name in contract.CONSUMER_ONLY + contract.PRODUCER_ONLY:
            assert name in source, f"{name} is not used by the {lang} worker"


def test_kafka_client_settings_are_sent_as_the_same_json_to_every_language() -> None:
    assert json.loads(contract.producer_env(REQ, "r", "p")["PRODUCER_CONF"]) == config.PRODUCER_CONF
    assert json.loads(contract.consumer_env(REQ, "r", "w")["CONSUMER_CONF"]) == config.CONSUMER_CONF


# Settings that tune the Kafka client. They may be written only in config.py, never in a worker: as a quoted librdkafka
# property name ("linger.ms") or as a KafkaJS-style option key (acks:). Prose in comments is allowed.
TUNING_PROPERTIES = ["acks", "linger.ms", "batch.num.messages", "queue.buffering.max.messages", "compression.type",
                     "session.timeout.ms", "fetch.wait.max.ms", "auto.offset.reset", "enable.auto.commit"]  # fmt: skip
TUNING_OPTIONS = ["acks", "fromBeginning", "autoCommit", "sessionTimeout", "maxWaitTimeInMs", "compression"]


def test_workers_contain_no_kafka_tuning_of_their_own() -> None:
    """Fair comparison: every tuning value comes from config.py, so Python and Node.js can never drift apart."""
    literal = re.compile("[\"'](" + "|".join(re.escape(p) for p in TUNING_PROPERTIES) + ")[\"']")
    option = re.compile(r"\b(" + "|".join(TUNING_OPTIONS) + r")\s*:")
    for lang in config.LANGS:
        for name, text in _sources(lang).items():
            if name.rsplit("/", 1)[-1] in TRANSLATION_FILES:
                continue
            code = [ln for ln in text.splitlines() if not ln.strip().startswith(("#", "//", "*", "/*"))]
            offenders = [ln.strip() for ln in code if literal.search(ln) or option.search(ln)]
            assert not offenders, f"{lang}/{name} hard-codes Kafka tuning: {offenders}"


def multi(count: int, messages: int, rate: int, **extra) -> SetupRequest:
    return SetupRequest(messages=messages, rate=rate, producer={"count": count}, **extra)


def test_the_messages_are_split_between_the_producers_without_losing_any() -> None:
    req = multi(3, 100, 0)
    shares = [int(contract.producer_env(req, "r", f"p{i}", i)["MESSAGES"]) for i in range(3)]
    assert sum(shares) == 100
    assert shares == [34, 33, 33]


def test_the_target_rate_is_split_between_the_producers() -> None:
    req = multi(4, 1000, 20_000)
    assert {contract.producer_env(req, "r", "p", i)["RATE"] for i in range(4)} == {"5000"}
    assert contract.producer_env(multi(2, 1000, 0), "r", "p", 0)["RATE"] == "0"  # 0 stays "as fast as possible"


def test_every_producer_knows_its_index_and_the_payload_kind() -> None:
    env = contract.producer_env(multi(2, 10, 0, payload="random"), "r", "p", 1)
    assert (env["PRODUCER_INDEX"], env["PAYLOAD"]) == ("1", "random")


def test_the_compression_of_the_run_replaces_the_default() -> None:
    env = contract.producer_env(multi(1, 10, 0, compression="zstd"), "r", "p", 0)
    conf = json.loads(env["PRODUCER_CONF"])
    assert conf["compression.type"] == "zstd"
    assert {k: v for k, v in conf.items() if k != "compression.type"} == {
        k: v for k, v in config.PRODUCER_CONF.items() if k != "compression.type"
    }  # everything else is the shared client setting


def test_each_producer_needs_at_least_one_message() -> None:
    with pytest.raises(ValidationError, match="at least one message per producer"):
        multi(8, 3, 0)


# --------------------------------------------------------------------------------------------- the reports
ROLE_FILES = {
    "python": {"consumer": "consumer.py", "producer": "producer.py"},
    "node": {"consumer": "src/consumer.ts", "producer": "src/producer.ts"},
    "java": {"consumer": "src/main/java/lab/Consumer.java", "producer": "src/main/java/lab/Producer.java"},
    "go": {"consumer": "consumer.go", "producer": "producer.go"},
    "rust": {"consumer": "src/consumer.rs", "producer": "src/producer.rs"},
}  # fmt: skip


def _mentions(source: str, key: str) -> bool:
    """The report field appears as a quoted name ("key") or as an object key (key:)."""
    return bool(re.search(rf'["\']{key}["\']|\b{key}\s*:', source))


def test_every_language_sends_the_same_report_fields() -> None:
    """The backend reads one report format. A language that drops or renames a field would silently break the numbers."""
    fields = {"consumer": contract.CONSUMER_REPORT, "producer": contract.PRODUCER_REPORT}
    for lang, files in ROLE_FILES.items():
        for role, file in files.items():
            source = _sources(lang)[file]
            missing = [k for k in fields[role] if not _mentions(source, k)]
            assert not missing, f"the {lang} {role} does not send {missing}"


def test_the_backend_reads_every_report_field() -> None:
    """The other direction: what the workers send is what handle_report knows about."""
    source = "".join(Path(m.__file__).read_text(encoding="utf-8") for m in (runs, analysis))
    for key in ("received", "duplicates", "gaps", "errors", "offsets", "hist", "sent", "delivered", "done"):
        assert f'"{key}"' in source, f"the backend never reads the report field {key}"


def test_workers_get_the_ordering_and_producers_get_the_user_count() -> None:
    req = SetupRequest(users=50, workers={"ordering": "none"})
    assert contract.consumer_env(req, "r", "w")["ORDERING"] == "none"
    assert contract.producer_env(req, "r", "p")["USERS"] == "50"


def test_every_python_worker_file_at_least_compiles() -> None:
    """A syntax slip in a worker only shows when its container starts and crashes; this catches it before."""
    for name, source in _sources("python").items():
        compile(source, f"workers/python/{name}", "exec")
