"""Warnings about a setup: they say what to expect and never refuse anything."""

from app import advice, config
from app.models import SetupRequest


def texts(**changes) -> str:
    return " ".join(advice.warnings(SetupRequest(**changes)))


def test_a_normal_setup_has_no_warning() -> None:
    assert advice.warnings(SetupRequest()) == []


def test_a_huge_amount_of_data_warns_about_the_disk() -> None:
    assert "will pass through Kafka" in texts(messages=100_000_000, message_size=900_000)


def test_a_message_above_the_broker_limit_warns_that_kafka_will_refuse_it() -> None:
    assert "refuse it" in texts(message_size=config.KAFKA_MAX_MESSAGE_BYTES + 1)


def test_a_rate_that_cannot_finish_in_the_time_limit_says_so() -> None:
    assert "stopped after" in texts(messages=50_000_000, rate=1_000)


def test_a_rate_above_what_one_sender_reaches_suggests_more_senders() -> None:
    assert "add senders" in texts(rate=config.WARN_RATE_PER_SENDER * 2)
    assert "add senders" not in texts(rate=config.WARN_RATE_PER_SENDER * 2, producer={"count": 4})


def test_many_partitions_and_a_big_batch_are_explained() -> None:
    assert "partitions" in texts(partitions=config.WARN_PARTITIONS + 1)
    assert "connections" in texts(workers={"concurrency": config.WARN_CONCURRENCY + 1})


def test_fewer_users_than_the_batch_size_is_explained() -> None:
    assert "Only 10 users" in texts(users=10)
