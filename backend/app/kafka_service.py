"""Kafka broker lifecycle (as a sandbox container) and topic administration."""

import asyncio
import contextlib

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from . import config
from .docker_service import DockerService


class KafkaService:
    """Starts the Kafka broker as a sandbox container and manages queues (topics) through the admin API."""

    def __init__(self, docker: DockerService):
        self.docker = docker

    def broker_state(self) -> str | None:
        """Docker state of the broker container, or None if it does not exist."""
        st = self.docker.get_state(config.BROKER_NAME)
        return st["state"] if st else None

    def broker_memory(self, light: bool) -> int:
        """Memory limit of the broker for the chosen mode (regular JVM image or light native image)."""
        return config.BROKER_MEM_LIGHT_MB if light else config.BROKER_MEM_MB

    async def ensure_broker(self, light: bool, say) -> None:
        """Start the broker if needed and wait until it answers. `say(text)` reports progress."""
        if self.broker_state() == "running":
            return
        image = config.KAFKA_NATIVE_IMAGE if light else config.KAFKA_IMAGE
        loop = asyncio.get_running_loop()

        def on_line(line):
            loop.call_soon_threadsafe(say, line)

        say(f"Pulling {image} if needed")
        await asyncio.to_thread(self.docker.pull_image, image, on_line)
        env = {
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{config.BROKER_NAME}:9092",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@{config.BROKER_NAME}:9093",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        }
        if not light:
            env["KAFKA_HEAP_OPTS"] = "-Xms512m -Xmx512m"
        say("Starting the Kafka broker")
        await asyncio.to_thread(
            self.docker.run_container,
            config.BROKER_NAME,
            image,
            "broker",
            "",
            "",
            "kafka",
            env,
            self.broker_memory(light),
        )
        for _ in range(60):
            try:
                admin = AIOKafkaAdminClient(bootstrap_servers=config.BROKER_BOOTSTRAP)
                await admin.start()
                await admin.close()
                say("Kafka is ready")
                return
            except Exception:  # noqa: BLE001
                await asyncio.sleep(2)
        raise RuntimeError("Kafka did not become ready in time. Check the broker logs.")

    async def _admin(self):
        admin = AIOKafkaAdminClient(bootstrap_servers=config.BROKER_BOOTSTRAP)
        await admin.start()
        return admin

    async def topics(self) -> list[dict]:
        """Queues of the broker with their partition counts (internal topics hidden)."""
        if self.broker_state() != "running":
            return []
        admin = await self._admin()
        try:
            names = [t for t in await admin.list_topics() if not t.startswith("__")]
            if not names:
                return []
            described = await admin.describe_topics(names)
            return sorted(
                [{"name": d["topic"], "partitions": len(d["partitions"])} for d in described],
                key=lambda x: x["name"],
            )
        finally:
            await admin.close()

    async def create_topic(self, name: str, partitions: int) -> None:
        """Create a topic with the given partitions; an existing topic is left unchanged."""
        admin = await self._admin()
        try:
            with contextlib.suppress(TopicAlreadyExistsError):
                await admin.create_topics([NewTopic(name=name, num_partitions=partitions, replication_factor=1)])
        finally:
            await admin.close()

    async def delete_topic(self, name: str) -> None:
        """Delete a topic."""
        admin = await self._admin()
        try:
            await admin.delete_topics([name])
        finally:
            await admin.close()


class OffsetReader:
    """Reads end offsets of all partitions of a topic straight from the broker (for lag)."""

    def __init__(self, topic: str, partitions: int):
        self.tps = [TopicPartition(topic, i) for i in range(partitions)]
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=config.BROKER_BOOTSTRAP, enable_auto_commit=False, group_id=None
        )

    async def start(self):
        """Connect the reader and assign all partitions."""
        await self.consumer.start()
        self.consumer.assign(self.tps)

    async def end_offsets(self) -> list[int]:
        """Current end offset of every partition, straight from the broker."""
        ends = await self.consumer.end_offsets(self.tps)
        return [ends[tp] for tp in self.tps]

    async def stop(self):
        """Close the reader, ignoring errors."""
        with contextlib.suppress(Exception):
            await self.consumer.stop()
