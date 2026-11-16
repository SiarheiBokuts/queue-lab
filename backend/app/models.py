from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# These are not limits of the lab: they are the largest numbers the clients and the JVM can read (a 32-bit integer, the
# safe integer of JavaScript, the largest batch the Python Kafka client accepts). Above them a worker would crash.
INT32 = 2_147_483_647
INT53 = 2**53 - 1
MAX_BATCH = 1_000_000

Lang = Literal["python", "node", "java", "go", "rust"]  # keep in sync with config.LANGS (a test checks it)


class WorkersSpec(BaseModel):
    """Consumer group: language, how many containers and the memory limit of each."""

    lang: Lang = "node"
    count: int = Field(5, ge=1, le=INT32)
    memory_mb: int = Field(400, ge=128, le=INT32)
    # The batch size: a worker reads up to this many messages, groups them by user and handles the groups at the same
    # time. The messages of one user are always handled one after another (like payments of one customer); different
    # users run side by side. 1 = one message at a time.
    concurrency: int = Field(100, ge=1, le=MAX_BATCH)

class ProducerSpec(BaseModel):
    """The producer container."""

    lang: Lang = "python"
    memory_mb: int = Field(384, ge=128, le=INT32)
    count: int = Field(1, ge=1, le=INT32)  # several producers share the load, for a rate one process cannot reach


class ScenarioSpec(BaseModel):
    """What the workers do with each message and its parameter (delay in ms or loop iterations)."""

    type: Literal["echo", "delay", "json"] = "echo"
    param: int = Field(0, ge=0, le=INT32)


class SetupRequest(BaseModel):
    """Everything the run form sends. Limits here are the single place where input ranges are enforced."""

    queue: str = Field("lab-queue", pattern=r"^[a-zA-Z0-9._-]{1,64}$")
    partitions: int = Field(5, ge=1, le=INT32)
    workers: WorkersSpec = WorkersSpec()
    producer: ProducerSpec = ProducerSpec()
    messages: int = Field(100_000, ge=1, le=INT53)
    message_size: int = Field(256, ge=64, le=INT32)  # Kafka's default limit is 1 MB per message
    rate: int = Field(0, ge=0, le=INT32)  # target messages per second over all producers; 0 = as fast as possible
    compression: Literal["none", "lz4", "snappy", "gzip", "zstd"] = "none"  # applied by the producers
    payload: Literal["compressible", "random"] = "compressible"  # random data does not compress, like encrypted data
    users: int = Field(1000, ge=1, le=INT32)  # how many different users (message keys) the messages are spread over
    scenario: ScenarioSpec = ScenarioSpec()

    @model_validator(mode="after")
    def check_producers_fit_messages(self):
        """Every producer needs at least one message to send."""
        if self.messages < self.producer.count:
            raise ValueError("There must be at least one message per producer.")
        return self


class SettingsUpdate(BaseModel):
    """Partial update of the settings."""

    budget_mb: int | None = Field(None, ge=512, le=INT32)
    light_mode: bool | None = None
    remove_on_stop: bool | None = None
    welcomed: bool | None = None  # the first-visit page has been seen


class ComparisonRequest(BaseModel):
    """Run the same setup once per language, one after the other with a pause, and compare the runs."""

    setup: SetupRequest = SetupRequest()
    langs: list[Lang] = ["node", "python"]  # in this order; at least two different languages
    cooldown_s: int = Field(5, ge=0, le=INT32)  # pause between two runs so the machine settles
    # The order of the languages is drawn at random when the comparison starts, so no language is always the first one
    # to run on a cold machine or the last one on a warm machine.
    random_order: bool = True
    producer_follows: bool = True  # the producer uses the same language as the workers (otherwise it stays as set)

    @field_validator("langs")
    @classmethod
    def check_langs(cls, langs: list[str]) -> list[str]:
        """Two or more languages, each only once."""
        if len(set(langs)) != len(langs):
            raise ValueError("Pick every language only once.")
        if len(langs) < 2:
            raise ValueError("Pick at least two languages to compare.")
        return langs
