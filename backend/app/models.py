from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

Lang = Literal["python", "node", "java", "go", "rust"]  # keep in sync with config.LANGS (a test checks it)


class WorkersSpec(BaseModel):
    """Consumer group: language, how many containers and the memory limit of each."""

    lang: Lang = "node"
    count: int = Field(5, ge=1, le=32)
    memory_mb: int = Field(400, ge=128, le=8192)
    # How many messages one worker handles at the same time: that many "lanes" behind the single reading loop. While one
    # message waits for a database or an API, the others are processed, like a production service does. 1 = one at a time.
    concurrency: int = Field(100, ge=1, le=1024)
    # "key": all messages of one user go down the same lane, so they are handled strictly in order (payments of one
    # user) while different users run side by side. "none": any lane, no order guaranteed, the fastest.
    ordering: Literal["key", "none"] = "key"


class ProducerSpec(BaseModel):
    """The producer container."""

    lang: Lang = "python"
    memory_mb: int = Field(384, ge=128, le=4096)
    count: int = Field(1, ge=1, le=8)  # several producers share the load, for a rate one process cannot reach


class ScenarioSpec(BaseModel):
    """What the workers do with each message and its parameter (delay in ms or hash iterations)."""

    type: Literal["echo", "delay", "cpu"] = "echo"
    param: int = Field(0, ge=0, le=1_000_000)


class SetupRequest(BaseModel):
    """Everything the run form sends. Limits here are the single place where input ranges are enforced."""

    queue: str = Field("lab-queue", pattern=r"^[a-zA-Z0-9._-]{1,64}$")
    partitions: int = Field(5, ge=1, le=64)
    workers: WorkersSpec = WorkersSpec()
    producer: ProducerSpec = ProducerSpec()
    messages: int = Field(100_000, ge=1, le=50_000_000)
    message_size: int = Field(256, ge=64, le=900_000)  # Kafka's default limit is 1 MB per message
    rate: int = Field(0, ge=0, le=5_000_000)  # target messages per second over all producers; 0 = as fast as possible
    compression: Literal["none", "lz4", "snappy", "gzip", "zstd"] = "none"  # applied by the producers
    payload: Literal["compressible", "random"] = "compressible"  # random data does not compress, like encrypted data
    users: int = Field(1000, ge=1, le=1_000_000)  # how many different users (message keys) the messages are spread over
    scenario: ScenarioSpec = ScenarioSpec()

    @model_validator(mode="after")
    def check_producers_fit_messages(self):
        """Every producer needs at least one message to send."""
        if self.messages < self.producer.count:
            raise ValueError("There must be at least one message per producer.")
        return self


class SettingsUpdate(BaseModel):
    """Partial update of the settings."""

    budget_mb: int | None = Field(None, ge=512, le=1_000_000)
    light_mode: bool | None = None
    remove_on_stop: bool | None = None


class ComparisonRequest(BaseModel):
    """Run the same setup once per language, one after the other with a pause, and compare the runs."""

    setup: SetupRequest = SetupRequest()
    langs: list[Lang] = ["node", "python"]  # in this order; at least two different languages
    cooldown_s: int = Field(5, ge=0, le=120)  # pause between two runs so the machine settles
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
