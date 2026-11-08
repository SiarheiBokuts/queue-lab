"""Workers and senders get the same CPU ceiling; the Kafka broker is not limited."""

from types import SimpleNamespace

from app.docker_service import DockerService


class FakeContainers:
    def __init__(self) -> None:
        self.kwargs: dict = {}

    def run(self, image, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(name=kwargs.get("name"))


def service() -> tuple[DockerService, FakeContainers]:
    svc = object.__new__(DockerService)
    fake = FakeContainers()
    svc.client = SimpleNamespace(containers=fake, api=SimpleNamespace(remove_container=lambda *a, **k: None))
    svc.sandbox_project = "p-sandbox"
    svc.network = "n"
    return svc, fake


def test_a_container_with_a_cpu_ceiling_gets_exactly_that_ceiling() -> None:
    svc, fake = service()
    svc.run_container("c", "img", "consumer", "q", "r", "go", {}, 300, cpus=1.5)
    assert fake.kwargs["nano_cpus"] == 1_500_000_000


def test_the_broker_is_not_limited() -> None:
    svc, fake = service()
    svc.run_container("k", "img", "broker", None, None, "kafka", {}, 1024)
    assert fake.kwargs["nano_cpus"] is None
