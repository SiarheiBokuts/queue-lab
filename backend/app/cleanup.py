"""Removes every lab container and the sandbox network. Use it when the app was killed and left containers behind.

Cross-platform (it needs only Docker), and it uses the same constants as the app, so nothing is defined twice:

    docker compose run --rm app python -m app.cleanup
"""

from . import config
from .docker_service import DockerService


def main() -> int:
    docker = DockerService()
    if not docker.connect():
        print(f"Docker is not reachable: {docker.error}")
        return 1
    removed = len(docker.sandbox_summaries())
    docker.remove_sandbox(remove_network=True)
    print(f"Removed {removed} container(s) labeled {config.SANDBOX_FILTER} and the network {docker.network}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
