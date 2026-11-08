# Local Deployment

## ADDED Requirements

### Requirement: One-command startup
The system SHALL start completely with a single command (`docker compose up`) on a machine that has only Docker installed,
and SHALL serve the web UI at `http://localhost:<port>`.

#### Scenario: First start on a clean machine
- **WHEN** a user clones the repository and runs `docker compose up --build`
- **THEN** the app container starts, the web UI is reachable at `http://localhost:8080`
- **AND** no other manual setup step is required

#### Scenario: Custom port
- **WHEN** the environment variable `QUEUELAB_PORT` is set before startup
- **THEN** the UI is served on that port

### Requirement: App and sandbox are separate levels
The system SHALL keep the application and the test containers at two separate levels: the application in the Compose project
`queue-lab` with its own network, and every dynamically created Kafka, producer and worker container in the Compose project
`queue-lab-sandbox` with its own network.

#### Scenario: Startup creates only the app level
- **WHEN** the user runs `docker compose up --build`
- **THEN** only the app container, its network and its volume are created
- **AND** no sandbox container exists yet

#### Scenario: Dynamic container joins the sandbox
- **WHEN** the backend creates a Kafka, producer or worker container
- **THEN** the container carries the labels `com.docker.compose.project=queue-lab-sandbox`, `queuelab.level=sandbox`,
  `queuelab.role`, `queuelab.queue`, `queuelab.run` and `queuelab.lang`
- **AND** it is attached to the sandbox network only
- **AND** it appears in a group separate from the app in Docker tooling

#### Scenario: Sandbox cannot reach app internals
- **WHEN** a worker runs in the sandbox network
- **THEN** it can reach Kafka and the app's report endpoint by the alias `queuelab-app`
- **AND** it has no access to the app's volume or to the Docker socket

### Requirement: Sandbox cleanup
The system SHALL remove the sandbox when the application stops gracefully, unless the user turned this off, and SHALL provide a
fallback cleanup for the case where the application was killed.

#### Scenario: Stopping the app removes the sandbox
- **WHEN** the user runs `docker compose down`
- **THEN** the backend receives SIGTERM, removes all containers labeled `queuelab.level=sandbox` and the sandbox network
- **AND** then exits within the configured grace period

#### Scenario: Setting keeps the sandbox
- **WHEN** the setting "remove lab containers when the app stops" is off
- **THEN** sandbox containers keep running after the app stops and are adopted when the app starts again

#### Scenario: Fallback cleanup
- **WHEN** the app was killed and sandbox containers remain
- **THEN** running the `cleanup` script removes all containers labeled `queuelab.level=sandbox` and the sandbox network

### Requirement: Pre-flight check
The system SHALL verify at startup and on demand that Docker is reachable and that enough memory is available,
and SHALL show an understandable message in the UI for each failed check.

#### Scenario: Docker socket is not reachable
- **WHEN** the backend cannot talk to the Docker Engine
- **THEN** the UI shows a message explaining that Docker is unreachable and how to fix it
- **AND** creating containers is disabled

#### Scenario: Not enough memory
- **WHEN** the memory available to Docker is below the minimum needed for Kafka
- **THEN** the UI warns about it and explains how to raise the Docker memory limit

### Requirement: Local-only access
The system SHALL bind its published port to `127.0.0.1` only, because the backend holds access to the Docker socket.

#### Scenario: Port binding
- **WHEN** the stack is started with the default configuration
- **THEN** the UI port is published on `127.0.0.1` and is not reachable from other machines

### Requirement: Safe reset
The system SHALL provide a reset action that removes only containers labeled `queuelab.level=sandbox`.

#### Scenario: Reset leaves foreign containers alone
- **WHEN** the user triggers reset while unrelated containers are running on the same Docker host
- **THEN** only containers labeled `queuelab.level=sandbox` are stopped and removed
- **AND** unrelated containers are untouched

#### Scenario: Adopt existing resources after restart
- **WHEN** the app container restarts while labeled containers are still running
- **THEN** the UI shows those containers and their state
