# Worker Containers

## ADDED Requirements

### Requirement: Create worker containers dynamically
The system SHALL let the user create one or more consumer containers for a queue, choosing the language (Python or Node.js),
the number of containers and the memory limit per container, and SHALL create them through the Docker Engine API.

#### Scenario: Pick the language
- **WHEN** the user opens the worker form
- **THEN** Python and Node.js are selectable, and the choice applies to that worker group

#### Scenario: Same run, two languages
- **WHEN** the user runs the same configuration once with Python workers and once with Node.js workers
- **THEN** both runs use the same worker contract and produce results in the same format, so they can be compared

#### Scenario: Create five workers
- **WHEN** the user requests 5 Node.js workers with 400 MB each for queue `orders`
- **THEN** the admission check passes and 5 containers are created, each with a 400 MB limit
- **AND** all of them consume from `orders` in one consumer group

#### Scenario: Minimum memory
- **WHEN** the user requests a worker memory limit below 128 MB
- **THEN** the request is rejected with a validation message

### Requirement: Worker image is built through the Docker API
The system SHALL build the worker image through the Docker API on first use from the bundled `workers/python/` or `workers/node/` sources (one image per language),
cache it for later use and stream the build progress to the UI.

#### Scenario: First use builds the image
- **WHEN** the user creates workers and the image does not exist
- **THEN** the image is built and the UI shows the build progress
- **AND** the containers are created only after a successful build

#### Scenario: Later use reuses the image
- **WHEN** the image already exists
- **THEN** no rebuild happens and containers start immediately

### Requirement: Worker contract
The system SHALL configure each worker through environment variables and SHALL accept periodic reports from it,
so that further languages can be added later by implementing the same contract.

#### Scenario: Worker reports progress
- **WHEN** a worker is running
- **THEN** it reports processed count, last sequence number per partition, errors and latency data roughly every second

### Requirement: Idle-worker warning
The system SHALL warn the user when the number of consumers in a group exceeds the number of partitions,
because the extra consumers will receive no messages.

#### Scenario: More workers than partitions
- **WHEN** the user requests 8 workers for a queue with 6 partitions
- **THEN** the UI warns that 2 workers will stay idle
- **AND** the user can still proceed

### Requirement: Stop and remove workers
The system SHALL let the user stop and remove individual workers or all workers of a queue.

#### Scenario: Remove one worker
- **WHEN** the user removes one worker of a group
- **THEN** its container is removed, its memory is released from the budget
- **AND** the remaining workers rebalance the partitions

### Requirement: Same Kafka settings for every language
The system SHALL send the same Kafka client settings (batching, acknowledgements, queue size, compression, offset reset, commit and timeouts) to the producer and the consumers of every language, defined in one place in the backend, and the workers SHALL NOT contain tuning values of their own.

#### Scenario: Comparing Python and Node.js
- **WHEN** the user runs the same setup once with Python workers and once with Node.js workers
- **THEN** both receive identical Kafka client settings, so any difference in the results comes from the language and its client library

#### Scenario: Settings are visible
- **WHEN** the user opens the Code page
- **THEN** the shared client settings are listed

### Requirement: Production-style handling of messages
A worker SHALL read with one loop and hand every message to one of N lanes (N is "at the same time per worker"), so up to
N messages are handled at once and a full lane pauses the reading (back-pressure). With ordering "key" all messages of one
user SHALL use the same lane and be handled in order; with ordering "none" any lane MAY be used. Computing work SHALL use
all available cores in every language (a thread pool in Node.js, a process pool in Python).

#### Scenario: Waiting does not block other messages
- **WHEN** the scenario is a delay of 15 ms and a worker handles 100 messages at once
- **THEN** it handles about 6,000 messages per second, not about 66

### Requirement: Messages belong to users
Every message SHALL carry a key naming its user. A user SHALL always be sent to the same partition, so Kafka keeps that
user's messages in order. The number of users is a setting of the run.