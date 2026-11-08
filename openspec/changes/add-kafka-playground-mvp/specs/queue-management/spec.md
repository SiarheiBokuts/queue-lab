# Queue Management

## ADDED Requirements

### Requirement: Create a Kafka queue
The system SHALL let the user create a queue of type Kafka with a name and a number of partitions from 1 to 64.

#### Scenario: Create a queue
- **WHEN** the user submits a queue name `orders` and 6 partitions
- **THEN** a Kafka topic `orders` with 6 partitions exists
- **AND** the queue appears in the queue list with its partition count

#### Scenario: Invalid partition count
- **WHEN** the user submits 0 or more than 64 partitions
- **THEN** the request is rejected with a clear validation message
- **AND** no topic is created

#### Scenario: Duplicate name
- **WHEN** the user creates a queue with a name that already exists
- **THEN** the request is rejected and the existing queue is unchanged

### Requirement: Broker is started on demand
The system SHALL start the Kafka broker container (`apache/kafka`, KRaft mode) the first time a queue is created,
and SHALL reuse the same broker for all queues in the MVP.

#### Scenario: First queue starts the broker
- **WHEN** the user creates the first queue and no broker is running
- **THEN** the system checks the resource budget, starts the broker, waits until it is ready and then creates the topic
- **AND** the UI shows the startup progress

#### Scenario: Broker start is refused by the budget
- **WHEN** the broker's memory limit does not fit into the remaining budget
- **THEN** the broker is not started and the user sees why

### Requirement: Optional light mode
The system SHALL let the user choose, before the broker is first started, between the regular JVM Kafka image
(default) and the lighter `apache/kafka-native` image, and SHALL show the chosen mode and its memory use.

#### Scenario: Default mode
- **WHEN** the user creates the first queue without changing the mode
- **THEN** the regular `apache/kafka` JVM image is used

#### Scenario: Light mode on a small machine
- **WHEN** the user selects light mode
- **THEN** the broker starts from `apache/kafka-native` with a smaller memory limit
- **AND** the UI labels the mode as intended for local testing

#### Scenario: Changing mode with a running broker
- **WHEN** the user changes the mode while the broker is running
- **THEN** the UI asks for confirmation because the broker and its queues will be recreated

### Requirement: List and delete queues
The system SHALL list existing queues with partition count and message totals, and SHALL let the user delete a queue.

#### Scenario: Delete a queue
- **WHEN** the user deletes a queue
- **THEN** its topic is removed and its worker and producer containers are stopped and removed

### Requirement: Queue types are extensible
The system SHALL hide the broker behind an adapter interface so other queue types can be added without changing the API and UI contracts.

#### Scenario: Only Kafka is available in the MVP
- **WHEN** the user opens the "create queue" form
- **THEN** Kafka is the only selectable queue type
