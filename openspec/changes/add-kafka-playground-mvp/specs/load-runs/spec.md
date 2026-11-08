# Load Runs

## ADDED Requirements

### Requirement: Configure and start a run
The system SHALL let the user configure a run for a queue: the number of messages (1 to 1,000,000), the message size in bytes,
a processing scenario and its parameters, and SHALL start the run with one action.

#### Scenario: Start a run
- **WHEN** the user sets 100,000 messages of 512 bytes with scenario `echo` and presses start
- **THEN** a producer container is created and sends exactly 100,000 messages to the queue
- **AND** the workers process them

#### Scenario: Message limit
- **WHEN** the user enters more than 50,000,000 messages
- **THEN** the request is rejected with a validation message

### Requirement: Hard sustained load options
The system SHALL let the user set a target send rate (messages per second, 0 = as fast as possible) split over 1 to 8 producers,
a compression codec (none, lz4, snappy, gzip, zstd), and the content type (compressible or random).

#### Scenario: Rate-limited load
- **WHEN** the user sets 30,000 msg/s with 3 producers
- **THEN** each producer paces itself to its share and the report shows the rate actually reached against the target

### Requirement: Network needed against actual
The report SHALL show the data size the messages need and what the containers really moved (senders, broker in/out, workers),
and the compression ratio.

### Requirement: Same test in several languages
The system SHALL run the same setup once per chosen language (two or more of Python, Node.js, Java, Go, Rust), one after another
with a configurable pause, and SHALL then offer a side-by-side comparison. All languages use identical Kafka client settings.

#### Scenario: Compare five languages
- **WHEN** the user chooses compare mode with five languages
- **THEN** five runs are executed in order with the pause between them, and a comparison page opens with the best value per metric
