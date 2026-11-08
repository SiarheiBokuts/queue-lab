# Live Monitoring

## ADDED Requirements

### Requirement: Per-container resource usage
The system SHALL show live CPU, memory (usage and limit), network and disk I/O for every container it created,
updated at least once per second.

#### Scenario: Watch a worker
- **WHEN** a worker is processing messages
- **THEN** the UI shows its CPU percent, memory usage against its limit, network and disk I/O in live charts

#### Scenario: Container near its memory limit
- **WHEN** a container uses more than 90% of its memory limit
- **THEN** the UI highlights it

### Requirement: Per-container detail page
The system SHALL provide a detail page for every container it created, showing its role, language, image, configured limits,
current state, restart count, live CPU, memory, network and disk charts, its partition assignment and its logs.

#### Scenario: Open a container
- **WHEN** the user opens a worker from the container list or from the topology
- **THEN** the page shows that worker's live charts, assigned partitions, messages per second, errors and logs

#### Scenario: Container was killed by memory limit
- **WHEN** a container is stopped by the out-of-memory killer
- **THEN** the detail page and the container list show the OOM reason and the time of the event

### Requirement: Machine memory versus budget
The system SHALL show total and available memory of the Docker host (labelled as the Docker VM where applicable),
the memory used by queue-lab containers and the configured budget in one view.

#### Scenario: Budget gauge
- **WHEN** the user opens the dashboard
- **THEN** it shows used, budget and available memory as a gauge that updates live

### Requirement: Queue metrics
The system SHALL show messages per second, end offset, committed offset and lag per partition, the consumer-to-partition
assignment and rebalance events.

#### Scenario: Lag per partition
- **WHEN** consumers are slower than the producer
- **THEN** the UI shows growing lag per partition and which worker owns each partition

#### Scenario: Rebalance event
- **WHEN** a worker is added or removed
- **THEN** a rebalance event with a timestamp appears in the event list and the assignment view updates

### Requirement: Latency
The system SHALL show end-to-end latency (p50, p95, p99) measured from the message send timestamp to the worker receive time.

#### Scenario: Latency percentiles
- **WHEN** a run is in progress
- **THEN** the UI shows p50, p95 and p99 latency, updated live

### Requirement: Live logs
The system SHALL stream the logs of any created container to the UI.

#### Scenario: Open worker logs
- **WHEN** the user opens the logs of a worker
- **THEN** new log lines appear live without reloading the page

### Requirement: Live topology view
The system SHALL show an interactive topology of the current setup: the producer, the queue with each partition,
and every worker, connected by animated links whose speed reflects the real message throughput.

#### Scenario: Topology reflects the setup
- **WHEN** a queue with 6 partitions and 3 workers exists
- **THEN** the canvas shows the producer, 6 partitions and 3 workers with the partition-to-worker links

#### Scenario: Topology reflects load
- **WHEN** throughput rises
- **THEN** the link animation speeds up
- **AND** a worker close to its memory limit is highlighted

#### Scenario: Open details from the canvas
- **WHEN** the user clicks a node
- **THEN** a side panel shows that container's live charts and logs

### Requirement: Partition lag heatmap
The system SHALL show lag per partition over time as a heatmap, so uneven partition load is visible at a glance.

#### Scenario: Hot partition
- **WHEN** one partition accumulates much more lag than the others
- **THEN** its cells are visibly hotter in the heatmap

### Requirement: Single live channel
The system SHALL deliver live data to the browser over one WebSocket connection and SHALL reconnect automatically after a drop.

#### Scenario: Reconnect
- **WHEN** the connection is lost and restored
- **THEN** the UI resumes live updates without a manual page reload
