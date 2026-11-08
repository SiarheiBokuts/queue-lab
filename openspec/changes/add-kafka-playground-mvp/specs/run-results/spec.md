# Run Results

## ADDED Requirements

### Requirement: Run summary
The system SHALL produce a summary for every finished run containing: configuration (partitions, workers, memory per worker,
message count and size, scenario), duration, average and peak throughput, latency percentiles, lost, duplicated and
out-of-order counts, and peak CPU and memory per container.

#### Scenario: Completed run summary
- **WHEN** a run completes
- **THEN** the UI shows its summary with all of the listed fields

### Requirement: Persistent history
The system SHALL store runs and per-second samples in SQLite on a persistent volume, so history survives restarts.

#### Scenario: History after restart
- **WHEN** the app is restarted
- **THEN** previous runs are still listed and can be opened

### Requirement: Per-container history
The system SHALL store per-second samples for every container of a run (CPU, memory, network, disk, messages per second,
errors, lag of its partitions) and SHALL let the user inspect any container of a finished run.

#### Scenario: Inspect a worker after the run
- **WHEN** the user opens a finished run and selects `worker-5`
- **THEN** the charts show that worker's CPU, memory against its limit and throughput over the whole run
- **AND** its peak values and any out-of-memory event are listed

#### Scenario: Compare containers within a run
- **WHEN** the user selects several containers of one run
- **THEN** their metrics are overlaid on the same chart so uneven load is visible

### Requirement: Run comparison
The system SHALL let the user select two runs and compare their configurations and results side by side.

#### Scenario: Compare worker counts
- **WHEN** the user compares a run with 2 workers against a run with 5 workers
- **THEN** the UI shows both configurations, the difference in throughput, lag and latency, and overlays their charts

### Requirement: Run charts
The system SHALL show charts for throughput, lag, latency and container resource usage over the time of a finished run.

#### Scenario: Open a finished run
- **WHEN** the user opens a finished run
- **THEN** the charts show the recorded samples over its duration

### Requirement: Delete history
The system SHALL let the user delete a single run or the whole history.

#### Scenario: Delete one run
- **WHEN** the user deletes a run
- **THEN** it and its samples are removed and no longer listed

### Requirement: Message window snapshot
The system SHALL record the message window of every run, from the first message sent to the last message processed, and SHALL show it as a timeline with the sending duration, the end-to-end duration, the tail after sending ended and the startup latency.

#### Scenario: Timeline of a finished run
- **WHEN** the user opens a finished run
- **THEN** the report shows the phases sending and catching up with their durations in seconds from the start of the producer
- **AND** the throughput, latency and lag charts mark where sending started, sending ended and everything was processed

### Requirement: Resource analysis inside the window
The system SHALL compute and show, for every container, resource usage restricted to the message window: average and peak CPU, CPU time used, average and peak memory, memory headroom against the limit, network in and out, and CPU time per 1,000 messages.

#### Scenario: Zoom into a container
- **WHEN** the user selects a container in the run report
- **THEN** its CPU, memory, network and messages-per-second charts are shown for the message window only
- **AND** the user can switch to the whole run

#### Scenario: Short runs keep resolution
- **WHEN** a run is in progress
- **THEN** container metrics are sampled at least twice per second so that runs lasting a few seconds still produce a readable chart

### Requirement: Plain-language verdict
The system SHALL generate a short list of conclusions in plain language for every finished run: correctness, timing, whether consumers kept up, bottlenecks (CPU, delay ceiling, idle workers), uneven load, memory pressure and latency.

#### Scenario: Consumers were behind
- **WHEN** processing finished well after sending ended
- **THEN** the verdict says that consumers were slower than the producer and states the peak lag and the time to drain it

### Requirement: Load distribution
The system SHALL show how many messages each worker processed with its share and partitions, and the peak lag of each partition.

#### Scenario: Uneven partitions
- **WHEN** partitions do not divide evenly between workers
- **THEN** the report shows unequal shares and the verdict explains why
### Requirement: Network needed and actual
The report SHALL show the size of the data the messages need, what compression saved, and what the senders, the broker and
the workers really moved. Before a run the form SHALL show an approximate size after compression, based on measured ratios.

### Requirement: Times in a readable unit
Times SHALL be shown as milliseconds, seconds or minutes, whichever reads best. A latency at the measurement limit of the
histogram SHALL be shown as "at least" that value.

### Requirement: Clear states
A container SHALL be shown as Running, Starting, Done, Stopped, Crashed or Out of memory (never the raw Docker word), each
with an explanation. A run that stops because a container ran out of memory SHALL say which container and what to change.