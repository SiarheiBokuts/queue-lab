# Web UI

## ADDED Requirements

### Requirement: Dark, English-only interface
The system SHALL present the web interface in English only and with a dark theme only, with no language switcher and no theme switcher in the MVP.

#### Scenario: Interface language and theme
- **WHEN** a user opens the site in any browser, regardless of the browser language or the system theme
- **THEN** all texts are in English and the dark theme is used

### Requirement: Main screens
The system SHALL provide these screens: Dashboard, Create setup, Container detail, Runs and Settings, reachable from a persistent navigation.

#### Scenario: Navigate between screens
- **WHEN** the user selects an item in the navigation
- **THEN** the corresponding screen opens without a full page reload
- **AND** live data continues to arrive over the same connection

### Requirement: Dashboard
The system SHALL show on the Dashboard: the current queue and run state, the memory budget bar split by Kafka, producer and workers,
the live topology, headline metric tiles (messages per second, p99 latency, total lag, lost and duplicated), a throughput chart,
the partition lag heatmap and a table of all containers with CPU, memory against limit and messages per second.

#### Scenario: Dashboard during a run
- **WHEN** a run is in progress
- **THEN** all of the listed elements update live

#### Scenario: Empty state
- **WHEN** no queue exists yet
- **THEN** the Dashboard invites the user to create the first queue instead of showing empty charts

### Requirement: Create setup screen
The system SHALL provide one form to configure partitions, worker language, worker count, memory per worker, producer language,
message count, message size, processing scenario and its parameters, and SHALL show the resulting memory plan before submission.

#### Scenario: Memory plan updates while typing
- **WHEN** the user changes the worker count or the memory per worker
- **THEN** the form shows the total requested memory, the remaining budget and whether the request fits

#### Scenario: One button
- **WHEN** the user submits the form
- **THEN** the system creates the queue, starts the workers and the producer and starts the run, showing each step

### Requirement: Settings screen
The system SHALL provide a Settings screen with the memory budget in megabytes, the Kafka mode (regular or light), and the option to
remove lab containers when the app stops.

#### Scenario: Change the budget
- **WHEN** the user saves a new memory budget
- **THEN** the dashboard budget bar and the admission checks use the new value immediately

### Requirement: Clear errors
The system SHALL show errors in plain English that state what happened and what the user can do, and SHALL NOT show raw exception text.

#### Scenario: Docker is not running
- **WHEN** Docker is not reachable
- **THEN** a banner explains that Docker needs to be running and container actions are unavailable

### Requirement: Scenario descriptions
The system SHALL describe each processing scenario in the run form in detail: what it simulates, what the worker does and what the user will see in the results, and SHALL provide the scenario parameter with a hint.

#### Scenario: Choosing a scenario
- **WHEN** the user selects a scenario
- **THEN** the form shows what it simulates, what the worker does, what to look for in the results and its parameter

### Requirement: Code visibility
The system SHALL let the user read the real code that runs inside the containers, for both Python and Node.js: the producer, the per-message handler, the scenario branch and the full source files, taken from the same files that are built into the worker images.

#### Scenario: Show the code of a scenario
- **WHEN** the user presses "Show the code of this step" in the run form
- **THEN** the scenario branch and the handler are shown in the chosen language with a copy button

#### Scenario: Code page
- **WHEN** the user opens the Code page
- **THEN** the message format, the producer, the handler and the full source files are available for each language