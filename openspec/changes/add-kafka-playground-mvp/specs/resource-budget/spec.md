# Resource Budget

## ADDED Requirements

### Requirement: User-defined memory budget
The system SHALL let the user set a memory budget in megabytes for all containers created by the app,
including the Kafka broker, the producer and the workers.

#### Scenario: Set a budget
- **WHEN** the user enters a budget of 2048 MB
- **THEN** the budget is saved and survives restarts
- **AND** the UI shows used, remaining and total budget

#### Scenario: Budget above available memory
- **WHEN** the user enters a budget larger than the memory currently available to Docker
- **THEN** the UI shows a warning with the available amount
- **AND** the system still enforces the real available memory when creating containers

### Requirement: Admission check before creating containers
The system SHALL check every container-creating request against both the remaining budget and the real available memory
(minus a safety reserve) before creating anything, and SHALL create nothing if the check fails.

#### Scenario: Five workers fit
- **WHEN** the remaining budget is 2048 MB and the user requests 5 workers of 400 MB each (2000 MB)
- **THEN** the request is accepted

#### Scenario: Request exceeds the budget
- **WHEN** the user requests 6 workers of 400 MB each (2400 MB) with 2048 MB remaining
- **THEN** the request is rejected as a whole, with the required, remaining and missing amounts
- **AND** no container is created

#### Scenario: Budget allows it but the machine does not
- **WHEN** the budget has room but the real available memory minus the safety reserve is lower than requested
- **THEN** the request is rejected and the message names the real available memory as the limit

#### Scenario: Pre-creation estimate in the UI
- **WHEN** the user changes the number of workers or the memory per worker in the form
- **THEN** the UI shows the total requested memory and whether it fits, before the user submits

### Requirement: Memory limit on every container
The system SHALL apply a hard memory limit to every container it creates, equal to the amount counted in the budget.

#### Scenario: Limit is applied
- **WHEN** a worker with 400 MB is created
- **THEN** its container is created with a 400 MB memory limit
- **AND** the budget counts 400 MB for it

### Requirement: Budget is released
The system SHALL release a container's memory from the budget when the container is removed.

#### Scenario: Removing workers frees the budget
- **WHEN** the user removes 2 workers of 400 MB
- **THEN** the remaining budget increases by 800 MB
