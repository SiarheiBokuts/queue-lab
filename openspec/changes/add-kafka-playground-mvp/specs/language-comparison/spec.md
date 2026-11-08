# Language Comparison

## ADDED Requirements

### Requirement: The same test in several languages
The system SHALL run the same setup once per chosen language (two or more of Python, Node.js, Java, Go, Rust), one after
another with a configurable pause, and SHALL then offer a side-by-side comparison. By default all languages are chosen,
the order is drawn at random when the comparison starts (so no language always runs first), and the load is one million
messages with lz4 compression.

#### Scenario: Random order
- **WHEN** the user starts a comparison of five languages with the default settings
- **THEN** the languages run in a random order and the dashboard shows that order, which language is running and the pause

#### Scenario: Fixed order
- **WHEN** the user turns random order off
- **THEN** the languages run in the order the user picked them

### Requirement: Equal conditions
Every language SHALL receive the same Kafka client settings from one place, SHALL read the same environment variables,
SHALL send the same report fields and SHALL do the same steps for every message. Each language MAY implement the steps in
the way that is fastest in that language. Tests enforce the shared settings, variables, report fields and the absence of
tuning values in worker code.

### Requirement: A winner decided by categories
The comparison SHALL score every run from 0 to 100 in speed (40 %), latency (30 %) and efficiency (30 %), where 100 means
best or within 5 % of the best on every metric of the category. A run that lost or duplicated messages SHALL NOT win. A
winner SHALL be named only when it leads the second place by at least five points; otherwise the comparison reports no
clear winner. Runs within 5 % of the best on a metric SHALL all be marked best, not "worse".

### Requirement: Every number side by side
The comparison SHALL show every metric of every run in one table (speed, time, latency, messages waiting, CPU, memory,
network, lost and duplicated messages) and a verdict in plain words. A latency at the measurement limit SHALL be shown as
"at least".