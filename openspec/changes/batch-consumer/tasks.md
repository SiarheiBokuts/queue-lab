# Tasks

Nothing here is started except where marked done. Order matters: 1 first, because the rest builds on it.

## 1. CPU task, the same plain loop everywhere
- [x] Python, Node.js, Java, Go, Rust: `total = (total + i * i) % 1000` in a loop (adds up squares, keeps three digits)
- [x] Fix the Python and Java tests that failed after this change
- [x] Unit test per language: `burn(4) == 14`, `burn(10) == 285`

## 2. One batch loop in every language
- [x] Python: `consume(N, 0.05)`, group by key, a thread per group, commit after the batch
- [x] Node.js: read up to N (check the `consume(N)` call of the client), promises per group, commit after the batch
- [x] Go: read up to N, goroutine per group, commit after the batch
- [x] Rust: read up to N, thread per group, commit after the batch
- [x] Java: `max.poll.records = N`, thread per group, `commitSync` after the batch
- [x] Remove the order setting: `ORDERING` from `contract.py`, `models.py`, the form, the five workers, the tests and
  the texts; the batch is always grouped by user
- [x] Remove the lanes: files, tests, `LANE_QUEUE`, lane hash, their texts in the UI and README
- [x] Keep the `@scenario-start` and `@scenario-end` markers, so the Code page still shows the handler
- [x] Split the code the same way in every language: a stats unit and a batch unit next to the consumer

## 2b. The same structure and comments
- [x] Units with the same names in every language: `read_batch`, `group_by_user`, `handle`, `handle_group`,
  `run_batch`, `commit`, `ConsumerStats` (table in proposal.md), each as a separate small method
- [x] The same one-sentence comment above each unit, in every language
- [x] Contract test: every consumer defines these units and reads `CONCURRENCY` as the batch size and the wait from the settings
- [x] The Code page shows the units of the chosen language side by side with the others

## 3. Settings, the same for everyone
- [x] One source of truth: the batch size and the longest wait are sent by the backend to every worker, no default of
  its own in any language (today Python and Java read up to 500, the others one)
- [x] `enable.auto.commit` becomes `false` in `config.py`; Java gets the same through `KafkaConf.java`
- [x] "Different users" stays in the main load block with the default 1000 (grouping by user is always on); a hint
  appears when there are fewer users than the batch size: "at most one message per user runs at a time"
- [x] The info icon and tooltip of both fields say it plainly: messages of one user are always handled one after
  another (like in production), messages of different users at the same time, so the number of users limits how many
  run together
- [x] `concurrency` in the form is the batch size (default 30), one text explains both meanings
- [x] `acks` stays `1`: with one broker `all` would mean the same; the README says so
- [x] Contract test: same variables, same report fields, no Kafka tuning in workers, same batch size everywhere

## 4. Report and checks
- [x] Count order violations per user in the consumer report (a sequence number lower than the last of that user)
- [x] Show the idle time at the end of a batch (waiting for the slowest group) on the run page
- [x] Live run of all five languages: no losses, no duplicates, no order violations

## 4b. The statistics are counted the same way everywhere
- [x] Review `ConsumerStats` and the producer report in all five languages, side by side: what counts as received,
  duplicate, gap and error; when `firstAt` and `lastAt` are set; how the latency is taken (the clock at the moment the
  batch arrives or at the message); how the histogram buckets and the max latency are counted; which offset is reported
- [x] Fix every difference found, so one message gives the same numbers in any language
- [x] Test with the same messages per language (a fixed list of messages gives the same received, gaps, duplicates and histogram)
- [x] Resources (CPU, memory, network) stay measured by the app from Docker, never by the workers (already so; keep it)

## 4c. Lost and duplicated messages, simpler and exact
Today every consumer keeps the last sequence number per (sender, partition): the next must be last + 1, a higher one
counts a gap, a lower or equal one a duplicate. Problems: it is a small state machine written five times; a worker that
starts (or gets a partition after a rebalance) takes its first message as the baseline, so a batch that is delivered
again after a crash is not seen as a duplicate; and gaps and duplicates are counted per worker, not for the whole run.
- [x] Replace it with counting the unique messages. Every message already has a unique number: (sender, partition,
  sequence). A worker reports the received sequence numbers per (sender, partition) as ranges (`0-4999, 5001-9000`),
  which stay tiny when nothing is lost. The backend merges the ranges of all workers of the run:
  - lost = sent minus the numbers received by anyone (the senders report how many they sent per partition)
  - duplicated = numbers received more than once (overlapping ranges, or a repeat inside one worker)
  - so a replay after a crash or a rebalance is a duplicate, and the report says which sender and partition
- Decided (the user agreed, "or whatever is more correct"): ranges, with two details that make it exact:
  - a worker also counts a repeat it sees itself (a number already inside its ranges), so a replay inside one worker is
    never lost when ranges are merged
  - ranges per (sender, partition) are capped (for example 1000); beyond the cap the worker keeps counting received and
    repeated messages but stops listing ranges, and the report says "too many gaps to list", so a broken run cannot
    make a huge report
- [x] The consumer code becomes one small step ("remember this number"), the same in every language; the judging
  logic lives once, in the backend (`analysis.py`), and is tested once
- [x] Keep the report fields `duplicates` and `gaps` for the UI (computed by the backend), so the pages do not change
- [x] Test: a fixed list with a lost number, a repeated number and a replayed range gives the exact counts
- [x] Add a way to see that it works: a Kafka run with a worker killed in the middle (restart) must show duplicates

## 4d. The diagrams (keep them, they work)
The two Mermaid diagrams of the README are liked and stay. They are updated to the batch design and reviewed:
- [x] "How it works" (browser, app, Docker, sandbox): stays as is, checked against the code
- [x] The worker diagram changes from "lanes" to the batch: Kafka, read a batch (up to N), group by user, groups run
  at the same time (messages of one user one after another), the scenario step, commit, next batch
- [x] A third small diagram for 2 workers and users: Kafka partitions, which worker owns which, which users are where
  (the example from the discussion), so "one user, one partition, one worker" is clear at a glance
- [x] Where to show them: README (all three, short text under each); the Code page (the batch diagram above the
  code of the chosen language); the new-run page (the 2-workers diagram, near the concurrency setting)

## 4e. Results without harsh grades
Review of `analysis.scores` found: the 0 to 100 points and the weights (40/30/30) are our own opinion; speed counts
three metrics that say the same (rate, total time, send rate), so it is counted three times; "efficiency" includes the
sender's CPU and memory, which are the same for every language when one sender is used; one lost or repeated
message "disqualifies" a run, but with commit after the batch a repeat after a crash is expected; the words "winner",
"cannot win" and "x worse" judge instead of describe.
- [x] Remove the overall score, the winner and the disqualification (`scores`, `CLEAR_LEAD`, `SCORE_CATEGORIES`, the
  "How the winner is decided" text, the winner card)
- [x] Show a profile per language instead: for each group of metrics (speed, latency, processor, memory, network) the
  value, a bar relative to the best one, and neutral words ("1.8x the CPU of Go", not "1.8x worse")
- [x] No verdicts at all. The results depend on the scenario (echo, delay, cpu), the message size, the batch size and
  the machine, so a sentence like "Go was best" would be true for one setup only. Remove the verdict texts: the
  `insights` and `highlights` conclusions of a run, the `leader` sentences and `verdict` of `compare`, and the
  `RunVerdict` component. The pages show numbers, charts and the setup that produced them
- [x] Every result shows its scenario and setup next to it (scenario, parameter, messages, size, batch size,
  users, memory, cores per container), so a number is never read without its conditions
- [x] What stays are facts about the data, not judgements: setups that differ besides the language, lost or repeated
  messages, a latency at the measurement limit, a run that did not finish
- [x] Lost and duplicated messages are shown as a fact with an explanation (what a repeat after a restart means), as a
  warning on that run, and they no longer remove it from the comparison
- [x] Do not count the sender's metrics when one sender is used for all runs; count them only with "Compare the senders too"
- [x] The comparison page keeps the table of every number with the best one in green, and the charts

## 4f. Other UI improvements to look at
- [x] Empty states with one clear next step (no runs yet, no queue yet)
- [x] The run page answers three things first: did it finish, was anything lost, how fast; details below
- [ ] The same units and number formats on every page (MB, ms, messages per second, percent of a core). Partly done: the speed labels are the same, not every page was audited
- [x] Check the light theme on every page (only some were checked)
- [x] Long lines in the Vue templates (219) split, the same for the new pages
- [ ] Keyboard and screen reader basics: labels on icon buttons, visible focus, contrast in both themes. Partly done: icon buttons are labelled and the focus ring is visible, the contrast was not measured

## 5. Documentation
- [x] README: a short section "How this differs from production" (one broker, `acks`, no TLS, no replication)
- [x] README and the Code page: the batch loop in one diagram
- [x] Table of the Kafka settings and where they are decided (`config.py`), with the one known gap
- [x] CHANGELOG entry

## Done in this change already
- [x] First-visit welcome page: Docker found, its cores and memory, choice of the memory budget (2, 4, 8 GB or own),
  what the lab can do. Stored as the setting `welcomed`.
- [x] Settings page text about the processor corrected (containers get a fair share of the cores)

## Open questions
- `acks=1` with one broker, or `acks=all`. With one broker both mean the same; `all` only matters with replicas.
- The next batch starts when the whole batch is done (decided, reason in proposal.md).
