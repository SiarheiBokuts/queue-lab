# Instructions for AI assistants

queue-lab is a local lab: a FastAPI backend and a Vue 3 + TypeScript frontend start real Kafka and sender/worker
containers in Python, Node.js, Java, Go and Rust, and show speed, latency, CPU, memory and a language comparison.
Read [README.md](README.md) for what it does and [CONTRIBUTING.md](CONTRIBUTING.md) for the commands, tests and style.
This file holds only what an assistant must keep in mind.

## Working with git (the owner decides what leaves the machine)

- **Never push** (`git push`, force-push, deleting remote branches) unless the user says so in the current request.
  An earlier "push" does not cover later changes.
- **Few commits: do a task, then commit it as one commit**, not thirty small ones. This is about not scattering a task
  over many commits while it is being done. It does not forbid a follow-up commit on top of a pushed one: after a push,
  or after a review, add a new commit. Do not rewrite (`--amend`, rebase) a commit that is already pushed unless the
  user asks for it. If the user does ask, the force-push it needs (`--force-with-lease`, never plain `--force`) is
  still a separate push that the user must order.
- Work in a branch, never commit to `main`. The user opens the pull request himself.
- Commit messages: a short subject, then what changed and why. No `Co-Authored-By` or "Generated with" lines.
- Subagents must not commit, push or switch branches.

## Principles of the code

The code is read by people who know some other language than the one in front of them. So it is **simple, clear and the
same in every language**, and still production-like.

1. **One structure, five languages.** The same steps, in the same order, with the same names (in the idiom of the
   language: `messageMaker`, `MessageMaker`, `message_maker`) and the same comments. A change to a step, a name, a
   comment or a statistic is made in all five workers, and `backend/tests/test_contract.py` checks that they agree.
2. **A file reads top to bottom in groups.**
   - Producer: settings, counters, client, message, run. The sending loop is the part shown on the Code page.
   - Consumer: read a batch, group by user, handle the groups at the same time (a user's messages in order), commit.
3. **Small functions with one job and a name that says it** (`readSettings`, `make`, `produce`, `confirmed`). The
   main loop calls them and shows only the steps: make a message, send it, count it, keep the pace.
4. **What is remembered between steps lives in a small type** (counters, message maker), not in loose variables of a
   long function.
5. **Plain code over clever code.** No tricks, macros, deep generics or one-liners that need a puzzle to read. Prefer a
   few clear lines to a short obscure one. A language feature that hides what a real service would do is not used.
6. **Comments say what and why**, briefly, in plain English, above the code or after a short line. Name a limit, a
   unit and a reason (for example "random padding is cut from one pre-generated megabyte, so generating it costs
   almost nothing"). Do not comment what the code already says.
7. **Settings are read in one place, with literal names** (`MESSAGES`, `MSG_SIZE`, ...), because the contract test reads
   them. Kafka client settings come from the backend, not from a worker.
8. **Same behaviour, same numbers.** Pacing, headers, keys, padding and the report fields are identical, so a
   difference in a result comes from the language and not from the code around it.
9. **Production-like, honestly.** Use the standard client of each language and its normal settings. Do not tune one
   language to look better and do not add a library only one language has; where a language differs (Node.js sends in
   batches, Java has no read-ahead queue) it is written in README "Known limits".
10. **Formatting is the tool's job** (ruff, Biome, gofmt, rustfmt, google-java-format, Prettier). Do not format by
    hand against the tool; the checks fail on it.

## Rules of the project
- **One source of truth for Kafka settings:** `backend/app/config.py`. Workers only receive them.
- **Each worker has a page in `docs/workers/`** (what it does, what is special in the language and why, why we think it
  is fair, open questions). A change to a worker that changes how it works, or a difference found between languages,
  is written there and in the README table in the same change. Say what is not known as plainly as what is.
- **One source of truth for memory and CPU numbers** (defaults, minimums, budget buttons, the broker size): `config.py`.
  The screens read them from `/api/state` (`limits`, built in `budget.limits`); a page, a form or a text never repeats
  one. The README table of Docker memory is written by hand from them: change both together.
- **No limits on the numbers of a run** (messages, rate, partitions, ...), only warnings (`backend/app/advice.py`).
  Only the memory budget refuses a run. Bounds in `models.py` are type limits, not lab limits.
- **Numbers, not verdicts:** no scores, grades or "winner" in the report or the comparison.
- The Node.js worker stays on Node.js 24 and the Python worker on Python 3.14 until their Kafka clients ship binaries
  for Node.js 26 / Python 3.15 (see the Dockerfiles). Do not bump them.
- The Code page shows the region between `@producer-start` / `@producer-end` (and the consumer markers) of each worker:
  keep the markers on their own lines.

## Checking a change

Everything runs through Docker, nothing is installed on the host (commands in CONTRIBUTING.md): the unit tests and
linters of every part, then `docker compose -f compose.e2e.yaml run --rm e2e` against the running app. A change to a
worker is finished only after a real run of the five languages shows nothing lost, nothing repeated and no order
violations (the screenshots command does such a run).

- Docker Desktop keeps its disk on a volume with limited space: do not use `--no-cache` or prune unless needed, and do
  not restart or stop Docker Desktop without asking. Deleting the user's run history or Docker data needs a clear yes.
- PowerShell on Windows: no `&&`, no `<` redirection; pipe with `Get-Content file | docker ...`.

## Talking to the user

- Answer in Russian, plainly, and say often what you are doing.
- Name every command or check by what it does, with the tool in brackets, for example "formatting (gofmt)".
- Keep the docs short; update README, CONTRIBUTING and CHANGELOG together with the code.
