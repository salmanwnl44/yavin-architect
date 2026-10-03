# Getting started with Yavin Architect

This guide takes you from a fresh checkout to a design session you can watch, steer and
approve, and to a golden-task run. It covers Windows without Docker and Linux with Docker
Compose. Commands are shown for PowerShell and for bash where they differ.

What you are starting:

- **PostgreSQL 16** holds the ledger and every read model.
- **A Temporal dev server** keeps each session's history, so a session survives a crash.
- **The worker** (`architect worker`) does the session's work.
- **The CLI** (`architect ...`) starts sessions and talks to them. An HTTP API with the same
  operations is available through `architect serve`.

## 1. Install

You need Python 3.11 or newer and git.

```
git clone https://github.com/salmanwnl44/yavin-architect.git
cd yavin-architect
python -m venv .venv
```

Activate the environment and install:

```
# PowerShell
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"

# bash
source .venv/bin/activate
pip install -e ".[dev]"
```

## 2. Start PostgreSQL and Temporal

### Linux, with Docker Compose

```
docker compose --profile sessions up -d
```

That starts PostgreSQL on port 5432 and a Temporal dev server on port 7233. Its web UI is at
http://localhost:8233. The default settings already point at both.

### Windows, without Docker

**PostgreSQL.** Download the PostgreSQL 16 "binaries" zip for Windows from EnterpriseDB
(for example `https://get.enterprisedb.com/postgresql/postgresql-16.9-1-windows-x64-binaries.zip`),
unzip it to a folder of your choice, then, in that folder:

```
.\pgsql\bin\initdb.exe -D data -U architect --auth=trust -E UTF8
.\pgsql\bin\pg_ctl.exe -D data -l pg.log -o "-p 5432 -c listen_addresses=127.0.0.1" start
.\pgsql\bin\createdb.exe -h 127.0.0.1 -U architect architect
```

Point the application at it. Use `127.0.0.1`, not `localhost`: on Windows `localhost` can
resolve to IPv6 first, where this server does not listen.

```
$env:ARCHITECT_DATABASE_URL = "postgresql://architect:architect@127.0.0.1:5432/architect"
```

Stop it later with `.\pgsql\bin\pg_ctl.exe -D data stop`.

**Temporal.** Install the Temporal CLI (see the Temporal documentation for your platform)
and run a dev server in a terminal of its own:

```
temporal server start-dev
```

The golden runner and the tests do not need this step: when no server is configured they
start a dev server that the Temporal SDK downloads by itself.

### Both platforms

Create the tables:

```
architect init-db
```

## 3. Put the key in the shell

The application reads its Anthropic key from `ARCHITECT_ANTHROPIC_API_KEY`, and falls back to
`ANTHROPIC_API_KEY`. Set it in the shell that starts the worker. Never put it in a file in
the repository.

```
# PowerShell
$env:ARCHITECT_ANTHROPIC_API_KEY = "..."

# bash
export ARCHITECT_ANTHROPIC_API_KEY=...
```

Without a key the worker has only the mock provider, which cannot design anything. The mock
golden run in section 9 needs no key.

## 4. Start the worker

In a terminal of its own, with the environment variables above:

```
architect worker
```

It stays in the foreground. Sessions keep their state in Temporal, so you can stop and
restart the worker at any time: a session continues where it was.

## 5. Start a session from a brief

A brief is a Markdown file that says what you want designed. State requirements with numbers
and units wherever you can: a requirement with a metric, a target and a unit becomes a
checkable claim, and one without them becomes an open risk.

```markdown
# Brief: event ingest pipeline

Producers send events to an API gateway, which enqueues them; workers write them to a store.

## Requirements

- [peak-ingest] The pipeline must sustain a peak of 1500 req/s.
- [ack-latency] Acknowledgement latency must stay under p99 < 300 ms.
- [robust] The system should be robust.
```

The `[slug]` at the start of a requirement is optional. It fixes that requirement's id
(`req_peak-ingest`), which you need when you supply a model that refers to it.

```
architect session start --project demo --brief brief.md --preset quick
```

The project must exist first; create it once with `architect new-project demo`. The start
command prints the session id, `ses_...`.

Presets set the limits:

| Preset | Rounds | Wall clock | Tokens | USD | Human gates |
| --- | --- | --- | --- | --- | --- |
| quick | 3 | 6 h | 2,000,000 | 25 | at the end |
| deep | 5 | 10 h | 6,000,000 | 75 | after the first attack, and at the end |
| exhaustive | 10 | 24 h | 25,000,000 | uncapped | at the end |

Override any of them, for example `--override usd=3 --override max_rounds=2`.

To **review an existing design** instead of drafting one, pass it as a seed. The seed is a
System Model JSON document (`phase0-contracts/system_model.schema.json`). It is committed
through the same validation as any change, the draft phase is skipped, and the session goes
straight to checking it:

```
architect session start --project demo --brief brief.md --seed model.json
```

## 6. Watch it

```
architect session watch --session ses_...
```

The view redraws every two seconds and shows the status and outcome, the phase timeline with
durations, the current round, what blocks the gate, each failing check with one line of
evidence, the open risks, spend against budget, and the last five agent messages. Press
Ctrl-C to leave; the session keeps running. `--once` prints one frame.

Other things you can do while it runs:

```
architect session status --session ses_...                 # the live state as JSON
architect session steer  --session ses_... --text "prefer at-least-once delivery"
architect session pause  --session ses_...
architect session resume --session ses_...
architect session cancel --session ses_...                 # packages the best so far, then ends
```

A steer is recorded as your guidance and is in the context of every later step.

## 7. Decide at the gate

A session ends at a human gate with a package, in status `awaiting_approval`. That is true
whether it converged, ran out of rounds, or was stopped by its budget or the wall clock. Only
`cancel` ends without a gate. What you can do there depends on the package:

| Decision | When it applies | What it does |
| --- | --- | --- |
| `approve` | the package's gate verdict is ALLOWED | ends the session as `approved` |
| `approve-with-risks --reason "..."` | any package | signs one waiver per blocking reason, in your name, with your reason; ends as `approved_with_risks` |
| `reject` | any package | ends as `rejected` |
| `extend` | the session was stopped by budget or wall clock | raises the limits and resumes from the best version so far |

```
architect session approve --session ses_...
architect session approve-with-risks --session ses_... --reason "accepted for the pilot"
architect session reject --session ses_...
architect session extend --session ses_... --usd 5 --tokens 500000
architect session extend --session ses_... --wall-clock-minutes 120 --rounds 2
```

A decision that does not apply is refused with the reason, and the gate stays open. The
amounts given to `extend` are added to the current limits. Your name on a decision comes
from `--signer`, else `ARCHITECT_USER`, else your login name.

## 8. Read the result

```
architect session show --project demo --session ses_...
```

This prints the timeline, each round with the structural diff its repair made, the gate
verdict with its blocking reasons, the open risks and the package key. The package itself is
a JSON file under `data/objects/`, named by that key. It holds the best model version, every
check result, which components satisfy which requirement, the open risks with their
evidence, the decisions recorded, and the spend.

Compare any two model versions:

```
architect model diff --project demo mv_AAAA mv_BBBB
```

Ask why an element is in the model: the requirements it satisfies, the decisions that
affect it, the claims those rest on, and the sources with their locations:

```
architect why --project demo --element cmp_GATEWAY0001
architect why --project demo --element cmp_GATEWAY0001 --version mv_AAAA
```

## 9. Run a golden task

A golden task is a reference problem with planted flaws and an answer key. `gt-001` is an
ingest pipeline whose seed model has four flaws, each of which a specific check must catch.

Without a key, with the task's scripted architect:

```
architect golden run gt-001 --mode review
architect golden run gt-001 --mode review --kill-after attack
architect golden run gt-001 --mode design
```

- `review` starts from the task's seed model; `design` starts from the brief alone.
- `--kill-after attack` ends the worker process once the first attack phase has completed
  and starts a new one. The session must resume from its history.
- The runner starts its own worker process, so you do not need `architect worker` for it.

Each run prints a scorecard and writes it as JSON: which planted flaws were caught and in
which round, which were repaired, the outcome, the gate verdict, requirement coverage, the
linter's risks, whether the resume was clean, rounds, duration, tokens and usd.

With the real gateway, under the task's cap of $3 per session:

```
architect golden run gt-001 --mode review --live --kill-after attack
architect golden run gt-001 --mode design --live
```

Live scorecards are written to `goldens/results/`. The runner approves a package whose gate
is ALLOWED and rejects any other. It never signs a waiver.

## Where things are

| What | Where |
| --- | --- |
| Limits and presets | `config/presets.yaml` |
| Models, tiers and prices | `config/models.yaml` |
| Raw sources and session packages | `data/objects/` (set `ARCHITECT_OBJECT_STORE` to move it) |
| Database | `ARCHITECT_DATABASE_URL` |
| Temporal | `ARCHITECT_TEMPORAL_ADDRESS` (default `localhost:7233`) |
| What has been built, and how it was tested | `PROGRESS.md` |
