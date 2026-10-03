# Yavin Architect

The design-verification harness that runs alongside Yavin (the IDE). Python end to end;
Postgres is the only datastore so far. The architecture spec is in `docs/`; `PROGRESS.md`
says what has landed and how to run it.

## Ground truth

`phase0-contracts/` is **FROZEN at v1.1** (`phase0-contracts/CHANGELOG.md` lists what changed
from the v0.1 draft and from v1.0). Never edit, reformat, lint-fix or add to anything in it.
Read it before coding, in this order: `README.md`, the five `*.schema.json` files,
`fixture/fixture_ledger.jsonl`, `fixture/replay.py`.

`fixture/replay.py` is the reference semantics: the Arbiter must refuse every ledger
`replay.py` refuses, and accept what it accepts except where the contracts README lists a
rule as Arbiter-enforced (model version parents and uniqueness, the model fold, proposal id
uniqueness).

If a contract blocks you, log the friction in `contracts-PROPOSALS.md` and work around it.
Do not change the contracts.

## Architecture rules (mechanical, not conventions)

1. **The ledger is truth.** Events are append-only. No code path ever UPDATEs or DELETEs a
   committed event, and a Postgres trigger on `events` raises if one tries.
2. **Single writer.** Only the Arbiter (`architect.arbiter`) commits. The API, the CLI and the
   tests all submit candidates to it. `tests/test_architecture.py` fails if any other module
   writes to `events`.
3. **Validation at the boundary.** Every committed event validates against
   `ledger_events.schema.json`, and embedded objects deep-validate against their own schemas
   (claim, model patch, objection, proposed check). Invalid means a typed rejection and
   nothing written.
4. **Arbiter state is a projection.** The `arb_*` tables are maintained in the same
   transaction as each commit and are rebuildable from the ledger alone
   (`architect rebuild-state`). Dropping them must never lose information. Anything a rule
   needs must therefore be derivable from committed events.
5. **Every rejection is typed and tested.** A rejection is `{code, detail, json_path?}` with
   HTTP 422 (schema or rule) or 409 (`BASE_MOVED`, `DUPLICATE_*`). Each code has a test
   proving the event is refused and that nothing was written.
6. **Read models are projections too.** The `proj_*` tables are written only by the projector
   (`architect.projector`), each batch in the same transaction as its `proj_cursors` row, and
   are rebuildable from the ledger alone (`architect rebuild-projections`). The projector
   never writes an event and never calls the Arbiter. The fold is deterministic: no
   wall-clock values and no generated ids. The GET endpoints over them read `proj_*` only.
7. **Checks are pure.** A check is `check(model, ctx, params) -> CheckOutcome` with no
   database, network, clock or randomness, and imports nothing from the api, db, Arbiter or
   projector modules (`tests/test_architecture.py` enforces it). The runner records every
   result as a `check.result` event through the Arbiter, idempotent on the inputs hash.
8. **One path to any model.** Every LLM call goes through `architect.gateway`. Only
   `gateway/providers/` imports an LLM SDK or speaks HTTP to a model server, and only
   `config/models.yaml` names a model id, a family or a price (`tests/test_gateway.py`
   enforces both). Every attempt is recorded in the append-only `gw_calls` log; keys are
   read from the environment and never stored, logged or raised.
9. **Models propose, the pipeline decides.** In ingestion (`architect.ingestion`) a model
   returns only subject, predicate, object, magnitude, conditions and a verbatim quote; the
   pipeline sets status, evidence, taint, provenance and ids, drops any candidate whose quote
   is not in its segment, wraps every segment as untrusted data, and never executes a tool
   (`tests/test_ingestion.py` enforces the import rule). Grade and confidence are computed in
   the projection, never written to the ledger.
10. **Until confidence is calibrated (M14), gating and inclusion decisions use grade and
    epistemic status only. Confidence may rank, never decide.**
11. **A session is a durable workflow; its side effects are activities.** The Temporal
    workflow (`architect.sessions.workflow`) is deterministic: no I/O, no clock but
    `workflow.now()`, no randomness, and it imports nothing from the database, the gateway or
    the Arbiter (`tests/test_sessions.py` enforces it). Every activity is idempotent: Arbiter
    keys, version, claim, proposal, task and message ids derive from
    (session_id, phase, round, step), never from time or a random id. Agents propose and the
    Arbiter decides; its rejections go back to the agent as structured errors for a bounded
    retry. Every agent message is a typed protocol message in the append-only `ag_messages`.
    Waivers are human-only: the Architect may only request one, as an open risk.

## Layout

- `src/architect/arbiter.py`: the commit path (lock, idempotency, stamp, validate, write).
- `src/architect/rules.py`: per-event-type `check` (may reject) and `apply` (state fold).
- `src/architect/state.py`: the `arb_*` projection tables.
- `src/architect/ledger.py`: read side (pages, head, dump, hash-chain verification).
- `src/architect/rebuild.py`: rebuild `arb_*` from the ledger and diff.
- `src/architect/model_fold.py`: the one fold of a model version from the one before it,
  used by the Arbiter's rules and by the projector.
- `src/architect/projections.py`: the `proj_*` fold, one handler per projected event type.
- `src/architect/projector.py`: the projector worker (cursor, batches, LISTEN/NOTIFY, rebuild,
  content hash).
- `src/architect/readmodel.py`: queries over `proj_*` for the GET endpoints.
- `src/architect/checks/`: the checks engine. `c0NN.py` are pure check functions,
  `catalog.json` + `catalog.py` the catalog and registry, `runner.py` the only module there
  that reads a database or records results (through the Arbiter).
- `src/architect/gateway/`: the model gateway. `gateway.py` is the one public entry;
  `providers/` holds the mock, Anthropic and OpenAI-compatible adapters; `config/models.yaml`
  (repo root) is the model table.
- `src/architect/ingestion/`: sources (`sources.py`, the one place `git` runs), segments
  (`parse.py`), the two extraction passes (`extract.py`), commit (`commit.py`), the job runner
  (`pipeline.py`), grades and confidence (`grades.py`); `config/ingest.yaml` and
  `config/confidence.yaml` hold their settings.
- `src/architect/sessions/`: design sessions (M6). `workflow.py` is the deterministic
  Temporal workflow (phases, loop, convergence rule, signals, status query); `activities.py`
  every side effect; `agent.py` the Architect agent v1 (prompts, output schemas, protocol
  messages, `ag_messages`); `compiler.py` the Context Compiler; `linter.py` the requirement
  linter; `worker.py` the Temporal worker; `service.py` the client side (start, signal,
  query, the `ses_sessions` read model); `config/presets.yaml` the presets.
- `src/architect/api.py`, `src/architect/cli.py`: FastAPI app and the `architect` entrypoint.
- `src/architect/schema.sql`: all DDL, idempotent, applied by `architect init-db` and on startup.

## Commands

```
pip install -e ".[dev]"
docker compose up -d postgres            # or any Postgres 16; set ARCHITECT_DATABASE_URL
ruff check .
pytest
docker compose --profile sessions up -d  # Temporal dev server for sessions; then:
architect worker                         # the session worker (ARCHITECT_TEMPORAL_ADDRESS)
```

The session tests run on Temporal's time-skipping test environment (downloaded on first
use); two of them need a real dev server and start one themselves unless
`ARCHITECT_TEMPORAL_ADDRESS` names one, as CI does.

Live provider tests are marked `live` and deselected by default; `pytest -m live -v` with a
key in the shell runs them.

Tests create a throwaway schema per test inside the database named by
`ARCHITECT_DATABASE_URL` (default `postgresql://architect:architect@localhost:5432/architect`).

## Working agreements

- Small commits per logical step. Update `PROGRESS.md` when a milestone step lands.
- Adding an event type or a rule means: a `Rule` in `rules.py`, any new code in the rejection
  table in `PROGRESS.md`, and a refusal test for every new rejection code.
- Adding a read model means: its table in `schema.sql` and in `PROJ_TABLES`, a handler in
  `projections.py`, and a test that a rebuild reproduces it.
- Adding a check means: a `c0NN.py` module with `CHECK_ID`, `USES` and `check`, its entry in
  `checks/catalog.json` and in `REGISTRY`, and unit tests for its pass, fail and empty cases.
- After a PR merges, delete its remote branch — don't ask.
- Adding an activity means: an idempotency scheme derived from (session, phase, round, step),
  a result the workflow can act on without I/O, and a test in which it is retried or replayed.
- Out of scope until their milestone: the alternatives tournament (K > 1, M11), AI adversaries
  (M10), web research and the web and arXiv connectors, entity resolution beyond exact slugs,
  a graph database, vector search, UI, auth, multi-tenancy.

### Module report (mandatory)
Every session ends with exactly this block and nothing after it:
1. Module name and one-line result (DONE / BLOCKED / PARTIAL).
2. Exit-test table: test | green/red | evidence (test name or command output).
3. CI run id and result for the final pushed commit.
4. Files changed — short summary by area.
5. Deviations from the prompt, each with the reason.
6. New entries added to contracts-PROPOSALS.md (or 'none').
7. Open questions for the owner (or 'none').

### Test integrity
Exit tests are the answer key and are written before or alongside the code. Never weaken,
skip or rewrite a test to make it pass. If a test seems wrong, stop and report it as an open
question.
