# Yavin Architect

The design-verification harness that runs alongside Yavin (the IDE). Python end to end;
Postgres is the only datastore so far. The architecture spec is in `docs/`; `PROGRESS.md`
says what has landed and how to run it.

## Ground truth

`phase0-contracts/` is **FROZEN at v1.2** (`phase0-contracts/CHANGELOG.md` lists what changed
from the v0.1 draft, from v1.0 and from v1.1). Never edit, reformat, lint-fix or add to
anything in it; a new version is a module of its own, sanctioned by the owner.
Read it before coding, in this order: `README.md`, the five `*.schema.json` files,
`fixture/fixture_ledger.jsonl`, `fixture/replay.py`.

`fixture/replay.py` is the reference semantics: the Arbiter must refuse every ledger
`replay.py` refuses, and accept what it accepts except where the contracts README lists a
rule as Arbiter-enforced (model version parents and uniqueness, the model fold, proposal id
uniqueness). Since v1.2 it folds model versions per branch and holds findings and session
decisions to their rules.

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
   `ses_sessions` is one of them since contracts v1.2: sessions say everything through
   events (`session.status_changed` for every status change and every gate decision), and
   no session module writes the table. A project's head is the head of branch `main`.
7. **Checks are pure.** A check is `check(model, ctx, params) -> CheckOutcome` with no
   database, network, clock or randomness, and imports nothing from the api, db, Arbiter or
   projector modules (`tests/test_architecture.py` enforces it). The runner records every
   result as a `check.result` event through the Arbiter, idempotent on the inputs hash.
8. **One path to any model.** Every LLM call goes through `architect.gateway`. Only
   `gateway/providers/` imports an LLM SDK or speaks HTTP to a model server, and only
   `config/models.yaml` names a model id, a family or a price (`tests/test_gateway.py`
   enforces both). Every attempt is recorded in the append-only `gw_calls` log, written
   AHEAD: a `started` row with the reservation before the provider is called, then the row
   that closes it. A started row nothing closed is swept as `abandoned` and its reserved
   cost stays charged: spend may over-count, never under-count. Embeddings take the same
   path (`Gateway.embed`). Keys are read from the environment and never stored, logged or
   raised.
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
    (session_id, phase, round, step), never from time or a random id. Every activity
    heartbeats (the wrapper in `activities.py` does it), so one whose worker died is retried
    within `temporal.heartbeat_seconds`, not after its full timeout. Agents propose and the
    Arbiter decides; its rejections go back to the agent as structured errors for a bounded
    retry. Every agent message is a typed protocol message in the append-only `ag_messages`.
    Waivers are human-only: the Architect may only request one, as an open risk.
12. **Signals record; the workflow decides when they apply.** A signal handler only writes to
    workflow state (a flag or a buffer). The run consumes it at a well-defined point: a step
    boundary, the loop's exit, or a human gate. A signal is never lost and never applied
    twice, and one that cannot apply (a decision while no gate is open, `approve` on a
    BLOCKED package, `approve_with_risks` without a reason) is refused with a recorded
    reason. Every outcome but `cancelled` ends at a human gate with its package.
    `approve_with_risks` is the only path that signs a waiver, and it signs as the human.
13. **Retrieval is claim-first, and the knowledge plane is derived.** Search returns claims
    with status, grade, conditions, taint and evidence locators, never bare text
    (`tests/test_m8_retrieval.py` enforces it). The graph is a projection (`proj_graph_*`,
    `proj_entity_*`, folded only by the projector); an edge without provenance is
    un-storable. The search index, the vectors, the communities table, the AGE graph and
    the pgvector table are indexes: dropping them loses nothing. Each of GraphStore and
    VectorIndex has two backends that must give identical answers (the parity tests), so the
    extension-free fallback is never a second-class path. Entity merges and community
    summaries are events through the Arbiter; `architect.knowledge` imports no provider SDK.

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
  query, reads of the `ses_sessions` projection); `config/presets.yaml` the presets.
- `src/architect/knowledge/`: the knowledge plane (M8). `graph.py` what the graph is, the
  GraphStore interface and its SQL backend; `graph_age.py` the Apache AGE backend;
  `vectors.py` the VectorIndex interface, exact and pgvector; `index.py` the search index
  (claim text, entity names, embeddings) that follows the read models; `retrieval.py` hybrid
  search and RRF; `resolution.py` entity resolution; `communities.py` Leiden and summaries;
  `plane.py` the one object the API, the CLI and the compiler use; `config/knowledge.yaml`
  the settings. The graph's fold is in `projections.py`; embeddings are in
  `gateway/embedding.py`. `ci/knowledge-postgres/` builds Postgres 16 with AGE and pgvector.
- `src/architect/console.py`: the session snapshot and its pure rendering (`session watch`),
  and the why-trace formatter. `src/architect/modeldiff.py`: the structural diff between two
  model versions, pure.
- `src/architect/golden/`: the golden runner (`runner.py`, a session with the worker in its
  own process), its worker process (`worker.py`), the scripted architect for mock mode
  (`scripted.py`) and the scorecard (`scorecard.py`). `goldens/` at the repo root holds the
  tasks (`gt-001/`: brief, seed model, answer key, mock outputs), the scorecard schema and
  `results/` for live scorecards.
- `docs/getting-started.md`: how to run all of it, on Windows without Docker and on Linux.
- `src/architect/api.py`, `src/architect/cli.py`: FastAPI app and the `architect` entrypoint.
- `src/architect/schema.sql`: all DDL, idempotent. Applied on startup only when the schema is
  not current (a table is missing, or the file's hash in `schema_meta` differs), so a
  starting process takes no table locks; `architect init-db` forces it.

## Commands

```
pip install -e ".[dev]"
docker compose up -d postgres            # Postgres 16 with AGE and pgvector (built from ci/);
                                         # any Postgres 16 works: set ARCHITECT_DATABASE_URL
ruff check .
pytest
docker compose --profile sessions up -d  # Temporal dev server for sessions; then:
architect worker                         # the session worker (ARCHITECT_TEMPORAL_ADDRESS)
architect golden run gt-001 --mode review --kill-after attack   # mock; add --live for the real gateway
                                         # --kill-mode external: an OS-level kill (default with --live)
```

The session tests run on Temporal's time-skipping test environment (downloaded on first
use); two of them need a real dev server and start one themselves unless
`ARCHITECT_TEMPORAL_ADDRESS` names one, as CI does.

Tests marked `needs_age` or `needs_pgvector` are deselected (not skipped) when the test
database lacks the extension, and `fastembed_smoke` when the optional package is absent; CI
runs the suite on the knowledge image and on a plain Postgres, plus the smoke job.

Live provider tests are marked `live` and deselected by default; `pytest -m live -v` with a
key in the shell runs them. The app reads its key from `ARCHITECT_ANTHROPIC_API_KEY`, falling
back to `ANTHROPIC_API_KEY`.

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
- Adding a GraphStore or VectorIndex capability means: both backends, and a parity test.
- No DDL on a hot path: `CREATE ... IF NOT EXISTS` and `ALTER TABLE` still lock the table.
  Schema changes go in `schema.sql`; anything created lazily checks the catalog first.
- Adding an activity means: an idempotency scheme derived from (session, phase, round, step),
  a result the workflow can act on without I/O, and a test in which it is retried or replayed.
- A flaky test is a defect, in the test or in the code. Reproduce it in a loop with the
  assertion output kept, fix the root cause, and prove the fix with 100 consecutive green
  iterations. A session test waits on activity events (`session_fixtures.ActivityGate`) or
  on a state that holds until the test acts; it never waits on a transient state and never
  sleeps to see what happens.
- Out of scope until their milestone: the alternatives tournament (K > 1, M11), AI adversaries
  (M10), web research and the web and arXiv connectors, discovery detectors (contradictions,
  gaps: M9), UI, auth, multi-tenancy.

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
