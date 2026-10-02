# Yavin Architect

The design-verification harness that runs alongside Yavin (the IDE). Python end to end;
Postgres is the only datastore so far. The architecture spec is in `docs/`; `PROGRESS.md`
says what has landed and how to run it.

## Ground truth

`phase0-contracts/` is **FROZEN at v1.0** (`phase0-contracts/CHANGELOG.md` lists what changed
from the v0.1 draft). Never edit, reformat, lint-fix or add to anything in it.
Read it before coding, in this order: `README.md`, the five `*.schema.json` files,
`fixture/fixture_ledger.jsonl`, `fixture/replay.py`.

`fixture/replay.py` is the reference semantics: the Arbiter must refuse every ledger
`replay.py` refuses, and accept what it accepts except where the contracts README lists a
rule as Arbiter-enforced (model version parents, proposal id uniqueness).

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

## Layout

- `src/architect/arbiter.py`: the commit path (lock, idempotency, stamp, validate, write).
- `src/architect/rules.py`: per-event-type `check` (may reject) and `apply` (state fold).
- `src/architect/state.py`: the `arb_*` projection tables.
- `src/architect/ledger.py`: read side (pages, head, dump, hash-chain verification).
- `src/architect/rebuild.py`: rebuild `arb_*` from the ledger and diff.
- `src/architect/projections.py`: the `proj_*` fold, one handler per projected event type.
- `src/architect/projector.py`: the projector worker (cursor, batches, LISTEN/NOTIFY, rebuild,
  content hash).
- `src/architect/readmodel.py`: queries over `proj_*` for the GET endpoints.
- `src/architect/api.py`, `src/architect/cli.py`: FastAPI app and the `architect` entrypoint.
- `src/architect/schema.sql`: all DDL, idempotent, applied by `architect init-db` and on startup.

## Commands

```
pip install -e ".[dev]"
docker compose up -d postgres            # or any Postgres 16; set ARCHITECT_DATABASE_URL
ruff check .
pytest
```

Tests create a throwaway schema per test inside the database named by
`ARCHITECT_DATABASE_URL` (default `postgresql://architect:architect@localhost:5432/architect`).

## Working agreements

- Small commits per logical step. Update `PROGRESS.md` when a milestone step lands.
- Adding an event type or a rule means: a `Rule` in `rules.py`, any new code in the rejection
  table in `PROGRESS.md`, and a refusal test for every new rejection code.
- Adding a read model means: its table in `schema.sql` and in `PROJ_TABLES`, a handler in
  `projections.py`, and a test that a rebuild reproduces it.
- Out of scope until their milestone: LLM calls, extraction, the checks engine, Temporal
  workflows, a graph database, vector search, UI, auth, multi-tenancy.
