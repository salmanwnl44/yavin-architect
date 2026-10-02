# Progress

## M0: scaffold (done)

- Python 3.11+ project, src layout, package `architect`, `architect` console entrypoint.
- `docker-compose.yml`: `postgres:16`, plus a Temporal dev server behind the `sessions`
  profile (unused until the sessions milestone).
- pytest + ruff. `phase0-contracts/` is excluded from ruff because it is frozen.
- GitHub Actions CI (`.github/workflows/ci.yml`) runs, in order:
  `python3 phase0-contracts/validate.py`, `python3 phase0-contracts/fixture/replay.py`,
  `ruff check .`, `pytest`.
- `CLAUDE.md` carries the architecture rules.
- `architect.contracts` loads the five frozen schemas and builds the validators.

### Open

- `phase0-contracts/fixture/` (`fixture_ledger.jsonl`, `replay.py`) is not in the repo yet.
  The CI replay step fails until the owner adds it.
