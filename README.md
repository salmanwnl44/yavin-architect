# Yavin Architect

The design-verification harness that runs alongside Yavin. This repository currently holds:

- `phase0-contracts/`: the frozen Phase 0 contract schemas everything builds against.
- `src/architect/`: Milestone 1, the append-only project ledger and the Arbiter, the single
  writer that validates and commits every event.

See `PROGRESS.md` for what has landed and the exact commands to run it, and `CLAUDE.md` for
the architecture rules.
