# Yavin Architect — Phase 0 Contracts (v0.1 DRAFT)

These five schemas are the frozen interfaces everything in Phase 1 builds against.
They are the machine-readable form of the architecture spec; nothing in them is new —
each file implements a section of the spec, and the spec wins on any conflict until sign-off.

| File | Governs | Spec section |
| --- | --- | --- |
| `ledger_events.schema.json` | Every event in the append-only project ledger (the truth plane) | §5-P2, §6, §7 |
| `claim.schema.json` | The atomic unit of knowledge: typed claims with evidence, conditions, taint, bitemporal stamps | §7 |
| `system_model.schema.json` | The typed, versioned System Model the IDE canvas edits (components, flows, SLOs, trust boundaries, FMEA) | §11 |
| `agent_protocol.schema.json` | Every message between the Harness and its stateless agent workers | §12 |
| `check_catalog.schema.json` | The verification check catalog (L0–L5) as versioned data | §14 |

## Status and versioning rules

- **Status: DRAFT v0.1** — for owner review. Sign-off freezes it as v1.0.
- After freeze, a contract never changes in place. A change is a **new version** plus a
  migration event in the ledger (the §5-P2 discipline applied to the contracts themselves).
- v0.1 keeps each file **self-contained** (shared enums like `EpistemicStatus`, `Severity`,
  `Taint` are duplicated per file) so each schema validates standalone. v0.2 consolidates
  them behind `$ref`s to a common schema once tooling is in place.
- Cross-schema references (e.g. a ledger event carrying a claim) are **by convention** in
  v0.1 (`"type": "object"` + description naming the governing schema); v0.2 makes them
  mechanical `$ref`s.

## Phase 0 exit test (spec §21)

1. All five schemas meta-validate against JSON Schema 2020-12. ✔ (checked in CI-style script)
2. Sample instances round-trip: a claim, a ledger event, an objection. ✔
3. **Next deliverable:** a fixture ledger (one tiny design session as ~40 events) that a
   hand-written replayer turns into a graph projection and a model version. That fixture is
   the contract's real test and Phase 1's first integration test.

## Harness benchmarking (decision record §22, Entry 2)

No public benchmark exists for design-verification harnesses (SWE-bench and friends test
coding agents). The harness therefore carries its own benchmark, reported per release:

1. **Golden tasks** — reference design problems with planted flaws; headline metric is
   planted-flaw catch-rate (§18).
2. **Postmortem replays** — reconstruct the pre-incident design of systems with publicly
   documented failures; the harness must flag the documented cause. This grounds the
   benchmark in reality, not in flaws we invented.
3. Component-level proxies where public datasets exist: claim extraction vs. information-
   extraction sets, retrieval vs. standard QA sets.

## Identifier conventions

ULID-style ids with typed prefixes: `evt_` event, `clm_` claim, `src_` source, `cmp_`
component, `if_` interface, `flw_` flow, `req_` requirement, `adr_` decision, `obj_`
objection, `chk_` check result, `ses_` session, `tsk_` task, `mv_` model version,
`msg_` protocol message, `exp_` experiment, `wvr_` waiver.
