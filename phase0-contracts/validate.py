#!/usr/bin/env python3
"""Phase 0 exit test, step 1-2: meta-validate all schemas, smoke-test sample instances."""
import json, glob, sys
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker

ok = True

# `format` is an assertion in these contracts, not an annotation (v1.0, P-2). date-time is
# only checked when rfc3339-validator is installed, so its absence is a failure, not a pass.
FORMAT = FormatChecker()
if "date-time" not in FORMAT.checkers:
    ok = False
    print("FORMAT FAIL date-time is not enforced: install rfc3339-validator")

def load(p):
    with open(p) as f:
        return json.load(f)

# 1. Meta-validation: every schema is itself valid JSON Schema 2020-12
schemas = {}
for path in sorted(glob.glob("phase0-contracts/*.schema.json")):
    path = Path(path).as_posix()  # same key on Windows and POSIX (v1.0, P-1)
    s = load(path)
    try:
        Draft202012Validator.check_schema(s)
        schemas[path] = s
        print(f"META OK   {path}")
    except Exception as e:
        ok = False
        print(f"META FAIL {path}: {e}")

def smoke(name, schema_path, instance, expect_valid=True):
    global ok
    v = Draft202012Validator(schemas[schema_path], format_checker=FORMAT)
    errs = sorted(v.iter_errors(instance), key=lambda e: e.path)
    valid = not errs
    status = "OK" if valid == expect_valid else "FAIL"
    if valid != expect_valid:
        ok = False
    print(f"SMOKE {status} {name} (valid={valid}, expected={expect_valid})")
    if valid != expect_valid:
        for e in errs[:3]:
            print(f"    - {list(e.path)}: {e.message[:140]}")

# 2a. A documented claim with evidence — must pass
claim_good = {
    "id": "clm_01HXAMPLE0AA",
    "subject": {"entity_type": "technique", "id": "speculative-decoding"},
    "predicate": "REDUCES",
    "object": {"entity_type": "metric", "id": "p50-latency"},
    "magnitude": {"value": -41, "unit": "%"},
    "conditions": {"gpu": "A100-80GB", "batch": 32},
    "status": "documented",
    "evidence": [{"source": "src_01HXAMPLE0BB", "span": "Table 4", "kind": "benchmark"}],
    "taint": {"origin": "external_untrusted", "license": "CC-BY-4.0"},
    "recorded_at": "2026-10-02T06:40:00+05:30",
    "provenance": {"extractor": {"model_tier": "tier-cheap", "prompt_hash": "a91c", "pipeline_version": 1}},
}
smoke("claim: documented w/ evidence", "phase0-contracts/claim.schema.json", claim_good, True)

# 2b. A documented claim WITHOUT evidence — must fail (no-silent-promotion)
claim_bad = dict(claim_good)
claim_bad.pop("evidence")
smoke("claim: documented w/o evidence rejected", "phase0-contracts/claim.schema.json", claim_bad, False)

# 2c. A load-bearing assumption without a verification plan — must fail
claim_assume = dict(claim_good)
claim_assume.update({"status": "assumed", "owner": "saumya", "load_bearing": True})
claim_assume.pop("evidence")
smoke("claim: load-bearing assumption w/o plan rejected", "phase0-contracts/claim.schema.json", claim_assume, False)
claim_assume2 = dict(claim_assume)
claim_assume2["verification_plan"] = {"kind": "experiment", "ref": "exp_01HXAMPLE0CC"}
smoke("claim: load-bearing assumption w/ plan accepted", "phase0-contracts/claim.schema.json", claim_assume2, True)

# 2d. A ledger event: waiver.signed — must pass; wrong payload must fail
evt_good = {
    "event_id": "evt_01HXAMPLE0DD",
    "project_id": "proj-yavin-dogfood",
    "seq": 41,
    "ts": "2026-10-02T06:41:00+05:30",
    "actor": {"kind": "human", "id": "saumya", "role": "owner"},
    "type": "waiver.signed",
    "payload": {"waiver_id": "wvr_01HXAMPLE0EE", "target_ref": "C-010", "risk": "Single Postgres node in Phase 1 dev env", "signer": "saumya"},
    "idempotency_key": "waiver-c010-p1",
}
smoke("event: waiver.signed", "phase0-contracts/ledger_events.schema.json", evt_good, True)
evt_bad = dict(evt_good)
evt_bad["payload"] = {"waiver_id": "wvr_01HXAMPLE0EE"}
smoke("event: waiver w/o signer rejected", "phase0-contracts/ledger_events.schema.json", evt_bad, False)
evt_bad_ts = dict(evt_good)
evt_bad_ts["ts"] = "2026-10-02 06:41"
smoke("event: ts that is not RFC 3339 rejected", "phase0-contracts/ledger_events.schema.json", evt_bad_ts, False)

# 2d'. v1.2 events: a finding, its resolution, a session status change, a branch
evt_finding = dict(evt_good)
evt_finding.update({
    "event_id": "evt_01HXAMPLE0MM",
    "actor": {"kind": "system", "id": "discovery", "role": "discovery"},
    "type": "finding.raised",
    "payload": {
        "finding_id": "fnd_01HXAMPLE0NN",
        "kind": "contradiction",
        "severity": "minor",
        "summary": "Two claims give different p50 latency reductions for speculative decoding.",
        "refs": ["clm_01HXAMPLE0AA", "clm_01HXAMPLE0PP"],
        "evidence_claims": ["clm_01HXAMPLE0AA", "clm_01HXAMPLE0PP"],
        "suggested_action": {"kind": "condition_analysis", "detail": "The conditions differ on: batch, gpu."},
        "detector": {"id": "D1", "version": 1},
        "dedupe_key": "d1:contradiction:clm_01HXAMPLE0AA,clm_01HXAMPLE0PP",
    },
    "idempotency_key": "finding-d1-0001",
})
smoke("event: finding.raised (v1.2)", "phase0-contracts/ledger_events.schema.json", evt_finding, True)
evt_finding_bad = json.loads(json.dumps(evt_finding))
evt_finding_bad["payload"]["refs"] = []
smoke("event: finding without refs rejected", "phase0-contracts/ledger_events.schema.json", evt_finding_bad, False)
evt_finding_fact = json.loads(json.dumps(evt_finding))
evt_finding_fact["payload"]["kind"] = "fact"
smoke("event: finding of an unknown kind rejected", "phase0-contracts/ledger_events.schema.json", evt_finding_fact, False)
evt_resolved = dict(evt_good)
evt_resolved.update({
    "event_id": "evt_01HXAMPLE0QQ",
    "type": "finding.resolved",
    "payload": {"finding_id": "fnd_01HXAMPLE0NN", "resolution": "answered", "ref": "exp_01HXAMPLE0CC"},
    "idempotency_key": "finding-d1-0001-resolved",
})
smoke("event: finding.resolved (v1.2)", "phase0-contracts/ledger_events.schema.json", evt_resolved, True)
evt_status = dict(evt_good)
evt_status.update({
    "event_id": "evt_01HXAMPLE0RR",
    "session_id": "ses_01HXAMPLE0GG",
    "type": "session.status_changed",
    "payload": {"session_id": "ses_01HXAMPLE0GG", "status": "approved_with_risks", "outcome": "completed_with_risks",
                "decision": "approve_with_risks", "reason": "single node is fine in dev", "package_ref": "sha256:ab12"},
    "idempotency_key": "session-status-0004",
})
smoke("event: session.status_changed (v1.2)", "phase0-contracts/ledger_events.schema.json", evt_status, True)
evt_status_bad = json.loads(json.dumps(evt_status))
evt_status_bad["payload"]["decision"] = "shrug"
smoke("event: session decision outside the enum rejected", "phase0-contracts/ledger_events.schema.json", evt_status_bad, False)
evt_branch = dict(evt_good)
evt_branch.update({
    "event_id": "evt_01HXAMPLE0SS",
    "type": "model.version_created",
    "payload": {"version_id": "mv_01HXAMPLE0TT", "parent": "mv_01HXAMPLE0JJ", "branch": "alt-queue"},
    "idempotency_key": "branch-alt-queue",
})
smoke("event: model.version_created on a branch (v1.2)", "phase0-contracts/ledger_events.schema.json", evt_branch, True)

# 2e. An agent Objection — falsifiable_test required
obj_good = {
    "msg_id": "msg_01HXAMPLE0FF",
    "session_id": "ses_01HXAMPLE0GG",
    "task_id": "tsk_01HXAMPLE0HH",
    "agent": {"role": "adversary_distsys", "model_tier": "tier-frontier", "model_family": "open-weights-a"},
    "ts": "2026-10-02T06:42:00+05:30",
    "cost": {"tokens_in": 1200, "tokens_out": 300, "gpu_seconds": 4.2},
    "depends_on": ["evt_01HXAMPLE0DD"],
    "type": "Objection",
    "body": {
        "element_refs": ["flw_01HXAMPLE0II"],
        "narrative": "Rebalancing during partition double-assigns shard ownership.",
        "trigger_condition": "network partition longer than lease TTL",
        "severity": "critical",
        "falsifiable_test": "L2 template: single-owner invariant under partition in the lease protocol model",
    },
}
smoke("protocol: falsifiable objection", "phase0-contracts/agent_protocol.schema.json", obj_good, True)
obj_bad = json.loads(json.dumps(obj_good))
obj_bad["body"].pop("falsifiable_test")
smoke("protocol: un-falsifiable objection rejected", "phase0-contracts/agent_protocol.schema.json", obj_bad, False)

# 2f. A check-catalog entry
cat = {
    "catalog_version": "0.1.0",
    "checks": [{
        "id": "C-005",
        "title": "Capacity: downstream >= peak x headroom, units checked",
        "layer": "L1",
        "severity": "critical",
        "applies_to": {"element_kinds": ["flow"]},
        "implementation": {"kind": "builtin", "ref": "checks.capacity.headroom"},
        "params": {"headroom": 1.5},
        "version": 1,
    }],
}
smoke("catalog: C-005 entry", "phase0-contracts/check_catalog.schema.json", cat, True)

# 2g. A system-model fragment
model = {
    "version_id": "mv_01HXAMPLE0JJ",
    "project_id": "proj-yavin-dogfood",
    "elements": {
        "components": [{
            "id": "cmp_01HXAMPLE0KK", "name": "Arbiter", "kind": "service", "stateful": False,
            "interfaces": ["if_01HXAMPLE0LL"], "requirement_refs": ["req_single_writer"],
        }],
        "interfaces": [{
            "id": "if_01HXAMPLE0LL", "contract_ref": "openapi:arbiter.yaml#/commit", "style": "sync",
            "idempotent": True, "authn": "service_identity",
        }],
        "flows": [{
            "id": "flw_01HXAMPLE0II", "from": "cmp_agent_pool", "to": "cmp_01HXAMPLE0KK",
            "data_class": "internal", "rate": {"peak_qps": 200, "payload_bytes": 4096},
            "backpressure_ref": "queue:proposals",
        }],
    },
    "links": {"satisfies": [{"component": "cmp_01HXAMPLE0KK", "requirement": "req_single_writer"}]},
}
smoke("model: minimal version", "phase0-contracts/system_model.schema.json", model, True)

print("\nRESULT:", "ALL GREEN" if ok else "FAILURES PRESENT")
sys.exit(0 if ok else 1)
