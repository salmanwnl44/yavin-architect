#!/usr/bin/env python3
"""Phase 0 exit test, step 3a: build the fixture ledger.

One tiny but complete design session — "sharded KV store write path with
lease-based shard ownership" (the §22-4 domain) — hand-written as 40 ledger
events. This is the editable source; it emits fixture_ledger.jsonl, which
replay.py then validates and folds into projections.

The session tells the full story the spec requires the contracts to carry:
requirements in, research claims with evidence and conditions, a load-bearing
assumption with a verification plan, model versions built by patches, a failing
check and a critical objection, a repair, a probe that promotes the assumption
to measured (with the cause event recorded), an ADR with rejected alternatives,
a signed waiver, and anytime checkpoints.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUT = Path(__file__).resolve().parent / "fixture_ledger.jsonl"
IST = timezone(timedelta(hours=5, minutes=30))
T0 = datetime(2026, 10, 2, 7, 0, 0, tzinfo=IST)

PROJECT = "proj-architect-dogfood"
SES = "ses_FIX0000001"
TSK = "tsk_FIXT000001"

HUMAN = {"kind": "human", "id": "saumya", "role": "owner"}
ARBITER = {"kind": "system", "id": "arbiter", "role": "arbiter"}
ORCH = {"kind": "system", "id": "orchestrator", "role": "orchestrator"}
ARCHITECT = {"kind": "agent", "id": "architect-1", "role": "architect"}
RESEARCHER = {"kind": "agent", "id": "researcher-1", "role": "researcher"}
ADV_DS = {"kind": "agent", "id": "adv-distsys-1", "role": "adversary_distsys"}
VERIFIER = {"kind": "agent", "id": "verifier-1", "role": "verifier"}

events = []


def emit(etype, payload, actor, session=SES, task=None):
    seq = len(events)
    ev = {
        "event_id": f"evt_FIXE{seq:06d}",
        "project_id": PROJECT,
        "seq": seq,
        "ts": (T0 + timedelta(seconds=90 * seq)).isoformat(),
        "actor": actor,
        "type": etype,
        "payload": payload,
        "idempotency_key": f"fix-{seq:04d}-{etype}",
    }
    if session:
        ev["session_id"] = session
    if task:
        ev["task_id"] = task
    events.append(ev)
    return ev["event_id"]


def extractor(tier, phash="n/a"):
    return {"extractor": {"model_tier": tier, "prompt_hash": phash, "pipeline_version": 1}}


# ---------------------------------------------------------------- frame
emit("session.phase_changed", {"session_id": SES, "to": "frame"}, ORCH)                      # 0
emit("budget.updated", {                                                                      # 1
    "scope": {"session": SES},
    "limits": {"tokens": None, "usd": None, "wall_clock_minutes": 1440, "gpu_minutes": None},
}, ORCH)
emit("source.ingested", {                                                                     # 2
    "source_id": "src_FIXBRIEF01", "uri": "user://design-brief/kv-write-path",
    "content_hash": "sha256:fixbrief01", "media_type": "text/markdown", "taint_origin": "user",
}, HUMAN)
emit("claim.committed", {                                                                     # 3
    "claim_id": "clm_FIXREQ0001",
    "claim": {
        "id": "clm_FIXREQ0001",
        "subject": {"entity_type": "requirement", "id": "req_FIXQPS001"},
        "predicate": "CONSTRAINS",
        "object": {"entity_type": "system", "id": "sys-kv-write-path"},
        "magnitude": {"value": 2000, "unit": "writes/s"},
        "status": "documented",
        "evidence": [{"source": "src_FIXBRIEF01", "span": "brief ¶2", "kind": "statement"}],
        "taint": {"origin": "user"},
        "recorded_at": (T0 + timedelta(seconds=270)).isoformat(),
        "provenance": extractor("human"),
    },
}, ARBITER)
emit("claim.committed", {                                                                     # 4
    "claim_id": "clm_FIXDUR0001",
    "claim": {
        "id": "clm_FIXDUR0001",
        "subject": {"entity_type": "requirement", "id": "req_FIXDUR001"},
        "predicate": "CONSTRAINS",
        "object": {"entity_type": "property", "literal": "no acknowledged write lost on single-node failure"},
        "status": "documented",
        "evidence": [{"source": "src_FIXBRIEF01", "span": "brief ¶3", "kind": "statement"}],
        "taint": {"origin": "user"},
        "recorded_at": (T0 + timedelta(seconds=360)).isoformat(),
        "provenance": extractor("human"),
    },
}, ARBITER)

# ---------------------------------------------------------------- research
emit("session.phase_changed", {"session_id": SES, "from": "frame", "to": "research"}, ORCH)   # 5
emit("source.ingested", {                                                                     # 6
    "source_id": "src_FIXPAPER01", "uri": "https://example.org/leases-fault-tolerant-locks.pdf",
    "content_hash": "sha256:fixpaper01", "media_type": "application/pdf",
    "license": "CC-BY-4.0", "taint_origin": "external_untrusted",
}, RESEARCHER)
emit("source.ingested", {                                                                     # 7
    "source_id": "src_FIXSPEC001", "uri": "https://vendor.example/nvme-gen4-dc-spec.pdf",
    "content_hash": "sha256:fixspec001", "media_type": "application/pdf",
    "taint_origin": "external_untrusted",
}, RESEARCHER)
emit("claim.proposed", {                                                                      # 8
    "proposal_id": "prop-lease-1",
    "claim": {
        "id": "clm_FIXLEASE01",
        "subject": {"entity_type": "technique", "id": "lease-ownership"},
        "predicate": "GUARANTEES",
        "object": {"entity_type": "property", "literal": "single shard owner at any instant"},
        "conditions": {"clock_skew_ms_max": 250, "requires_fencing": True},
        "status": "documented",
        "evidence": [{"source": "src_FIXPAPER01", "span": "p.3 §2", "kind": "statement"}],
        "taint": {"origin": "external_untrusted", "license": "CC-BY-4.0"},
        "recorded_at": (T0 + timedelta(seconds=720)).isoformat(),
        "provenance": extractor("tier-cheap", "fx01"),
    },
}, RESEARCHER, task=TSK)
emit("claim.committed", {                                                                     # 9
    "claim_id": "clm_FIXLEASE01", "from_proposal": "prop-lease-1",
    "claim": events[8]["payload"]["claim"],
}, ARBITER)
emit("claim.proposed", {                                                                      # 10
    "proposal_id": "prop-fsync-1",
    "claim": {
        "id": "clm_FIXFSYNC01",
        "subject": {"entity_type": "hardware_part", "id": "nvme-gen4-dc"},
        "predicate": "HAS_FSYNC_P99",
        "object": {"entity_type": "metric", "id": "fsync-p99-latency"},
        "magnitude": {"value": 900, "unit": "us"},
        "conditions": {"queue_depth": 1, "sync_mode": "O_DSYNC"},
        "status": "documented",
        "evidence": [{"source": "src_FIXSPEC001", "span": "spec table 7", "kind": "table"}],
        "taint": {"origin": "external_untrusted"},
        "recorded_at": (T0 + timedelta(seconds=900)).isoformat(),
        "provenance": extractor("tier-cheap", "fx01"),
    },
}, RESEARCHER, task=TSK)
emit("claim.committed", {                                                                     # 11
    "claim_id": "clm_FIXFSYNC01", "from_proposal": "prop-fsync-1",
    "claim": events[10]["payload"]["claim"],
}, ARBITER)
emit("claim.committed", {                                                                     # 12
    "claim_id": "clm_FIXPAYLOAD1",
    "claim": {
        "id": "clm_FIXPAYLOAD1",
        "subject": {"entity_type": "workload", "id": "kv-write-path"},
        "predicate": "HAS_P99_PAYLOAD",
        "object": {"entity_type": "metric", "id": "payload-size-p99"},
        "magnitude": {"value": 4, "unit": "KiB"},
        "status": "assumed",
        "owner": "saumya",
        "load_bearing": True,
        "verification_plan": {"kind": "experiment", "ref": "exp_FIXPROBE01"},
        "taint": {"origin": "internal"},
        "recorded_at": (T0 + timedelta(seconds=1080)).isoformat(),
        "provenance": extractor("human"),
    },
}, ARBITER)

# ---------------------------------------------------------------- model
emit("session.phase_changed", {"session_id": SES, "from": "research", "to": "model"}, ORCH)   # 13
emit("model.version_created", {"version_id": "mv_FIXGENESIS"}, ARBITER)                       # 14

PATCH_1 = {
    "base_version": "mv_FIXGENESIS",
    "rationale": "Skeleton: client-facing router, sharded store, lease manager; one write flow; cluster trust boundary.",
    "ops": [
        {"op": "add_element", "element_type": "components", "element": {
            "id": "cmp_FIXCLIENT1", "name": "Client SDK", "kind": "external",
            "stateful": False, "requirement_refs": []}},
        {"op": "add_element", "element_type": "components", "element": {
            "id": "cmp_FIXROUTER1", "name": "Write Router", "kind": "service",
            "stateful": False, "interfaces": ["if_FIXWRITE01"],
            "requirement_refs": ["req_FIXQPS001"]}},
        {"op": "add_element", "element_type": "components", "element": {
            "id": "cmp_FIXSHARD01", "name": "Shard Store", "kind": "datastore",
            "stateful": True, "durability_class": "durable",
            "recovery": {"rpo_s": 0, "rto_s": 30, "path": "replica promotion"},
            "requirement_refs": ["req_FIXQPS001", "req_FIXDUR001"]}},
        {"op": "add_element", "element_type": "components", "element": {
            "id": "cmp_FIXLEASE01", "name": "Lease Manager", "kind": "service",
            "stateful": True, "durability_class": "rebuildable",
            "requirement_refs": ["req_FIXDUR001"]}},
        {"op": "add_element", "element_type": "interfaces", "element": {
            "id": "if_FIXWRITE01", "contract_ref": "openapi:kv.yaml#/put",
            "style": "sync", "idempotent": True, "authn": "service_identity"}},
        {"op": "add_element", "element_type": "flows", "element": {
            "id": "flw_FIXINGR001", "from": "cmp_FIXCLIENT1", "to": "cmp_FIXROUTER1",
            "via_interface": "if_FIXWRITE01", "data_class": "internal",
            "rate": {"peak_qps": 2000, "payload_bytes": 4096},
            "backpressure_ref": "queue:router-admission"}},
        {"op": "add_element", "element_type": "flows", "element": {
            "id": "flw_FIXWRIT001", "from": "cmp_FIXROUTER1", "to": "cmp_FIXSHARD01",
            "data_class": "internal",
            "rate": {"peak_qps": 2000, "payload_bytes": 4096},
            "encryption_in_transit": True,
            "backpressure_ref": "queue:write-buffer"}},
        {"op": "add_element", "element_type": "trust_boundaries", "element": {
            "id": "tb-cluster", "name": "KV cluster boundary",
            "member_elements": ["cmp_FIXROUTER1", "cmp_FIXSHARD01", "cmp_FIXLEASE01"]}},
        {"op": "add_link", "link_type": "satisfies",
         "link": {"component": "cmp_FIXROUTER1", "requirement": "req_FIXQPS001"}},
        {"op": "add_link", "link_type": "satisfies",
         "link": {"component": "cmp_FIXSHARD01", "requirement": "req_FIXQPS001"}},
        {"op": "add_link", "link_type": "satisfies",
         "link": {"component": "cmp_FIXSHARD01", "requirement": "req_FIXDUR001"}},
        {"op": "add_link", "link_type": "satisfies",
         "link": {"component": "cmp_FIXLEASE01", "requirement": "req_FIXDUR001"}},
        {"op": "add_link", "link_type": "depends_on",
         "link": {"from": "cmp_FIXROUTER1", "to": "cmp_FIXLEASE01", "kind": "sync"}},
        {"op": "add_link", "link_type": "depends_on",
         "link": {"from": "cmp_FIXROUTER1", "to": "cmp_FIXSHARD01", "kind": "sync"}},
    ],
}
emit("model.patch_proposed", {"proposal_id": "mp-0001", "base_version": "mv_FIXGENESIS",      # 15
                               "patch": PATCH_1}, ARCHITECT, task=TSK)
emit("model.patch_committed", {"version_id": "mv_FIXV000001", "base_version": "mv_FIXGENESIS", # 16
                                "patch": PATCH_1, "from_proposal": "mp-0001"}, ARBITER)

# ---------------------------------------------------------------- draft
emit("session.phase_changed", {"session_id": SES, "from": "model", "to": "draft"}, ORCH)      # 17
PATCH_2 = {
    "base_version": "mv_FIXV000001",
    "rationale": "Durability: write-ahead log on NVMe before ack (per clm_FIXFSYNC01 latency envelope).",
    "ops": [
        {"op": "add_element", "element_type": "components", "element": {
            "id": "cmp_FIXWAL0001", "name": "Write-Ahead Log", "kind": "datastore",
            "stateful": True, "durability_class": "durable",
            "recovery": {"rpo_s": 0, "rto_s": 60, "path": "log replay"},
            "requirement_refs": ["req_FIXDUR001"]}},
        {"op": "add_element", "element_type": "flows", "element": {
            "id": "flw_FIXWAL0001", "from": "cmp_FIXSHARD01", "to": "cmp_FIXWAL0001",
            "data_class": "internal",
            "rate": {"peak_qps": 2000, "payload_bytes": 4096},
            "encryption_in_transit": True,
            "backpressure_ref": "queue:wal-append"}},
        {"op": "update_element", "element_type": "trust_boundaries", "element_id": "tb-cluster",
         "element": {"member_elements": ["cmp_FIXROUTER1", "cmp_FIXSHARD01",
                                          "cmp_FIXLEASE01", "cmp_FIXWAL0001"]}},
        {"op": "add_link", "link_type": "satisfies",
         "link": {"component": "cmp_FIXWAL0001", "requirement": "req_FIXDUR001"}},
        {"op": "add_link", "link_type": "depends_on",
         "link": {"from": "cmp_FIXSHARD01", "to": "cmp_FIXWAL0001", "kind": "sync"}},
    ],
}
emit("model.patch_proposed", {"proposal_id": "mp-0002", "base_version": "mv_FIXV000001",      # 18
                               "patch": PATCH_2}, ARCHITECT, task=TSK)
emit("model.patch_committed", {"version_id": "mv_FIXV000002", "base_version": "mv_FIXV000001", # 19
                                "patch": PATCH_2, "from_proposal": "mp-0002"}, ARBITER)

# ---------------------------------------------------------------- attack
emit("session.phase_changed", {"session_id": SES, "from": "draft", "to": "attack"}, ORCH)     # 20
emit("check.result", {                                                                        # 21
    "result_id": "chk_FIXC005A01", "check_id": "C-005",
    "element_refs": ["flw_FIXWRIT001"], "status": "pass",
    "evidence": {"required_qps_with_headroom": 3000, "downstream_capacity_qps": 3600},
}, VERIFIER)
emit("check.result", {                                                                        # 22
    "result_id": "chk_FIXC008F01", "check_id": "C-008",
    "element_refs": ["flw_FIXINGR001"], "status": "fail",
    "evidence": {"crossing": "tb-cluster", "missing": ["input_validation", "encryption_in_transit"]},
}, VERIFIER)
OBJECTION = {
    "element_refs": ["cmp_FIXLEASE01", "cmp_FIXSHARD01"],
    "narrative": "On a partition longer than lease TTL plus clock skew, two shard processes can both believe they own shard 7; both acknowledge writes; the merge on heal silently loses one of them.",
    "trigger_condition": "network partition > lease TTL while clock skew exceeds the clm_FIXLEASE01 bound",
    "severity": "critical",
    "falsifiable_test": "L2 lease-protocol template: single-owner invariant under partition, with fencing epochs modeled",
    "detecting_check": {"proposed_check": {
        "id": "C-031",
        "title": "Fencing epoch declared and enforced for every lease-based ownership",
        "layer": "L0", "severity": "critical",
        "applies_to": {"element_kinds": ["component"],
                        "when": "ownership == 'lease'"},
        "implementation": {"kind": "builtin", "ref": "checks.consistency.fencing_declared"},
        "version": 1,
    }},
}
emit("objection.raised", {"objection_id": "obj_FIXSPLIT01", "objection": OBJECTION},          # 23
     ADV_DS, task=TSK)

# ---------------------------------------------------------------- repair
emit("session.phase_changed", {"session_id": SES, "from": "attack", "to": "repair"}, ORCH)    # 24
PATCH_3 = {
    "base_version": "mv_FIXV000002",
    "rationale": "Answer obj_FIXSPLIT01 and chk_FIXC008F01: fencing epochs enforced at WAL append; ingress flow validated and encrypted.",
    "ops": [
        {"op": "update_element", "element_type": "flows", "element_id": "flw_FIXINGR001",
         "element": {"input_validation": "schema check + 4 KiB size cap at router admission",
                      "encryption_in_transit": True}},
        {"op": "add_element", "element_type": "state_machines", "element": {
            "id": "sm-shard-ownership", "element_ref": "cmp_FIXSHARD01",
            "states": ["follower", "owner", "fenced"],
            "transitions": [
                {"from": "follower", "to": "owner", "on": "lease_granted"},
                {"from": "owner", "to": "fenced", "on": "lease_expired"},
                {"from": "fenced", "to": "follower", "on": "fence_ack"}],
            "invariants": ["at most one owner per shard per fencing epoch",
                            "WAL append rejects stale fencing epoch"]}},
    ],
}
emit("model.patch_proposed", {"proposal_id": "mp-0003", "base_version": "mv_FIXV000002",      # 25
                               "patch": PATCH_3}, ARCHITECT, task=TSK)
emit("model.patch_committed", {"version_id": "mv_FIXV000003", "base_version": "mv_FIXV000002", # 26
                                "patch": PATCH_3, "from_proposal": "mp-0003"}, ARBITER)
emit("objection.resolved", {"objection_id": "obj_FIXSPLIT01", "resolution": "patched",        # 27
                             "ref": "mv_FIXV000003"}, ARBITER)

# ---------------------------------------------------------------- verify
emit("session.phase_changed", {"session_id": SES, "from": "repair", "to": "verify"}, ORCH)    # 28
emit("source.ingested", {                                                                     # 29
    "source_id": "src_FIXPROBE01", "uri": "probe://payload-histogram/staging/run-1",
    "content_hash": "sha256:fixprobe01", "media_type": "application/json",
    "taint_origin": "internal",
}, VERIFIER)
emit("claim.committed", {                                                                     # 30
    "claim_id": "clm_FIXMEAS001",
    "claim": {
        "id": "clm_FIXMEAS001",
        "subject": {"entity_type": "workload", "id": "kv-write-path"},
        "predicate": "HAS_P99_PAYLOAD",
        "object": {"entity_type": "metric", "id": "payload-size-p99"},
        "magnitude": {"value": 3.2, "unit": "KiB"},
        "conditions": {"window": "7d", "env": "staging"},
        "status": "measured",
        "evidence": [{"source": "src_FIXPROBE01", "span": "histogram p99", "kind": "measurement"}],
        "taint": {"origin": "internal"},
        "recorded_at": (T0 + timedelta(seconds=2700)).isoformat(),
        "provenance": extractor("system"),
        "supersedes": "clm_FIXPAYLOAD1",
    },
}, ARBITER)
EXP_EVT = emit("experiment.recorded", {                                                       # 31
    "experiment_id": "exp_FIXPROBE01", "design_ref": "probe:payload-histogram",
    "result_claims": ["clm_FIXMEAS001"],
}, VERIFIER)
emit("claim.status_changed", {                                                                # 32
    "claim_id": "clm_FIXPAYLOAD1", "from": "assumed", "to": "measured",
    "cause_event": EXP_EVT,
}, ARBITER)
emit("check.result", {                                                                        # 33
    "result_id": "chk_FIXC009P01", "check_id": "C-009",
    "element_refs": ["clm_FIXPAYLOAD1"], "status": "pass",
    "evidence": {"load_bearing_assumptions_open": 0},
}, VERIFIER)

# ---------------------------------------------------------------- converge
emit("session.phase_changed", {"session_id": SES, "from": "verify", "to": "converge"}, ORCH)  # 34
emit("decision.recorded", {                                                                   # 35
    "adr_id": "adr_FIXLEASE01",
    "decision": {
        "title": "Lease-based shard ownership with fencing epochs",
        "choice": "Leases (TTL 5s) with monotonic fencing epochs, enforced at WAL append",
        "evidence_claims": ["clm_FIXLEASE01", "clm_FIXFSYNC01", "clm_FIXMEAS001"],
        "alternatives": [{
            "option": "Per-key consensus (Raft group per shard)",
            "rejected_because": "3x write amplification; fsync chain exceeds latency budget under clm_FIXFSYNC01 conditions"}],
        "assumptions": ["clm_FIXPAYLOAD1"],
        "affected_elements": ["cmp_FIXSHARD01", "cmp_FIXLEASE01", "cmp_FIXWAL0001"],
    },
}, ARBITER)
emit("waiver.signed", {                                                                       # 36
    "waiver_id": "wvr_FIXSPOF001",
    "target_ref": "C-010:cmp_FIXLEASE01",
    "risk": "Lease Manager runs single-instance in Phase-1 dev; SPOF accepted until Phase-2 HA work",
    "signer": "saumya",
}, HUMAN)
emit("session.checkpoint", {                                                                  # 37
    "session_id": SES, "phase": "converge", "best_version": "mv_FIXV000003",
    "open_risk_ids": ["wvr_FIXSPOF001"],
    "spend": {"tokens": 48200, "gpu_minutes": 6.5},
}, ORCH)

# ---------------------------------------------------------------- package
emit("session.phase_changed", {"session_id": SES, "from": "converge", "to": "package"}, ORCH) # 38
emit("session.checkpoint", {                                                                  # 39
    "session_id": SES, "phase": "package", "best_version": "mv_FIXV000003",
    "open_risk_ids": ["wvr_FIXSPOF001"],
    "spend": {"tokens": 51400, "gpu_minutes": 7.0},
}, ORCH)

OUT.write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n")
print(f"wrote {len(events)} events -> {OUT}")
