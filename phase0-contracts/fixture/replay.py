#!/usr/bin/env python3
"""Phase 0 exit test, step 3b: replay the fixture ledger.

Proves the contracts compose: every event validates against the ledger schema;
embedded claims, model patches, objections, and proposed checks deep-validate
against their own schemas (making the v0.1 "by convention" cross-references
mechanical); replay invariants hold (dense sequence, promotion rules, patch
chains, objection lifecycle); and the folded projections are themselves valid —
the final System Model version validates against system_model.schema.json.

Then it runs four real L0 checks (C-001, C-002, C-008, C-009) against the final
state and prints the gate verdict. Usage:

    python3 replay.py [path/to/ledger.jsonl]

Schemas are found relative to this file (../*.schema.json), so it runs from any
working directory.
"""
import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

HERE = Path(__file__).resolve().parent            # .../phase0-contracts/fixture
CONTRACTS = HERE.parent                            # .../phase0-contracts
LEDGER_PATH = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "fixture_ledger.jsonl"

def load_schema(name):
    return json.loads((CONTRACTS / name).read_text())

ledger_s = load_schema("ledger_events.schema.json")
claim_s = load_schema("claim.schema.json")
model_s = load_schema("system_model.schema.json")
proto_s = load_schema("agent_protocol.schema.json")
catalog_s = load_schema("check_catalog.schema.json")

def subdef(schema, name):
    """Standalone validator for one $def (carries the parent's $defs for internal refs)."""
    sub = dict(schema["$defs"][name])
    sub["$defs"] = schema["$defs"]
    return Draft202012Validator(sub)

V_EVENT = Draft202012Validator(ledger_s)
V_CLAIM = Draft202012Validator(claim_s)
V_MODEL = Draft202012Validator(model_s)
V_PATCH = subdef(proto_s, "ModelPatchProposal")
V_OBJECTION = subdef(proto_s, "Objection")
V_CHECK = subdef(catalog_s, "Check")

failures = []

def require(cond, msg):
    if not cond:
        failures.append(msg)

def deep_validate(validator, instance, label):
    errs = list(validator.iter_errors(instance))
    for e in errs[:3]:
        failures.append(f"{label}: {list(e.path)}: {e.message[:120]}")
    return not errs

# ------------------------------------------------------------------ load
events = [json.loads(line) for line in LEDGER_PATH.read_text().splitlines() if line.strip()]
print(f"ledger: {LEDGER_PATH.name} — {len(events)} events")

# ------------------------------------------------------------------ pass 1: schema + uniqueness + order
seen_event_ids, seen_idem = set(), set()
for i, ev in enumerate(events):
    deep_validate(V_EVENT, ev, f"event[{i}] ({ev.get('type')})")
    require(ev["seq"] == i, f"event[{i}]: seq {ev['seq']} != position {i} (dense order broken)")
    require(ev["event_id"] not in seen_event_ids, f"event[{i}]: duplicate event_id")
    require(ev["idempotency_key"] not in seen_idem, f"event[{i}]: duplicate idempotency_key")
    seen_event_ids.add(ev["event_id"])
    seen_idem.add(ev["idempotency_key"])

# ------------------------------------------------------------------ pass 2: fold + invariants
claims = {}            # claim_id -> claim dict (latest)
status = {}            # claim_id -> epistemic status
sources = set()
proposals = set()      # claim proposal ids
patch_proposals = set()
open_objections = {}   # objection_id -> severity
resolved_objections = set()
model = None           # current folded model
head = None            # current model version id
versions = []
decisions, waivers, checkpoints, check_events = [], [], [], []
event_type_by_id = {}
edges = {"EVIDENCES": 0, "SATISFIES": 0, "DEPENDS_ON": 0, "MITIGATES": 0, "DECISION_EVIDENCE": 0}

def apply_patch(patch, where):
    for op in patch["ops"]:
        kind = op["op"]
        if kind == "add_element":
            model["elements"].setdefault(op["element_type"], []).append(op["element"])
        elif kind == "update_element":
            rows = model["elements"].get(op["element_type"], [])
            for el in rows:
                if el.get("id") == op.get("element_id"):
                    el.update(op["element"])
                    break
            else:
                failures.append(f"{where}: update_element target {op.get('element_id')} not found")
        elif kind == "remove_element":
            rows = model["elements"].get(op["element_type"], [])
            model["elements"][op["element_type"]] = [el for el in rows if el.get("id") != op.get("element_id")]
        elif kind == "add_link":
            model["links"].setdefault(op["link_type"], []).append(op["link"])
        elif kind == "remove_link":
            rows = model["links"].get(op["link_type"], [])
            model["links"][op["link_type"]] = [l for l in rows if l != op.get("link")]

for i, ev in enumerate(events):
    etype, p = ev["type"], ev["payload"]
    event_type_by_id[ev["event_id"]] = etype
    where = f"event[{i}] {etype}"

    if etype == "source.ingested":
        sources.add(p["source_id"])

    elif etype == "claim.proposed":
        deep_validate(V_CLAIM, p["claim"], f"{where} embedded claim")
        proposals.add(p["proposal_id"])

    elif etype == "claim.committed":
        deep_validate(V_CLAIM, p["claim"], f"{where} embedded claim")
        require(p["claim_id"] == p["claim"]["id"], f"{where}: claim_id != claim.id")
        if "from_proposal" in p:
            require(p["from_proposal"] in proposals, f"{where}: unknown proposal {p['from_proposal']}")
        for evd in p["claim"].get("evidence", []):
            require(evd["source"] in sources, f"{where}: evidence cites un-ingested source {evd['source']}")
            edges["EVIDENCES"] += 1
        claims[p["claim_id"]] = p["claim"]
        status[p["claim_id"]] = p["claim"]["status"]

    elif etype == "claim.status_changed":
        require(p["claim_id"] in status, f"{where}: unknown claim {p['claim_id']}")
        require(status.get(p["claim_id"]) == p["from"],
                f"{where}: 'from' {p['from']} != current {status.get(p['claim_id'])}")
        require(p["cause_event"] in event_type_by_id,
                f"{where}: cause_event {p['cause_event']} not earlier in ledger")
        if p["to"] in ("measured", "observed"):   # §7 promotion rule, enforced on replay
            require(event_type_by_id.get(p["cause_event"]) == "experiment.recorded",
                    f"{where}: promotion to {p['to']} requires an experiment.recorded cause")
        status[p["claim_id"]] = p["to"]

    elif etype == "model.version_created":
        head = p["version_id"]
        model = {"elements": {}, "links": {}}
        versions.append(head)

    elif etype == "model.patch_proposed":
        deep_validate(V_PATCH, p["patch"], f"{where} patch")
        require(p["patch"]["base_version"] == p["base_version"], f"{where}: patch/base mismatch")
        require(p["base_version"] == head, f"{where}: base {p['base_version']} != head {head}")
        patch_proposals.add(p["proposal_id"])

    elif etype == "model.patch_committed":
        deep_validate(V_PATCH, p["patch"], f"{where} patch")
        require(p["base_version"] == head, f"{where}: base {p['base_version']} != head {head} (chain broken)")
        if "from_proposal" in p:
            require(p["from_proposal"] in patch_proposals, f"{where}: unknown proposal {p['from_proposal']}")
        apply_patch(p["patch"], where)
        head = p["version_id"]
        versions.append(head)

    elif etype == "objection.raised":
        deep_validate(V_OBJECTION, p["objection"], f"{where} objection")
        pc = p["objection"].get("detecting_check", {}).get("proposed_check")
        if pc:
            deep_validate(V_CHECK, pc, f"{where} proposed_check")
        open_objections[p["objection_id"]] = p["objection"]["severity"]

    elif etype == "objection.resolved":
        require(p["objection_id"] in open_objections, f"{where}: resolving unknown/closed objection")
        open_objections.pop(p["objection_id"], None)
        resolved_objections.add(p["objection_id"])

    elif etype == "experiment.recorded":
        for cid in p["result_claims"]:
            require(cid in claims, f"{where}: result claim {cid} not committed")

    elif etype == "decision.recorded":
        for cid in p["decision"]["evidence_claims"]:
            require(cid in claims, f"{where}: ADR cites unknown claim {cid}")
            edges["DECISION_EVIDENCE"] += 1
        decisions.append(p["adr_id"])

    elif etype == "waiver.signed":
        require(ev["actor"]["kind"] == "human", f"{where}: waiver signed by non-human actor (P11)")
        waivers.append(p["waiver_id"])

    elif etype == "check.result":
        check_events.append((p["check_id"], p["status"]))

    elif etype == "session.checkpoint":
        checkpoints.append(p)

edges["SATISFIES"] = len(model["links"].get("satisfies", []))
edges["DEPENDS_ON"] = len(model["links"].get("depends_on", []))
edges["MITIGATES"] = len(model["links"].get("mitigates", []))

# ------------------------------------------------------------------ pass 3: final model validates
final_model = {"version_id": head, "project_id": events[0]["project_id"],
               "elements": model["elements"], "links": model["links"]}
deep_validate(V_MODEL, final_model, "final model")

# ------------------------------------------------------------------ pass 4: L0 checks on final state
l0 = {}

# C-001: every requirement satisfied or explicitly waived
req_ids = {c["subject"]["id"] for c in claims.values()
           if c["subject"].get("entity_type") == "requirement"}
satisfied = {l["requirement"] for l in model["links"].get("satisfies", [])}
unsat = req_ids - satisfied
l0["C-001"] = ("pass", "") if not unsat else ("fail", f"unsatisfied: {sorted(unsat)}")

# C-002: no orphan components (kind=external exempt)
orphan_ok = {l["component"] for l in model["links"].get("satisfies", [])}
orphans = [c["id"] for c in model["elements"].get("components", [])
           if c["kind"] != "external" and c["id"] not in orphan_ok and not c.get("requirement_refs")]
l0["C-002"] = ("pass", "") if not orphans else ("fail", f"orphans: {orphans}")

# C-008: every cross-boundary flow declares input validation + encryption
bad_flows = []
for tb in model["elements"].get("trust_boundaries", []):
    members = set(tb["member_elements"])
    for fl in model["elements"].get("flows", []):
        crossing = (fl["from"] in members) != (fl["to"] in members)
        if crossing and not (fl.get("input_validation") and fl.get("encryption_in_transit")):
            bad_flows.append(fl["id"])
l0["C-008"] = ("pass", "") if not bad_flows else ("fail", f"undeclared crossings: {bad_flows}")

# C-009: no load-bearing assumption left open
open_assumed = [cid for cid, c in claims.items()
                if c.get("load_bearing") and status.get(cid) == "assumed"]
l0["C-009"] = ("pass", "") if not open_assumed else ("fail", f"open load-bearing assumptions: {open_assumed}")

# ------------------------------------------------------------------ gate verdict
open_criticals = [o for o, sev in open_objections.items() if sev == "critical"]
critical_check_fails = [cid for cid, (st, _) in l0.items() if st == "fail" and cid != "C-002"]
gate_ok = not open_criticals and not critical_check_fails and not open_assumed

# ------------------------------------------------------------------ report
by_status = {}
for cid in claims:
    by_status[status[cid]] = by_status.get(status[cid], 0) + 1

print(f"sources ingested:   {len(sources)}")
print(f"claims committed:   {len(claims)}  {by_status}")
print(f"graph edges:        {sum(edges.values())}  {edges}")
print(f"model versions:     {len(versions)}  head={head}")
print(f"  components={len(model['elements'].get('components', []))}"
      f" interfaces={len(model['elements'].get('interfaces', []))}"
      f" flows={len(model['elements'].get('flows', []))}"
      f" state_machines={len(model['elements'].get('state_machines', []))}"
      f" trust_boundaries={len(model['elements'].get('trust_boundaries', []))}")
print(f"objections:         raised={len(resolved_objections) + len(open_objections)}"
      f" resolved={len(resolved_objections)} open={len(open_objections)}")
print(f"decisions (ADR):    {len(decisions)}   waivers: {len(waivers)}   checkpoints: {len(checkpoints)}")
print(f"recorded checks:    {check_events}")
for cid, (st, detail) in sorted(l0.items()):
    print(f"L0 {cid} on final state: {st.upper()}  {detail}")
print(f"gate IMPLEMENTATION_READY: {'ALLOWED' if gate_ok else 'BLOCKED'}"
      f"  (open criticals={len(open_criticals)}, open load-bearing assumptions={len(open_assumed)})")

if failures:
    print(f"\n{len(failures)} FAILURES:")
    for f in failures:
        print(f"  - {f}")
    print("\nRESULT: REPLAY FAILED")
    sys.exit(1)
print("\nRESULT: REPLAY GREEN — Phase 0 exit test complete")
