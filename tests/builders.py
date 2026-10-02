"""Builders for candidates and a hand-written sample ledger.

The sample ledger is test data for the ingest/dump/rebuild/verify paths. It is not the frozen
Phase 0 fixture (phase0-contracts/fixture/), which has its own tests in test_exit_fixture.py.
"""

from __future__ import annotations

import itertools
from typing import Any

Event = dict[str, Any]

HUMAN = {"kind": "human", "id": "saumya", "role": "owner"}
AGENT = {"kind": "agent", "id": "adversary-1", "role": "adversary_distsys"}
SYSTEM = {"kind": "system", "id": "harness"}

_keys = itertools.count()


def ident(prefix: str, name: str) -> str:
    """A contract-shaped id: `<prefix>_` plus at least ten alphanumerics."""
    return f"{prefix}_{name.upper():0>10}"


def candidate(
    type: str, payload: dict[str, Any], *, actor: dict[str, Any] | None = None, **extra: Any
) -> Event:
    """A candidate with a fresh idempotency key. Pass event_id/ts/idempotency_key to pin them."""
    return {
        "actor": actor or SYSTEM,
        "type": type,
        "payload": payload,
        "idempotency_key": f"test-key-{next(_keys):06d}",
        **extra,
    }


def source(name: str = "paper") -> Event:
    payload = {
        "source_id": ident("src", name),
        "uri": f"https://example.org/{name}.pdf",
        "content_hash": f"sha256:{name}",
        "media_type": "application/pdf",
        "taint_origin": "external_untrusted",
    }
    return candidate("source.ingested", payload)


def claim(name: str = "latency", *, status: str = "documented", **overrides: Any) -> dict[str, Any]:
    """A claim object valid for its status; overrides replace fields, None removes one."""
    body: dict[str, Any] = {
        "id": ident("clm", name),
        "subject": {"entity_type": "technique", "id": "lease-fencing"},
        "predicate": "REDUCES",
        "object": {"entity_type": "metric", "id": "split-brain-window"},
        "status": status,
        "taint": {"origin": "external_untrusted"},
        "recorded_at": "2026-10-02T06:40:00+05:30",
        "provenance": {
            "extractor": {"model_tier": "tier-cheap", "prompt_hash": "a91c", "pipeline_version": 1}
        },
    }
    if status in ("documented", "measured", "observed"):
        body["evidence"] = [{"source": ident("src", "paper"), "kind": "benchmark"}]
    if status == "assumed":
        body["owner"] = "saumya"
    if status == "inferred":
        body["provenance"]["derived_from"] = [ident("clm", "latency")]
    for key, value in overrides.items():
        if value is None:
            body.pop(key, None)
        else:
            body[key] = value
    return body


def claim_committed(body: dict[str, Any], **payload: Any) -> Event:
    return candidate("claim.committed", {"claim_id": body["id"], "claim": body, **payload})


def patch(base: str) -> dict[str, Any]:
    return {
        "base_version": base,
        "rationale": "add fencing epochs to the lease protocol",
        "ops": [{"op": "add_element", "element_type": "component", "element": {"name": "Fencer"}}],
    }


def patch_committed(version: str, base: str, **payload: Any) -> Event:
    return candidate(
        "model.patch_committed",
        {"version_id": version, "base_version": base, "patch": patch(base), **payload},
    )


def objection(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "element_refs": [ident("flw", "writes")],
        "narrative": "Rebalancing during a partition double-assigns shard ownership.",
        "trigger_condition": "network partition longer than the lease TTL",
        "severity": "critical",
        "falsifiable_test": "single-owner invariant holds under partition in the lease model",
    }
    for key, value in overrides.items():
        if value is None:
            body.pop(key, None)
        else:
            body[key] = value
    return body


def proposed_check() -> dict[str, Any]:
    return {
        "id": "C-042",
        "title": "Lease holders carry a fencing epoch",
        "layer": "L1",
        "severity": "critical",
        "applies_to": {"element_kinds": ["flow"]},
        "implementation": {"kind": "builtin", "ref": "checks.lease.fencing"},
        "version": 1,
    }


def sample_ledger(project_id: str = "proj-sample") -> list[Event]:
    """One small design session as full ledger events: 24 events, all 19 event types."""
    session = ident("ses", "design")
    mv1, mv2, mv3 = (ident("mv", f"v{n}") for n in (1, 2, 3))
    documented = claim("latency")
    assumption = claim(
        "ttl",
        status="assumed",
        load_bearing=True,
        verification_plan={"kind": "experiment", "ref": ident("exp", "probe")},
    )
    measured = claim(
        "probe",
        status="measured",
        evidence=[{"source": ident("src", "probe"), "kind": "measurement"}],
        magnitude={"value": 4.20, "unit": "ms"},
        conditions={"nodes": 5, "partitioned": True},
    )
    inferred = claim("derived", status="inferred")
    risky_patch = patch(mv1)
    repair_patch = patch(mv2)

    steps: list[tuple[str, dict[str, Any], dict[str, Any]]] = [
        ("session.phase_changed", {"session_id": session, "to": "frame"}, SYSTEM),
        (
            "budget.updated",
            {"scope": {"session": session}, "limits": {"tokens": 2000000, "usd": None}},
            HUMAN,
        ),
        ("source.ingested", source("paper")["payload"], SYSTEM),
        ("source.ingested", source("probe")["payload"], SYSTEM),
        ("claim.proposed", {"proposal_id": "prop-claim-1", "claim": documented}, AGENT),
        (
            "claim.committed",
            {"claim_id": documented["id"], "claim": documented, "from_proposal": "prop-claim-1"},
            SYSTEM,
        ),
        ("claim.committed", {"claim_id": assumption["id"], "claim": assumption}, SYSTEM),
        ("model.version_created", {"version_id": mv1}, SYSTEM),
        (
            "model.patch_proposed",
            {"proposal_id": "prop-patch-1", "base_version": mv1, "patch": risky_patch},
            AGENT,
        ),
        (
            "model.patch_committed",
            {
                "version_id": mv2,
                "base_version": mv1,
                "patch": risky_patch,
                "from_proposal": "prop-patch-1",
            },
            SYSTEM,
        ),
        (
            "check.result",
            {
                "result_id": ident("chk", "c008"),
                "check_id": "C-008",
                "element_refs": [ident("flw", "writes")],
                "status": "fail",
            },
            SYSTEM,
        ),
        (
            "objection.raised",
            {
                "objection_id": ident("obj", "splitbrain"),
                "objection": objection(detecting_check={"proposed_check": proposed_check()}),
            },
            AGENT,
        ),
        (
            "model.patch_committed",
            {"version_id": mv3, "base_version": mv2, "patch": repair_patch},
            SYSTEM,
        ),
        (
            "objection.resolved",
            {"objection_id": ident("obj", "splitbrain"), "resolution": "patched", "ref": mv3},
            SYSTEM,
        ),
        ("claim.committed", {"claim_id": measured["id"], "claim": measured}, SYSTEM),
        (
            "experiment.recorded",
            {"experiment_id": ident("exp", "probe"), "result_claims": [measured["id"]]},
            SYSTEM,
        ),
        (
            "claim.status_changed",
            {
                "claim_id": assumption["id"],
                "from": "assumed",
                "to": "measured",
                "cause_event": ident("evt", "s15"),
            },
            SYSTEM,
        ),
        (
            "decision.recorded",
            {
                "adr_id": ident("adr", "fencing"),
                "decision": {
                    "title": "Fence lease holders with epochs",
                    "choice": "fencing epochs",
                    "evidence_claims": [documented["id"], assumption["id"]],
                    "alternatives": [
                        {"option": "longer TTL", "rejected_because": "hides the race"}
                    ],
                    "assumptions": [assumption["id"]],
                    "affected_elements": [ident("flw", "writes")],
                },
            },
            HUMAN,
        ),
        (
            "waiver.signed",
            {
                "waiver_id": ident("wvr", "singlenode"),
                "target_ref": "C-010",
                "risk": "Single Postgres node in the Phase 1 dev environment",
                "signer": "saumya",
            },
            HUMAN,
        ),
        (
            "entity.merged",
            {"kept_id": "lease-fencing", "merged_ids": ["fencing-token"], "method": "blocking"},
            SYSTEM,
        ),
        ("entity.merge_reverted", {"merge_event": ident("evt", "s19")}, HUMAN),
        ("claim.committed", {"claim_id": inferred["id"], "claim": inferred}, SYSTEM),
        ("claim.retracted", {"claim_id": inferred["id"], "cause": "premise superseded"}, HUMAN),
        (
            "session.checkpoint",
            {
                "session_id": session,
                "phase": "package",
                "best_version": mv3,
                "open_risk_ids": [],
                "spend": {"tokens": 184000, "usd": 3.5},
            },
            SYSTEM,
        ),
    ]
    return [
        {
            "event_id": ident("evt", f"s{seq}"),
            "project_id": project_id,
            "seq": seq,
            "ts": f"2026-10-02T06:{seq:02d}:00+05:30",
            "actor": actor,
            "session_id": session,
            "type": type,
            "payload": payload,
            "idempotency_key": f"sample-{seq:04d}",
        }
        for seq, (type, payload, actor) in enumerate(steps)
    ]


def as_candidate(event: Event) -> Event:
    return {k: v for k, v in event.items() if k not in ("seq", "prev_hash")}
