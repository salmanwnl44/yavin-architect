"""Stage 5: an agreed candidate becomes claim.proposed + claim.committed; a quarantined one
becomes claim.proposed only. The pipeline fills status, evidence, taint, provenance and ids
(R1); nothing here ever writes a confidence."""

from __future__ import annotations

from typing import Any

from psycopg_pool import ConnectionPool

from architect.arbiter import Arbiter
from architect.ingestion.extract import PIPELINE_VERSION, Candidate
from architect.ingestion.normalize import typed_id
from architect.ingestion.sources import Source

ACTOR = {"kind": "agent", "id": "extractor", "role": "extractor"}
EVIDENCE_KIND = {"statement": "statement", "table": "table", "code": "code"}


def claim_id_for(source: Source, candidate: Candidate, pipeline_version: int) -> str:
    """'clm_' + 26 base32 chars of sha256 over (source_id, locator, normalized SPO,
    magnitude, pipeline version)."""
    magnitude = (
        ""
        if candidate.magnitude is None
        else f"{candidate.magnitude['value']}|{candidate.magnitude['unit']}"
    )
    return typed_id(
        "clm", source.source_id, candidate.locator, candidate.spo, magnitude, str(pipeline_version)
    )


def build_claim(
    source: Source,
    candidate: Candidate,
    *,
    segment_kind: str,
    model_tier: str,
    prompt_hash: str,
    pipeline_version: int,
    recorded_at: str,
) -> dict[str, Any]:
    claim: dict[str, Any] = {
        "id": claim_id_for(source, candidate, pipeline_version),
        "subject": candidate.subject,
        "predicate": candidate.predicate,
        "object": candidate.object,
        "status": "documented",
        "evidence": [
            {
                "source": source.source_id,
                "span": candidate.locator,
                "kind": EVIDENCE_KIND.get(segment_kind, "statement"),
            }
        ],
        "taint": {"origin": source.taint_origin}
        | ({"license": source.license} if source.license else {}),
        "recorded_at": recorded_at,
        "provenance": {
            "extractor": {
                "model_tier": model_tier,
                "prompt_hash": prompt_hash,
                "pipeline_version": pipeline_version,
            }
        },
    }
    if candidate.magnitude is not None:
        claim["magnitude"] = candidate.magnitude
    if candidate.conditions:
        claim["conditions"] = candidate.conditions
    assert "confidence" not in claim
    return claim


def propose(pool: ConnectionPool, project_id: str, claim: dict[str, Any]) -> dict[str, Any]:
    """claim.proposed, idempotent on the claim id."""
    proposal_id = f"prp-{claim['id'][4:]}"
    commit = Arbiter(pool).submit(
        project_id,
        {
            "actor": ACTOR,
            "type": "claim.proposed",
            "payload": {"proposal_id": proposal_id, "claim": claim},
            "idempotency_key": f"proposal:{claim['id']}",
        },
    )
    return commit.event


def commit_claim(pool: ConnectionPool, project_id: str, claim: dict[str, Any]) -> dict[str, Any]:
    """claim.committed from its proposal, idempotent on the claim id."""
    proposal_id = f"prp-{claim['id'][4:]}"
    commit = Arbiter(pool).submit(
        project_id,
        {
            "actor": ACTOR,
            "type": "claim.committed",
            "payload": {"claim_id": claim["id"], "claim": claim, "from_proposal": proposal_id},
            "idempotency_key": f"commit:{claim['id']}",
        },
    )
    return commit.event


__all__ = ["ACTOR", "PIPELINE_VERSION", "build_claim", "claim_id_for", "commit_claim", "propose"]
