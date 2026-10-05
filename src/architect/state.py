"""The Arbiter's lookup state: the arb_* projection tables, scoped to one project.

Everything here is derived from committed events and can be rebuilt from them
(architect.rebuild). Reads and writes go through the caller's cursor so they share the
commit transaction.
"""

from __future__ import annotations

from typing import Any

from psycopg import Cursor
from psycopg.types.json import Jsonb

STATE_TABLES = (
    "arb_sources",
    "arb_claims",
    "arb_proposals",
    "arb_model_heads",
    "arb_model_versions",
    "arb_objections",
    "arb_refs",
    "arb_findings",
)

MAIN = "main"  # the branch of an event that names none (contracts v1.2)


class ArbiterState:
    def __init__(self, cur: Cursor[dict[str, Any]], project_id: str) -> None:
        self._cur = cur
        self._pid = project_id

    def _exists(self, query: str, *params: Any) -> bool:
        return self._cur.execute(query, (self._pid, *params)).fetchone() is not None

    # sources
    def source_exists(self, source_id: str) -> bool:
        return self._exists(
            "SELECT 1 FROM arb_sources WHERE project_id = %s AND source_id = %s", source_id
        )

    def add_source(self, source_id: str) -> None:
        self._cur.execute(
            "INSERT INTO arb_sources (project_id, source_id) VALUES (%s, %s)",
            (self._pid, source_id),
        )

    # proposals: one id namespace per project, shared by both kinds
    def proposal_kind(self, proposal_id: str) -> str | None:
        row = self._cur.execute(
            "SELECT kind FROM arb_proposals WHERE project_id = %s AND proposal_id = %s",
            (self._pid, proposal_id),
        ).fetchone()
        return row["kind"] if row else None

    def add_proposal(self, proposal_id: str, kind: str) -> None:
        self._cur.execute(
            "INSERT INTO arb_proposals (project_id, proposal_id, kind) VALUES (%s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            (self._pid, proposal_id, kind),
        )

    # claims
    def claim_status(self, claim_id: str) -> str | None:
        row = self._cur.execute(
            "SELECT status FROM arb_claims WHERE project_id = %s AND claim_id = %s",
            (self._pid, claim_id),
        ).fetchone()
        return row["status"] if row else None

    def add_claim(self, claim_id: str, claim: dict[str, Any]) -> None:
        self._cur.execute(
            "INSERT INTO arb_claims (project_id, claim_id, status, load_bearing, claim) "
            "VALUES (%s, %s, %s, %s, %s)",
            (self._pid, claim_id, claim["status"], claim.get("load_bearing", False), Jsonb(claim)),
        )
        # the entities the claim names can be referred to, by `ent:<type>:<id>` or bare id
        for end in (claim["subject"], claim["object"]):
            if "id" in end:
                self.add_ref(str(end["id"]), "entity")
                self.add_ref(f"ent:{end['entity_type']}:{end['id']}", "entity")

    def set_claim_status(self, claim_id: str, status: str) -> None:
        self._cur.execute(
            "UPDATE arb_claims SET status = %s WHERE project_id = %s AND claim_id = %s",
            (status, self._pid, claim_id),
        )

    # model heads: one per branch
    def model_head(self, branch: str = MAIN) -> str | None:
        row = self._cur.execute(
            "SELECT head_version FROM arb_model_heads WHERE project_id = %s AND branch = %s",
            (self._pid, branch),
        ).fetchone()
        return row["head_version"] if row else None

    def has_model_versions(self) -> bool:
        return self._exists("SELECT 1 FROM arb_model_versions WHERE project_id = %s LIMIT 1")

    def version_branch(self, version_id: str) -> str | None:
        """The branch a committed version was committed on, or None if there is none."""
        row = self._cur.execute(
            "SELECT branch FROM arb_model_versions WHERE project_id = %s AND version_id = %s",
            (self._pid, version_id),
        ).fetchone()
        return row["branch"] if row else None

    def model(self, version_id: str) -> dict[str, Any] | None:
        """The materialized model of a committed version, or None if there is no such version."""
        row = self._cur.execute(
            "SELECT model FROM arb_model_versions WHERE project_id = %s AND version_id = %s",
            (self._pid, version_id),
        ).fetchone()
        return row["model"] if row else None

    def add_model_version(self, version_id: str, model: dict[str, Any], branch: str = MAIN) -> None:
        """Record a committed model version with its model and make it the head of its
        branch."""
        self._cur.execute(
            "INSERT INTO arb_model_versions (project_id, version_id, model, branch) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (project_id, version_id) "
            "DO UPDATE SET model = EXCLUDED.model, branch = EXCLUDED.branch",
            (self._pid, version_id, Jsonb(model), branch),
        )
        self._cur.execute(
            "INSERT INTO arb_model_heads (project_id, branch, head_version) VALUES (%s, %s, %s) "
            "ON CONFLICT (project_id, branch) DO UPDATE SET head_version = EXCLUDED.head_version",
            (self._pid, branch, version_id),
        )
        for elements in model.get("elements", {}).values():
            for element in elements:
                if "id" in element:
                    self.add_ref(str(element["id"]), "element")

    # what a finding's refs may name (contracts v1.2)
    def add_ref(self, ref_id: str, kind: str) -> None:
        self._cur.execute(
            "INSERT INTO arb_refs (project_id, ref_id, kind) VALUES (%s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            (self._pid, ref_id, kind),
        )

    def ref_resolves(self, ref_id: str) -> bool:
        """A committed claim, a source, a model version, or an entity, element or decision."""
        return (
            self.claim_status(ref_id) is not None
            or self.source_exists(ref_id)
            or self.model(ref_id) is not None
            or self._exists(
                "SELECT 1 FROM arb_refs WHERE project_id = %s AND ref_id = %s LIMIT 1", ref_id
            )
        )

    # findings (contracts v1.2)
    def finding_exists(self, finding_id: str) -> bool:
        return self._exists(
            "SELECT 1 FROM arb_findings WHERE project_id = %s AND finding_id = %s", finding_id
        )

    def finding_is_open(self, finding_id: str) -> bool:
        return self._exists(
            "SELECT 1 FROM arb_findings WHERE project_id = %s AND finding_id = %s AND open",
            finding_id,
        )

    def open_finding_with(self, dedupe_key: str) -> str | None:
        row = self._cur.execute(
            "SELECT finding_id FROM arb_findings WHERE project_id = %s AND dedupe_key = %s "
            "AND open",
            (self._pid, dedupe_key),
        ).fetchone()
        return row["finding_id"] if row else None

    def open_finding(self, finding_id: str, dedupe_key: str) -> None:
        self._cur.execute(
            "INSERT INTO arb_findings (project_id, finding_id, dedupe_key, open) "
            "VALUES (%s, %s, %s, true)",
            (self._pid, finding_id, dedupe_key),
        )

    def close_finding(self, finding_id: str) -> None:
        self._cur.execute(
            "UPDATE arb_findings SET open = false WHERE project_id = %s AND finding_id = %s",
            (self._pid, finding_id),
        )

    # objections
    def objection_is_open(self, objection_id: str) -> bool:
        return self._exists(
            "SELECT 1 FROM arb_objections WHERE project_id = %s AND objection_id = %s AND open",
            objection_id,
        )

    def open_objection(self, objection_id: str, severity: str) -> None:
        self._cur.execute(
            "INSERT INTO arb_objections (project_id, objection_id, severity, open) "
            "VALUES (%s, %s, %s, true) ON CONFLICT (project_id, objection_id) "
            "DO UPDATE SET severity = EXCLUDED.severity, open = true",
            (self._pid, objection_id, severity),
        )

    def close_objection(self, objection_id: str) -> None:
        self._cur.execute(
            "UPDATE arb_objections SET open = false WHERE project_id = %s AND objection_id = %s",
            (self._pid, objection_id),
        )

    # the ledger itself, for rules that cite an earlier event
    def committed_event_type(self, event_id: str) -> str | None:
        row = self._cur.execute(
            "SELECT type FROM events WHERE project_id = %s AND event_id = %s",
            (self._pid, event_id),
        ).fetchone()
        return row["type"] if row else None
