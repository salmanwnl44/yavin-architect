"""The Arbiter's lookup state: the arb_* projection tables, scoped to one project.

Everything here is derived from committed events and can be rebuilt from them
(architect.rebuild). Reads and writes go through the caller's cursor so they share the
commit transaction.
"""

from __future__ import annotations

from typing import Any

from psycopg import Cursor
from psycopg.types.json import Jsonb

STATE_TABLES = ("arb_sources", "arb_claims", "arb_proposals", "arb_model_heads", "arb_objections")


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

    # proposals
    def proposal_exists(self, proposal_id: str, kind: str) -> bool:
        return self._exists(
            "SELECT 1 FROM arb_proposals WHERE project_id = %s AND proposal_id = %s AND kind = %s",
            proposal_id,
            kind,
        )

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

    def set_claim_status(self, claim_id: str, status: str) -> None:
        self._cur.execute(
            "UPDATE arb_claims SET status = %s WHERE project_id = %s AND claim_id = %s",
            (status, self._pid, claim_id),
        )

    # model head
    def model_head(self) -> str | None:
        row = self._cur.execute(
            "SELECT head_version FROM arb_model_heads WHERE project_id = %s", (self._pid,)
        ).fetchone()
        return row["head_version"] if row else None

    def set_model_head(self, version_id: str) -> None:
        self._cur.execute(
            "INSERT INTO arb_model_heads (project_id, head_version) VALUES (%s, %s) "
            "ON CONFLICT (project_id) DO UPDATE SET head_version = EXCLUDED.head_version",
            (self._pid, version_id),
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
