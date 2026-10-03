"""The read models: fold one committed event into the proj_* tables.

A projection of the ledger, like arb_* (architect.state), but shaped for queries rather than
for the Arbiter's rules. `fold` is deterministic: the same events in the same order produce
the same rows, with no wall-clock values and no generated ids. It never writes to the ledger
and never calls the Arbiter. The projector (architect.projector) calls it inside the
transaction that advances the cursor.

Model versions are folded through architect.model_fold, the fold the Arbiter also uses, so
a version the Arbiter committed always materializes here.
"""

from __future__ import annotations

from typing import Any

from psycopg import Cursor
from psycopg.types.json import Jsonb

from architect.contracts import first_error, json_path, load_contracts
from architect.model_fold import Model, PatchError, apply_patch, child_of, empty_model

Event = dict[str, Any]

PROJECTION = "read_models"

# Every table the fold writes. proj_cursors is the projector's bookkeeping, not content.
PROJ_TABLES = (
    "proj_sources",
    "proj_claims",
    "proj_claim_status_history",
    "proj_model_versions",
    "proj_edges",
    "proj_objections",
    "proj_decisions",
    "proj_waivers",
    "proj_checks",
    "proj_session_timeline",
)

# §7 refutation propagation: a claim in one of these statuses compromises what derives from it.
COMPROMISING_STATUSES = ("refuted", "retracted")

# link list in the model -> (edge type, source field, destination field)
MODEL_LINK_EDGES = {
    "satisfies": ("SATISFIES", "component", "requirement"),
    "depends_on": ("DEPENDS_ON", "from", "to"),
    "mitigates": ("MITIGATES", "control", "risk"),
}

# Claims in a premise chain that reaches a refuted or retracted claim. The same closure the
# read side computes as of an earlier seq (architect.readmodel).
_RECOMPUTE_COMPROMISED = """
WITH RECURSIVE compromised (claim_id) AS (
    SELECT e.src
    FROM proj_edges e
    JOIN proj_claims premise ON premise.project_id = e.project_id AND premise.claim_id = e.dst
    WHERE e.project_id = %(pid)s AND e.edge_type = 'DERIVED_FROM'
      AND premise.status = ANY(%(bad)s)
  UNION
    SELECT e.src
    FROM proj_edges e
    JOIN compromised c ON c.claim_id = e.dst
    WHERE e.project_id = %(pid)s AND e.edge_type = 'DERIVED_FROM'
)
UPDATE proj_claims c
SET premise_compromised = (c.claim_id IN (SELECT claim_id FROM compromised))
WHERE c.project_id = %(pid)s
  AND c.premise_compromised <> (c.claim_id IN (SELECT claim_id FROM compromised))
"""


class ProjectionError(Exception):
    """An event cannot be folded: a projector bug, or a ledger the read models cannot hold."""


class _Fold:
    """The writes of one event."""

    def __init__(self, cur: Cursor[dict[str, Any]], event: Event) -> None:
        self.cur = cur
        self.pid: str = event["project_id"]
        self.seq: int = event["seq"]
        self.event = event
        self.payload: dict[str, Any] = event["payload"]
        self._edges = 0

    def edge(self, edge_type: str, src: str, dst: str, version_id: str | None = None) -> None:
        self.cur.execute(
            "INSERT INTO proj_edges (project_id, seq, ord, edge_type, src, dst, version_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (self.pid, self.seq, self._edges, edge_type, src, dst, version_id),
        )
        self._edges += 1

    # sources
    def source_ingested(self) -> None:
        p = self.payload
        self.cur.execute(
            "INSERT INTO proj_sources (project_id, source_id, uri, content_hash, media_type, "
            "license, taint_origin, seq) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                self.pid,
                p["source_id"],
                p["uri"],
                p["content_hash"],
                p["media_type"],
                p.get("license"),
                p["taint_origin"],
                self.seq,
            ),
        )

    # claims
    def claim_committed(self) -> None:
        claim = self.payload["claim"]
        claim_id = self.payload["claim_id"]
        derived_from = claim["provenance"].get("derived_from", [])
        self.cur.execute(
            "INSERT INTO proj_claims (project_id, claim_id, claim, status, load_bearing, "
            "taint_origin, supersedes, derived_from, premise_compromised, first_seq, last_seq) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, false, %s, %s)",
            (
                self.pid,
                claim_id,
                Jsonb(claim),
                claim["status"],
                claim.get("load_bearing", False),
                claim["taint"]["origin"],
                claim.get("supersedes"),
                derived_from,
                self.seq,
                self.seq,
            ),
        )
        self._open_status(claim_id, claim["status"], self.event["event_id"])
        for evidence in claim.get("evidence", []):
            self.edge("EVIDENCES", claim_id, evidence["source"])
        for premise in derived_from:
            self.edge("DERIVED_FROM", claim_id, premise)
        if "supersedes" in claim:
            self.edge("SUPERSEDES", claim_id, claim["supersedes"])
        if derived_from or claim["status"] in COMPROMISING_STATUSES:
            self._recompute_compromised()

    def claim_status_changed(self) -> None:
        self._set_status(self.payload["claim_id"], self.payload["to"], self.payload["cause_event"])

    def claim_retracted(self) -> None:
        self._set_status(self.payload["claim_id"], "retracted", self.event["event_id"])

    def _set_status(self, claim_id: str, status: str, cause_event: str) -> None:
        previous = self.cur.execute(
            "SELECT status FROM proj_claims WHERE project_id = %s AND claim_id = %s",
            (self.pid, claim_id),
        ).fetchone()
        if previous is None:
            raise ProjectionError(f"status change of claim {claim_id}, which was never committed")
        self.cur.execute(
            "UPDATE proj_claims SET status = %s, last_seq = %s "
            "WHERE project_id = %s AND claim_id = %s",
            (status, self.seq, self.pid, claim_id),
        )
        self.cur.execute(
            "UPDATE proj_claim_status_history SET to_seq = %s "
            "WHERE project_id = %s AND claim_id = %s AND to_seq IS NULL",
            (self.seq, self.pid, claim_id),
        )
        self._open_status(claim_id, status, cause_event)
        if status in COMPROMISING_STATUSES or previous["status"] in COMPROMISING_STATUSES:
            self._recompute_compromised()

    def _open_status(self, claim_id: str, status: str, cause_event: str) -> None:
        self.cur.execute(
            "INSERT INTO proj_claim_status_history (project_id, claim_id, from_seq, to_seq, "
            "status, cause_event) VALUES (%s, %s, %s, NULL, %s, %s)",
            (self.pid, claim_id, self.seq, status, cause_event),
        )

    def _recompute_compromised(self) -> None:
        self.cur.execute(
            _RECOMPUTE_COMPROMISED, {"pid": self.pid, "bad": list(COMPROMISING_STATUSES)}
        )

    # model versions
    def model_version_created(self) -> None:
        version_id, parent = self.payload["version_id"], self.payload.get("parent")
        if parent is None:
            model = empty_model(self.pid, version_id)
        else:
            model = child_of(self._model(parent), version_id)
        self._store_version(version_id, parent, model)

    def model_patch_committed(self) -> None:
        version_id, base = self.payload["version_id"], self.payload["base_version"]
        try:
            model = apply_patch(self._model(base), self.payload["patch"], version_id)
        except PatchError as error:
            raise ProjectionError(f"{self._where()}: {error}") from error
        self._store_version(version_id, base, model)

    def _model(self, version_id: str) -> Model:
        row = self.cur.execute(
            "SELECT model FROM proj_model_versions WHERE project_id = %s AND version_id = %s",
            (self.pid, version_id),
        ).fetchone()
        if row is None:
            raise ProjectionError(f"{self._where()}: model version {version_id} is not projected")
        return row["model"]

    def _store_version(self, version_id: str, parent: str | None, model: Model) -> None:
        error = first_error(load_contracts().model, model)
        if error is not None:
            raise ProjectionError(
                f"{self._where()}: version {version_id} is not a valid system model at "
                f"{json_path(error.path)}: {error.message}"
            )
        exists = self.cur.execute(
            "SELECT 1 FROM proj_model_versions WHERE project_id = %s AND version_id = %s",
            (self.pid, version_id),
        ).fetchone()
        if exists is not None:
            raise ProjectionError(f"{self._where()}: model version {version_id} already exists")
        self.cur.execute(
            "INSERT INTO proj_model_versions (project_id, version_id, parent_version, "
            "committed_at_seq, model) VALUES (%s, %s, %s, %s, %s)",
            (self.pid, version_id, parent, self.seq, Jsonb(model)),
        )
        for link_type, (edge_type, src, dst) in MODEL_LINK_EDGES.items():
            for link in model["links"].get(link_type, []):
                self.edge(edge_type, link[src], link[dst], version_id)

    def _where(self) -> str:
        return f"seq {self.seq} ({self.event['type']})"

    # objections, decisions, waivers, checks
    def objection_raised(self) -> None:
        objection = self.payload["objection"]
        self.cur.execute(
            "INSERT INTO proj_objections (project_id, objection_id, severity, open, objection, "
            "raised_seq, resolved_seq, resolution, resolution_ref) "
            "VALUES (%s, %s, %s, true, %s, %s, NULL, NULL, NULL) "
            "ON CONFLICT (project_id, objection_id) DO UPDATE SET severity = EXCLUDED.severity, "
            "open = true, objection = EXCLUDED.objection, raised_seq = EXCLUDED.raised_seq, "
            "resolved_seq = NULL, resolution = NULL, resolution_ref = NULL",
            (
                self.pid,
                self.payload["objection_id"],
                objection["severity"],
                Jsonb(objection),
                self.seq,
            ),
        )

    def objection_resolved(self) -> None:
        self.cur.execute(
            "UPDATE proj_objections SET open = false, resolved_seq = %s, resolution = %s, "
            "resolution_ref = %s WHERE project_id = %s AND objection_id = %s",
            (
                self.seq,
                self.payload["resolution"],
                self.payload["ref"],
                self.pid,
                self.payload["objection_id"],
            ),
        )

    def decision_recorded(self) -> None:
        adr_id, decision = self.payload["adr_id"], self.payload["decision"]
        self.cur.execute(
            "INSERT INTO proj_decisions (project_id, seq, adr_id, decision) "
            "VALUES (%s, %s, %s, %s)",
            (self.pid, self.seq, adr_id, Jsonb(decision)),
        )
        for claim_id in decision["evidence_claims"]:
            self.edge("DECISION_EVIDENCE", adr_id, claim_id)

    def waiver_signed(self) -> None:
        p = self.payload
        self.cur.execute(
            "INSERT INTO proj_waivers (project_id, seq, waiver_id, target_ref, risk, signer) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (self.pid, self.seq, p["waiver_id"], p["target_ref"], p["risk"], p["signer"]),
        )

    def check_result(self) -> None:
        p = self.payload
        head = self.cur.execute(
            "SELECT version_id FROM proj_model_versions WHERE project_id = %s "
            "ORDER BY committed_at_seq DESC LIMIT 1",
            (self.pid,),
        ).fetchone()
        evidence = p.get("evidence")
        self.cur.execute(
            "INSERT INTO proj_checks (project_id, seq, result_id, check_id, status, "
            "element_refs, evidence, version_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                self.pid,
                self.seq,
                p["result_id"],
                p["check_id"],
                p["status"],
                Jsonb(p["element_refs"]),
                Jsonb(evidence) if evidence is not None else None,
                head["version_id"] if head else None,
            ),
        )

    # session timeline
    def session_phase_changed(self) -> None:
        p = self.payload
        self._timeline(p["session_id"], "phase_changed", p.get("from"), p["to"], None)

    def session_checkpoint(self) -> None:
        p = self.payload
        detail = {k: v for k, v in p.items() if k not in ("session_id", "phase")}
        self._timeline(p["session_id"], "checkpoint", None, p["phase"], detail)

    def _timeline(
        self,
        session_id: str,
        kind: str,
        from_phase: str | None,
        phase: str,
        detail: dict[str, Any] | None,
    ) -> None:
        self.cur.execute(
            "INSERT INTO proj_session_timeline (project_id, seq, session_id, kind, from_phase, "
            "phase, detail, ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                self.pid,
                self.seq,
                session_id,
                kind,
                from_phase,
                phase,
                Jsonb(detail) if detail is not None else None,
                self.event["ts"],
            ),
        )


HANDLERS = {
    "source.ingested": _Fold.source_ingested,
    "claim.committed": _Fold.claim_committed,
    "claim.status_changed": _Fold.claim_status_changed,
    "claim.retracted": _Fold.claim_retracted,
    "model.version_created": _Fold.model_version_created,
    "model.patch_committed": _Fold.model_patch_committed,
    "decision.recorded": _Fold.decision_recorded,
    "objection.raised": _Fold.objection_raised,
    "objection.resolved": _Fold.objection_resolved,
    "waiver.signed": _Fold.waiver_signed,
    "check.result": _Fold.check_result,
    "session.phase_changed": _Fold.session_phase_changed,
    "session.checkpoint": _Fold.session_checkpoint,
}

# Committed events with no read model yet. Listed so a new event type is a decision, not
# an accident.
NOT_PROJECTED = frozenset(
    {
        "claim.proposed",
        "model.patch_proposed",
        "entity.merged",
        "entity.merge_reverted",
        "experiment.recorded",
        "budget.updated",
    }
)


def fold(cur: Cursor[dict[str, Any]], event: Event) -> None:
    """Fold one committed event into the proj_* tables through `cur`."""
    handler = HANDLERS.get(event["type"])
    if handler is not None:
        handler(_Fold(cur, event))
    elif event["type"] not in NOT_PROJECTED:
        raise ProjectionError(f"seq {event['seq']}: no projection rule for {event['type']}")
