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
from architect.gateway.cache import canonical as _canonical
from architect.ingestion import grades
from architect.ingestion.normalize import spo_key
from architect.knowledge.graph import ABOUT, claim_label, entity_node
from architect.model_fold import Model, PatchError, apply_patch, child_of, empty_model

Event = dict[str, Any]


def json_dumps(value: Any) -> str:
    return _canonical(value).decode()


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
    "proj_budgets",
    "proj_claim_proposals",
    "proj_experiments",
    "proj_session_timeline",
    "proj_graph_nodes",
    "proj_graph_edges",
    "proj_entity_merges",
    "proj_entity_alias",
    "proj_findings",
    "ses_sessions",
)

MAIN = "main"  # the branch of an event that names none (contracts v1.2)

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

    # the knowledge graph (M8)
    def node(
        self,
        node_id: str,
        node_type: str,
        label: str,
        entity_type: str | None = None,
        origin: str = "event",
    ) -> None:
        """A graph node, introduced by this event unless an earlier one already did."""
        self.cur.execute(
            "INSERT INTO proj_graph_nodes (project_id, node_id, node_type, entity_type, label, "
            "origin, seq) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (project_id, node_id) DO NOTHING",
            (self.pid, node_id, node_type, entity_type, label, origin, self.seq),
        )

    def claim_edge(self, edge_type: str, src: str, dst: str, claim_id: str) -> None:
        """An edge derived from a claim. ord continues the event's edge counter, so (seq, ord)
        names one edge across proj_edges and proj_graph_edges."""
        self.cur.execute(
            "INSERT INTO proj_graph_edges (project_id, seq, ord, edge_type, src, dst, claim_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (self.pid, self.seq, self._edges, edge_type, src, dst, claim_id),
        )
        self._edges += 1

    def _claim_graph(self, claim_id: str, claim: dict[str, Any]) -> None:
        self.node(claim_id, "claim", claim_label(claim))
        ends = [entity_node(claim["subject"]), entity_node(claim["object"])]
        for end in ends:
            if end is not None:
                node_id, node_type, entity_type, label = end
                self.node(node_id, node_type, label, entity_type)
        subject, obj = ends
        if subject is not None and obj is not None:
            self.claim_edge(claim["predicate"], subject[0], obj[0], claim_id)
        for node_id in dict.fromkeys(end[0] for end in ends if end is not None):
            self.claim_edge(ABOUT, claim_id, node_id, claim_id)

    def _model_graph(self, model: Model) -> None:
        """The elements of the head model are nodes; the version just stored is the head."""
        self.cur.execute(
            "DELETE FROM proj_graph_nodes WHERE project_id = %s AND origin = 'model'", (self.pid,)
        )
        for element_type, elements in model.get("elements", {}).items():
            node_type = "requirement" if element_type == "requirements" else "element"
            for element in elements:
                if "id" in element:
                    label = str(element.get("name") or element["id"])
                    self.node(element["id"], node_type, label, element_type, origin="model")

    def entity_merged(self) -> None:
        p = self.payload
        self.cur.execute(
            "INSERT INTO proj_entity_merges (project_id, seq, event_id, kept_id, merged_ids, "
            "method, reverted_seq) VALUES (%s, %s, %s, %s, %s, %s, NULL)",
            (
                self.pid,
                self.seq,
                self.event["event_id"],
                p["kept_id"],
                p["merged_ids"],
                p["method"],
            ),
        )
        self._recompute_aliases()

    def entity_merge_reverted(self) -> None:
        self.cur.execute(
            "UPDATE proj_entity_merges SET reverted_seq = %s WHERE project_id = %s "
            "AND event_id = %s AND reverted_seq IS NULL",
            (self.seq, self.pid, self.payload["merge_event"]),
        )
        self._recompute_aliases()

    def _recompute_aliases(self) -> None:
        """entity -> kept id, from the merges that stand, in commit order. A merge into an
        entity that was itself merged away follows it to where it went."""
        merges = self.cur.execute(
            "SELECT kept_id, merged_ids FROM proj_entity_merges WHERE project_id = %s "
            "AND reverted_seq IS NULL ORDER BY seq",
            (self.pid,),
        ).fetchall()
        parent: dict[str, str] = {}

        def find(node: str) -> str:
            while parent.get(node, node) != node:
                node = parent[node]
            return node

        for merge in merges:
            root = find(merge["kept_id"])
            for merged in merge["merged_ids"]:
                other = find(merged)
                if other != root:
                    parent[other] = root
        self.cur.execute("DELETE FROM proj_entity_alias WHERE project_id = %s", (self.pid,))
        for entity in sorted(parent):
            root = find(entity)
            if root != entity:
                self.cur.execute(
                    "INSERT INTO proj_entity_alias (project_id, entity_id, canonical_id) "
                    "VALUES (%s, %s, %s)",
                    (self.pid, entity, root),
                )

    # sources
    def source_ingested(self) -> None:
        p = self.payload
        self.node(p["source_id"], "source", p["uri"])
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
    def claim_proposed(self) -> None:
        claim = self.payload["claim"]
        self.cur.execute(
            "INSERT INTO proj_claim_proposals (project_id, proposal_id, claim_id, claim, seq) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (project_id, proposal_id) DO NOTHING",
            (self.pid, self.payload["proposal_id"], claim["id"], Jsonb(claim), self.seq),
        )

    def claim_committed(self) -> None:
        claim = self.payload["claim"]
        claim_id = self.payload["claim_id"]
        derived_from = claim["provenance"].get("derived_from", [])
        proposal = self.payload.get("from_proposal")
        # M5: both extraction passes agreed when the extractor commits from its own proposal
        agreed = proposal is not None and self.event["actor"].get("id") == "extractor"
        self.cur.execute(
            "INSERT INTO proj_claims (project_id, claim_id, claim, status, load_bearing, "
            "taint_origin, supersedes, derived_from, premise_compromised, first_seq, last_seq, "
            "two_pass_agreement) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, false, %s, %s, %s)",
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
                agreed,
            ),
        )
        if proposal is not None:
            self.cur.execute(
                "UPDATE proj_claim_proposals SET committed = true "
                "WHERE project_id = %s AND proposal_id = %s",
                (self.pid, proposal),
            )
        self._open_status(claim_id, claim["status"], self.event["event_id"])
        for evidence in claim.get("evidence", []):
            self.edge("EVIDENCES", claim_id, evidence["source"])
        for premise in derived_from:
            self.edge("DERIVED_FROM", claim_id, premise)
        if "supersedes" in claim:
            self.edge("SUPERSEDES", claim_id, claim["supersedes"])
        self._claim_graph(claim_id, claim)
        if derived_from or claim["status"] in COMPROMISING_STATUSES:
            self._recompute_compromised()
        self._recompute_grades()

    def claim_status_changed(self) -> None:
        self._set_status(self.payload["claim_id"], self.payload["to"], self.payload["cause_event"])
        self._recompute_grades()

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

    def _recompute_grades(self) -> None:
        """M5: grade and confidence for every committed claim of the project, from the sources
        that evidence them, the claims that corroborate them and the verification events that
        reference them. Deterministic, so a rebuild reproduces it."""
        rows = self.cur.execute(
            "SELECT c.claim_id, c.claim, c.status, c.taint_origin, c.two_pass_agreement, "
            "c.grade, c.confidence, "
            "(SELECT coalesce(array_agg(DISTINCT s.content_hash), '{}') FROM proj_sources s "
            " WHERE s.project_id = c.project_id AND s.source_id IN "
            " (SELECT jsonb_array_elements(c.claim -> 'evidence') ->> 'source')) AS hashes, "
            "(SELECT count(*) FROM proj_checks k WHERE k.project_id = c.project_id "
            " AND k.status = 'pass' AND k.element_refs ? c.claim_id) "
            "+ (SELECT count(*) FROM proj_experiments x WHERE x.project_id = c.project_id "
            " AND x.result_claims ? c.claim_id) AS verifications "
            "FROM proj_claims c WHERE c.project_id = %s ORDER BY c.first_seq",
            (self.pid,),
        ).fetchall()
        keyed: dict[str, list[tuple[str, set[str]]]] = {}
        for row in rows:
            claim = row["claim"]
            key = (
                spo_key(claim["subject"], claim["predicate"], claim["object"])
                + "|"
                + json_dumps(claim.get("magnitude"))
            )
            keyed.setdefault(key, []).append((row["claim_id"], set(row["hashes"])))
        for row in rows:
            claim = row["claim"]
            key = (
                spo_key(claim["subject"], claim["predicate"], claim["object"])
                + "|"
                + json_dumps(claim.get("magnitude"))
            )
            own = set(row["hashes"])
            others: set[str] = set()
            for claim_id, hashes in keyed[key]:
                if claim_id != row["claim_id"]:
                    others |= hashes - own
            corroborations = len(others)
            verifications = int(row["verifications"])
            new_grade = grades.grade(row["status"], corroborations, verifications)
            value, inputs = grades.confidence(
                source_tier=row["taint_origin"],
                corroborations=corroborations,
                two_pass_agreement=row["two_pass_agreement"],
                verification_events=verifications,
            )
            if new_grade != row["grade"] or value != row["confidence"]:
                self.cur.execute(
                    "UPDATE proj_claims SET grade = %s, confidence = %s, confidence_inputs = %s "
                    "WHERE project_id = %s AND claim_id = %s",
                    (new_grade, value, Jsonb(inputs), self.pid, row["claim_id"]),
                )
            else:
                self.cur.execute(
                    "UPDATE proj_claims SET confidence_inputs = %s WHERE project_id = %s "
                    "AND claim_id = %s AND confidence_inputs = '{}'::jsonb",
                    (Jsonb(inputs), self.pid, row["claim_id"]),
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
        self._store_version(version_id, parent, model, self.payload.get("branch", MAIN))

    def model_patch_committed(self) -> None:
        version_id, base = self.payload["version_id"], self.payload["base_version"]
        try:
            model = apply_patch(self._model(base), self.payload["patch"], version_id)
        except PatchError as error:
            raise ProjectionError(f"{self._where()}: {error}") from error
        self._store_version(version_id, base, model, self.payload.get("branch", MAIN))

    def _model(self, version_id: str) -> Model:
        row = self.cur.execute(
            "SELECT model FROM proj_model_versions WHERE project_id = %s AND version_id = %s",
            (self.pid, version_id),
        ).fetchone()
        if row is None:
            raise ProjectionError(f"{self._where()}: model version {version_id} is not projected")
        return row["model"]

    def _store_version(
        self, version_id: str, parent: str | None, model: Model, branch: str = MAIN
    ) -> None:
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
            "committed_at_seq, model, branch) VALUES (%s, %s, %s, %s, %s, %s)",
            (self.pid, version_id, parent, self.seq, Jsonb(model), branch),
        )
        for link_type, (edge_type, src, dst) in MODEL_LINK_EDGES.items():
            for link in model["links"].get(link_type, []):
                self.edge(edge_type, link[src], link[dst], version_id)
        if branch == MAIN:  # the project's head is main's head; other branches are alternatives
            self._model_graph(model)

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
        self.node(adr_id, "adr", str(decision.get("title") or adr_id))

    def waiver_signed(self) -> None:
        p = self.payload
        self.cur.execute(
            "INSERT INTO proj_waivers (project_id, seq, waiver_id, target_ref, risk, signer) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (self.pid, self.seq, p["waiver_id"], p["target_ref"], p["risk"], p["signer"]),
        )
        session_id = self.event.get("session_id")
        if session_id is not None:  # signed at a session's gate: the session's row lists it
            signed = [{"waiver_id": p["waiver_id"], "target_ref": p["target_ref"]}]
            self._session(session_id, "waivers = waivers || %s::jsonb", Jsonb(signed))

    # findings (contracts v1.2, P-12)
    def finding_raised(self) -> None:
        p = self.payload
        self.cur.execute(
            "INSERT INTO proj_findings (project_id, finding_id, kind, severity, summary, refs, "
            "evidence_claims, suggested_action, detector, dedupe_key, open, raised_seq) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, true, %s)",
            (
                self.pid,
                p["finding_id"],
                p["kind"],
                p["severity"],
                p["summary"],
                Jsonb(p["refs"]),
                Jsonb(p.get("evidence_claims", [])),
                Jsonb(p["suggested_action"]),
                Jsonb(p["detector"]),
                p["dedupe_key"],
                self.seq,
            ),
        )

    def finding_resolved(self) -> None:
        p = self.payload
        self.cur.execute(
            "UPDATE proj_findings SET open = false, resolved_seq = %s, resolution = %s, "
            "resolution_ref = %s WHERE project_id = %s AND finding_id = %s",
            (self.seq, p["resolution"], p.get("ref"), self.pid, p["finding_id"]),
        )

    def experiment_recorded(self) -> None:
        p = self.payload
        self.cur.execute(
            "INSERT INTO proj_experiments (project_id, seq, experiment_id, result_claims) "
            "VALUES (%s, %s, %s, %s)",
            (self.pid, self.seq, p["experiment_id"], Jsonb(p["result_claims"])),
        )
        self._recompute_grades()

    def check_result(self) -> None:
        p = self.payload
        evidence = p.get("evidence")
        # contracts v1.1 (P-10): the result names its model version; a v1.0 result may name it
        # in evidence.model_version instead; one that names none judged the head of its time.
        named = p.get("model_version")
        if named is None and isinstance(evidence, dict):
            named = evidence.get("model_version")
        if isinstance(named, str):
            head = {"version_id": named}
        else:
            head = self.cur.execute(
                "SELECT version_id FROM proj_model_versions WHERE project_id = %s "
                "AND branch = %s ORDER BY committed_at_seq DESC LIMIT 1",
                (self.pid, MAIN),
            ).fetchone()
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
        if p["status"] == "pass":
            self._recompute_grades()

    # budgets (read by the model gateway)
    def budget_updated(self) -> None:
        p = self.payload
        self.cur.execute(
            "INSERT INTO proj_budgets (project_id, seq, scope, limits) VALUES (%s, %s, %s, %s)",
            (self.pid, self.seq, Jsonb(p.get("scope", {})), Jsonb(p["limits"])),
        )

    # sessions: the timeline, and the session's row (a projection since contracts v1.2)
    def _session(self, session_id: str, assignments: str, *values: Any) -> None:
        """Update the session's row and stamp it with this event's ts. A session that has no
        row (its ledger predates session.status_changed) is left without one."""
        self.cur.execute(
            f"UPDATE ses_sessions SET {assignments}, updated_at = %s::timestamptz "
            "WHERE project_id = %s AND session_id = %s",
            (*values, self.event["ts"], self.pid, session_id),
        )

    def session_phase_changed(self) -> None:
        p = self.payload
        self._timeline(p["session_id"], "phase_changed", p.get("from"), p["to"], None)
        self._session(
            p["session_id"], "phase = %s, round = coalesce(%s, round)", p["to"], p.get("round")
        )

    def session_checkpoint(self) -> None:
        p = self.payload
        detail = {k: v for k, v in p.items() if k not in ("session_id", "phase")}
        self._timeline(p["session_id"], "checkpoint", None, p["phase"], detail)
        self._session(
            p["session_id"],
            "best_version = %s, open_risks = %s, spend = %s, "
            "package_key = coalesce(%s, package_key)",
            p.get("best_version"),
            Jsonb(p["open_risk_ids"]),
            Jsonb({"tokens": p["spend"]["tokens"], "usd": p["spend"].get("usd", 0)}),
            p.get("package_ref"),
        )

    def session_status_changed(self) -> None:
        """Every status change of a session and every decision at its gates (P-11). The first
        event of a session creates its row; a refused decision only records the refusal."""
        p = self.payload
        session_id = p["session_id"]
        self.cur.execute(
            "INSERT INTO ses_sessions (project_id, session_id, preset, limits, status, phase, "
            "brief_source_id, started_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, NULL, %s, %s, %s::timestamptz) "
            "ON CONFLICT (project_id, session_id) DO NOTHING",
            (
                self.pid,
                session_id,
                p.get("preset", ""),
                Jsonb(p.get("limits", {})),
                p["status"],
                p.get("brief_source_id"),
                self.event["ts"],
                self.event["ts"],
            ),
        )
        if p.get("refused"):
            refusal = {"decision": p.get("decision"), "why": p.get("reason")}
            self._session(session_id, "last_refusal = %s", Jsonb(refusal))
            return
        sets, values = ["status = %s"], [p["status"]]
        if "outcome" in p:
            sets.append("outcome = %s")
            values.append(p["outcome"])
        elif p.get("decision") == "extend":
            sets.append("outcome = NULL")  # the loop runs again: it has not ended
        if p["status"] == "failed":
            sets.append("failure = %s")
            values.append(p.get("reason"))
        for column, key in (("package_key", "package_ref"), ("gate_verdict", "gate_verdict")):
            if key in p:
                sets.append(f"{column} = %s")
                values.append(p[key])
        if "limits" in p:
            sets.append("limits = %s")
            values.append(Jsonb(p["limits"]))
        self._session(session_id, ", ".join(sets), *values)

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
    "claim.proposed": _Fold.claim_proposed,
    "claim.committed": _Fold.claim_committed,
    "experiment.recorded": _Fold.experiment_recorded,
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
    "budget.updated": _Fold.budget_updated,
    "entity.merged": _Fold.entity_merged,
    "entity.merge_reverted": _Fold.entity_merge_reverted,
    "session.status_changed": _Fold.session_status_changed,
    "finding.raised": _Fold.finding_raised,
    "finding.resolved": _Fold.finding_resolved,
}

# Committed events with no read model yet. Listed so a new event type is a decision, not
# an accident.
NOT_PROJECTED = frozenset(
    {
        "model.patch_proposed",
    }
)


def fold(cur: Cursor[dict[str, Any]], event: Event) -> None:
    """Fold one committed event into the proj_* tables through `cur`."""
    handler = HANDLERS.get(event["type"])
    if handler is not None:
        handler(_Fold(cur, event))
    elif event["type"] not in NOT_PROJECTED:
        raise ProjectionError(f"seq {event['seq']}: no projection rule for {event['type']}")
