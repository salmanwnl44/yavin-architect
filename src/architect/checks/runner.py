"""The check runner: build the context from the read models, run the catalog, record each
result as a check.result event through the Arbiter, and compute the IMPLEMENTATION_READY gate
from the results on record.

This is the one module of the checks package that reads a database or writes events. It
catches the projector up before reading, and again after recording, so what it reads and
what the GET endpoints then show are current.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from psycopg_pool import ConnectionPool

from architect import readmodel
from architect.arbiter import Arbiter
from architect.checks.catalog import Catalog, Entry, evaluate, inputs_hash, load_catalog
from architect.checks.context import CheckContext, index_elements
from architect.checks.outcome import CheckOutcome
from architect.errors import Rejection
from architect.projections import COMPROMISING_STATUSES
from architect.projector import Projector

ACTOR = {"kind": "system", "id": "check-runner", "role": "verifier"}


@dataclass(frozen=True)
class CheckRun:
    """One check's outcome in one run, and how it was recorded."""

    check_id: str
    severity: str
    status: str
    element_refs: list[str]
    evidence: dict[str, Any]
    inputs_hash: str
    event_id: str | None  # None on a dry run
    replayed: bool | None  # True when the Arbiter already had this result


@dataclass(frozen=True)
class RunReport:
    model_version: str
    as_of_seq: int
    catalog_version: str
    results: list[CheckRun]

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_version": self.model_version,
            "as_of_seq": self.as_of_seq,
            "catalog_version": self.catalog_version,
            "results": [vars(run) for run in self.results],
        }


def latest_seq(pool: ConnectionPool, project_id: str) -> int | None:
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT max(seq) AS last_seq FROM events WHERE project_id = %s", (project_id,)
        ).fetchone()
    return row["last_seq"]


def build_context(
    pool: ConnectionPool, project_id: str, model: dict[str, Any], as_of_seq: int
) -> CheckContext:
    """The project's knowledge as of `as_of_seq`, from the read models."""
    claims: dict[str, Any] = {}
    requirements: list[str] = []
    for row in readmodel.list_claims(pool, project_id, as_of_seq=as_of_seq):
        claims[row["claim_id"]] = {
            "claim": row["claim"],
            "status": row["status"],
            "load_bearing": row["load_bearing"],
            "first_seq": row["first_seq"],
        }
        subject = row["claim"]["subject"]
        if subject.get("entity_type") == "requirement" and "id" in subject:
            if row["status"] not in COMPROMISING_STATUSES:
                requirements.append(subject["id"])
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT target_ref, waiver_id FROM proj_waivers WHERE project_id = %s AND seq <= %s "
            "ORDER BY seq",
            (project_id, as_of_seq),
        ).fetchall()
    waivers = {row["target_ref"]: row["waiver_id"] for row in rows}
    return CheckContext(
        as_of_seq=as_of_seq,
        requirements=tuple(dict.fromkeys(requirements)),
        claims=claims,
        waivers=waivers,
        elements=index_elements(model),
    )


def _result_id(check_id: str, digest: str) -> str:
    """Deterministic, contract-shaped: chk_ plus 26 hex characters."""
    return "chk_" + hashlib.sha256(f"{check_id}:{digest}".encode()).hexdigest()[:26]


def _evidence(
    outcome: CheckOutcome,
    entry: Entry,
    catalog: Catalog,
    version_id: str,
    as_of_seq: int,
    digest: str,
) -> dict[str, Any]:
    return {
        "model_version": version_id,
        "as_of_seq": as_of_seq,
        "catalog_version": catalog.version,
        "check_version": entry["version"],
        "params": entry.get("params", {}),
        "inputs_hash": digest,
        **outcome.evidence,
    }


def run(
    pool: ConnectionPool,
    project_id: str,
    version_id: str,
    as_of_seq: int | None = None,
    *,
    catalog: Catalog | None = None,
    record: bool = True,
) -> RunReport:
    """Run the catalog against a model version and record one check.result per check.

    A result whose inputs have not changed is already on record: the Arbiter answers its
    idempotency key with the original event and nothing new is written.
    """
    catalog = catalog or load_catalog()
    Projector(pool).catch_up(project_id)
    version = readmodel.model_version(pool, project_id, version_id)
    if version is None:
        raise Rejection(
            "MODEL_VERSION_NOT_FOUND",
            f"no projected model version {version_id} in project {project_id!r}",
        )
    if as_of_seq is None:
        as_of_seq = latest_seq(pool, project_id)
    model = version["model"]
    ctx = build_context(pool, project_id, model, as_of_seq)
    arbiter = Arbiter(pool)
    results: list[CheckRun] = []
    for entry in catalog.checks:
        outcome = evaluate(entry, model, ctx)
        digest = inputs_hash(entry, model, ctx)
        evidence = _evidence(outcome, entry, catalog, version_id, as_of_seq, digest)
        event_id = replayed = None
        if record:
            candidate = {
                "actor": ACTOR,
                "type": "check.result",
                "payload": {
                    "result_id": _result_id(entry["id"], digest),
                    "check_id": entry["id"],
                    "element_refs": outcome.element_refs,
                    "status": outcome.status,
                    "evidence": evidence,
                },
                "idempotency_key": f"check:{version_id}:{entry['id']}:{entry['version']}:{digest}",
            }
            commit = arbiter.submit(project_id, candidate)
            event_id, replayed = commit.event["event_id"], commit.replayed
        results.append(
            CheckRun(
                entry["id"],
                entry["severity"],
                outcome.status,
                outcome.element_refs,
                evidence,
                digest,
                event_id,
                replayed,
            )
        )
    if record:
        Projector(pool).catch_up(project_id)
    return RunReport(version_id, as_of_seq, catalog.version, results)


def latest_recorded_as_of(pool: ConnectionPool, project_id: str, version_id: str) -> int | None:
    """The as_of_seq of the most recent battery recorded for a version, or None."""
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT max((evidence ->> 'as_of_seq')::bigint) AS as_of FROM proj_checks "
            "WHERE project_id = %s AND version_id = %s AND evidence ? 'as_of_seq'",
            (project_id, version_id),
        ).fetchone()
    return row["as_of"]


def recorded(
    pool: ConnectionPool, project_id: str, version_id: str, as_of_seq: int | None = None
) -> dict[str, dict[str, Any]]:
    """The latest recorded result per check for a model version (and seq, when given)."""
    query = (
        "SELECT DISTINCT ON (check_id) check_id, seq, result_id, status, element_refs, evidence "
        "FROM proj_checks WHERE project_id = %(pid)s AND version_id = %(version)s"
    )
    params: dict[str, Any] = {"pid": project_id, "version": version_id}
    if as_of_seq is not None:
        query += " AND (evidence ->> 'as_of_seq')::bigint = %(as_of)s"
        params["as_of"] = as_of_seq
    query += " ORDER BY check_id, seq DESC"
    with pool.connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return {row["check_id"]: row for row in rows}


def gate(
    pool: ConnectionPool,
    project_id: str,
    version_id: str,
    as_of_seq: int | None = None,
    *,
    catalog: Catalog | None = None,
) -> dict[str, Any]:
    """IMPLEMENTATION_READY from the results on record for (version, as_of_seq).

    Recording a battery appends events, so the ledger's latest seq is always past the
    battery's as_of_seq; without an explicit seq the gate judges the most recent battery
    recorded for the version.

    Blocking: a critical check without a recorded result, a critical check that failed or
    could not be evaluated, or a critical objection open as of the seq that touches the
    version. Major and minor failures are warnings.
    """
    catalog = catalog or load_catalog()
    Projector(pool).catch_up(project_id)
    version = readmodel.model_version(pool, project_id, version_id)
    if version is None:
        raise Rejection(
            "MODEL_VERSION_NOT_FOUND",
            f"no projected model version {version_id} in project {project_id!r}",
        )
    if as_of_seq is None:
        as_of_seq = latest_recorded_as_of(pool, project_id, version_id)
    if as_of_seq is None:
        as_of_seq = latest_seq(pool, project_id)
    results = recorded(pool, project_id, version_id, as_of_seq)
    reasons: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for entry in catalog.checks:
        result = results.get(entry["id"])
        finding = {"check_id": entry["id"], "severity": entry["severity"]}
        if result is None:
            if entry["severity"] == "critical":
                reasons.append(
                    finding | {"status": "not_evaluated", "reason": "no recorded result"}
                )
            continue
        finding |= {"status": result["status"], "element_refs": result["element_refs"]}
        if result["status"] not in ("fail", "error"):
            continue
        if entry["severity"] == "critical":
            why = "cannot verify" if result["status"] == "error" else "violated"
            reasons.append(finding | {"reason": why})
        else:
            warnings.append(finding)
    element_ids = set(index_elements(version["model"]))
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT objection_id, objection FROM proj_objections "
            "WHERE project_id = %s AND severity = 'critical' AND raised_seq <= %s "
            "AND (resolved_seq IS NULL OR resolved_seq > %s) ORDER BY raised_seq",
            (project_id, as_of_seq, as_of_seq),
        ).fetchall()
    for row in rows:
        refs = set(row["objection"].get("element_refs", []))
        if refs & element_ids or version_id in refs:
            reasons.append(
                {
                    "objection_id": row["objection_id"],
                    "severity": "critical",
                    "status": "open",
                    "reason": "open critical objection",
                    "element_refs": sorted(refs & element_ids),
                }
            )
    return {
        "model_version": version_id,
        "as_of_seq": as_of_seq,
        "verdict": "ALLOWED" if not reasons else "BLOCKED",
        "reasons": reasons,
        "warnings": warnings,
    }
