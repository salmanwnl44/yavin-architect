"""The console: what a session looks like right now, and why an element is in the model.

`snapshot` reads the session read model and the projections into plain data. `render` and
`render_text` are PURE functions of that snapshot: no database, no clock (the snapshot
carries its own `as_of`), so a frame can be tested by rendering it to a string. `watch`
(in the CLI) is the only loop.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from typing import Any

from psycopg_pool import ConnectionPool
from rich.console import Console, Group, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from architect.checks import runner
from architect.ingestion.objectstore import ObjectStore
from architect.sessions import service

FINAL_STATUSES = ("approved", "approved_with_risks", "rejected", "cancelled", "failed")
_ENVELOPE = frozenset(
    {
        "model_version",
        "as_of_seq",
        "catalog_version",
        "check_version",
        "params",
        "inputs_hash",
        "waived",
        "limitations",
    }
)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def evidence_line(evidence: dict[str, Any] | None, limit: int = 110) -> str:
    """A check's evidence on one line: what it found, without the run's envelope."""
    core = {
        k: v for k, v in (evidence or {}).items() if k not in _ENVELOPE and v not in ({}, [], None)
    }
    text = json.dumps(core, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def message_line(message: dict[str, Any], limit: int = 90) -> str:
    """An agent message on one line."""
    body, kind = message.get("body") or {}, message.get("type")
    if kind == "Task":
        text = str(body.get("goal", ""))
    elif kind == "ClaimProposal":
        claim = body.get("claim", {})
        subject = claim.get("subject", {})
        text = f"{subject.get('id', subject.get('literal', '?'))} {claim.get('predicate', '')}"
        if claim.get("magnitude"):
            text += f" {claim['magnitude']['value']:g} {claim['magnitude']['unit']}"
    elif kind == "ModelPatchProposal":
        text = f"{len(body.get('ops', []))} ops: {body.get('rationale', '')}"
    elif kind == "Question":
        text = str(body.get("question", ""))
    else:
        text = json.dumps(body, sort_keys=True)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


# ---------------------------------------------------------------- the snapshot
def snapshot(
    pool: ConnectionPool, store: ObjectStore, project_id: str, session_id: str
) -> dict[str, Any] | None:
    """Everything one frame shows, as plain data. None when the session is unknown."""
    row = service.session_row(pool, project_id, session_id)
    if row is None:
        return None
    package = service.load_package(store, row["package_key"]) if row.get("package_key") else None
    gate = None
    failing: list[dict[str, Any]] = []
    best = row.get("best_version")
    if best:
        gate = runner.gate(pool, project_id, best)
        recorded = runner.recorded(pool, project_id, best, gate["as_of_seq"])
        for finding in [*gate["reasons"], *gate["warnings"]]:
            check_id = finding.get("check_id")
            if check_id is None:
                continue
            result = recorded.get(check_id, {})
            failing.append(
                {
                    "check_id": check_id,
                    "severity": finding.get("severity"),
                    "status": finding.get("status"),
                    "element_refs": finding.get("element_refs", []),
                    "evidence": evidence_line(result.get("evidence")),
                }
            )
    with pool.connection() as conn:
        messages = conn.execute(
            "SELECT type, body FROM ag_messages WHERE project_id = %s AND session_id = %s "
            "ORDER BY n DESC LIMIT 5",
            (project_id, session_id),
        ).fetchall()
    return {
        "as_of": _now(),
        "session": row,
        "timeline": service.timeline(pool, project_id, session_id),
        "gate": gate,
        "failing_checks": failing,
        "open_risks": [r["id"] for r in package["open_risks"]] if package else row["open_risks"],
        "messages": [{"type": m["type"], "summary": message_line(m)} for m in reversed(messages)],
    }


# ---------------------------------------------------------------- the pure rendering
def phase_rows(snapshot: dict[str, Any]) -> list[tuple[str, str, str]]:
    """(phase, started, duration) per phase change; the last one runs until `as_of` unless
    the session is final or waiting for a human."""
    changes = [s for s in snapshot["timeline"] if s["kind"] == "phase_changed"]
    status = snapshot["session"]["status"]
    still_running = status in ("running", "paused")
    rows: list[tuple[str, str, str]] = []
    for i, step in enumerate(changes):
        started = _parse(step["ts"])
        if i + 1 < len(changes):
            ended: datetime | None = _parse(changes[i + 1]["ts"])
        elif still_running:
            ended = _parse(snapshot["as_of"])
        else:
            ended = None
        duration = "-" if ended is None else _duration((ended - started).total_seconds())
        rows.append((step["phase"], started.strftime("%H:%M:%S"), duration))
    return rows


def _duration(seconds: float) -> str:
    seconds = max(0.0, seconds)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, rest = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{rest:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _against(spent: float, cap: float | None, unit: str = "") -> str:
    shown = f"{spent:,.4f}" if isinstance(spent, float) and unit == "$" else f"{spent:,}"
    if cap is None:
        return f"{unit}{shown} / uncapped"
    cap_shown = f"{cap:,.2f}" if unit == "$" else f"{int(cap):,}"
    percent = 0.0 if not cap else 100.0 * float(spent) / float(cap)
    return f"{unit}{shown} / {unit}{cap_shown} ({percent:.0f}%)"


def render(snapshot: dict[str, Any]) -> RenderableType:
    """One frame. A pure function of the snapshot."""
    row = snapshot["session"]
    limits, spend = row.get("limits") or {}, row.get("spend") or {}
    header = Text()
    header.append(f"{row['session_id']}  ", style="bold")
    header.append(f"status {row['status']}")
    header.append(f"  outcome {row.get('outcome') or '-'}")
    header.append(f"  preset {row['preset']}  round {row['round']}")
    header.append(f"  phase {row.get('phase') or '-'}")
    parts: list[RenderableType] = [Panel(header, title="session", title_align="left")]

    timeline = Table(title="phase timeline", title_justify="left", expand=False)
    for column in ("phase", "started", "duration"):
        timeline.add_column(column)
    for phase, started, duration in phase_rows(snapshot):
        timeline.add_row(phase, started, duration)
    parts.append(timeline)

    gate = snapshot.get("gate")
    verdict = gate["verdict"] if gate else "not computed"
    gate_lines = [f"gate IMPLEMENTATION_READY: {verdict}"]
    for reason in (gate or {}).get("reasons", []):
        subject = reason.get("check_id") or reason.get("objection_id")
        refs = ", ".join(reason.get("element_refs", [])) or "-"
        gate_lines.append(f"  blocking {subject} {reason.get('status')} on {refs}")
    parts.append(Text("\n".join(gate_lines)))

    checks = Table(title="failing checks", title_justify="left", expand=False)
    for column in ("check", "severity", "status", "elements", "evidence"):
        checks.add_column(column, overflow="fold")
    for check in snapshot.get("failing_checks", []):
        checks.add_row(
            check["check_id"],
            str(check.get("severity")),
            str(check.get("status")),
            ", ".join(check.get("element_refs", [])) or "-",
            check.get("evidence", ""),
        )
    if snapshot.get("failing_checks"):
        parts.append(checks)
    else:
        parts.append(Text("failing checks: none"))

    risks = snapshot.get("open_risks") or []
    parts.append(Text("open risks:\n" + ("\n".join(f"  {r}" for r in risks) or "  none")))

    budget = [
        "spend vs budget:",
        "  tokens " + _against(int(spend.get("tokens", 0)), limits.get("tokens")),
        "  usd    " + _against(float(spend.get("usd", 0.0)), limits.get("usd"), "$"),
    ]
    parts.append(Text("\n".join(budget)))

    messages = snapshot.get("messages") or []
    lines = ["last agent messages:"] + (
        [f"  {m['type']}: {m['summary']}" for m in messages] or ["  none yet"]
    )
    parts.append(Text("\n".join(lines)))

    if row.get("failure"):
        parts.append(Text(f"failure: {row['failure']}"))
    if row.get("last_refusal"):
        refusal = row["last_refusal"]
        parts.append(Text(f"last refused decision: {refusal['decision']}: {refusal['why']}"))
    if row["status"] == "awaiting_approval":
        if row.get("phase") != "package":
            options = "approve (continue) | reject"
        elif verdict == "ALLOWED":
            options = "approve | reject"
        elif row.get("outcome") in service.EXTENDABLE_OUTCOMES:
            options = "approve-with-risks --reason | extend | reject"
        else:
            options = "approve-with-risks --reason | reject"
        parts.append(Text(f"waiting for the owner: {options}"))
    return Group(*parts)


def render_text(snapshot: dict[str, Any], width: int = 120) -> str:
    """The frame as plain text (what the tests assert on)."""
    buffer = io.StringIO()
    console = Console(
        file=buffer, width=width, force_terminal=False, color_system=None, legacy_windows=False
    )
    console.print(render(snapshot))
    return buffer.getvalue()


# ---------------------------------------------------------------- why
def format_why(trace: dict[str, Any]) -> list[str]:
    """The M2 why-trace as indented lines: element -> requirements -> ADRs -> evidence claims
    -> sources with their locators."""

    def claim_lines(claim: dict[str, Any], indent: str) -> list[str]:
        if "subject" not in claim:
            return [f"{indent}{claim['claim_id']} (not committed in this project)"]
        subject, obj = claim["subject"], claim["object"]
        line = (
            f"{indent}{claim['claim_id']} [{claim['status']}] "
            f"{subject.get('id', subject.get('literal'))} {claim['predicate']} "
            f"{obj.get('id', obj.get('literal'))}"
        )
        if claim.get("premise_compromised"):
            line += "  (a premise is refuted or retracted)"
        out = [line]
        for source in claim.get("sources", []):
            locator = f" @ {source['span']}" if source.get("span") else ""
            origin = f" [{source['taint_origin']}]" if source.get("taint_origin") else ""
            out.append(
                f"{indent}  source {source['source_id']}{locator}: "
                f"{source.get('uri', '(not ingested)')}{origin}"
            )
        return out

    element = trace["element"]
    name = element.get("name") or element.get("contract_ref") or ""
    lines = [
        f"{trace['element_id']} ({trace['element_type']}) {name}".rstrip()
        + f"  in {trace['model_version']}",
        "requirements it satisfies:",
    ]
    if not trace["requirements"]:
        lines.append("  none")
    for requirement in trace["requirements"]:
        lines.append(f"  {requirement['requirement']}")
        for claim in requirement["claims"] or []:
            lines += claim_lines(claim, "    ")
        if not requirement["claims"]:
            lines.append("    (no claim states this requirement)")
    lines.append("decisions that affect it:")
    if not trace["decisions"]:
        lines.append("  none")
    for decision in trace["decisions"]:
        lines.append(f"  {decision['adr_id']} {decision['title']}: {decision['choice']}")
        lines.append("    evidence:")
        for claim in decision["evidence_claims"]:
            lines += claim_lines(claim, "      ")
        if not decision["evidence_claims"]:
            lines.append("      none")
        if decision["assumptions"]:
            lines.append("    assumptions:")
            for claim in decision["assumptions"]:
                lines += claim_lines(claim, "      ")
    return lines
