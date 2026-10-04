"""The scorecard of one golden run: built from the ledger, the session package and the
task's expected.yaml, validated against goldens/scorecard.schema.json on write.

Pure functions over plain data, except `write`. A scorecard holds ids, counts, verdicts and
spend: never a key, a prompt or a model's text.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import best_match

from architect.golden.tasks import GoldenTask, goldens_dir

FAILING = ("fail", "error")


def results_by_version(events: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    """model version -> check id -> the latest check.result payload recorded for it."""
    out: dict[str, dict[str, dict[str, Any]]] = {}
    for event in events:
        if event["type"] == "check.result":
            payload = event["payload"]
            out.setdefault(payload.get("model_version", ""), {})[payload["check_id"]] = payload
    return out


def score_flaws(
    planted: list[dict[str, Any]],
    rounds: list[dict[str, Any]],
    results: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    """For each planted flaw: the round whose battery caught it (its check failing on its
    element), and the round after whose repair the check no longer names the element."""
    scored = []
    for flaw in planted:
        caught_in = repaired_in = None
        for round_ in rounds:
            number = round_["round"]
            for stage in ("attack", "verify"):
                version = (round_.get(stage) or {}).get("version")
                result = results.get(version or "", {}).get(flaw["check"])
                if result is None:
                    continue
                names_it = result["status"] == "fail" and flaw["element"] in result["element_refs"]
                if caught_in is None and names_it:
                    caught_in = number
                elif caught_in is not None and stage == "verify" and not names_it:
                    if result["status"] in ("pass", "fail", "skipped") and repaired_in is None:
                        repaired_in = number
            if repaired_in is not None:
                break
        scored.append(
            {
                "id": flaw["id"],
                "check": flaw["check"],
                "element": flaw["element"],
                "caught": caught_in is not None,
                "caught_in_round": caught_in,
                "repaired": repaired_in is not None,
                "repaired_in_round": repaired_in,
            }
        )
    return scored


def unexpected_on_seed(
    planted: list[dict[str, Any]],
    rounds: list[dict[str, Any]],
    results: dict[str, dict[str, dict[str, Any]]],
) -> list[str]:
    """Checks that failed or errored on the first attacked version besides the planted ones."""
    if not rounds:
        return []
    version = (rounds[0].get("attack") or {}).get("version")
    expected = {(f["check"], f["element"]) for f in planted}
    out = []
    for check_id, result in sorted(results.get(version or "", {}).items()):
        if result["status"] not in FAILING:
            continue
        for element in result["element_refs"] or ["-"]:
            if (check_id, element) not in expected:
                out.append(f"{check_id}:{element}")
    return out


def build(
    *,
    task: GoldenTask,
    mode: str,
    provider: str,
    session_id: str,
    project_id: str,
    started_at: str,
    duration_s: float,
    events: list[dict[str, Any]],
    package: dict[str, Any] | None,
    final: dict[str, Any],
    status_at_gate: str | None,
    closed_with: str | None,
    kill: dict[str, Any],
    usd_cap: float | None,
    final_model: dict[str, Any] | None,
) -> dict[str, Any]:
    expected = task.expected
    rounds = (package or {}).get("rounds", [])
    results = results_by_version(events)
    planted = expected.get("planted_flaws", []) if mode == "review" else []
    flaws = score_flaws(planted, rounds, results)
    risks = [r["id"] for r in (package or {}).get("open_risks", [])]
    risk_by_id = {r["id"]: r for r in (package or {}).get("open_risks", [])}

    best = final.get("best_version")
    c001 = results.get(best or "", {}).get("C-001")
    uncovered = list(c001["element_refs"]) if c001 and c001["status"] in FAILING else []
    listed = set((risk_by_id.get("check-fail:C-001") or {}).get("element_refs", []))
    coverage = {
        "check": "C-001",
        "status": c001["status"] if c001 else None,
        "requirements": list((c001 or {}).get("evidence", {}).get("requirements", [])),
        "uncovered": uncovered,
        "uncovered_listed_as_risks": set(uncovered) <= listed,
    }

    linter_found = [r for r in risks if r.startswith("requirement-unmeasurable:")]
    linter_expected = list(expected.get("linter", {}).get("expected_risks", []))
    seqs = [e["seq"] for e in events]
    keys = [e["idempotency_key"] for e in events]
    seq_dense = seqs == list(range(len(seqs)))
    duplicates = len(keys) - len(set(keys))
    kill_resume = {
        "performed": bool(kill.get("performed")),
        "after": kill.get("after"),
        "clean": (
            seq_dense and duplicates == 0 and kill.get("worker_exit_code") is not None
            if kill.get("performed")
            else None
        ),
        "worker_exit_code": kill.get("worker_exit_code"),
        "duplicate_idempotency_keys": duplicates,
        "seq_dense": seq_dense,
    }

    spend = (package or {}).get("spend") or final.get("spend") or {}
    usd = float(spend.get("usd", 0.0))
    has_model = bool(final_model and any(final_model.get("elements", {}).values()))
    under_cap = usd_cap is None or usd <= usd_cap
    if mode == "review":
        by_round = int(expected.get("thresholds", {}).get("review", {}).get("caught_by_round", 1))
        criteria = {
            f"all_planted_flaws_caught_by_round_{by_round}": bool(flaws)
            and all(f["caught"] and f["caught_in_round"] <= by_round for f in flaws),
            "final_model_exists": has_model,
            "package_exists": package is not None,
            "under_usd_cap": under_cap,
        }
        if kill_resume["performed"]:
            criteria["kill_resume_clean"] = bool(kill_resume["clean"])
    else:
        criteria = {
            "model_produced": has_model,
            "requirements_covered_or_listed_as_risks": (
                coverage["status"] in ("pass", "skipped") or coverage["uncovered_listed_as_risks"]
            )
            and c001 is not None,
            "package_exists": package is not None,
            "under_usd_cap": under_cap,
        }
    gate = (package or {}).get("gate") or {}
    return {
        "task": task.task_id,
        "mode": mode,
        "provider": provider,
        "session_id": session_id,
        "project_id": project_id,
        "started_at": started_at,
        "duration_s": round(duration_s, 1),
        "planted_flaws": flaws,
        "flaws_caught": sum(1 for f in flaws if f["caught"]),
        "flaws_repaired": sum(1 for f in flaws if f["repaired"]),
        "unexpected_failures_on_seed": (
            unexpected_on_seed(planted, rounds, results) if mode == "review" else []
        ),
        "outcome": final.get("outcome"),
        "status_at_gate": status_at_gate,
        "final_status": final.get("status") or "unknown",
        "closed_with": closed_with,
        "failure": final.get("failure"),
        "gate": {"verdict": gate.get("verdict"), "reasons": list(gate.get("reasons", []))},
        "requirement_coverage": coverage,
        "linter": {
            "risks_found": linter_found,
            "expected_risks": linter_expected,
            "expected_found": set(linter_expected) <= set(linter_found),
        },
        "kill_resume": kill_resume,
        "kill_mode": kill.get("mode") if kill.get("performed") else None,
        "events_at_kill": kill.get("events_at_kill"),
        "rounds": len(rounds),
        "final_model_version": best,
        "package_key": final.get("package_key"),
        "open_risks": risks,
        "tokens": int(spend.get("tokens", 0)),
        "usd": round(usd, 6),
        "usd_cap": usd_cap,
        "criteria": criteria,
        "passed": all(criteria.values()),
    }


def schema() -> dict[str, Any]:
    return json.loads((goldens_dir() / "scorecard.schema.json").read_text(encoding="utf-8"))


def validate(scorecard: dict[str, Any]) -> None:
    """ValueError naming the first violation of goldens/scorecard.schema.json."""
    validator = Draft202012Validator(schema(), format_checker=FormatChecker())
    error = best_match(validator.iter_errors(scorecard))
    if error is not None:
        where = "/".join(str(p) for p in error.absolute_path) or "(root)"
        raise ValueError(f"scorecard invalid at {where}: {error.message}")


def write(scorecard: dict[str, Any], path: Path) -> Path:
    validate(scorecard)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(scorecard, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
    return path


def format_scorecard(card: dict[str, Any]) -> str:
    """The scorecard as a few readable lines."""
    lines = [
        f"golden {card['task']}  mode {card['mode']}  provider {card['provider']}  "
        f"{'PASSED' if card['passed'] else 'FAILED'}",
        f"session {card['session_id']} in {card['project_id']}",
        f"outcome {card['outcome']}  status {card['final_status']}  "
        f"gate {card['gate']['verdict']}  rounds {card['rounds']}  "
        f"{card['duration_s']:.0f}s  {card['tokens']:,} tokens  ${card['usd']:.4f}"
        + (f" of ${card['usd_cap']:.2f}" if card["usd_cap"] is not None else ""),
    ]
    for flaw in card["planted_flaws"]:
        caught = f"caught in round {flaw['caught_in_round']}" if flaw["caught"] else "NOT caught"
        repaired = (
            f"repaired in round {flaw['repaired_in_round']}" if flaw["repaired"] else "not repaired"
        )
        lines.append(f"  {flaw['id']} {flaw['check']} on {flaw['element']}: {caught}, {repaired}")
    if card["mode"] == "review":
        lines.append(
            f"planted flaws caught {card['flaws_caught']}/{len(card['planted_flaws'])}, "
            f"repaired {card['flaws_repaired']}/{len(card['planted_flaws'])} (reported, not gated)"
        )
        if card.get("unexpected_failures_on_seed"):
            lines.append(
                "  also failing on the seed: " + ", ".join(card["unexpected_failures_on_seed"])
            )
    coverage = card["requirement_coverage"]
    lines.append(
        f"requirement coverage C-001: {coverage['status']}"
        + (f", uncovered {coverage['uncovered']}" if coverage["uncovered"] else "")
    )
    lines.append(
        f"linter risks: {card['linter']['risks_found']} "
        f"(expected found: {card['linter']['expected_found']})"
    )
    kill = card["kill_resume"]
    if kill["performed"]:
        lines.append(
            f"kill/resume after {kill['after']} ({card.get('kill_mode')} kill): "
            f"clean {kill['clean']} "
            f"(exit code {kill['worker_exit_code']}, duplicate keys "
            f"{kill['duplicate_idempotency_keys']}, seq dense {kill['seq_dense']})"
        )
    for reason in card["gate"]["reasons"]:
        subject = reason.get("check_id") or reason.get("objection_id")
        lines.append(f"  blocking: {subject} {reason.get('status')}")
    for name, ok in card["criteria"].items():
        lines.append(f"  [{'x' if ok else ' '}] {name}")
    if card.get("failure"):
        lines.append(f"failure: {card['failure']}")
    return "\n".join(lines)
