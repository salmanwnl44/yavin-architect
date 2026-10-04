"""Every side effect of a design session, as Temporal activities.

Each activity is idempotent: Arbiter idempotency keys, version ids, claim ids, proposal ids,
task and message ids all derive from (session_id, phase, round, step), never from time or a
random id, so a retried or replayed activity writes nothing twice (A2). Model calls go
through the gateway under the scope {session, phase}; the gateway's cache answers a repeated
prompt without a second charge. Only this module (and the client-side service) touch the
database, the gateway, the Arbiter or the object store on behalf of a session.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from temporalio import activity
from temporalio.exceptions import ApplicationError

from architect import readmodel
from architect.arbiter import Arbiter, Commit
from architect.checks import runner
from architect.errors import Rejection
from architect.gateway.errors import (
    BudgetExceeded,
    NoEligibleModel,
    ReplayMiss,
    StructuredOutputInvalid,
)
from architect.gateway.gateway import Gateway
from architect.gateway.gateway import prompt_hash as gateway_prompt_hash
from architect.gateway.request import GatewayResponse
from architect.gateway.untrusted import with_rule
from architect.ingestion.config import IngestConfig, load_ingest_config
from architect.ingestion.normalize import slug, typed_id, whitespace
from architect.ingestion.objectstore import ObjectStore
from architect.ingestion.parse import parse_file
from architect.ingestion.sources import Ingestor
from architect.projector import Projector
from architect.sessions import agent, linter
from architect.sessions.compiler import CompileTask, compile, rank_claims
from architect.sessions.config import SessionConfig
from architect.sessions.types import spend_zero

SESSION_FAILURE = "SessionFailure"  # non-retryable: the session ends as `failed`
PIPELINE_VERSION = 1  # of the session's own claim pipeline (requirements, steers)

ACTIVITY_NAMES = (
    "session_start",
    "phase_changed",
    "frame",
    "research",
    "genesis",
    "seed",
    "draft",
    "attack",
    "repair",
    "steer",
    "checkpoint",
    "package",
    "sign_waivers",
    "extend_budget",
    "record_status",
)


def waiver_targets(blocking: list[dict[str, Any]]) -> list[str]:
    """What approve_with_risks waives: one target per (check, element) among the blocking
    reasons, "<check_id>:<element_id>", so the waiver covers those elements and no others. A
    reason that names no element (a check with no result, or one that could not say which
    element) is waived as the whole check; an objection by its id. In order, without repeats."""
    targets: list[str] = []
    for reason in blocking:
        check_id = reason.get("check_id")
        if check_id:
            elements = reason.get("element_refs") or []
            targets += [f"{check_id}:{element}" for element in elements] or [check_id]
        elif reason.get("objection_id"):
            targets.append(reason["objection_id"])
    return list(dict.fromkeys(targets))


def seed_ops(seed: Any) -> list[dict[str, Any]]:
    """A SystemModel as the patch that builds it on an empty model: one add_element per
    element, one add_link per link, in the seed's own order. The seed's version_id and
    project_id are ignored: the session's apply."""
    ops: list[dict[str, Any]] = []
    for element_type, elements in (seed.get("elements") or {}).items():
        for element in elements:
            ops.append({"op": "add_element", "element_type": element_type, "element": element})
    for link_type, links in (seed.get("links") or {}).items():
        for link in links:
            ops.append({"op": "add_link", "link_type": link_type, "link": link})
    return ops


class _BudgetStop(Exception):
    """The gateway refused a call over the session's cap: the session stops by rule (d)."""


class SessionActivities:
    def __init__(
        self,
        pool: ConnectionPool,
        gateway: Gateway,
        store: ObjectStore,
        config: SessionConfig,
        ingest_config: IngestConfig | None = None,
    ) -> None:
        self._pool = pool
        self._gateway = gateway
        self._store = store
        self._config = config
        self._ingest_config = ingest_config or load_ingest_config()
        self._arbiter = Arbiter(pool)
        # Test hooks: called with (name, args) before, and (name, args, result) after, every
        # activity. Raising from either fails that activity attempt.
        self.before_activity: Callable[[str, dict[str, Any]], None] | None = None
        self.after_activity: Callable[[str, dict[str, Any], dict[str, Any]], None] | None = None

    # ------------------------------------------------------------------ registration
    def all(self) -> list[Callable[..., Any]]:
        return [self._wrap(name, getattr(self, f"_{name}")) for name in ACTIVITY_NAMES]

    def _wrap(self, name: str, fn: Callable[[dict[str, Any]], dict[str, Any]]) -> Callable:
        def run(args: dict[str, Any]) -> dict[str, Any]:
            if self.before_activity is not None:
                self.before_activity(name, args)
            result = fn(args)
            if self.after_activity is not None:
                self.after_activity(name, args, result)
            return result

        run.__name__ = name
        run.__qualname__ = name
        return activity.defn(name=name)(run)

    # ------------------------------------------------------------------ helpers
    def _catch_up(self, project_id: str) -> None:
        Projector(self._pool).catch_up(project_id)

    def _spend(self, session_id: str) -> dict[str, Any]:
        row = self._gateway.spend({"session": session_id})
        if row is None:
            return spend_zero()
        return {"tokens": int(row["tokens"]), "usd": float(row["usd"])}

    def _remaining(self, args: dict[str, Any]) -> dict[str, Any]:
        """What the agent is told about the budget: the workflow's coarse buckets, which a
        live run and its replay share (exact spend is in the checkpoints and gw_spend)."""
        return dict(args.get("remaining_bucket", {}))

    def _event(
        self,
        project_id: str,
        session_id: str,
        type_: str,
        payload: dict[str, Any],
        key: str,
        *,
        actor: dict[str, Any] | None = None,
        task_id: str | None = None,
    ) -> Commit:
        candidate: dict[str, Any] = {
            "actor": actor or agent.ORCHESTRATOR,
            "session_id": session_id,
            "type": type_,
            "payload": payload,
            "idempotency_key": key,
        }
        if task_id is not None:
            candidate["task_id"] = task_id
        return self._arbiter.submit(project_id, candidate)

    def _call(
        self,
        args: dict[str, Any],
        purpose: str,
        phase: str,
        round_: int,
        messages: list[dict[str, str]],
        input_taints: list[str],
    ) -> tuple[GatewayResponse, str]:
        """The gateway's answer and the prompt hash it was recorded under (computed here, so a
        replay into another ledger carries the same provenance)."""
        request = agent.architect_request(
            purpose=purpose,
            round_=round_,
            session_id=args["session_id"],
            phase=phase,
            tier=self._config.architect_tier,
            max_tokens=self._config.architect_max_tokens,
            messages=messages,
            input_taints=input_taints,
        )
        digest = gateway_prompt_hash(
            with_rule(request.system, list(request.input_taints)),
            [m.model_dump() for m in request.messages],
            request.output_schema,
            request.temperature,
            request.max_tokens,
        )
        try:
            return self._gateway.call(request), digest
        except BudgetExceeded as refused:
            raise _BudgetStop(str(refused)) from refused
        except (NoEligibleModel, ReplayMiss) as fatal:
            raise ApplicationError(str(fatal), type=SESSION_FAILURE, non_retryable=True) from fatal

    def _update_row(self, project_id: str, session_id: str, **fields: Any) -> None:
        if not fields:
            return
        columns = ", ".join(f"{k} = %s" for k in fields)
        values = [Jsonb(v) if isinstance(v, (dict, list)) else v for v in fields.values()]
        with self._pool.connection() as conn:
            conn.execute(
                f"UPDATE ses_sessions SET {columns}, updated_at = now() "
                "WHERE project_id = %s AND session_id = %s",
                (*values, project_id, session_id),
            )

    def _commit_claim(
        self,
        project_id: str,
        session_id: str,
        claim: dict[str, Any],
        key: str,
        task_id: str | None = None,
    ) -> str:
        """claim.proposed (by the architect) then claim.committed (by the orchestrator).
        A claim the project already holds under the same id counts as committed."""
        proposal_id = f"prp-{claim['id'][4:]}"
        try:
            self._event(
                project_id,
                session_id,
                "claim.proposed",
                {"proposal_id": proposal_id, "claim": claim},
                f"{key}:proposed",
                actor=agent.ACTOR,
                task_id=task_id,
            )
            self._event(
                project_id,
                session_id,
                "claim.committed",
                {"claim_id": claim["id"], "claim": claim, "from_proposal": proposal_id},
                f"{key}:committed",
                task_id=task_id,
            )
        except Rejection as rejection:
            if rejection.code not in ("DUPLICATE_PROPOSAL", "DUPLICATE_CLAIM_ID"):
                raise
        return claim["id"]

    # ------------------------------------------------------------------ session_start
    def _session_start(self, args: dict[str, Any]) -> dict[str, Any]:
        project_id, session_id = args["project_id"], args["session_id"]
        ingestor = Ingestor(self._pool, self._store, self._ingest_config)
        brief = ingestor.ingest_text(
            project_id, args["brief"], uri=f"session://{session_id}/brief.md", origin="user"
        )
        self._catch_up(project_id)
        source_ids = [brief.source_id]
        for item in args.get("sources", []):
            if item.startswith("src_"):
                with self._pool.connection() as conn:
                    known = conn.execute(
                        "SELECT 1 FROM proj_sources WHERE project_id = %s AND source_id = %s",
                        (project_id, item),
                    ).fetchone()
                if known is None:
                    raise ApplicationError(
                        f"source {item} is not in project {project_id!r}",
                        type=SESSION_FAILURE,
                        non_retryable=True,
                    )
                source_ids.append(item)
            else:
                path = Path(item)
                if not path.is_file():
                    raise ApplicationError(
                        f"source path {item} does not exist",
                        type=SESSION_FAILURE,
                        non_retryable=True,
                    )
                source_ids.append(ingestor.ingest_file(project_id, path).source_id)
        limits = args["limits"]
        self._event(
            project_id,
            session_id,
            "budget.updated",
            {
                "scope": {"session": session_id},
                "limits": {
                    "tokens": limits.get("tokens"),
                    "usd": limits.get("usd"),
                    "wall_clock_minutes": limits.get("wall_clock_minutes"),
                },
            },
            f"session:{session_id}:budget",
        )
        with self._pool.connection() as conn:
            conn.execute(
                "INSERT INTO ses_sessions (project_id, session_id, preset, limits, status, "
                "phase, brief_source_id, started_at) VALUES (%s, %s, %s, %s, 'running', NULL, "
                "%s, %s) ON CONFLICT (project_id, session_id) DO NOTHING",
                (
                    project_id,
                    session_id,
                    args["preset"],
                    Jsonb(limits),
                    brief.source_id,
                    args["started_at"],
                ),
            )
        self._catch_up(project_id)
        return {
            "brief_source_id": brief.source_id,
            "source_ids": source_ids,
            "spend": self._spend(session_id),
        }

    # ------------------------------------------------------------------ phase_changed
    def _phase_changed(self, args: dict[str, Any]) -> dict[str, Any]:
        project_id, session_id = args["project_id"], args["session_id"]
        payload: dict[str, Any] = {"session_id": session_id, "to": args["to"]}
        if args.get("from"):
            payload["from"] = args["from"]
        self._event(
            project_id,
            session_id,
            "session.phase_changed",
            payload,
            f"session:{session_id}:phase:{args['round']}:{args['to']}",
        )
        self._update_row(
            project_id, session_id, phase=args["to"], round=args["round"], status="running"
        )
        self._catch_up(project_id)
        return {}

    # ------------------------------------------------------------------ frame
    def _frame(self, args: dict[str, Any]) -> dict[str, Any]:
        project_id, session_id = args["project_id"], args["session_id"]
        brief: str = args["brief"]
        brief_source = args["brief_source_id"]
        ranked = rank_claims(self._pool, project_id, brief)
        compiled = compile(
            self._pool,
            CompileTask(
                project_id=project_id,
                session_id=session_id,
                goal="Frame the brief: list its requirements, constraints and unknowns.",
                phase="frame",
                round=0,
                token_target=self._config.token_target,
                brief=brief,
                brief_source_id=brief_source,
                research_claim_ids=[row["claim_id"] for row in ranked],
                remaining_budget=self._remaining(args),
            ),
        )
        messages = [{"role": "user", "content": compiled.text}]
        try:
            response, prompt_hash = self._call(
                args, "frame", "frame", 0, messages, compiled.input_taints
            )
        except _BudgetStop as stop:
            return {"stopped": "budget", "detail": str(stop), "spend": self._spend(session_id)}
        except StructuredOutputInvalid as invalid:
            raise ApplicationError(
                f"the architect produced no valid frame: {invalid}",
                type=SESSION_FAILURE,
                non_retryable=True,
            ) from invalid
        tid = agent.task_id(session_id, "frame", 0, "frame")
        ts = args["started_at"]
        self._record_task(args, compiled, tid, "frame", 0, response, ts)
        parsed = response.parsed
        segments = parse_file(brief_source, brief.encode("utf-8"), "text/markdown", "brief.md")

        def locator_of(quote: str | None) -> str:
            if quote:
                needle = whitespace(quote)
                for segment in segments:
                    if needle and needle in whitespace(segment.text):
                        return segment.locator
            return "brief.md"

        def base_claim(subject: dict[str, Any], predicate: str, obj: dict[str, Any], span: str):
            return {
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "status": "documented",
                "evidence": [{"source": brief_source, "span": span, "kind": "statement"}],
                "taint": {"origin": "user"},
                "recorded_at": ts,
                "provenance": {
                    "extractor": {
                        "model_tier": self._config.architect_tier,
                        "prompt_hash": prompt_hash,
                        "pipeline_version": PIPELINE_VERSION,
                    }
                },
            }

        requirements: list[dict[str, Any]] = []
        risks: list[dict[str, Any]] = []
        labels = linter.labelled_requirements(brief)
        for n, requirement in enumerate(parsed["requirements"]):
            label = linter.label_for(requirement, labels)
            if label is not None:  # the brief's own id for this requirement wins
                requirement = {**requirement, "slug": label}
            result = linter.lint(requirement)
            if result.measurable:
                obj = {"entity_type": "metric", "id": slug(result.metric or "metric")}
            else:
                obj = {"entity_type": "text", "literal": result.text or result.requirement_id}
            claim = base_claim(
                {"entity_type": "requirement", "id": result.requirement_id},
                "CONSTRAINS",
                obj,
                locator_of(requirement.get("quote")),
            )
            magnitude = ""
            if result.measurable:
                claim["magnitude"] = {"value": result.target, "unit": result.unit}
                magnitude = f"{result.target}|{result.unit}"
            claim["id"] = typed_id("clm", brief_source, result.requirement_id, magnitude)
            self._commit_claim(
                project_id, session_id, claim, f"session:{session_id}:frame:req:{n}", tid
            )
            self._record_claim_proposal(args, tid, f"req-{n}", claim, response, ts)
            requirements.append(
                {
                    "id": result.requirement_id,
                    "claim_id": claim["id"],
                    "measurable": result.measurable,
                    "text": result.text,
                    "reason": result.reason,
                }
            )
            if not result.measurable:
                risks.append(
                    {
                        "id": f"requirement-unmeasurable:{result.requirement_id}",
                        "kind": "requirement_unmeasurable",
                        "detail": result.reason,
                        "claim_id": claim["id"],
                    }
                )
        constraints: list[dict[str, Any]] = []
        for n, constraint in enumerate(parsed.get("constraints", [])):
            cid = f"con_{slug(constraint.get('slug') or constraint['text'])}"
            claim = base_claim(
                {"entity_type": "constraint", "id": cid},
                "CONSTRAINS",
                {"entity_type": "text", "literal": constraint["text"]},
                locator_of(constraint.get("quote")),
            )
            claim["id"] = typed_id("clm", brief_source, cid)
            self._commit_claim(
                project_id, session_id, claim, f"session:{session_id}:frame:con:{n}", tid
            )
            self._record_claim_proposal(args, tid, f"con-{n}", claim, response, ts)
            constraints.append({"id": cid, "claim_id": claim["id"], "text": constraint["text"]})
        for unknown in parsed.get("unknowns", []):
            risks.append(
                {
                    "id": f"unknown:{slug(unknown.get('slug') or unknown['text'])}",
                    "kind": "unknown",
                    "detail": unknown["text"],
                }
            )
        self._catch_up(project_id)
        return {
            "requirements": requirements,
            "constraints": constraints,
            "open_risks": risks,
            "call_ids": [response.call_id],
            "spend": self._spend(session_id),
        }

    def _record_task(
        self,
        args: dict[str, Any],
        compiled: Any,
        tid: str,
        phase: str,
        round_: int,
        response: GatewayResponse,
        ts: str,
        step: str = "task",
    ) -> str:
        limits = args.get("limits", {})
        body = {
            "goal": compiled.text.split("\n", 2)[1] if "\n" in compiled.text else "task",
            "scope_element_ids": compiled.scope_element_ids,
            "budget": {
                "tokens": limits.get("tokens"),
                "usd": limits.get("usd"),
                "wall_clock_minutes": limits.get("wall_clock_minutes"),
            },
            "context_manifest": compiled.manifest,
        }
        msg = agent.message(
            session_id=args["session_id"],
            task_id_=tid,
            msg_id_=agent.msg_id(args["session_id"], phase, round_, step, "Task"),
            type_="Task",
            body=body,
            agent={"role": "system", "model_tier": "system"},
            ts=ts,
            depends_on=compiled.depends_on,
        )
        agent.record_message(
            self._pool,
            args["project_id"],
            msg,
            call_ids=[response.call_id],
            context_manifest=compiled.manifest,
            context_dropped=compiled.dropped,
        )
        return msg["msg_id"]

    def _record_claim_proposal(
        self,
        args: dict[str, Any],
        tid: str,
        step: str,
        claim: dict[str, Any],
        response: GatewayResponse,
        ts: str,
    ) -> None:
        msg = agent.message(
            session_id=args["session_id"],
            task_id_=tid,
            msg_id_=agent.msg_id(args["session_id"], "frame", 0, step, "ClaimProposal"),
            type_="ClaimProposal",
            body={"claim": claim},
            agent={
                "role": "architect",
                "model_tier": self._config.architect_tier,
                "model_family": response.family,
            },
            ts=ts,
            cost={
                "tokens_in": response.tokens_in,
                "tokens_out": response.tokens_out,
                "usd": response.usd,
            },
        )
        agent.record_message(self._pool, args["project_id"], msg, call_ids=[response.call_id])

    # ------------------------------------------------------------------ research
    def _research(self, args: dict[str, Any]) -> dict[str, Any]:
        self._catch_up(args["project_id"])
        ranked = rank_claims(self._pool, args["project_id"], args["brief"])
        return {
            "claim_ids": [row["claim_id"] for row in ranked],
            "scores": {row["claim_id"]: row["score"] for row in ranked},
        }

    # ------------------------------------------------------------------ genesis
    def _genesis(self, args: dict[str, Any]) -> dict[str, Any]:
        project_id, session_id = args["project_id"], args["session_id"]
        self._catch_up(project_id)
        head = readmodel.head_model(self._pool, project_id)
        version_id = typed_id("mv", session_id, "genesis")
        payload: dict[str, Any] = {"version_id": version_id}
        if head is not None:
            payload["parent"] = head["version_id"]
        self._event(
            project_id,
            session_id,
            "model.version_created",
            payload,
            f"session:{session_id}:model:genesis",
        )
        self._catch_up(project_id)
        return {"version_id": version_id, "parent": payload.get("parent")}

    # ------------------------------------------------------------------ seed (review mode)
    def _seed(self, args: dict[str, Any]) -> dict[str, Any]:
        """Commit the seed model as the first patch after genesis, through the Arbiter like
        any other patch. A seed the Arbiter refuses ends the session as failed, with the
        typed rejection as the reason."""
        project_id, session_id = args["project_id"], args["session_id"]
        head: str = args["head_version"]
        seed = args["seed"]
        version_id = typed_id("mv", session_id, "seed")
        try:
            if not isinstance(seed, dict) or not isinstance(seed.get("elements", {}), dict):
                raise Rejection("SCHEMA_INVALID", "the seed is not a SystemModel object", "$")
            if not isinstance(seed.get("links", {}), dict):
                raise Rejection("SCHEMA_INVALID", "the seed's links are not an object", "$.links")
            body = {
                "base_version": head,
                "ops": seed_ops(seed),
                "rationale": "seed model supplied at session start (review mode)",
            }
            proposal_id = f"prp-{session_id}-seed"
            self._event(
                project_id,
                session_id,
                "model.patch_proposed",
                {"proposal_id": proposal_id, "base_version": head, "patch": body},
                f"session:{session_id}:seed:proposed",
            )
            self._event(
                project_id,
                session_id,
                "model.patch_committed",
                {
                    "version_id": version_id,
                    "base_version": head,
                    "patch": body,
                    "from_proposal": proposal_id,
                },
                f"session:{session_id}:seed:committed",
            )
        except Rejection as rejection:
            raise ApplicationError(
                "the seed model was refused: " + json.dumps(rejection.body(), sort_keys=True),
                type=SESSION_FAILURE,
                non_retryable=True,
            ) from rejection
        self._catch_up(project_id)
        return {"version_id": version_id, "ops": len(body["ops"])}

    # ------------------------------------------------------------------ draft / repair
    def _draft(self, args: dict[str, Any]) -> dict[str, Any]:
        return self._patch_phase("draft", "draft", args)

    def _repair(self, args: dict[str, Any]) -> dict[str, Any]:
        return self._patch_phase("repair", "repair", args)

    def _patch_phase(self, purpose: str, phase: str, args: dict[str, Any]) -> dict[str, Any]:
        project_id, session_id = args["project_id"], args["session_id"]
        round_: int = args["round"]
        head: str = args["head_version"]
        ts = args["started_at"]
        self._catch_up(project_id)
        goal = (
            "Draft the System Model that satisfies every requirement."
            if purpose == "draft"
            else "Repair the head model so the failing checks pass, or request waivers."
        )
        compiled = compile(
            self._pool,
            CompileTask(
                project_id=project_id,
                session_id=session_id,
                goal=goal,
                phase=phase,
                round=round_,
                token_target=self._config.token_target,
                head_version=head,
                research_claim_ids=args.get("research_claim_ids", []),
                failing_checks=args.get("failing", []),
                remaining_budget=self._remaining(args),
            ),
        )
        conversation = [{"role": "user", "content": compiled.text}]
        version_id = typed_id("mv", session_id, phase, str(round_))
        risks: list[dict[str, Any]] = []
        waivers: list[dict[str, Any]] = []
        adrs: list[str] = []
        call_ids: list[str] = []
        rejections: list[dict[str, Any]] = []
        attempts = 1 + self._config.rejection_retries
        for attempt in range(1, attempts + 1):
            try:
                response, _ = self._call(
                    args, purpose, phase, round_, conversation, compiled.input_taints
                )
            except _BudgetStop as stop:
                return {
                    "stopped": "budget",
                    "detail": str(stop),
                    "version_id": None,
                    "open_risks": risks,
                    "waiver_requests": waivers,
                    "adrs": adrs,
                    "spend": self._spend(session_id),
                }
            except StructuredOutputInvalid as invalid:
                rejections.append({"code": "INVALID_OUTPUT", "detail": str(invalid)})
                break
            call_ids.append(response.call_id)
            step = f"attempt-{attempt}"
            tid = agent.task_id(session_id, phase, round_, step)
            self._record_task(args, compiled, tid, phase, round_, response, ts, step)
            parsed = response.parsed
            waivers += self._record_waivers(args, tid, phase, round_, attempt, parsed, response, ts)
            self._record_questions(args, tid, phase, round_, attempt, parsed, response, ts)
            ops = parsed.get("ops", [])
            if not ops:
                adrs += self._record_decisions(
                    args, compiled.claim_ids, tid, phase, round_, parsed.get("decisions", [])
                )
                for waiver in waivers:
                    risks.append(waiver_risk(waiver))
                self._catch_up(project_id)
                return {
                    "version_id": None,
                    "reason": "no_ops",
                    "open_risks": risks,
                    "waiver_requests": waivers,
                    "adrs": adrs,
                    "call_ids": call_ids,
                    "rejections": rejections,
                    "spend": self._spend(session_id),
                }
            body = {"base_version": head, "ops": ops, "rationale": parsed["rationale"]}
            problem = agent.patch_proposal_error(body)
            if problem is not None:
                rejection = Rejection("SCHEMA_INVALID", problem, "$.payload.patch")
            else:
                proposal_msg = agent.message(
                    session_id=session_id,
                    task_id_=tid,
                    msg_id_=agent.msg_id(session_id, phase, round_, step, "ModelPatchProposal"),
                    type_="ModelPatchProposal",
                    body=body,
                    agent={
                        "role": "architect",
                        "model_tier": self._config.architect_tier,
                        "model_family": response.family,
                    },
                    ts=ts,
                    cost={
                        "tokens_in": response.tokens_in,
                        "tokens_out": response.tokens_out,
                        "usd": response.usd,
                    },
                    depends_on=compiled.depends_on,
                )
                agent.record_message(
                    self._pool, project_id, proposal_msg, call_ids=[response.call_id]
                )
                proposal_id = f"prp-{session_id}-{phase}-{round_}-{attempt}"
                try:
                    self._event(
                        project_id,
                        session_id,
                        "model.patch_proposed",
                        {
                            "proposal_id": proposal_id,
                            "base_version": head,
                            "patch": body,
                            "proposer_msg_id": proposal_msg["msg_id"],
                        },
                        f"session:{session_id}:{phase}:{round_}:{attempt}:proposed",
                        actor=agent.ACTOR,
                        task_id=tid,
                    )
                    self._event(
                        project_id,
                        session_id,
                        "model.patch_committed",
                        {
                            "version_id": version_id,
                            "base_version": head,
                            "patch": body,
                            "from_proposal": proposal_id,
                        },
                        f"session:{session_id}:{phase}:{round_}:committed",
                        task_id=tid,
                    )
                    rejection = None
                except Rejection as refused:
                    rejection = refused
            if rejection is None:
                adrs += self._record_decisions(
                    args, compiled.claim_ids, tid, phase, round_, parsed.get("decisions", [])
                )
                for waiver in waivers:
                    risks.append(waiver_risk(waiver))
                self._catch_up(project_id)
                return {
                    "version_id": version_id,
                    "attempts": attempt,
                    "open_risks": risks,
                    "waiver_requests": waivers,
                    "adrs": adrs,
                    "call_ids": call_ids,
                    "rejections": rejections,
                    "spend": self._spend(session_id),
                }
            rejections.append(rejection.body())
            conversation = conversation + [
                {"role": "assistant", "content": response.text},
                {"role": "user", "content": agent.rejection_feedback(rejection)},
            ]
        risks.append(
            {
                "id": f"architect-could-not-produce-valid-patch:{phase}:{round_}",
                "kind": "architect_failed",
                "detail": rejections[-1] if rejections else {},
                "attempts": len(rejections),
            }
        )
        for waiver in waivers:
            risks.append(waiver_risk(waiver))
        self._catch_up(project_id)
        return {
            "version_id": None,
            "reason": "rejected",
            "open_risks": risks,
            "waiver_requests": waivers,
            "adrs": adrs,
            "call_ids": call_ids,
            "rejections": rejections,
            "spend": self._spend(session_id),
        }

    def _record_waivers(
        self,
        args: dict[str, Any],
        tid: str,
        phase: str,
        round_: int,
        attempt: int,
        parsed: dict[str, Any],
        response: GatewayResponse,
        ts: str,
    ) -> list[dict[str, Any]]:
        """A5: the architect only REQUESTS a waiver, as a Question for the owner."""
        out: list[dict[str, Any]] = []
        for n, waiver in enumerate(parsed.get("waiver_requests", [])):
            msg = agent.message(
                session_id=args["session_id"],
                task_id_=tid,
                msg_id_=agent.msg_id(
                    args["session_id"], phase, round_, f"attempt-{attempt}-waiver-{n}", "Question"
                ),
                type_="Question",
                body={
                    "blocking": False,
                    "question": (
                        f"Waiver requested for {waiver['check_id']} on {waiver['element_id']}: "
                        f"{waiver['risk']}"
                    ),
                    "options_considered": ["patch the model", "sign a waiver (human only)"],
                },
                agent={"role": "architect", "model_tier": self._config.architect_tier},
                ts=ts,
            )
            agent.record_message(self._pool, args["project_id"], msg, call_ids=[response.call_id])
            out.append(dict(waiver, msg_id=msg["msg_id"], round=round_))
        return out

    def _record_questions(
        self,
        args: dict[str, Any],
        tid: str,
        phase: str,
        round_: int,
        attempt: int,
        parsed: dict[str, Any],
        response: GatewayResponse,
        ts: str,
    ) -> None:
        for n, question in enumerate(parsed.get("questions", [])):
            msg = agent.message(
                session_id=args["session_id"],
                task_id_=tid,
                msg_id_=agent.msg_id(
                    args["session_id"], phase, round_, f"attempt-{attempt}-question-{n}", "Question"
                ),
                type_="Question",
                body={"blocking": False, **question},
                agent={"role": "architect", "model_tier": self._config.architect_tier},
                ts=ts,
            )
            agent.record_message(self._pool, args["project_id"], msg, call_ids=[response.call_id])

    def _record_decisions(
        self,
        args: dict[str, Any],
        context_claim_ids: list[str],
        tid: str,
        phase: str,
        round_: int,
        decisions: list[dict[str, Any]],
    ) -> list[str]:
        """decision.recorded for each ADR whose evidence is drawn from the compiled context;
        one citing anything else is not recorded (and the Arbiter would refuse an unknown
        claim anyway: UNKNOWN_CLAIM)."""
        allowed = set(context_claim_ids)
        recorded: list[str] = []
        for n, decision in enumerate(decisions):
            if not set(decision["evidence_claims"]) <= allowed:
                continue
            adr_id = typed_id("adr", args["session_id"], phase, str(round_), str(n))
            try:
                self._event(
                    args["project_id"],
                    args["session_id"],
                    "decision.recorded",
                    {"adr_id": adr_id, "decision": decision},
                    f"session:{args['session_id']}:{phase}:{round_}:adr:{n}",
                    actor=agent.ACTOR,
                    task_id=tid,
                )
            except Rejection:
                continue
            recorded.append(adr_id)
        return recorded

    # ------------------------------------------------------------------ attack / verify
    def _attack(self, args: dict[str, Any]) -> dict[str, Any]:
        project_id, version_id = args["project_id"], args["version_id"]
        report = runner.run(self._pool, project_id, version_id)
        verdict = runner.gate(self._pool, project_id, version_id, report.as_of_seq)
        failing: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []
        for result in report.results:
            result_id = runner._result_id(result.check_id, result.inputs_hash)
            summary = {
                "check_id": result.check_id,
                "severity": result.severity,
                "status": result.status,
                "element_refs": result.element_refs,
                "result_id": result_id,
                "event_id": result.event_id,
            }
            results.append(summary)
            if result.status in ("fail", "error") and result.severity in ("critical", "major"):
                evidence = {
                    k: v
                    for k, v in result.evidence.items()
                    if k
                    not in (
                        "model_version",
                        "as_of_seq",
                        "catalog_version",
                        "params",
                        "inputs_hash",
                        "check_version",
                    )
                }
                failing.append(summary | {"evidence": evidence})
        return {
            "version_id": version_id,
            "as_of_seq": report.as_of_seq,
            "gate": verdict,
            "blocking": len(verdict["reasons"]),
            "failing_count": sum(1 for r in report.results if r.status in ("fail", "error")),
            "failing": failing,
            "results": results,
        }

    # ------------------------------------------------------------------ steer
    def _steer(self, args: dict[str, Any]) -> dict[str, Any]:
        project_id, session_id, n = args["project_id"], args["session_id"], args["n"]
        text: str = args["text"]
        ingestor = Ingestor(self._pool, self._store, self._ingest_config)
        source = ingestor.ingest_text(
            project_id, text, uri=f"session://{session_id}/steer-{n}.md", origin="user"
        )
        self._catch_up(project_id)
        claim = {
            "id": typed_id("clm", session_id, "steer", str(n)),
            "subject": {"entity_type": "owner_guidance", "id": f"steer-{n}"},
            "predicate": "DIRECTS",
            "object": {"entity_type": "text", "literal": text},
            "status": "documented",
            "evidence": [{"source": source.source_id, "span": "steer", "kind": "statement"}],
            "taint": {"origin": "user"},
            "recorded_at": args["started_at"],
            "provenance": {
                "extractor": {"model_tier": "human", "prompt_hash": "steer", "pipeline_version": 1}
            },
        }
        self._commit_claim(project_id, session_id, claim, f"session:{session_id}:steer:{n}")
        self._catch_up(project_id)
        return {"claim_id": claim["id"], "source_id": source.source_id}

    # ------------------------------------------------------------------ checkpoint
    def _checkpoint(self, args: dict[str, Any]) -> dict[str, Any]:
        project_id, session_id = args["project_id"], args["session_id"]
        spend = self._spend(session_id)
        payload: dict[str, Any] = {
            "session_id": session_id,
            "phase": args["phase"],
            "open_risk_ids": list(dict.fromkeys(args.get("open_risk_ids", []))),
            "spend": {"tokens": spend["tokens"], "usd": spend["usd"]},
        }
        if args.get("best_version"):
            payload["best_version"] = args["best_version"]
        self._event(
            project_id,
            session_id,
            "session.checkpoint",
            payload,
            f"session:{session_id}:checkpoint:{args['key']}",
        )
        self._update_row(
            project_id,
            session_id,
            best_version=args.get("best_version"),
            open_risks=args.get("open_risk_ids", []),
            spend=spend,
        )
        self._catch_up(project_id)
        return {"spend": spend}

    # ------------------------------------------------------------------ package
    def _package(self, args: dict[str, Any]) -> dict[str, Any]:
        project_id, session_id = args["project_id"], args["session_id"]
        best: str | None = args.get("best_version")
        self._catch_up(project_id)
        risks: list[dict[str, Any]] = list(args.get("open_risks", []))
        gate = None
        results: list[dict[str, Any]] = []
        trace: list[dict[str, Any]] = []
        if best is not None:
            gate = runner.gate(self._pool, project_id, best)
            for row in runner.recorded(self._pool, project_id, best, gate["as_of_seq"]).values():
                results.append(
                    {
                        "check_id": row["check_id"],
                        "result_id": row["result_id"],
                        "status": row["status"],
                        "element_refs": row["element_refs"],
                        "evidence": row["evidence"],
                    }
                )
                if row["status"] in ("fail", "error"):
                    risks.append(
                        {
                            "id": f"check-{row['status']}:{row['check_id']}",
                            "kind": "check_failing",
                            "check_id": row["check_id"],
                            "status": row["status"],
                            "element_refs": row["element_refs"],
                            "evidence": row["evidence"],
                        }
                    )
            version = readmodel.model_version(self._pool, project_id, best)
            satisfies: dict[str, list[str]] = {}
            for link in (version or {}).get("model", {}).get("links", {}).get("satisfies", []):
                satisfies.setdefault(link["requirement"], []).append(link["component"])
            for row in readmodel.list_claims(self._pool, project_id):
                subject = row["claim"]["subject"]
                if subject.get("entity_type") == "requirement" and "id" in subject:
                    trace.append(
                        {
                            "requirement": subject["id"],
                            "claim_id": row["claim_id"],
                            "components": satisfies.get(subject["id"], []),
                        }
                    )
        stop_reason = args.get("stop_reason")
        if stop_reason == "budget":
            risks.append({"id": "budget-exhausted", "kind": "budget", "detail": args.get("detail")})
        if stop_reason == "time":
            risks.append(
                {"id": "wall-clock-exhausted", "kind": "time", "detail": args.get("detail")}
            )
        unique: dict[str, dict[str, Any]] = {}
        for risk in risks:
            unique.setdefault(risk["id"], risk)
        risks = list(unique.values())
        adrs: list[dict[str, Any]] = []
        if args.get("adrs"):
            with self._pool.connection() as conn:
                adrs = conn.execute(
                    "SELECT adr_id, seq, decision FROM proj_decisions "
                    "WHERE project_id = %s AND adr_id = ANY(%s) ORDER BY seq",
                    (project_id, args["adrs"]),
                ).fetchall()
        with self._pool.connection() as conn:
            timeline = conn.execute(
                "SELECT seq, kind, from_phase, phase, detail, ts FROM proj_session_timeline "
                "WHERE project_id = %s AND session_id = %s ORDER BY seq",
                (project_id, session_id),
            ).fetchall()
        spend = self._spend(session_id)
        package = {
            "session_id": session_id,
            "project_id": project_id,
            "preset": args.get("preset"),
            "limits": args.get("limits"),
            "outcome": args["outcome"],
            "stop_reason": stop_reason,
            "best_version": best,
            "gate": gate,
            "check_results": sorted(results, key=lambda r: r["check_id"]),
            "requirement_trace": trace,
            "open_risks": risks,
            "waiver_requests": args.get("waiver_requests", []),
            "steer_claims": args.get("steer_claims", []),
            "extensions": args.get("extensions", 0),
            "adrs": adrs,
            "rounds": args.get("rounds", []),
            "spend": spend,
            "timeline": timeline,
        }
        data = json.dumps(package, sort_keys=True, indent=1, default=str).encode("utf-8")
        key = self._store.put(data)
        self._update_row(
            project_id,
            session_id,
            package_key=key,
            open_risks=[r["id"] for r in risks],
            best_version=best,
            outcome=args["outcome"],
            spend=spend,
        )
        verdict = gate["verdict"] if gate else None
        self._update_row(project_id, session_id, gate_verdict=verdict)
        return {
            "package_key": key,
            "open_risks": risks,
            "spend": spend,
            "gate_verdict": verdict,
            "blocking": gate["reasons"] if gate else [],
        }

    # ------------------------------------------------------------------ the human's decisions
    def _sign_waivers(self, args: dict[str, Any]) -> dict[str, Any]:
        """approve_with_risks: one waiver.signed per (check, element) among the open blocking
        reasons (see waiver_targets), signed by the human who decided (the Arbiter refuses
        any other actor kind), with their reason as the risk."""
        project_id, session_id = args["project_id"], args["session_id"]
        signer = args.get("signer") or "owner"
        waivers: list[dict[str, Any]] = []
        for target in waiver_targets(args.get("blocking", [])):
            waiver_id = typed_id("wvr", session_id, str(args["n"]), target)
            self._event(
                project_id,
                session_id,
                "waiver.signed",
                {
                    "waiver_id": waiver_id,
                    "target_ref": target,
                    "risk": args["reason"],
                    "signer": signer,
                },
                f"session:{session_id}:waiver:{args['n']}:{target}",
                actor={"kind": "human", "id": signer},
            )
            waivers.append({"waiver_id": waiver_id, "target_ref": target})
        self._update_row(project_id, session_id, waivers=waivers)
        self._catch_up(project_id)
        return {"waivers": waivers}

    def _extend_budget(self, args: dict[str, Any]) -> dict[str, Any]:
        """extend: a new budget.updated with the raised limits, signed by the human, and, when
        the best version so far is not the head, a new head created from it so the loop
        resumes from the best."""
        project_id, session_id, n = args["project_id"], args["session_id"], args["n"]
        signer = args.get("signer") or "owner"
        limits = args["new_limits"]
        tokens = limits.get("tokens")
        self._event(
            project_id,
            session_id,
            "budget.updated",
            {
                "scope": {"session": session_id},
                "limits": {
                    "tokens": None if tokens is None else int(tokens),
                    "usd": limits.get("usd"),
                    "wall_clock_minutes": int(limits["wall_clock_minutes"]),
                },
            },
            f"session:{session_id}:budget:extend:{n}",
            actor={"kind": "human", "id": signer},
        )
        head, best = args.get("head_version"), args.get("best_version")
        if best and head and best != head:
            head = typed_id("mv", session_id, "extend", str(n))
            self._event(
                project_id,
                session_id,
                "model.version_created",
                {"version_id": head, "parent": best},
                f"session:{session_id}:extend:{n}:from-best",
            )
        self._update_row(project_id, session_id, limits=limits, status="running", outcome=None)
        self._catch_up(project_id)
        return {"head_version": head, "spend": self._spend(session_id)}

    # ------------------------------------------------------------------ record_status
    def _record_status(self, args: dict[str, Any]) -> dict[str, Any]:
        fields: dict[str, Any] = {"status": args["status"]}
        for key in (
            "outcome",
            "phase",
            "round",
            "best_version",
            "package_key",
            "failure",
            "last_refusal",
        ):
            if key in args:
                fields[key] = args[key]
        if "open_risk_ids" in args:
            fields["open_risks"] = args["open_risk_ids"]
        fields["spend"] = self._spend(args["session_id"])
        self._update_row(args["project_id"], args["session_id"], **fields)
        return {"spend": fields["spend"]}


def waiver_risk(waiver: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"waiver-requested:{waiver['check_id']}:{waiver['element_id']}",
        "kind": "waiver_requested",
        "detail": waiver["risk"],
        "check_id": waiver["check_id"],
        "element_id": waiver["element_id"],
    }
