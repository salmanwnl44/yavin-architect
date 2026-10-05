"""The design session as a Temporal workflow: nine phases, a budgeted attack/repair/verify
loop, a convergence rule, human gates, signals and a status query.

Deterministic by construction (A1): no I/O, no clock but workflow.now(), no randomness, and
nothing imported from the database, the gateway or the Arbiter. Every side effect is an
activity, named by string so this module never imports the activity code. The architecture
test in tests/test_sessions.py enforces the import rule.

Signals never act on their own. A signal handler only records what arrived in workflow
state (a flag, or an entry in a buffer); the run consumes it at a well-defined point: a step
boundary, the loop's exit, or a human gate. So a signal that races a step boundary is
neither lost nor applied twice, and one that cannot apply (a decision while no gate is
open, an approve on a BLOCKED package) is refused with a recorded reason.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError, FailureError

from architect.sessions.types import WORKFLOW_NAME, SessionInput, spend_zero

ACTIVITY_TIMEOUT = timedelta(minutes=30)
RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=["SessionFailure"],
)

# why the attack/repair/verify loop ended -> the session's outcome
OUTCOME_BY_STOP = {
    "allowed": "completed",
    "no_improvement": "completed_with_risks",
    "max_rounds": "completed_with_risks",
    "budget": "stopped_budget",
    "time": "stopped_time",
    "cancel": "cancelled",
    "rejected": "rejected",
}
# outcomes that end at the human gate with their package (M7); cancel and a mid-gate reject
# are the human's own decision already and end directly
GATED_OUTCOMES = ("completed", "completed_with_risks", "stopped_budget", "stopped_time")
EXTENDABLE_OUTCOMES = ("stopped_budget", "stopped_time")
EXTENSION_FIELDS = ("tokens", "usd", "wall_clock_minutes", "rounds")

BUDGET_BUCKETS = ((0.9, "more than 90%"), (0.5, "50-90%"), (0.2, "20-50%"), (0.05, "5-20%"))


def budget_bucket(remaining: float | None, cap: float | None) -> str:
    """A coarse view of what is left, stable across a live run and its replay."""
    if cap is None or cap <= 0:
        return "uncapped"
    if remaining is None:
        return "unknown"
    fraction = remaining / cap
    for threshold, label in BUDGET_BUCKETS:
        if fraction > threshold:
            return label
    return "under 5%"


def failure_reason(error: BaseException) -> str:
    """What an activity said when it failed: the message of the error it raised (the cause
    of the ActivityError), which for a refused proposal carries the typed rejection."""
    source = getattr(error, "cause", None) or error
    return str(getattr(source, "message", None) or source)


class _Stop(Exception):
    """The loop ends now, for `reason`; converge and package follow."""

    def __init__(self, reason: str, detail: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


@workflow.defn(name=WORKFLOW_NAME)
class DesignSessionWorkflow:
    def __init__(self) -> None:
        self.input: SessionInput | None = None
        self.limits: dict[str, Any] = {}
        self.status = "running"
        self.outcome: str | None = None
        self.failure: str | None = None
        self.phase: str | None = None
        self.round = 0
        self.head: str | None = None
        self.best_version: str | None = None
        self.open_risks: list[dict[str, Any]] = []  # what the phases reported
        self.package_risks: list[dict[str, Any]] | None = None  # as the last package lists them
        self.spend: dict[str, Any] = spend_zero()
        self.package_key: str | None = None
        self.gate_verdict: str | None = None
        self.blocking: list[dict[str, Any]] = []
        self.waivers: list[dict[str, Any]] = []
        self.brief_source_id: str | None = None
        self.research_ids: list[str] = []
        self.requirements: list[dict[str, Any]] = []
        self.rounds: list[dict[str, Any]] = []
        self.scores: dict[str, tuple[int, int]] = {}
        self.versions: list[str] = []
        self.waiver_requests: list[dict[str, Any]] = []
        self.adrs: list[str] = []
        self.stop_reason: str | None = None
        self.stop_detail: str | None = None
        self.done: set[str] = set()  # setup steps finished: frame, research, model, draft
        self.best_score: tuple[int, int] | None = None
        self.streak = 0
        self.mid_gate_done = False
        self.extensions = 0
        self.decision_count = 0
        self.checkpoints = 0
        self.last_checkpoint: datetime | None = None
        self.started: datetime | None = None
        self.deadline: datetime | None = None
        self.active = False
        self.finished = False
        # what signals leave behind, consumed only at well-defined points
        self.paused = False
        self.cancelled = False
        self.steers: list[str] = []
        self.steer_count = 0
        self.steer_claims: list[str] = []
        self.gate_open: str | None = None  # "after_attack" | "end" while a human gate waits
        self.decisions: list[dict[str, Any]] = []
        self.refusals: list[dict[str, Any]] = []
        self.refusal_signers: list[str | None] = []
        # what the ledger has been told (session.status_changed, contracts v1.2)
        self.status_events = 0
        self.refusals_recorded = 0
        self.recorded: tuple[Any, ...] = ("running", None, None, None)

    # ------------------------------------------------------------------ signals and queries
    @workflow.signal
    def pause(self) -> None:
        self.paused = True

    @workflow.signal
    def resume(self) -> None:
        self.paused = False

    @workflow.signal
    def cancel(self) -> None:
        self.cancelled = True

    @workflow.signal
    def steer(self, text: str) -> None:
        self.steers.append(text)

    @workflow.signal
    def approve(self, by: dict[str, Any] | None = None) -> None:
        self._decide("approve", by)

    @workflow.signal
    def approve_with_risks(self, decision: dict[str, Any] | None = None) -> None:
        self._decide("approve_with_risks", decision)

    @workflow.signal
    def reject(self, by: dict[str, Any] | None = None) -> None:
        self._decide("reject", by)

    @workflow.signal
    def extend(self, extension: dict[str, Any] | None = None) -> None:
        self._decide("extend", extension)

    def _decide(self, kind: str, arg: dict[str, Any] | None) -> None:
        decision = {**(arg or {}), "kind": kind}
        if self.gate_open is None:
            self._refuse(decision, "no human gate is open")
        else:
            self.decisions.append(decision)

    def _refuse(self, decision: dict[str, Any], why: str) -> None:
        self.refusals.append({"decision": decision["kind"], "why": why})
        self.refusal_signers.append(decision.get("signer"))

    @workflow.query
    def status(self) -> dict[str, Any]:
        return self.view()

    def view(self) -> dict[str, Any]:
        return {
            "session_id": self.input.session_id if self.input else None,
            "phase": self.phase,
            "round": self.round,
            "best_version": self.best_version,
            "open_risk_count": len(self.risk_ids()),
            "open_risk_ids": self.risk_ids(),
            "spend": self.spend,
            "limits": self.limits,
            "status": self.status,
            "outcome": self.outcome,
            "failure": self.failure,
            "stop_reason": self.stop_reason,
            "package_key": self.package_key,
            "gate_verdict": self.gate_verdict,
            "gate_open": self.gate_open,
            "last_refusal": self.refusals[-1] if self.refusals else None,
            "refusals": len(self.refusals),
            "waivers": list(self.waivers),
            "extensions": self.extensions,
            "steer_claims": list(self.steer_claims),
            "pending_steers": len(self.steers),
            "active": self.active,
        }

    def risk_ids(self) -> list[str]:
        risks = self.package_risks if self.package_risks is not None else self.open_risks
        return list(dict.fromkeys(risk["id"] for risk in risks))

    # ------------------------------------------------------------------ the run
    @workflow.run
    async def run(self, input: SessionInput) -> dict[str, Any]:
        self.input = input
        self.limits = dict(input.limits)
        self.started = workflow.now()
        self.deadline = self.started + timedelta(minutes=int(self.limits["wall_clock_minutes"]))
        heartbeat = asyncio.create_task(self._heartbeat())
        try:
            await self._session()
        except FailureError as error:
            await self._fail(failure_reason(error))
            raise
        except _Stop as stop:  # pragma: no cover - every _Stop is caught inside _loop
            await self._fail(str(stop))
            raise ApplicationError(str(stop), type="SessionFailure", non_retryable=True) from stop
        except Exception as error:
            await self._fail(f"unexpected error: {error!r}")
            raise ApplicationError(
                f"unexpected error: {error!r}", type="SessionFailure", non_retryable=True
            ) from error
        finally:
            self.finished = True
            heartbeat.cancel()
        return self.view()

    async def _fail(self, reason: str) -> None:
        self.status = self.outcome = "failed"
        self.failure = reason
        self.finished = True
        try:
            await self._record()
        except Exception:  # noqa: BLE001 - the failure itself is what gets reported
            pass

    async def _session(self) -> None:
        assert self.input is not None
        gates = self.limits.get("human_gates", ["end"])
        started = await self._act(
            "session_start",
            {
                "brief": self.input.brief,
                "preset": self.input.preset,
                "sources": list(self.input.sources),
            },
        )
        self.brief_source_id = started["brief_source_id"]
        self.spend = started["spend"]

        decided: dict[str, Any] | None = None  # the human decision that ended the session
        while True:
            await self._loop(gates)
            self.active = False
            # A steer that arrived after the loop's last step boundary has no boundary left
            # to be consumed at: record it here, so the owner's guidance is never lost.
            await self._drain_steers()

            await self._phase("converge")
            self.best_version = self._best()
            self.outcome = OUTCOME_BY_STOP[self.stop_reason or "allowed"]
            await self._checkpoint()

            await self._phase("package")
            package = await self._act(
                "package",
                {
                    "preset": self.input.preset,
                    "outcome": self.outcome,
                    "stop_reason": self.stop_reason,
                    "detail": self.stop_detail,
                    "best_version": self.best_version,
                    "open_risks": self.open_risks,
                    "waiver_requests": self.waiver_requests,
                    "adrs": self.adrs,
                    "rounds": self.rounds,
                    "steer_claims": list(self.steer_claims),
                    "extensions": self.extensions,
                },
            )
            self.package_key = package["package_key"]
            self.package_risks = package["open_risks"]
            self.spend = package["spend"]
            self.gate_verdict = package["gate_verdict"]
            self.blocking = package["blocking"]
            await self._checkpoint()

            if self.outcome not in GATED_OUTCOMES or "end" not in gates:
                self.status = self.outcome
                break
            decision = await self._human_gate("end")
            if decision is None:
                self.status = self.outcome = "cancelled"
                break
            decided = decision
            if decision["kind"] == "approve":
                self.status = "approved"
                break
            if decision["kind"] == "approve_with_risks":
                self.decision_count += 1
                signed = await self._act(
                    "sign_waivers",
                    {
                        "n": self.decision_count,
                        "reason": str(decision["reason"]).strip(),
                        "signer": decision.get("signer"),
                        "blocking": self.blocking,
                    },
                )
                self.waivers = signed["waivers"]
                self.status = "approved_with_risks"
                break
            if decision["kind"] == "reject":
                self.status = "rejected"
                break
            decided = None
            await self._extend(decision)
        self.finished = True
        await self._record(decided)

    # ------------------------------------------------------------------ the phases
    async def _setup(self) -> None:
        """frame, research, model and draft, each done once; a session extended after a stop
        in the middle of them continues where it stopped."""
        assert self.input is not None
        if "frame" not in self.done:
            await self._phase("frame")
            frame = await self._step(
                "frame", {"brief": self.input.brief, "brief_source_id": self.brief_source_id}
            )
            self.requirements = frame["requirements"]
            self.done.add("frame")
            await self._checkpoint()
        if "research" not in self.done:
            await self._phase("research")
            research = await self._step("research", {"brief": self.input.brief})
            self.research_ids = research["claim_ids"]
            self.done.add("research")
            await self._checkpoint()
        if "model" not in self.done:
            await self._phase("model")
            genesis = await self._step("genesis", {})
            self._saw(genesis["version_id"])
            self.best_version = self.head
            if self.input.seed is not None:
                # review mode: the seed is the first patch after genesis, through the Arbiter
                seeded = await self._step(
                    "seed", {"head_version": self.head, "seed": self.input.seed}
                )
                self._saw(seeded["version_id"])
            self.done.add("model")
            await self._checkpoint()
        if "draft" not in self.done:
            if self.input.seed is None:
                await self._phase("draft")
                draft = await self._step(
                    "draft",
                    {
                        "round": 1,
                        "head_version": self.head,
                        "research_claim_ids": self.research_ids,
                    },
                )
                if draft.get("version_id"):
                    self._saw(draft["version_id"])
                await self._checkpoint()
            self.done.add("draft")
            self.round = 1

    async def _loop(self, gates: list[str]) -> None:
        """Setup, then attack/repair/verify rounds until the convergence rule fires. Why it
        ended is left in stop_reason."""
        try:
            await self._setup()
            while True:
                record: dict[str, Any] = {"round": self.round}
                await self._phase("attack")
                attack = await self._step(
                    "attack", {"version_id": self.head, "round": self.round, "phase": "attack"}
                )
                record["attack"] = self._score(attack)
                if "after_attack" in gates and not self.mid_gate_done:
                    self.mid_gate_done = True
                    decision = await self._human_gate("after_attack")
                    if decision is None:
                        raise _Stop("cancel", "cancel signal")
                    if decision["kind"] == "reject":
                        raise _Stop("rejected", "rejected at the after-attack gate")
                    self.status = "running"
                    await self._record(decision)
                if attack["gate"]["verdict"] == "ALLOWED":
                    self.rounds.append(record)
                    raise _Stop("allowed")
                score = (attack["blocking"], attack["failing_count"])
                if self.best_score is None:
                    self.best_score = score

                await self._phase("repair")
                repair = await self._step(
                    "repair",
                    {
                        "round": self.round,
                        "head_version": self.head,
                        "failing": attack["failing"],
                        "research_claim_ids": self.research_ids,
                    },
                )
                record["repair"] = {
                    "version": repair.get("version_id"),
                    "reason": repair.get("reason"),
                    "waiver_requests": [w["check_id"] for w in repair.get("waiver_requests", [])],
                }
                if repair.get("version_id"):
                    self._saw(repair["version_id"])

                await self._phase("verify")
                verify = await self._step(
                    "attack", {"version_id": self.head, "round": self.round, "phase": "verify"}
                )
                record["verify"] = self._score(verify)
                self.rounds.append(record)
                await self._checkpoint()
                if verify["gate"]["verdict"] == "ALLOWED":
                    raise _Stop("allowed")
                score = (verify["blocking"], verify["failing_count"])
                if score < self.best_score:
                    self.best_score, self.streak = score, 0
                else:
                    self.streak += 1
                if self.streak >= 2:
                    raise _Stop("no_improvement", "no improvement for 2 consecutive rounds")
                if self.round >= int(self.limits["max_rounds"]):
                    raise _Stop("max_rounds", f"max_rounds {self.limits['max_rounds']} reached")
                self.round += 1
        except _Stop as stop:
            self.stop_reason, self.stop_detail = stop.reason, stop.detail

    # ------------------------------------------------------------------ human gates
    async def _human_gate(self, name: str) -> dict[str, Any] | None:
        """Wait for the owner at a gate. Returns the first decision that applies, or None on
        cancel. Decisions that do not apply are refused with a recorded reason and the gate
        stays open; steers that arrive meanwhile are recorded. Time spent here is not
        running time: the wall clock's deadline moves by as long as the gate was open."""
        assert self.deadline is not None
        self.active = False
        # open the gate before the status becomes visible: a decision sent the moment the
        # owner sees `awaiting_approval` must be kept
        self.gate_open = name
        opened = workflow.now()
        self.status = "awaiting_approval"
        await self._record()
        try:
            while True:
                await workflow.wait_condition(
                    lambda: bool(self.decisions) or self.cancelled or bool(self.steers)
                )
                if self.steers:
                    await self._drain_steers()
                    continue
                if self.cancelled:
                    return None
                decision = self.decisions.pop(0)
                problem = self._problem_with(decision)
                if problem is None:
                    return decision
                self._refuse(decision, problem)
                await self._record()
        finally:
            self.gate_open = None
            for late in self.decisions:  # sent after the one that decided: refused, not lost
                self._refuse(late, "the gate had already been decided")
            self.decisions.clear()
            self.deadline += workflow.now() - opened

    def _problem_with(self, decision: dict[str, Any]) -> str | None:
        """Why a decision cannot be applied at the open gate, or None."""
        kind = decision["kind"]
        if self.gate_open == "after_attack":
            if kind in ("approve", "reject"):
                return None
            return f"{kind} applies at the end gate; here approve continues and reject stops"
        if kind == "approve":
            if self.gate_verdict != "ALLOWED":
                return (
                    f"the package's gate is {self.gate_verdict}: approve_with_risks with a "
                    "reason, extend, or reject"
                )
            return None
        if kind == "approve_with_risks":
            if not str(decision.get("reason") or "").strip():
                return "approve_with_risks needs a non-empty reason"
            return None
        if kind == "extend":
            if self.outcome not in EXTENDABLE_OUTCOMES:
                return "extend applies to sessions stopped by their budget or the wall clock"
            for name in EXTENSION_FIELDS:
                value = decision.get(name)
                if value is None:
                    continue
                if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
                    return f"{name} must be a non-negative number"
            if self.outcome == "stopped_time" and not decision.get("wall_clock_minutes"):
                return "a session stopped by the wall clock needs wall_clock_minutes"
            if self.outcome == "stopped_budget" and not (
                decision.get("tokens") or decision.get("usd")
            ):
                return "a session stopped by its budget needs tokens or usd"
            return None
        return None  # reject always applies

    async def _extend(self, decision: dict[str, Any]) -> None:
        """Raise the limits by the amounts given, record them as a new budget.updated, and
        set the loop up to resume from the best version so far."""
        assert self.deadline is not None
        self.extensions += 1
        limits = dict(self.limits)
        if decision.get("tokens") and limits.get("tokens") is not None:
            limits["tokens"] = int(limits["tokens"] + decision["tokens"])
        if decision.get("usd") and limits.get("usd") is not None:
            limits["usd"] = limits["usd"] + decision["usd"]
        if decision.get("wall_clock_minutes"):
            minutes = decision["wall_clock_minutes"]
            limits["wall_clock_minutes"] = int(limits["wall_clock_minutes"] + minutes)
            self.deadline = max(self.deadline, workflow.now()) + timedelta(minutes=minutes)
        if decision.get("rounds"):
            limits["max_rounds"] = int(limits["max_rounds"] + decision["rounds"])
        self.limits = limits
        result = await self._act(
            "extend_budget",
            {
                "n": self.extensions,
                "new_limits": limits,
                "signer": decision.get("signer"),
                "head_version": self.head,
                "best_version": self.best_version,
            },
        )
        if result.get("head_version"):
            self._saw(result["head_version"])
        self.spend = result["spend"]
        if "draft" in self.done:
            self.round += 1  # a fresh round: its phase events and version ids are new
        self.streak = 0
        self.stop_reason = self.stop_detail = self.outcome = None
        self.package_risks = None
        self.status = "running"
        await self._record(decision, limits=limits)

    # ------------------------------------------------------------------ steps
    def _base(self) -> dict[str, Any]:
        assert self.input is not None and self.deadline is not None
        remaining = (self.deadline - workflow.now()).total_seconds() / 60
        limits = self.limits
        return {
            "project_id": self.input.project_id,
            "session_id": self.input.session_id,
            "limits": limits,
            "started_at": self.input.started_at,
            "remaining_minutes": max(0, int(remaining)),
            "remaining_bucket": {
                "tokens": budget_bucket(
                    None
                    if limits.get("tokens") is None
                    else limits["tokens"] - self.spend["tokens"],
                    limits.get("tokens"),
                ),
                "usd": budget_bucket(
                    None if limits.get("usd") is None else limits["usd"] - self.spend["usd"],
                    limits.get("usd"),
                ),
                "wall_clock": budget_bucket(remaining, limits["wall_clock_minutes"]),
            },
        }

    async def _act(self, name: str, extra: dict[str, Any]) -> dict[str, Any]:
        return await workflow.execute_activity(
            name,
            self._base() | extra,
            result_type=dict,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            # every activity heartbeats, so one whose worker died is retried on another
            # worker after this long instead of waiting out the whole timeout
            heartbeat_timeout=timedelta(seconds=int(self.limits.get("heartbeat_seconds", 30))),
            retry_policy=RETRY,
        )

    async def _step(self, name: str, extra: dict[str, Any]) -> dict[str, Any]:
        """A phase activity: honours pause, steer, cancel and the wall clock first, then
        absorbs what the activity reports (risks, spend, waivers, ADRs, a budget stop)."""
        await self._gate()
        self.active = True
        try:
            result = await self._act(name, extra)
        finally:
            self.active = False
        self._absorb(result)
        if result.get("stopped") == "budget":
            raise _Stop("budget", result.get("detail"))
        return result

    def _absorb(self, result: dict[str, Any]) -> None:
        known = {risk["id"] for risk in self.open_risks}
        for risk in result.get("open_risks", []):
            if risk["id"] not in known:
                self.open_risks.append(risk)
                known.add(risk["id"])
        self.waiver_requests += result.get("waiver_requests", [])
        self.adrs += result.get("adrs", [])
        if "spend" in result:
            self.spend = result["spend"]

    def _saw(self, version_id: str) -> None:
        self.head = version_id
        if version_id not in self.versions:
            self.versions.append(version_id)

    def _score(self, attack: dict[str, Any]) -> dict[str, Any]:
        version = attack["version_id"]
        self.scores[version] = (attack["blocking"], attack["failing_count"])
        self.best_version = self._best()
        return {
            "version": version,
            "verdict": attack["gate"]["verdict"],
            "blocking": attack["blocking"],
            "failing": attack["failing_count"],
            "checks": {r["check_id"]: r["status"] for r in attack["results"]},
        }

    def _best(self) -> str | None:
        """K = 1: fewest blocking reasons, ties to the later version."""
        scored = [v for v in self.versions if v in self.scores]
        if not scored:
            return self.head
        return min(scored, key=lambda v: (self.scores[v][0], -self.versions.index(v)))

    async def _drain_steers(self) -> None:
        """Record every buffered steer, in arrival order, each exactly once. The signal only
        appends to `self.steers`; this is the one place they are consumed, and it is called
        at every well-defined point: each step boundary, the loop's exit, and while the
        session waits for a human."""
        while self.steers:
            text = self.steers.pop(0)
            self.steer_count += 1
            steered = await self._act("steer", {"n": self.steer_count, "text": text})
            self.steer_claims.append(steered["claim_id"])

    async def _gate(self) -> None:
        """A step boundary: where cancel, steer, pause and the wall clock take effect."""
        while True:
            if self.cancelled:
                raise _Stop("cancel", "cancel signal")
            if self.steers:
                await self._drain_steers()
                continue
            if self.paused:
                self.status = "paused"
                await self._record()
                await workflow.wait_condition(lambda: not self.paused or self.cancelled)
                if not self.cancelled:
                    self.status = "running"
                    await self._record()
                continue
            assert self.deadline is not None
            if workflow.now() >= self.deadline:
                raise _Stop(
                    "time",
                    f"wall clock of {self.limits['wall_clock_minutes']} minutes exhausted",
                )
            return

    async def _phase(self, to: str) -> None:
        previous, self.phase = self.phase, to
        await self._act("phase_changed", {"from": previous, "to": to, "round": self.round})

    async def _checkpoint(self) -> None:
        key = f"{self.phase}:{self.round}:{self.checkpoints}"
        self.checkpoints += 1
        result = await self._act(
            "checkpoint",
            {
                "phase": self.phase or "frame",
                "best_version": self.best_version,
                "open_risk_ids": self.risk_ids(),
                "package_key": self.package_key,
                "key": key,
            },
        )
        self.spend = result["spend"]
        self.last_checkpoint = workflow.now()

    async def _record(
        self, decision: dict[str, Any] | None = None, limits: dict[str, Any] | None = None
    ) -> None:
        """Put the session's status in the ledger: one session.status_changed when the
        status, outcome, package or gate verdict changed or a decision was taken, and one
        (marked refused) for every decision refused since the last record."""
        state = (self.status, self.outcome, self.package_key, self.gate_verdict)
        emit = decision is not None or state != self.recorded
        refusals = [
            refusal | {"signer": signer, "n": n}
            for n, (refusal, signer) in enumerate(
                zip(self.refusals, self.refusal_signers, strict=True)
            )
            if n >= self.refusals_recorded
        ]
        if not emit and not refusals:
            return
        if emit:
            self.status_events += 1
        reason = self.failure if self.status == "failed" else self.stop_reason
        if decision is not None and decision.get("reason"):
            reason = str(decision["reason"]).strip()
        await self._act(
            "record_status",
            {
                "n": self.status_events,
                "emit": emit,
                "status": self.status,
                "outcome": self.outcome,
                "reason": reason,
                "package_key": self.package_key,
                "gate_verdict": self.gate_verdict,
                "limits": limits,
                "decision": (
                    {"kind": decision["kind"], "signer": decision.get("signer")}
                    if decision is not None
                    else None
                ),
                "refusals": refusals,
            },
        )
        self.recorded = state
        self.refusals_recorded = len(self.refusals)

    async def _heartbeat(self) -> None:
        """A checkpoint at least every `checkpoint_minutes` of workflow time while a phase
        runs. Nothing ticks while the session is paused or awaiting a human: no compute held."""
        every = timedelta(minutes=int(self.limits.get("checkpoint_minutes", 5)))
        while not self.finished:
            await workflow.wait_condition(lambda: self.active or self.finished)
            if self.finished:
                return
            await workflow.sleep(every)
            if self.finished or not self.active:
                continue
            if self.last_checkpoint is None or workflow.now() - self.last_checkpoint >= every:
                await self._checkpoint()
