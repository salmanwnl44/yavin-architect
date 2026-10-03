"""The design session as a Temporal workflow: nine phases, a budgeted attack/repair/verify
loop, a convergence rule, signals and a status query.

Deterministic by construction (A1): no I/O, no clock but workflow.now(), no randomness, and
nothing imported from the database, the gateway or the Arbiter. Every side effect is an
activity, named by string so this module never imports the activity code. The architecture
test in tests/test_sessions.py enforces the import rule.
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
        self.status = "running"
        self.outcome: str | None = None
        self.phase: str | None = None
        self.round = 0
        self.head: str | None = None
        self.best_version: str | None = None
        self.open_risks: list[dict[str, Any]] = []
        self.spend: dict[str, Any] = spend_zero()
        self.package_key: str | None = None
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
        self.checkpoints = 0
        self.last_checkpoint: datetime | None = None
        self.started: datetime | None = None
        self.deadline: datetime | None = None
        self.active = False
        self.finished = False
        # signals
        self.paused = False
        self.cancelled = False
        self.decision: str | None = None
        self.steers: list[str] = []
        self.steer_count = 0
        self.steer_claims: list[str] = []

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
    def approve(self) -> None:
        self.decision = "approve"

    @workflow.signal
    def reject(self) -> None:
        self.decision = "reject"

    @workflow.signal
    def steer(self, text: str) -> None:
        self.steers.append(text)

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
            "status": self.status,
            "outcome": self.outcome,
            "stop_reason": self.stop_reason,
            "package_key": self.package_key,
            "steer_claims": list(self.steer_claims),
            "pending_steers": len(self.steers),
            "active": self.active,
        }

    def risk_ids(self) -> list[str]:
        return list(dict.fromkeys(risk["id"] for risk in self.open_risks))

    # ------------------------------------------------------------------ the run
    @workflow.run
    async def run(self, input: SessionInput) -> dict[str, Any]:
        self.input = input
        self.started = workflow.now()
        self.deadline = self.started + timedelta(minutes=int(input.limits["wall_clock_minutes"]))
        heartbeat = asyncio.create_task(self._heartbeat())
        try:
            await self._session()
        except FailureError:
            await self._fail()
            raise
        except _Stop as stop:  # pragma: no cover - every _Stop is caught inside _session
            await self._fail()
            raise ApplicationError(str(stop), type="SessionFailure", non_retryable=True) from stop
        except Exception as error:
            await self._fail()
            raise ApplicationError(
                f"unexpected error: {error!r}", type="SessionFailure", non_retryable=True
            ) from error
        finally:
            self.finished = True
            heartbeat.cancel()
        return self.view()

    async def _fail(self) -> None:
        self.status = self.outcome = "failed"
        self.finished = True
        try:
            await self._act("record_status", {"status": "failed", "outcome": "failed"})
        except Exception:  # noqa: BLE001 - the failure itself is what gets reported
            pass

    async def _session(self) -> None:
        assert self.input is not None
        limits = self.input.limits
        gates = limits.get("human_gates", ["end"])
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
        try:
            await self._phase("frame")
            frame = await self._step(
                "frame", {"brief": self.input.brief, "brief_source_id": self.brief_source_id}
            )
            self.requirements = frame["requirements"]
            await self._checkpoint()

            await self._phase("research")
            research = await self._step("research", {"brief": self.input.brief})
            self.research_ids = research["claim_ids"]
            await self._checkpoint()

            await self._phase("model")
            genesis = await self._step("genesis", {})
            self._saw(genesis["version_id"])
            self.best_version = self.head
            await self._checkpoint()

            await self._phase("draft")
            draft = await self._step(
                "draft",
                {"round": 1, "head_version": self.head, "research_claim_ids": self.research_ids},
            )
            if draft.get("version_id"):
                self._saw(draft["version_id"])
            await self._checkpoint()

            self.round = 1
            best_score: tuple[int, int] | None = None
            streak = 0
            while True:
                record: dict[str, Any] = {"round": self.round}
                await self._phase("attack")
                attack = await self._step(
                    "attack", {"version_id": self.head, "round": self.round, "phase": "attack"}
                )
                record["attack"] = self._score(attack)
                if self.round == 1 and "after_attack" in gates:
                    await self._human_gate()
                if attack["gate"]["verdict"] == "ALLOWED":
                    self.rounds.append(record)
                    raise _Stop("allowed")
                score = (attack["blocking"], attack["failing_count"])
                if best_score is None:
                    best_score = score

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
                if score < best_score:
                    best_score, streak = score, 0
                else:
                    streak += 1
                if streak >= 2:
                    raise _Stop("no_improvement", "no improvement for 2 consecutive rounds")
                if self.round >= int(limits["max_rounds"]):
                    raise _Stop("max_rounds", f"max_rounds {limits['max_rounds']} reached")
                self.round += 1
        except _Stop as stop:
            self.stop_reason, self.stop_detail = stop.reason, stop.detail
        self.active = False
        # A steer that arrived after the loop's last step boundary has no gate left to be
        # consumed at: record it here, so the owner's guidance is never lost.
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
            },
        )
        self.package_key = package["package_key"]
        self.open_risks = package["open_risks"]
        self.spend = package["spend"]
        await self._checkpoint()

        if self.outcome in ("completed", "completed_with_risks") and "end" in gates:
            # clear any stale decision before the status becomes visible: one that arrives
            # after that must be kept
            self.decision = None
            self.status = "awaiting_approval"
            await self._record()
            await self._await_decision()
            if self.cancelled:
                self.status = self.outcome = "cancelled"
            else:
                self.status = "approved" if self.decision == "approve" else "rejected"
        else:
            self.status = self.outcome
        self.finished = True
        await self._record()

    # ------------------------------------------------------------------ steps
    def _base(self) -> dict[str, Any]:
        assert self.input is not None and self.deadline is not None
        remaining = (self.deadline - workflow.now()).total_seconds() / 60
        limits = self.input.limits
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
        for risk in result.get("open_risks", []):
            if risk["id"] not in self.risk_ids():
                self.open_risks.append(risk)
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
        """Record every buffered steer, in arrival order, each exactly once. Signals only
        append to `self.steers`; this is the one place they are consumed, and it is called
        at every well-defined point: each step boundary, the loop's exit, and while the
        session waits for a human."""
        while self.steers:
            text = self.steers.pop(0)
            self.steer_count += 1
            steered = await self._act("steer", {"n": self.steer_count, "text": text})
            self.steer_claims.append(steered["claim_id"])

    async def _await_decision(self) -> None:
        """Wait at a human gate for a decision or a cancel; steers that arrive meanwhile are
        recorded, not left in the buffer."""
        while True:
            await workflow.wait_condition(
                lambda: self.decision is not None or self.cancelled or bool(self.steers)
            )
            if self.steers:
                await self._drain_steers()
                continue
            return

    async def _gate(self) -> None:
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
                    f"wall clock of {self.input.limits['wall_clock_minutes']} minutes exhausted",
                )
            return

    async def _human_gate(self) -> None:
        """The deep preset's gate after the first attack: the owner approves or rejects."""
        self.active = False
        self.decision = None
        self.status = "awaiting_approval"
        await self._record()
        await self._await_decision()
        if self.cancelled:
            raise _Stop("cancel", "cancel signal")
        decision, self.decision = self.decision, None
        if decision == "reject":
            raise _Stop("rejected", "rejected at the after-attack gate")
        self.status = "running"
        await self._record()

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
                "key": key,
            },
        )
        self.spend = result["spend"]
        self.last_checkpoint = workflow.now()

    async def _record(self) -> None:
        await self._act(
            "record_status",
            {
                "status": self.status,
                "outcome": self.outcome,
                "phase": self.phase,
                "round": self.round,
                "best_version": self.best_version,
                "open_risk_ids": self.risk_ids(),
                "package_key": self.package_key,
            },
        )

    async def _heartbeat(self) -> None:
        """A checkpoint at least every `checkpoint_minutes` of workflow time while a phase
        runs. Nothing ticks while the session is paused or awaiting a human: no compute held."""
        assert self.input is not None
        every = timedelta(minutes=int(self.input.limits.get("checkpoint_minutes", 5)))
        while not self.finished:
            await workflow.wait_condition(lambda: self.active or self.finished)
            if self.finished:
                return
            await workflow.sleep(every)
            if self.finished or not self.active:
                continue
            if self.last_checkpoint is None or workflow.now() - self.last_checkpoint >= every:
                await self._checkpoint()
