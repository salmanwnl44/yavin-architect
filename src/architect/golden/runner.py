"""`architect golden run`: drive one real session on a golden task and score it.

The runner creates a fresh project, starts the session through Temporal like any client, and
runs the worker in a process of its own. With kill_after="attack" the first worker ends
abruptly once the first attack phase has completed and a second worker is started: the
session must resume from its history. At the end gate the runner approves an ALLOWED
package and rejects any other; it never signs a waiver. Then it reads the ledger and the
package and writes the scorecard.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from temporalio.client import Client, WorkflowFailureError
from ulid import ULID

from architect import ledger, readmodel
from architect.db import database_url, ensure_schema, open_pool
from architect.gateway.config import ANTHROPIC_KEY_ENVS, anthropic_api_key
from architect.golden import scorecard as scoring
from architect.golden.tasks import MODES, GoldenTask, goldens_dir, load_task
from architect.golden.worker import DYING, READY
from architect.ingestion.objectstore import LocalObjectStore
from architect.projector import Projector
from architect.sessions import service
from architect.sessions.config import TEMPORAL_ADDRESS_ENV, load_session_config

FINAL = ("approved", "approved_with_risks", "rejected", "cancelled", "failed")
KILL_POINTS = ("attack",)


class GoldenError(Exception):
    """The run could not be carried out (as opposed to a run that scored badly)."""


def default_out(task_id: str, mode: str, live: bool) -> Path:
    """Live scorecards are evidence and live under goldens/results/; mock ones are scratch."""
    day = datetime.now(UTC).strftime("%Y-%m-%d")
    if live:
        return goldens_dir() / "results" / f"{day}-{task_id}-{mode}.json"
    return Path("data") / "golden" / f"{day}-{task_id}-{mode}-mock.json"


class _Worker:
    def __init__(self, process: subprocess.Popen, log_path: Path, log_file: Any) -> None:
        self.process, self.log_path, self._log_file = process, log_path, log_file

    def stderr_tail(self) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
        except OSError:
            return ""

    async def line(self, timeout: float) -> str:
        return await asyncio.wait_for(asyncio.to_thread(self.process.stdout.readline), timeout)

    async def ready(self, timeout: float = 180.0) -> None:
        line = await self.line(timeout)
        if READY not in line:
            raise GoldenError(f"the worker did not start: {line!r}\n{self.stderr_tail()}")

    def stop(self) -> None:
        """End the worker: close its stdin (it exits on that), and kill it if it does not."""
        if self.process.stdin is not None and not self.process.stdin.closed:
            self.process.stdin.close()
        if self.process.poll() is None:
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.process.stdout is not None:
            self.process.stdout.close()
        self._log_file.close()  # or Windows cannot remove the scratch directory


def _spawn(
    *,
    dsn: str,
    address: str,
    task_queue: str,
    store_dir: Path,
    task: GoldenTask,
    mode: str,
    provider: str,
    exit_after: str | None,
    scratch: Path,
    n: int,
) -> _Worker:
    command = [
        sys.executable, "-m", "architect.golden.worker",
        "--address", address, "--task-queue", task_queue, "--store", str(store_dir),
        "--task", task.task_id, "--mode", mode, "--provider", provider,
        "--exit-when-stdin-closes",
    ]  # fmt: skip
    if exit_after:
        command += ["--exit-after", exit_after]
    # the database url (and, in live mode, the key) reach the worker through the environment,
    # never the command line
    env = {
        **os.environ,
        "ARCHITECT_DATABASE_URL": dsn,
        "ARCHITECT_GOLDENS_DIR": str(task.root.parent),
    }
    log_path = scratch / f"worker-{n}.log"
    log_file = open(log_path, "w", encoding="utf-8")  # noqa: SIM115 - closed by _Worker.stop
    process = subprocess.Popen(  # noqa: S603 - our own module, our own interpreter
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=log_file,
        text=True,
        env=env,
    )
    return _Worker(process, log_path, log_file)


async def run_golden(
    task_id: str,
    *,
    mode: str = "review",
    live: bool = False,
    kill_after: str | None = None,
    dsn: str | None = None,
    address: str | None = None,
    store_dir: Path | None = None,
    out: Path | None = None,
    timeout_s: float | None = None,
    log: Callable[[str], None] = lambda line: None,
) -> dict[str, Any]:
    """Run the task and return its scorecard (also written to `out`)."""
    if mode not in MODES:
        raise GoldenError(f"mode must be one of {MODES}")
    if kill_after is not None and kill_after not in KILL_POINTS:
        raise GoldenError(f"--kill-after supports {KILL_POINTS}")
    task = load_task(task_id)
    provider = "live" if live else "mock"
    if live and not anthropic_api_key():
        raise GoldenError(f"a live run needs {ANTHROPIC_KEY_ENVS[0]} in the environment")
    timeout = timeout_s or (3600.0 if live else 600.0)
    dsn = dsn or database_url()
    store = LocalObjectStore(store_dir) if store_dir else LocalObjectStore()
    run_id = str(ULID())
    project_id = f"golden-{task_id}-{mode}-{run_id[-10:].lower()}"
    task_queue = f"golden-{run_id.lower()}"
    usd_cap = float(task.expected.get("usd_cap", 3.0))
    config = load_session_config()
    session = service.build_input(
        config,
        project_id=project_id,
        brief=task.brief,
        preset=task.expected.get("preset", "quick"),
        overrides={"usd": usd_cap},
        seed=task.seed if mode == "review" else None,
    )
    kill: dict[str, Any] = {"performed": False, "after": kill_after, "worker_exit_code": None}
    workers: list[_Worker] = []
    pool = open_pool(dsn, max_size=4)
    try:
        ensure_schema(pool)
        ledger.create_project(pool, project_id)
        async with contextlib.AsyncExitStack() as stack:
            address = address or os.environ.get(TEMPORAL_ADDRESS_ENV)
            if address:
                client = await Client.connect(address)
            else:
                # no server configured: the SDK's own dev server, for this run only
                from temporalio.testing import WorkflowEnvironment

                environment = await WorkflowEnvironment.start_local()
                stack.push_async_callback(environment.shutdown)
                client = environment.client
                address = client.service_client.config.target_host
            scratch = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="golden-")))
            stack.callback(lambda: [w.stop() for w in workers])

            def spawn(exit_after: str | None) -> _Worker:
                worker = _spawn(
                    dsn=dsn, address=address, task_queue=task_queue, store_dir=store.root,
                    task=task, mode=mode, provider=provider, exit_after=exit_after,
                    scratch=scratch, n=len(workers) + 1,
                )  # fmt: skip
                workers.append(worker)
                return worker

            first = spawn(kill_after)
            await first.ready()
            log(f"session {session.session_id} in {project_id} ({mode}, {provider})")
            started_at = service.now_rfc3339()
            clock = time.monotonic()
            handle = await service.start(client, session, task_queue)

            if kill_after is not None:
                line = await first.line(timeout)
                if DYING not in line:
                    raise GoldenError(
                        f"the worker ended before the {kill_after} phase: {line!r}\n"
                        f"{first.stderr_tail()}"
                    )
                code = await asyncio.wait_for(asyncio.to_thread(first.process.wait), 60)
                kill |= {"performed": True, "worker_exit_code": code}
                log(f"worker 1 exited after {kill_after} (code {code}); starting worker 2")
                await spawn(None).ready()

            view: dict[str, Any] = {}
            deadline = time.monotonic() + timeout
            while True:
                view = await handle.query("status")
                if view["status"] == "awaiting_approval" or view["status"] in FINAL:
                    break
                if time.monotonic() > deadline:
                    raise GoldenError(f"the session did not finish in {timeout:.0f}s: {view}")
                if all(w.process.poll() is not None for w in workers):
                    raise GoldenError(f"no worker is alive:\n{workers[-1].stderr_tail()}")
                await asyncio.sleep(1.0)
            status_at_gate = view["status"] if view["status"] == "awaiting_approval" else None
            closed_with = None
            if status_at_gate:
                closed_with = "approve" if view.get("gate_verdict") == "ALLOWED" else "reject"
                log(
                    f"at the gate: {view.get('outcome')}, {view.get('gate_verdict')}; {closed_with}"
                )
                await handle.signal(closed_with, {"signer": "golden-runner"})
            try:
                final = await asyncio.wait_for(handle.result(), 300)
            except WorkflowFailureError:
                final = await handle.query("status")
            duration = time.monotonic() - clock

        Projector(pool).catch_up(project_id)
        events = list(ledger.iter_events(pool, project_id))
        package = (
            service.load_package(store, final["package_key"]) if final.get("package_key") else None
        )
        best = final.get("best_version")
        version = readmodel.model_version(pool, project_id, best) if best else None
        card = scoring.build(
            task=task,
            mode=mode,
            provider=provider,
            session_id=session.session_id,
            project_id=project_id,
            started_at=started_at,
            duration_s=duration,
            events=events,
            package=package,
            final=final,
            status_at_gate=status_at_gate,
            closed_with=closed_with,
            kill=kill,
            usd_cap=usd_cap,
            final_model=version["model"] if version else None,
        )
    finally:
        for worker in workers:
            worker.stop()
        pool.close()
    scoring.write(card, out or default_out(task_id, mode, live))
    return card
