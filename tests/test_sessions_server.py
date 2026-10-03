"""Sessions on a REAL Temporal dev server: S11 (the planted-flaw session end to end) and S6
(a worker killed after the attack phase; a new worker resumes from history).

CI starts `temporal server start-dev` and sets ARCHITECT_TEMPORAL_ADDRESS; without it the
SDK downloads and starts a dev server of its own, so these tests never skip."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from architect import ledger, readmodel
from conftest import PROJECT
from session_fixtures import (
    S1_STORY,
    TASK_QUEUE,
    ScriptedArchitect,
    create_project,
    dev_server,
    events_of,
    is_final_or_awaiting,
    make_activities,
    make_gateway,
    result_of,
    run_to_end,
    session_input,
    unique_session_id,
    wait_for,
)

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def queue_name() -> str:
    return f"{TASK_QUEUE}-{uuid.uuid4().hex[:8]}"


# --- S11: S1 on the real dev server ----------------------------------------------------------


def test_s11_the_planted_flaw_session_runs_green_on_a_real_dev_server(pool, tmp_path):
    create_project(pool, PROJECT)
    architect_ = ScriptedArchitect(S1_STORY)
    activities = make_activities(pool, make_gateway(pool, architect_), tmp_path / "objects")
    session_id = unique_session_id()

    async def body() -> dict[str, Any]:
        async with dev_server() as client:
            return await run_to_end(
                client,
                activities,
                session_input(PROJECT, session_id=session_id),
                task_queue=queue_name(),
            )

    final = asyncio.run(body())
    assert final["status"] == "approved" and final["outcome"] == "completed"
    assert architect_.served == {("frame", 0): 1, ("draft", 1): 1, ("repair", 1): 1}
    phases = [e["payload"]["to"] for e in events_of(pool, PROJECT, "session.phase_changed")]
    assert phases == [
        "frame",
        "research",
        "model",
        "draft",
        "attack",
        "repair",
        "verify",
        "converge",
        "package",
    ]
    checks = {
        (e["payload"]["check_id"], e["payload"]["model_version"]): e["payload"]["status"]
        for e in events_of(pool, PROJECT, "check.result")
    }
    draft, repair = [
        e["payload"]["version_id"] for e in events_of(pool, PROJECT, "model.patch_committed")
    ]
    assert checks[("C-007", draft)] == "fail" and checks[("C-012", draft)] == "fail"
    assert checks[("C-007", repair)] == "pass" and checks[("C-012", repair)] == "pass"
    assert final["best_version"] == repair
    assert ledger.verify_chain(pool, PROJECT)[1] == []


# --- S6: kill and resume ----------------------------------------------------------------------

KILLED_WORKER = """
import asyncio, os, sys, threading, time
from pathlib import Path
dsn, address, queue, store_dir, tests_dir = sys.argv[1:6]
sys.path.insert(0, tests_dir)
from session_fixtures import S1_STORY, ScriptedArchitect, make_activities, make_gateway
from architect.db import open_pool
from architect.sessions.worker import build_worker
from temporalio.client import Client

pool = open_pool(dsn, max_size=4)
acts = make_activities(pool, make_gateway(pool, ScriptedArchitect(S1_STORY)), Path(store_dir))
dying = False

def after(name, args, result):
    global dying
    if name == "attack" and args.get("phase") == "attack" and not dying:
        dying = True
        print("attack done, dying", flush=True)
        threading.Thread(target=lambda: (time.sleep(1.0), os._exit(7)), daemon=True).start()

def before(name, args):
    if dying:
        raise RuntimeError("this worker is dying")

acts.after_activity = after
acts.before_activity = before

async def main():
    client = await Client.connect(address)
    worker = build_worker(client, task_queue=queue, activities=acts)
    print("worker ready", flush=True)
    await worker.run()

asyncio.run(main())
"""


def model_fingerprint(model: dict[str, Any]) -> str:
    """The model's content without the ids that name the run (version, project)."""
    content = {"elements": model["elements"], "links": model["links"]}
    return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()


def test_s6_a_killed_worker_is_resumed_by_a_new_one_without_duplicates(dsn, pool, tmp_path):
    create_project(pool, PROJECT)
    create_project(pool, "p2")
    queue = queue_name()
    reference = make_activities(
        pool, make_gateway(pool, ScriptedArchitect(S1_STORY)), tmp_path / "ref"
    )
    resumer = make_activities(
        pool, make_gateway(pool, ScriptedArchitect(S1_STORY)), tmp_path / "objects"
    )
    resumed_by: list[str] = []
    resumer.before_activity = lambda name, args: resumed_by.append(name)
    interrupted_id, uninterrupted_id = unique_session_id(), unique_session_id()

    async def body() -> tuple[dict[str, Any], dict[str, Any]]:
        async with dev_server() as client:
            address = client.service_client.config.target_host
            # the reference: the same script, uninterrupted, in another project
            steady = await run_to_end(
                client,
                reference,
                session_input("p2", session_id=uninterrupted_id),
                task_queue=queue_name(),
            )
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    KILLED_WORKER,
                    dsn,
                    address,
                    queue,
                    str(tmp_path / "objects"),
                    str(Path(__file__).parent),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                line = await asyncio.to_thread(process.stdout.readline)
                assert "worker ready" in line, process.stderr.read()
                from architect.sessions.service import start

                handle = await start(
                    client, session_input(PROJECT, session_id=interrupted_id), queue
                )
                line = await asyncio.to_thread(process.stdout.readline)
                assert "attack done" in line, process.stderr.read()
                await asyncio.to_thread(process.wait, 60)
                assert process.returncode == 7, "the worker killed itself after the attack phase"
            finally:
                if process.poll() is None:
                    process.kill()
                process.stdout.close()
                process.stderr.close()
            # a new worker resumes from history
            from architect.sessions.worker import build_worker

            async with build_worker(client, task_queue=queue, activities=resumer):
                view = await wait_for(handle, is_final_or_awaiting, timeout=240)
                assert view["status"] == "awaiting_approval"
                await handle.signal("approve")
                return steady, await result_of(handle)

    steady, final = asyncio.run(body())
    assert final["status"] == "approved" and final["outcome"] == "completed"
    assert "attack" not in resumed_by[:1] and "repair" in resumed_by, "the new worker continued"
    assert "frame" not in resumed_by and "draft" not in resumed_by, "finished phases never re-ran"

    events = events_of(pool, PROJECT)
    assert [e["seq"] for e in events] == list(range(len(events))), "dense seq"
    keys = [e["idempotency_key"] for e in events]
    assert len(keys) == len(set(keys)), "every idempotency key unique"
    assert len(events_of(pool, PROJECT, "model.patch_committed")) == 2
    assert len({e["payload"]["to"] for e in events_of(pool, PROJECT, "session.phase_changed")}) == 9
    assert ledger.verify_chain(pool, PROJECT)[1] == []

    resumed_model = readmodel.model_version(pool, PROJECT, final["best_version"])["model"]
    steady_model = readmodel.model_version(pool, "p2", steady["best_version"])["model"]
    assert model_fingerprint(resumed_model) == model_fingerprint(steady_model)
