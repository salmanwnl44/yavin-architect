"""L4: a real quick session on a tiny brief with the real gateway and a small token cap.
Manual, deselected in CI.

Set ANTHROPIC_API_KEY in the shell (and ARCHITECT_TEMPORAL_ADDRESS when a dev server is
already running; otherwise the SDK starts one), then:

    pytest -m live -v -k l4

It prints the timeline summary, the gate, the open risks and the usd spent.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import pytest

from architect.gateway.config import load_config
from architect.gateway.gateway import Gateway, default_providers
from architect.ingestion.objectstore import LocalObjectStore
from architect.sessions.activities import SessionActivities
from architect.sessions.config import load_session_config
from architect.sessions.service import build_input, load_package, timeline
from conftest import PROJECT
from session_fixtures import create_project, dev_server, run_to_end, unique_session_id

pytestmark = pytest.mark.live

TINY_BRIEF = """# Brief: a URL shortener

A small HTTP service maps short codes to long URLs and redirects. Requirements: redirect
latency p99 < 50 ms; sustain 500 req/s at peak; no stored mapping may be lost.
"""


def test_l4_a_real_quick_session(pool, tmp_path, capsys):
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.fail("set ANTHROPIC_API_KEY in the shell to run the live tests")
    create_project(pool, PROJECT)
    gateway = Gateway(pool, load_config(), default_providers())
    store = LocalObjectStore(tmp_path / "objects")
    activities = SessionActivities(pool, gateway, store, load_session_config())
    session = build_input(
        load_session_config(),
        project_id=PROJECT,
        brief=TINY_BRIEF,
        preset="quick",
        overrides={"tokens": 150000, "usd": 3.0, "max_rounds": 2},
        session_id=unique_session_id(),
    )

    async def body() -> dict[str, Any]:
        async with dev_server() as client:
            return await run_to_end(client, activities, session, task_queue="architect-live")

    final = asyncio.run(body())
    package = load_package(store, final["package_key"]) if final["package_key"] else {}
    with capsys.disabled():
        print(f"\nsession {session.session_id}: {final['status']} ({final['outcome']})")
        for step in timeline(pool, PROJECT, session.session_id):
            if step["kind"] == "phase_changed":
                print(f"  {step['ts']} -> {step['phase']}")
        gate = package.get("gate") or {}
        print(f"gate: {gate.get('verdict')} {[r.get('check_id') for r in gate.get('reasons', [])]}")
        print("open risks:", [r["id"] for r in package.get("open_risks", [])])
        print(f"usd: {final['spend']['usd']:.4f} tokens: {final['spend']['tokens']}")
    assert final["status"] in ("approved", "stopped_budget", "stopped_time")
    assert final["package_key"]
