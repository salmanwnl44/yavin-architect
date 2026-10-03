"""The worker process of a golden run: `python -m architect.golden.worker ...`.

A real Temporal worker in its own process, so the runner can lose one and start another.
With --exit-after attack it ends itself abruptly (os._exit, no cleanup) once the first
attack phase has completed, refusing any further activity in the meantime so nothing is
half done when it dies: the session's history is then all the next worker has.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import threading
import time
from typing import Any

from temporalio.client import Client

from architect.db import database_url, ensure_schema, open_pool
from architect.gateway.gateway import Gateway, default_providers
from architect.golden.scripted import scripted_gateway
from architect.golden.tasks import load_task
from architect.ingestion.objectstore import LocalObjectStore
from architect.sessions.activities import SessionActivities
from architect.sessions.config import load_session_config
from architect.sessions.worker import build_worker

EXIT_AFTER_KILL = 7
READY = "worker ready"
DYING = "worker exiting after the attack phase"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="architect.golden.worker")
    parser.add_argument("--dsn", default=None, help="default: $ARCHITECT_DATABASE_URL")
    parser.add_argument("--address", required=True)
    parser.add_argument("--task-queue", required=True)
    parser.add_argument("--store", required=True)
    parser.add_argument("--task", required=True)
    parser.add_argument("--mode", required=True, choices=["review", "design"])
    parser.add_argument("--provider", required=True, choices=["live", "mock"])
    parser.add_argument("--exit-after", default=None, choices=["attack"])
    parser.add_argument(
        "--exit-when-stdin-closes",
        action="store_true",
        help="end when the parent closes this process's stdin (or dies)",
    )
    args = parser.parse_args(argv)

    pool = open_pool(args.dsn or database_url(), max_size=6)
    ensure_schema(pool)
    if args.provider == "live":
        gateway = Gateway(pool, providers=default_providers())
    else:
        gateway = scripted_gateway(pool, load_task(args.task).story(args.mode))
    activities = SessionActivities(
        pool, gateway, LocalObjectStore(args.store), load_session_config()
    )
    dying = threading.Event()

    def before(name: str, activity_args: dict[str, Any]) -> None:
        if dying.is_set():
            raise RuntimeError("this worker is exiting; the next one takes the activity")

    def after(name: str, activity_args: dict[str, Any], result: dict[str, Any]) -> None:
        if (
            args.exit_after == "attack"
            and name == "attack"
            and activity_args.get("phase") == "attack"
            and not dying.is_set()
        ):
            dying.set()
            print(DYING, flush=True)
            # let the attack's completion reach the server, then die without cleanup
            threading.Thread(
                target=lambda: (time.sleep(1.0), os._exit(EXIT_AFTER_KILL)), daemon=True
            ).start()

    activities.before_activity = before
    activities.after_activity = after

    if args.exit_when_stdin_closes:
        # The runner owns this process through its stdin: closing it (or the runner dying)
        # ends the worker. A plain kill is not enough where the interpreter runs behind a
        # launcher process (a Windows virtualenv): it would only kill the launcher.
        def until_stdin_closes() -> None:
            sys.stdin.buffer.read()
            os._exit(0)

        threading.Thread(target=until_stdin_closes, daemon=True).start()

    async def serve() -> None:
        client = await Client.connect(args.address)
        worker = build_worker(client, task_queue=args.task_queue, activities=activities)
        print(READY, flush=True)
        await worker.run()

    asyncio.run(serve())
    return 0


if __name__ == "__main__":
    sys.exit(main())
