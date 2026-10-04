"""The Temporal worker: the workflow plus the activities, on one task queue."""

from __future__ import annotations

import asyncio
import contextlib
from concurrent.futures import ThreadPoolExecutor

from psycopg_pool import ConnectionPool
from temporalio.client import Client
from temporalio.worker import Worker

from architect.gateway.gateway import Gateway
from architect.ingestion.objectstore import ObjectStore
from architect.sessions.activities import SessionActivities
from architect.sessions.config import SessionConfig, load_session_config, temporal_address
from architect.sessions.workflow import DesignSessionWorkflow


def build_worker(
    client: Client, *, task_queue: str, activities: SessionActivities, max_workers: int = 8
) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[DesignSessionWorkflow],
        activities=activities.all(),
        activity_executor=ThreadPoolExecutor(max_workers=max_workers),
    )


async def serve(
    pool: ConnectionPool,
    gateway: Gateway,
    store: ObjectStore,
    *,
    config: SessionConfig | None = None,
    address: str | None = None,
    task_queue: str | None = None,
    stop: asyncio.Event | None = None,
) -> None:
    """Run a worker until `stop` is set (or forever)."""
    config = config or load_session_config()
    client = await Client.connect(address or temporal_address())
    activities = SessionActivities(pool, gateway, store, config)
    worker = build_worker(client, task_queue=task_queue or config.task_queue, activities=activities)
    sweeper = asyncio.create_task(sweep_lost_calls(gateway))
    try:
        if stop is None:
            await worker.run()
            return
        async with worker:
            await stop.wait()
    finally:
        sweeper.cancel()


async def sweep_lost_calls(gateway: Gateway, every_s: float | None = None) -> None:
    """The periodic sweep: model calls that were started and never closed, by any worker or
    job, are closed as abandoned once they are older than the gateway's abandon_after_s."""
    every = every_s if every_s is not None else gateway.config.abandon_after_s
    while True:
        await asyncio.sleep(every)
        with contextlib.suppress(Exception):  # the next round tries again
            await asyncio.to_thread(gateway.sweep_abandoned)
