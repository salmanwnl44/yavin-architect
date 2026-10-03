"""The Temporal worker: the workflow plus the activities, on one task queue."""

from __future__ import annotations

import asyncio
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
    if stop is None:
        await worker.run()
        return
    async with worker:
        await stop.wait()
