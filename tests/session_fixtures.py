"""Fixtures for the session tests: the brief, the scripted Architect, the planted-flaw model
and its repairs, and the helpers that run a session on a Temporal test environment.

The scripted Architect is a gateway provider that answers by (purpose, round) from the
marker line the agent puts in every system prompt, so CI never calls a real model and every
story (S1 planted flaws, S2 unrepairable, S3 rejection loop, S4 budget...) is a table.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import os
import re
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any

from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.testing import WorkflowEnvironment
from ulid import ULID

from architect import ledger
from architect.gateway.config import from_mapping
from architect.gateway.gateway import Gateway
from architect.gateway.providers.base import ProviderCall, ProviderResult
from architect.gateway.providers.mock import MockProvider
from architect.ingestion.config import IngestConfig
from architect.ingestion.objectstore import LocalObjectStore
from architect.sessions.activities import SessionActivities
from architect.sessions.config import SessionConfig, load_session_config
from architect.sessions.service import build_input, start
from architect.sessions.types import SessionInput
from architect.sessions.worker import build_worker

TASK_QUEUE = "architect-sessions-test"
SESSION_ID = "ses_TESTSESSION001"
STARTED_AT = "2026-10-03T09:00:00Z"

TEST_MODELS: dict[str, Any] = {
    "tiers": {
        "tier-cheap": [{"provider": "mock", "model": "mock-small", "family": "mock-a"}],
        "tier-mid": [{"provider": "mock", "model": "mock-medium", "family": "mock-a"}],
        "tier-frontier": [{"provider": "mock", "model": "mock-large", "family": "mock-a"}],
    },
    "prices": {
        "mock-small": {"input": 1.0, "output": 5.0},
        "mock-medium": {"input": 2.0, "output": 10.0},
        "mock-large": {"input": 4.0, "output": 20.0},
    },
    "retries": {"max_attempts": 3, "base_delay_s": 0.0, "max_delay_s": 0.0},
    "structured": {"max_retries": 2},
}

BRIEF = """# Brief: event ingest pipeline

Producers send events to an API gateway, which enqueues them on a queue; a worker consumes
the queue and writes each event to a datastore. Everything runs in one region on Kubernetes.

## Requirements

- The pipeline must sustain a peak of 5000 msg/s.
- Acknowledgement latency must stay under p99 < 200 ms.
- No acknowledged event may be lost.

## Unknowns

- The expected payload size per event.
"""

REQ_THROUGHPUT = "req_peak-throughput"
REQ_LATENCY = "req_ack-latency"
REQ_NO_LOSS = "req_no-loss"

FRAME: dict[str, Any] = {
    "requirements": [
        {
            "slug": "peak-throughput",
            "text": "The pipeline must sustain a peak of 5000 msg/s.",
            "metric": "throughput",
            "target": 5000,
            "unit": "msg/s",
            "quote": "sustain a peak of 5000 msg/s",
        },
        {
            "slug": "ack-latency",
            "text": "Acknowledgement latency must stay under p99 < 200 ms.",
            "metric": "latency",
            "target": 200,
            "unit": "ms",
            "quote": "p99 < 200 ms",
        },
        {
            "slug": "no-loss",
            "text": "No acknowledged event may be lost.",
            "quote": "No acknowledged event may be lost",
        },
    ],
    "constraints": [
        {
            "slug": "single-region-k8s",
            "text": "Everything runs in one region on Kubernetes.",
            "quote": "runs in one region on Kubernetes",
        }
    ],
    "unknowns": [{"slug": "payload-size", "text": "The expected payload size per event."}],
}

PRODUCERS, GATEWAY, QUEUE, WORKER, STORE = (
    "cmp_PRODUCERS01",
    "cmp_GATEWAY001",
    "cmp_QUEUE00001",
    "cmp_WORKER0001",
    "cmp_STORE00001",
)
IF_INGEST, IF_ENQUEUE, IF_CONSUME, IF_STORE = (
    "if_INGEST0001",
    "if_ENQUEUE001",
    "if_CONSUME001",
    "if_STORE00001",
)
FLW_INGEST, FLW_ENQUEUE, FLW_CONSUME, FLW_WRITE = (
    "flw_INGEST0001",
    "flw_ENQUEUE001",
    "flw_CONSUME001",
    "flw_WRITE00001",
)


def _component(cid: str, name: str, kind: str, refs: list[str], **extra: Any) -> dict[str, Any]:
    return {
        "op": "add_element",
        "element_type": "components",
        "element": {
            "id": cid,
            "name": name,
            "kind": kind,
            "stateful": extra.pop("stateful", False),
            "requirement_refs": refs,
            **extra,
        },
    }


def _interface(iid: str, contract: str, style: str, authn: str) -> dict[str, Any]:
    return {
        "op": "add_element",
        "element_type": "interfaces",
        "element": {
            "id": iid,
            "contract_ref": contract,
            "style": style,
            "idempotent": True,
            "authn": authn,
        },
    }


def _flow(fid: str, src: str, dst: str, via: str, **extra: Any) -> dict[str, Any]:
    return {
        "op": "add_element",
        "element_type": "flows",
        "element": {
            "id": fid,
            "from": src,
            "to": dst,
            "via_interface": via,
            "data_class": "internal",
            "rate": {"peak_qps": 5000, "payload_bytes": 2048},
            **extra,
        },
    }


def _capacity(cid: str, value: float) -> dict[str, Any]:
    return {
        "op": "add_element",
        "element_type": "capacity_params",
        "element": {
            "id": f"cap-{cid[4:].lower()}-max-qps",
            "name": f"{cid}.max_qps",
            "value": value,
            "unit": "qps",
            "applies_to": cid,
            "metric": "max_qps",
        },
    }


def _unit(uid: str, name: str, replicas: int) -> dict[str, Any]:
    return {
        "op": "add_element",
        "element_type": "deployment_units",
        "element": {"id": uid, "name": name, "runtime": "k8s", "replicas": replicas},
    }


def _satisfies(component: str, requirement: str) -> dict[str, Any]:
    return {
        "op": "add_link",
        "link_type": "satisfies",
        "link": {"component": component, "requirement": requirement},
    }


# The first draft, with two planted flaws: the worker is stateful without a durability class
# (C-007, critical) and the gateway->queue flow has no backpressure_ref (C-012, major).
DRAFT_V1: dict[str, Any] = {
    "rationale": "Gateway -> queue -> worker -> datastore; the queue absorbs bursts.",
    "ops": [
        _component(PRODUCERS, "Producers", "external", []),
        _component(
            GATEWAY,
            "API gateway",
            "gateway",
            [REQ_THROUGHPUT, REQ_LATENCY],
            interfaces=[IF_INGEST],
            deployment_unit="du-gateway",
        ),
        _component(
            QUEUE,
            "Event queue",
            "queue",
            [REQ_NO_LOSS],
            stateful=True,
            durability_class="durable",
            recovery={"rpo_s": 0, "rto_s": 30, "path": "replicated log, leader election"},
            interfaces=[IF_ENQUEUE],
            deployment_unit="du-queue",
        ),
        _component(
            WORKER,
            "Ingest worker",
            "service",
            [REQ_THROUGHPUT],
            stateful=True,
            interfaces=[IF_CONSUME],
            deployment_unit="du-worker",
        ),
        _component(
            STORE,
            "Event store",
            "datastore",
            [REQ_NO_LOSS],
            stateful=True,
            durability_class="durable",
            recovery={"rpo_s": 0, "rto_s": 60, "path": "restore from synchronous replica"},
            interfaces=[IF_STORE],
            deployment_unit="du-store",
        ),
        _interface(IF_INGEST, "openapi:ingest-api#/paths/~1events", "sync", "api_key"),
        _interface(
            IF_ENQUEUE, "asyncapi:events-topic#/channels/events", "async", "service_identity"
        ),
        _interface(
            IF_CONSUME, "asyncapi:events-topic#/channels/events", "async", "service_identity"
        ),
        _interface(IF_STORE, "proto:store.v1.Store/Put", "sync", "mtls"),
        _flow(FLW_INGEST, PRODUCERS, GATEWAY, IF_INGEST),
        _flow(FLW_ENQUEUE, GATEWAY, QUEUE, IF_ENQUEUE),
        _flow(FLW_CONSUME, QUEUE, WORKER, IF_CONSUME, backpressure_ref="consumer-prefetch-limit"),
        _flow(FLW_WRITE, WORKER, STORE, IF_STORE),
        _capacity(GATEWAY, 10000),
        _capacity(QUEUE, 20000),
        _capacity(WORKER, 8000),
        _capacity(STORE, 9000),
        _unit("du-gateway", "gateway pods", 3),
        _unit("du-queue", "queue brokers", 3),
        _unit("du-worker", "worker pods", 4),
        _unit("du-store", "store replicas", 3),
        _satisfies(GATEWAY, REQ_THROUGHPUT),
        _satisfies(GATEWAY, REQ_LATENCY),
        _satisfies(WORKER, REQ_THROUGHPUT),
        _satisfies(QUEUE, REQ_NO_LOSS),
        _satisfies(STORE, REQ_NO_LOSS),
    ],
    "decisions": [
        {
            "title": "Decouple ingestion from storage with a durable queue",
            "choice": "a replicated log between the gateway and the workers",
            "evidence_claims": [],
            "alternatives": [
                {
                    "option": "synchronous writes",
                    "rejected_because": "couples ack latency to the store",
                }
            ],
            "assumptions": [],
            "affected_elements": [GATEWAY, QUEUE, WORKER],
        },
        {
            "title": "A decision citing a claim outside the compiled context",
            "choice": "must not be recorded",
            "evidence_claims": ["clm_NOTINCONTEXT01"],
            "alternatives": [],
            "assumptions": [],
            "affected_elements": [STORE],
        },
    ],
}

# The same draft with an invalid element (no `kind`): the Arbiter refuses it with
# INVALID_MODEL_RESULT, and the architect gets the refusal back.
DRAFT_INVALID: dict[str, Any] = {
    "rationale": "a first attempt with a malformed component",
    "ops": [
        {
            "op": "add_element",
            "element_type": "components",
            "element": {
                "id": WORKER,
                "name": "Ingest worker",
                "stateful": True,
                "requirement_refs": [],
            },
        }
    ],
}

REPAIR_FIX: dict[str, Any] = {
    "rationale": "declare the worker's recovery and put backpressure on the enqueue path",
    "ops": [
        {
            "op": "update_element",
            "element_type": "components",
            "element_id": WORKER,
            "element": {
                "durability_class": "rebuildable",
                "recovery": {"rto_s": 60, "path": "replay from the queue offset"},
            },
        },
        {
            "op": "update_element",
            "element_type": "flows",
            "element_id": FLW_ENQUEUE,
            "element": {"backpressure_ref": "gateway-returns-429-above-queue-depth"},
        },
    ],
}

REPAIR_PARTIAL: dict[str, Any] = {
    "rationale": "backpressure on the enqueue path; the worker's durability is still open",
    "ops": [REPAIR_FIX["ops"][1]],
}

REPAIR_WAIVER: dict[str, Any] = {
    "rationale": "the worker holds only in-flight batches; asking for a waiver",
    "ops": [],
    "waiver_requests": [
        {
            "check_id": "C-007",
            "element_id": WORKER,
            "risk": "in-flight batches are lost on a worker crash and re-read from the queue",
        }
    ],
}

REPAIR_NOOP: dict[str, Any] = {"rationale": "nothing more to repair", "ops": []}

Story = dict[tuple[str, int], list[dict[str, Any]]]

S1_STORY: Story = {("frame", 0): [FRAME], ("draft", 1): [DRAFT_V1], ("repair", 1): [REPAIR_FIX]}
S2_STORY: Story = {
    ("frame", 0): [FRAME],
    ("draft", 1): [DRAFT_V1],
    ("repair", 1): [REPAIR_PARTIAL],
    ("repair", 2): [REPAIR_WAIVER],
    ("repair", 3): [REPAIR_NOOP],
}
S3_RETRY_STORY: Story = {
    ("frame", 0): [FRAME],
    ("draft", 1): [DRAFT_INVALID, DRAFT_V1],
    ("repair", 1): [REPAIR_FIX],
}
S3_GIVE_UP_STORY: Story = {
    ("frame", 0): [FRAME],
    ("draft", 1): [DRAFT_INVALID],
    ("repair", 1): [REPAIR_NOOP],
    ("repair", 2): [REPAIR_NOOP],
    ("repair", 3): [REPAIR_NOOP],
}

MARKER = re.compile(r"\[architect purpose=(\w+) round=(\d+)\]")


class ScriptedArchitect(MockProvider):
    """Answers by (purpose, round) from the agent's marker line; each successive call for the
    same key takes the next scripted output, and the last one repeats."""

    def __init__(self, story: Story, *, tokens_in: int = 1000, tokens_out: int = 500) -> None:
        super().__init__("mock")
        self.story = {key: list(outputs) for key, outputs in story.items()}
        self.served: dict[tuple[str, int], int] = {}
        self.tokens_in, self.tokens_out = tokens_in, tokens_out

    def complete(self, call: ProviderCall) -> ProviderResult:
        if self.fail_if_called:
            raise AssertionError("the scripted architect was called, and this test forbids it")
        self.calls.append(call)
        found = MARKER.search(call.system)
        assert found, "an architect call carries no purpose marker"
        key = (found.group(1), int(found.group(2)))
        outputs = self.story.get(key)
        assert outputs, f"the story has no output for {key}"
        n = self.served.get(key, 0)
        self.served[key] = n + 1
        output = outputs[min(n, len(outputs) - 1)]
        return ProviderResult(
            text=json.dumps(output, sort_keys=True),
            tokens_in=self.tokens_in,
            tokens_out=self.tokens_out,
        )


def test_session_config(**overrides: Any) -> SessionConfig:
    """The shipped presets with a small architect max_tokens, so budgets are easy to reason
    about in tests."""
    base = load_session_config()
    return dataclasses.replace(base, architect_max_tokens=512, task_queue=TASK_QUEUE, **overrides)


def make_gateway(pool, provider: MockProvider, mode: str = "live") -> Gateway:
    return Gateway(pool, from_mapping(TEST_MODELS), {"mock": provider}, mode=mode)


def make_activities(
    pool, gateway: Gateway, store_dir: Path, config: SessionConfig | None = None
) -> SessionActivities:
    return SessionActivities(
        pool, gateway, LocalObjectStore(store_dir), config or test_session_config(), IngestConfig()
    )


def session_input(
    project_id: str,
    *,
    preset: str = "quick",
    overrides: dict[str, Any] | None = None,
    session_id: str = SESSION_ID,
    brief: str = BRIEF,
    config: SessionConfig | None = None,
) -> SessionInput:
    return build_input(
        config or test_session_config(),
        project_id=project_id,
        brief=brief,
        preset=preset,
        overrides=overrides,
        session_id=session_id,
        started_at=STARTED_AT,
    )


async def wait_for(
    handle: WorkflowHandle,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    timeout: float = 180.0,
    interval: float = 0.1,
) -> dict[str, Any]:
    """Poll the status query until the predicate holds. Polling (rather than awaiting the
    result) keeps the time-skipping environment from skipping time on the session's timers."""
    deadline = time.monotonic() + timeout
    while True:
        view = await handle.query("status")
        if predicate(view):
            return view
        if view["status"] == "failed":
            await result_of(handle)  # raises with the failure chain
            raise AssertionError(f"the session failed: {view}")
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting; last status {view}")
        await asyncio.sleep(interval)


FINAL = ("approved", "rejected", "stopped_budget", "stopped_time", "cancelled", "failed")


def is_final_or_awaiting(view: dict[str, Any]) -> bool:
    return view["status"] in FINAL or view["status"] == "awaiting_approval"


async def run_to_end(
    client: Client,
    activities: SessionActivities,
    input: SessionInput,
    *,
    decide: str | None = "approve",
    task_queue: str = TASK_QUEUE,
) -> dict[str, Any]:
    """Run one session on a worker until it waits for the owner or ends, optionally decide,
    and return the workflow's final view."""
    async with build_worker(client, task_queue=task_queue, activities=activities):
        handle = await start(client, input, task_queue)
        view = await wait_for(handle, is_final_or_awaiting)
        if view["status"] == "awaiting_approval" and decide is not None:
            await handle.signal(decide)
        return await result_of(handle)


async def result_of(handle: WorkflowHandle) -> dict[str, Any]:
    """The workflow's result, or an AssertionError that names the whole failure chain."""
    try:
        return await handle.result()
    except WorkflowFailureError as failure:
        chain = []
        cause: BaseException | None = failure.cause
        while cause is not None:
            chain.append(f"{type(cause).__name__}: {getattr(cause, 'message', cause)}")
            cause = getattr(cause, "cause", None) or cause.__cause__
        history = await handle.fetch_history()
        tail = [
            f"{e.event_id}:{e.event_type}@{e.event_time.ToDatetime().isoformat()}"
            for e in history.events[-12:]
        ]
        closing = [
            str(e)[:1500]
            for e in history.events
            if "TIMED_OUT" in str(e.event_type) or "FAILED" in str(e.event_type)
        ]
        raise AssertionError(
            "workflow failed: "
            + " <- ".join(chain)
            + "\nhistory tail: "
            + " ".join(tail)
            + "\nclosing events: "
            + "\n".join(closing[-3:])
        ) from failure


@contextlib.contextmanager
def background_worker(activities: SessionActivities) -> Iterator[str]:
    """A time-skipping environment and a worker on a thread of their own, for the synchronous
    CLI and API tests. Yields the server's address."""
    ready, stop = threading.Event(), threading.Event()
    holder: dict[str, Any] = {}

    async def main() -> None:
        try:
            async with await WorkflowEnvironment.start_time_skipping() as env:
                async with build_worker(env.client, task_queue=TASK_QUEUE, activities=activities):
                    holder["address"] = env.client.service_client.config.target_host
                    ready.set()
                    while not stop.is_set():
                        await asyncio.sleep(0.1)
        except BaseException as error:  # noqa: BLE001 - surfaced to the test thread
            holder["error"] = error
            ready.set()

    thread = threading.Thread(target=lambda: asyncio.run(main()), daemon=True)
    thread.start()
    assert ready.wait(120), "the background worker never came up"
    if "error" in holder:
        raise AssertionError(f"the background worker failed: {holder['error']!r}")
    try:
        yield holder["address"]
    finally:
        stop.set()
        thread.join(60)


@contextlib.asynccontextmanager
async def dev_server() -> AsyncIterator[Client]:
    """A REAL Temporal dev server: the one at ARCHITECT_TEMPORAL_ADDRESS when CI started it
    (`temporal server start-dev`), else one the SDK downloads and starts locally. Never
    skipped."""
    address = os.environ.get("ARCHITECT_TEMPORAL_ADDRESS")
    if address:
        yield await Client.connect(address)
        return
    async with await WorkflowEnvironment.start_local() as env:
        yield env.client


def unique_session_id() -> str:
    return f"ses_{ULID()}"


def events_of(pool, project_id: str, *types: str) -> list[dict[str, Any]]:
    return [e for e in ledger.iter_events(pool, project_id) if not types or e["type"] in types]


def create_project(pool, project_id: str) -> None:
    ledger.create_project(pool, project_id)
