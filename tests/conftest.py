"""Each test gets its own throwaway Postgres schema, so tests never share ledger rows."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool

from architect import ledger
from architect.api import create_app
from architect.arbiter import Arbiter
from architect.db import database_url, ensure_schema, open_pool
from architect.state import STATE_TABLES

PROJECT = "p1"


def _extensions() -> set[str]:
    """The Postgres extensions the test database has or could install (age, vector)."""
    try:
        with psycopg.connect(database_url(), connect_timeout=5) as conn:
            rows = conn.execute(
                "SELECT name FROM pg_available_extensions WHERE name IN ('age', 'vector')"
            ).fetchall()
    except psycopg.Error:
        return set()
    return {row[0] for row in rows}


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Tests that need something only some machines have are DESELECTED where it is missing
    (reported as deselected: not failed, not skipped), also under `pytest -m live`:

    - live_openai_compat: an OpenAI-compatible server (OPENAI_COMPAT_BASE_URL);
    - needs_age, needs_pgvector: the Apache AGE or pgvector extension in the test database
      (CI's knowledge image has both; a plain Postgres runs the fallback backends);
    - fastembed_smoke: the optional fastembed package (CI's smoke job installs it).
    """
    import importlib.util

    extensions = _extensions()
    missing = {
        "live_openai_compat": not os.environ.get("OPENAI_COMPAT_BASE_URL"),
        "needs_age": "age" not in extensions,
        "needs_pgvector": "vector" not in extensions,
        "fastembed_smoke": importlib.util.find_spec("fastembed") is None,
    }
    unavailable = [
        item
        for item in items
        if any(absent and item.get_closest_marker(marker) for marker, absent in missing.items())
    ]
    if unavailable:
        config.hook.pytest_deselected(items=unavailable)
        items[:] = [item for item in items if item not in unavailable]


@pytest.fixture(autouse=True)
def _no_app_key_outside_live_tests(request: pytest.FixtureRequest, monkeypatch) -> None:
    """A real ARCHITECT_ANTHROPIC_API_KEY in the developer's shell must never reach a test that
    is not marked live: the default gateway would register the real provider and spend money.
    Tests that need the variable set it themselves."""
    if "live" not in request.keywords:
        monkeypatch.delenv("ARCHITECT_ANTHROPIC_API_KEY", raising=False)


@pytest.fixture(scope="session")
def admin() -> Iterator[psycopg.Connection]:
    with psycopg.connect(database_url(), autocommit=True) as conn:
        yield conn


@pytest.fixture
def dsn(admin: psycopg.Connection) -> Iterator[str]:
    schema = sql.Identifier(f"t_{uuid.uuid4().hex}")
    admin.execute(sql.SQL("CREATE SCHEMA {}").format(schema))
    yield make_conninfo(database_url(), options=f"-c search_path={schema.as_string()}")
    # An Apache AGE graph is a schema of its own: drop the ones this test's projects loaded.
    # On a connection that is closed right after, never on `admin`: a session that has
    # dropped an AGE graph can fail on a later, unrelated statement.
    try:
        graphs = admin.execute(
            sql.SQL("SELECT graph FROM {}.kg_age_sync").format(schema)
        ).fetchall()
    except psycopg.Error:
        graphs = []  # the schema was never initialized
    if graphs:
        with psycopg.connect(database_url(), autocommit=True) as age:
            for (graph,) in graphs:
                age.execute(
                    "SELECT ag_catalog.drop_graph(name, true) FROM ag_catalog.ag_graph "
                    "WHERE name = %s",
                    (graph,),
                )
    admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(schema))


@pytest.fixture
def pool(dsn: str) -> Iterator[ConnectionPool]:
    pool = open_pool(dsn, max_size=4)
    ensure_schema(pool)
    yield pool
    pool.close()


@pytest.fixture
def arbiter(pool: ConnectionPool) -> Arbiter:
    ledger.create_project(pool, PROJECT)
    return Arbiter(pool)


@pytest.fixture
def client(dsn: str) -> Iterator[TestClient]:
    with TestClient(create_app(dsn)) as client:
        assert client.post("/v1/projects", json={"project_id": PROJECT}).status_code == 201
        yield client


Fingerprint = dict[str, Any]


@pytest.fixture
def fingerprint(dsn: str) -> Iterator[Callable[[], Fingerprint]]:
    """Everything a commit could touch, read straight from the tables: events and arb_* rows."""

    with psycopg.connect(dsn, autocommit=True) as conn:

        def read() -> Fingerprint:
            out: Fingerprint = {}
            for table in ("events", *STATE_TABLES):
                rows = conn.execute(f"SELECT to_jsonb(t) FROM {table} t").fetchall()
                out[table] = sorted(json.dumps(row[0], sort_keys=True) for row in rows)
            return out

        yield read
