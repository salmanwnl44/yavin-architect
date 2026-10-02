"""Each test gets its own throwaway Postgres schema, so tests never share ledger rows."""

from __future__ import annotations

import json
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


@pytest.fixture(scope="session")
def admin() -> Iterator[psycopg.Connection]:
    with psycopg.connect(database_url(), autocommit=True) as conn:
        yield conn


@pytest.fixture
def dsn(admin: psycopg.Connection) -> Iterator[str]:
    schema = sql.Identifier(f"t_{uuid.uuid4().hex}")
    admin.execute(sql.SQL("CREATE SCHEMA {}").format(schema))
    yield make_conninfo(database_url(), options=f"-c search_path={schema.as_string()}")
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
