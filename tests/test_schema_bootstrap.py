"""ensure_schema applies the DDL only when it has something to do.

Found in CI (M8): a command that started while a worker was in the middle of a gateway
transaction deadlocked with it, because every process re-applied schema.sql at startup and
its ALTER TABLE statements take exclusive table locks one table after another.
"""

from __future__ import annotations

import threading
from typing import Any

import psycopg
import pytest
from psycopg.rows import dict_row

from architect.cli import main
from architect.db import SCHEMA_KEY, ensure_schema


def waiting_for_lock(dsn: str, relation: str) -> bool:
    """Whether some backend is waiting for a lock on the relation right now."""
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM pg_locks WHERE NOT granted AND relation = to_regclass(%s)",
            (relation,),
        ).fetchone()
    return row["n"] > 0


def in_thread(fn: Any) -> tuple[threading.Thread, dict[str, Any]]:
    out: dict[str, Any] = {}

    def run() -> None:
        try:
            out["result"] = fn()
        except BaseException as error:  # noqa: BLE001 - the test inspects it
            out["error"] = error

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, out


def test_a_current_schema_is_not_touched_while_a_writer_holds_its_tables(dsn, pool):
    assert ensure_schema(pool) is False, "the pool fixture already applied it"
    with psycopg.connect(dsn) as writer:
        # a gateway transaction in flight: it has written gw_spend and gw_calls is next
        writer.execute("INSERT INTO gw_spend (scope_key, scope) VALUES ('{}', '{}')")
        writer.execute("LOCK TABLE gw_calls IN ROW EXCLUSIVE MODE")
        thread, out = in_thread(lambda: ensure_schema(pool))
        thread.join(30)
        assert not thread.is_alive(), "ensure_schema waited for the writer"
        assert out == {"result": False}
        # a whole command starts and finishes too
        thread, out = in_thread(lambda: main(["--database-url", dsn, "verify", "--project", "x"]))
        thread.join(30)
        assert not thread.is_alive() and "error" not in out
        writer.rollback()


def test_applying_the_ddl_under_a_writer_is_the_deadlock_and_the_fast_path_avoids_it(dsn, pool):
    """The failure CI saw, made to happen: the DDL takes gw_calls, then waits for gw_spend,
    which the writer holds; the writer then asks for gw_calls. Postgres breaks the cycle by
    failing one of the two. `force=True` is the path every start used to take."""
    with psycopg.connect(dsn) as writer:
        writer.execute("INSERT INTO gw_spend (scope_key, scope) VALUES ('{}', '{}')")
        thread, out = in_thread(lambda: ensure_schema(pool, force=True))
        for _ in range(600):  # until the DDL is queued behind the writer: a state that holds
            if waiting_for_lock(dsn, "gw_spend"):
                break
            threading.Event().wait(0.05)
        assert waiting_for_lock(dsn, "gw_spend"), "the forced DDL did not reach gw_spend"
        deadlocked: list[BaseException] = []
        try:
            writer.execute("LOCK TABLE gw_calls IN ROW EXCLUSIVE MODE")
            writer.rollback()
        except psycopg.errors.DeadlockDetected as error:
            deadlocked.append(error)
            writer.rollback()
        thread.join(60)
        assert not thread.is_alive()
        if isinstance(out.get("error"), psycopg.errors.DeadlockDetected):
            deadlocked.append(out["error"])
        assert len(deadlocked) == 1, "exactly one side of the cycle was failed by Postgres"
    # the same writer, the start every process now makes: nothing waits, nothing fails
    with psycopg.connect(dsn) as writer:
        writer.execute("INSERT INTO gw_spend (scope_key, scope) VALUES ('{}', '{}')")
        assert ensure_schema(pool) is False
        writer.execute("LOCK TABLE gw_calls IN ROW EXCLUSIVE MODE")
        writer.rollback()


def test_a_missing_table_or_a_changed_schema_is_applied_again(dsn, pool, capsys):
    def tables() -> set[str]:
        with pool.connection() as conn:
            rows = conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()"
            ).fetchall()
        return {row["tablename"] for row in rows}

    everything = tables()
    assert {"events", "gw_calls", "proj_graph_edges", "kg_communities", "schema_meta"} <= everything
    assert ensure_schema(pool) is False

    with pool.connection() as conn:
        conn.execute("DROP TABLE kg_age_sync")
    assert ensure_schema(pool) is True and tables() == everything
    assert ensure_schema(pool) is False

    # schema.sql changed since it was applied (an upgrade): the recorded hash differs
    with pool.connection() as conn:
        conn.execute(
            "UPDATE schema_meta SET value = 'an older schema' WHERE key = %s", (SCHEMA_KEY,)
        )
    assert ensure_schema(pool) is True and ensure_schema(pool) is False
    with pool.connection() as conn:
        recorded = conn.execute(
            "SELECT value FROM schema_meta WHERE key = %s", (SCHEMA_KEY,)
        ).fetchone()["value"]
    assert len(recorded) == 64

    # init-db applies it whatever the record says
    assert main(["--database-url", dsn, "init-db"]) == 0
    assert "schema is up to date" in capsys.readouterr().out
    assert ensure_schema(pool, force=True) is True


def test_two_processes_starting_on_an_empty_schema_apply_it_once(dsn):
    from architect.db import open_pool

    pools = [open_pool(dsn, max_size=1) for _ in range(4)]
    try:
        started = [in_thread(lambda p=p: ensure_schema(p)) for p in pools]
        for thread, _ in started:
            thread.join(120)
        results = [out.get("result") for _, out in started]
        assert all("error" not in out for _, out in started), started
        assert sorted(results) == [False, False, False, True], "one applied it, the rest saw it"
    finally:
        for pool in pools:
            pool.close()


@pytest.mark.needs_pgvector
def test_the_pgvector_table_is_created_once_and_never_locked_again(dsn, pool):
    from architect import ledger
    from architect.knowledge.config import load_knowledge_config
    from architect.knowledge.vectors import PgVectorIndex, pgvector_schema

    ledger.create_project(pool, "p1")
    config = load_knowledge_config()
    first = PgVectorIndex(pool, config, pgvector_schema(pool))
    first.upsert("p1", "claim", "m", [("a", "h", [1.0, 0.0, 0.0])])
    with psycopg.connect(dsn) as writer:
        writer.execute(
            "INSERT INTO emb_pgvector_3 (project_id, kind, id, model, embedding) "
            "VALUES ('p1', 'claim', 'b', 'm', '[0,1,0]'::public.vector)"
        )
        # another instance, its first use, while that insert is uncommitted: no DDL, no wait
        second = PgVectorIndex(pool, config, pgvector_schema(pool))
        thread, out = in_thread(lambda: second.search("p1", "claim", "m", [1.0, 0.0, 0.0], 5))
        thread.join(30)
        assert not thread.is_alive() and out["result"][0][0] == "a"
        writer.rollback()
