"""The `architect` command line. Writes go through the Arbiter, like everything else."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from contextlib import closing
from pathlib import Path

from psycopg_pool import ConnectionPool

from architect import ledger, projector
from architect.arbiter import ARBITER_STAMPED, Arbiter
from architect.db import database_url, ensure_schema, open_pool
from architect.errors import Rejection
from architect.projections import ProjectionError
from architect.projector import DEFAULT_BATCH, DEFAULT_POLL_SECONDS, Projector
from architect.rebuild import rebuild_state


def _ingest(pool: ConnectionPool, args: argparse.Namespace) -> int:
    """Submit each line of a ledger file as a candidate, keeping its event_id and ts."""
    arbiter = Arbiter(pool)
    ledger.create_project(pool, args.project)
    committed = replayed = renumbered = 0
    with open(args.path, encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"{args.path}:{line_no}: not JSON: {exc}", file=sys.stderr)
                return 1
            if not isinstance(event, dict):
                print(f"{args.path}:{line_no}: not a JSON object", file=sys.stderr)
                return 1
            file_seq = event.get("seq")
            candidate = {k: v for k, v in event.items() if k not in ARBITER_STAMPED}
            # The ledger is ingested into the project named on the command line.
            candidate["project_id"] = args.project
            try:
                commit = arbiter.submit(args.project, candidate)
            except Rejection as rejection:
                print(
                    f"{args.path}:{line_no}: rejected {json.dumps(rejection.body())}",
                    file=sys.stderr,
                )
                print(f"stopped: {committed} committed, {replayed} replayed before the rejection")
                return 1
            if commit.replayed:
                replayed += 1
            else:
                committed += 1
            if file_seq is not None and file_seq != commit.event["seq"]:
                renumbered += 1
    print(f"ingested {args.path} into {args.project}: {committed} committed, {replayed} replayed")
    if renumbered:
        print(f"note: {renumbered} events carry a different seq here than in the file")
    return 0


def _dump(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    count = 0
    out = open(args.output, "w", encoding="utf-8", newline="\n") if args.output else sys.stdout
    try:
        for event in ledger.iter_events(pool, args.project):
            out.write(json.dumps(event, ensure_ascii=False) + "\n")
            count += 1
    finally:
        if args.output:
            out.close()
    print(f"dumped {count} events from {args.project}", file=sys.stderr)
    return 0


def _rebuild_state(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    folded, diffs = rebuild_state(pool, args.project)
    print(f"rebuilt arb_* state for {args.project} from {folded} events")
    drift = [d for d in diffs if not d.empty]
    for diff in diffs:
        print(f"  {diff.table}: {len(diff.stale)} stale, {len(diff.missing)} missing")
        for row in diff.stale:
            print(f"    - {json.dumps(row, sort_keys=True)}")
        for row in diff.missing:
            print(f"    + {json.dumps(row, sort_keys=True)}")
    if drift:
        print("diff vs previous state: NOT EMPTY (the state has been replaced by the rebuild)")
        return 1
    print("diff vs previous state: empty")
    return 0


def _project(pool: ConnectionPool, args: argparse.Namespace) -> int:
    """The projector worker: fold committed events into the proj_* read models."""
    worker = Projector(pool, batch_size=args.batch_size)
    try:
        if args.once:
            print(f"projected {worker.catch_up()} events")
        else:
            print(f"projecting; polling every {args.poll_seconds:g}s between notifications")
            worker.run(poll_seconds=args.poll_seconds)
    except ProjectionError as error:
        print(f"projection stopped: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        pass
    return 0


def _rebuild_projections(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    try:
        folded = Projector(pool, batch_size=args.batch_size).rebuild(args.project)
    except ProjectionError as error:
        print(f"projection stopped: {error}", file=sys.stderr)
        return 1
    print(f"rebuilt proj_* read models for {args.project} from {folded} events")
    print(f"content hash: {projector.content_hash(pool, args.project)}")
    return 0


def _verify(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    checked, breaks = ledger.verify_chain(pool, args.project)
    for brk in breaks:
        print(f"BREAK seq={brk.seq} event_id={brk.event_id}: {brk.problem}")
    if breaks:
        print(f"hash chain for {args.project}: {len(breaks)} break(s) in {checked} events")
        return 1
    print(f"hash chain for {args.project}: OK ({checked} events)")
    return 0


def _init_db(pool: ConnectionPool, args: argparse.Namespace) -> int:
    print("schema is up to date")  # ensure_schema already ran in main()
    return 0


def _project_known(pool: ConnectionPool, project_id: str) -> bool:
    with pool.connection() as conn:
        known = ledger.project_exists(conn, project_id)
    if not known:
        print(f"project {project_id!r} does not exist", file=sys.stderr)
    return known


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="architect", description="Yavin Architect ledger tools")
    parser.add_argument(
        "--database-url", default=None, help="Postgres DSN (default: $ARCHITECT_DATABASE_URL)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="submit a ledger .jsonl through the Arbiter")
    ingest.add_argument("path", type=Path)
    ingest.add_argument("--project", required=True)
    ingest.set_defaults(run=_ingest)

    dump = sub.add_parser("dump", help="write all events of a project in seq order")
    dump.add_argument("--project", required=True)
    dump.add_argument("-o", "--output", type=Path, default=None, help="default: stdout")
    dump.set_defaults(run=_dump)

    rebuild = sub.add_parser("rebuild-state", help="drop and rebuild arb_* from the ledger")
    rebuild.add_argument("--project", required=True)
    rebuild.set_defaults(run=_rebuild_state)

    verify = sub.add_parser("verify", help="recompute the prev_hash chain and report breaks")
    verify.add_argument("--project", required=True)
    verify.set_defaults(run=_verify)

    project = sub.add_parser("project", help="run the projector that maintains the read models")
    project.add_argument("--once", action="store_true", help="catch up and exit")
    project.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="longest wait between passes when no notification arrives",
    )
    project.add_argument("--batch-size", type=int, default=DEFAULT_BATCH)
    project.set_defaults(run=_project)

    rebuild_projections = sub.add_parser(
        "rebuild-projections", help="drop and rebuild proj_* from the ledger, print their hash"
    )
    rebuild_projections.add_argument("--project", required=True)
    rebuild_projections.add_argument("--batch-size", type=int, default=DEFAULT_BATCH)
    rebuild_projections.set_defaults(run=_rebuild_projections)

    init_db = sub.add_parser("init-db", help="create or update the database schema")
    init_db.set_defaults(run=_init_db)

    serve = sub.add_parser("serve", help="run the HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    dsn = args.database_url or database_url()
    if args.command == "serve":
        import uvicorn

        from architect.api import create_app

        uvicorn.run(create_app(dsn), host=args.host, port=args.port)
        return 0
    with closing(open_pool(dsn, max_size=2)) as pool:
        ensure_schema(pool)
        return args.run(pool, args)


if __name__ == "__main__":
    sys.exit(main())
