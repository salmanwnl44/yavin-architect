"""The `architect` command line. Writes go through the Arbiter, like everything else."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from contextlib import closing
from pathlib import Path

from psycopg_pool import ConnectionPool

from architect import ledger, projector, readmodel
from architect.arbiter import ARBITER_STAMPED, Arbiter
from architect.checks import runner as check_runner
from architect.db import database_url, ensure_schema, open_pool
from architect.errors import Rejection
from architect.gateway.errors import GatewayError
from architect.gateway.gateway import Gateway, default_providers
from architect.gateway.request import GatewayRequest
from architect.ingestion.config import load_ingest_config
from architect.ingestion.extract import PIPELINE_VERSION
from architect.ingestion.objectstore import LocalObjectStore
from architect.ingestion.pipeline import Pipeline
from architect.ingestion.sources import Ingestor
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


def _print_results(report: check_runner.RunReport) -> None:
    print(
        f"checks on {report.model_version} as of seq {report.as_of_seq} "
        f"(catalog {report.catalog_version})"
    )
    for result in report.results:
        refs = ", ".join(result.element_refs) if result.element_refs else "-"
        how = (
            ""
            if result.event_id is None
            else (" (on record)" if result.replayed else " (recorded)")
        )
        print(f"  {result.check_id} {result.severity:<8} {result.status:<7} {refs}{how}")


def _check(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    try:
        report = check_runner.run(
            pool, args.project, args.version, args.as_of_seq, record=not args.dry_run
        )
    except Rejection as rejection:
        print(f"rejected {json.dumps(rejection.body())}", file=sys.stderr)
        return 1
    _print_results(report)
    if args.dry_run:
        print("dry run: nothing recorded")
    return 0


def _gate(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    try:
        report = check_runner.run(pool, args.project, args.version, args.as_of_seq)
        verdict = check_runner.gate(pool, args.project, args.version, report.as_of_seq)
    except Rejection as rejection:
        print(f"rejected {json.dumps(rejection.body())}", file=sys.stderr)
        return 1
    _print_results(report)
    print(f"gate IMPLEMENTATION_READY: {verdict['verdict']}")
    for reason in verdict["reasons"]:
        print(f"  blocking: {json.dumps(reason, sort_keys=True)}")
    for warning in verdict["warnings"]:
        print(f"  warning: {json.dumps(warning, sort_keys=True)}")
    return 0 if verdict["verdict"] == "ALLOWED" else 2


def _gateway(pool: ConnectionPool, args: argparse.Namespace) -> int:
    """architect gateway call | spend | calls."""
    gateway = Gateway(pool, providers=default_providers())
    if args.gateway_command == "call":
        schema = json.loads(Path(args.schema).read_text(encoding="utf-8")) if args.schema else None
        request = GatewayRequest(
            role=args.role,
            tier=args.tier,
            purpose=args.purpose,
            system=args.system,
            messages=[{"role": "user", "content": args.prompt}],
            output_schema=schema,
            max_tokens=args.max_tokens,
            scope={"session": args.session} if args.session else {},
        )
        try:
            response = gateway.call(request)
        except GatewayError as error:
            print(f"gateway refused: {error}", file=sys.stderr)
            return 1
        print(json.dumps(response.model_dump(), indent=2, sort_keys=True))
        return 0
    if args.gateway_command == "spend":
        spend = gateway.spend({"session": args.session})
        limits = gateway.limits({"session": args.session})
        print(json.dumps({"spend": spend, "limits": limits}, indent=2, sort_keys=True, default=str))
        return 0
    from architect.gateway.recorder import recent

    for row in recent(pool, args.limit):
        print(json.dumps(row, sort_keys=True, default=str))
    return 0


def _ingest_source(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    ingestor = Ingestor(pool, LocalObjectStore(), load_ingest_config())
    try:
        if args.github:
            source = ingestor.ingest_github(args.project, args.github, args.ref)
        else:
            source = ingestor.ingest_file(args.project, args.file or args.pdf, args.origin)
    except Rejection as rejection:
        print(f"rejected {json.dumps(rejection.body())}", file=sys.stderr)
        return 1
    Projector(pool).catch_up(args.project)
    print(json.dumps(source.__dict__, indent=2, sort_keys=True))
    return 0


def _extract(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    Projector(pool).catch_up(args.project)
    pipeline = Pipeline(
        pool, Gateway(pool, providers=default_providers()), LocalObjectStore(), load_ingest_config()
    )
    try:
        version = args.pipeline_version if args.pipeline_version is not None else PIPELINE_VERSION
        report = pipeline.run(args.project, args.source, version)
    except (Rejection, GatewayError, LookupError) as error:
        print(f"extraction stopped: {error}", file=sys.stderr)
        return 1
    Projector(pool).catch_up(args.project)
    print(json.dumps(report.__dict__, indent=2, sort_keys=True, default=list))
    return 0


def _sources(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    Projector(pool).catch_up(args.project)
    for row in readmodel.list_sources(pool, args.project):
        print(json.dumps(row, sort_keys=True))
    return 0


def _claims(pool: ConnectionPool, args: argparse.Namespace) -> int:
    if not _project_known(pool, args.project):
        return 1
    Projector(pool).catch_up(args.project)
    for row in readmodel.claims_by_grade(pool, args.project, args.grade):
        print(json.dumps(row, sort_keys=True, default=str))
    return 0


def _parse_overrides(items: Sequence[str]) -> dict:
    """--override key=value: numbers as numbers, `null`/`none` as uncapped."""
    out: dict = {}
    for item in items:
        key, _, raw = item.partition("=")
        if not key or not raw:
            raise SystemExit(f"--override needs key=value, not {item!r}")
        lowered = raw.lower()
        if lowered in ("null", "none"):
            out[key] = None
        else:
            try:
                out[key] = int(raw)
            except ValueError:
                out[key] = float(raw)
    return out


def _worker(pool: ConnectionPool, args: argparse.Namespace) -> int:
    """architect worker: the Temporal worker for design sessions (workflows + activities)."""
    import asyncio

    from architect.sessions.config import load_session_config, temporal_address
    from architect.sessions.worker import serve

    config = load_session_config()
    wide = open_pool(args.database_url or database_url(), max_size=10)
    try:
        print(
            f"session worker on task queue {args.task_queue or config.task_queue} "
            f"at {args.address or temporal_address()}",
            flush=True,
        )
        asyncio.run(
            serve(
                wide,
                Gateway(wide, providers=default_providers()),
                LocalObjectStore(),
                config=config,
                address=args.address,
                task_queue=args.task_queue,
            )
        )
    except KeyboardInterrupt:
        pass
    finally:
        wide.close()
    return 0


def _session(pool: ConnectionPool, args: argparse.Namespace) -> int:
    """architect session start | status | show | pause | resume | cancel | approve | reject |
    steer."""
    import asyncio

    from architect.sessions import service
    from architect.sessions.config import load_session_config

    config = load_session_config()
    command = args.session_command
    if command == "start":
        if not _project_known(pool, args.project):
            return 1
        try:
            session = service.build_input(
                config,
                project_id=args.project,
                brief=Path(args.brief).read_text(encoding="utf-8"),
                preset=args.preset,
                overrides=_parse_overrides(args.override),
                sources=args.source,
            )
        except ValueError as error:
            print(f"cannot start: {error}", file=sys.stderr)
            return 1

        async def go() -> None:
            client = await service.connect(args.address)
            await service.start(client, session, args.task_queue or config.task_queue)

        asyncio.run(go())
        print(
            json.dumps(
                {
                    "session_id": session.session_id,
                    "preset": session.preset,
                    "limits": session.limits,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if command == "status":

        async def query() -> dict:
            return await service.status(await service.connect(args.address), args.session)

        print(json.dumps(asyncio.run(query()), indent=2, sort_keys=True))
        return 0
    if command == "show":
        if not _project_known(pool, args.project):
            return 1
        Projector(pool).catch_up(args.project)
        detail = service.show(pool, LocalObjectStore(), args.project, args.session)
        if detail is None:
            print(f"session {args.session} is not in project {args.project!r}", file=sys.stderr)
            return 1
        row = detail["session"]
        print(f"session {args.session}: {row['status']} ({row['preset']}, round {row['round']})")
        print("timeline:")
        for step in detail["timeline"]:
            if step["kind"] == "phase_changed":
                print(f"  {step['ts']}  -> {step['phase']}")
            else:
                spend = (step["detail"] or {}).get("spend", {})
                print(f"  {step['ts']}  checkpoint {step['phase']} spend {json.dumps(spend)}")
        for round_ in detail["rounds"]:
            print(f"round {round_['round']}: {json.dumps(round_, sort_keys=True)}")
        gate = detail["gate"]
        print(f"gate: {gate['verdict'] if gate else 'not computed'}")
        for reason in (gate or {}).get("reasons", []):
            print(f"  blocking: {json.dumps(reason, sort_keys=True)}")
        print("open risks:")
        for risk in detail["open_risks"]:
            print(f"  {risk['id'] if isinstance(risk, dict) else risk}")
        print(f"package: {detail['package_key'] or 'none yet'}")
        return 0

    async def send() -> None:
        client = await service.connect(args.address)
        await service.signal(client, args.session, command, getattr(args, "text", None))

    try:
        asyncio.run(send())
    except ValueError as error:
        print(f"cannot {command}: {error}", file=sys.stderr)
        return 1
    print(f"{command} sent to {args.session}")
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

    check = sub.add_parser("check", help="run the check catalog on a model version")
    check.add_argument("--project", required=True)
    check.add_argument("--version", required=True, help="model version id")
    check.add_argument("--as-of-seq", type=int, default=None, help="default: the latest seq")
    check.add_argument("--dry-run", action="store_true", help="compute and print; record nothing")
    check.set_defaults(run=_check)

    gate = sub.add_parser(
        "gate", help="run the catalog, record the results, print the IMPLEMENTATION_READY verdict"
    )
    gate.add_argument("--project", required=True)
    gate.add_argument("--version", required=True, help="model version id")
    gate.add_argument("--as-of-seq", type=int, default=None, help="default: the latest seq")
    gate.set_defaults(run=_gate)

    gateway = sub.add_parser(
        "gateway", help="the model gateway: call a tier, read spend, list calls"
    )
    gateway_sub = gateway.add_subparsers(dest="gateway_command", required=True)
    call = gateway_sub.add_parser("call", help="one model call through the gateway")
    call.add_argument("--tier", required=True, choices=["tier-cheap", "tier-mid", "tier-frontier"])
    call.add_argument("--purpose", required=True)
    call.add_argument("--prompt", required=True)
    call.add_argument("--system", default="")
    call.add_argument("--role", default="operator")
    call.add_argument("--schema", default=None, help="a JSON Schema file for structured output")
    call.add_argument("--max-tokens", type=int, default=1024)
    call.add_argument("--session", default=None, help="charge the call to this session")
    spend = gateway_sub.add_parser("spend", help="what a session has spent, and its limits")
    spend.add_argument("--session", required=True)
    calls = gateway_sub.add_parser("calls", help="the most recent calls in the call log")
    calls.add_argument("--limit", type=int, default=20)
    gateway.set_defaults(run=_gateway)

    ingest_source = sub.add_parser("ingest-source", help="ingest a file, a PDF or a git repository")
    what = ingest_source.add_mutually_exclusive_group(required=True)
    what.add_argument("--file", type=Path)
    what.add_argument("--pdf", type=Path)
    what.add_argument("--github", help="repository URL (cloned with git)")
    ingest_source.add_argument("--project", required=True)
    ingest_source.add_argument("--ref", default=None, help="branch or tag for --github")
    ingest_source.add_argument(
        "--origin",
        default="user",
        choices=["user", "internal", "external_trusted"],
        help="taint origin for a local file (a repository is always external)",
    )
    ingest_source.set_defaults(run=_ingest_source)

    extract = sub.add_parser("extract", help="extract claims from a source through two passes")
    extract.add_argument("--project", required=True)
    extract.add_argument("--source", required=True, help="source id")
    extract.add_argument("--pipeline-version", type=int, default=None)
    extract.set_defaults(run=_extract)

    sources = sub.add_parser("sources", help="the project's ingested sources")
    sources.add_argument("--project", required=True)
    sources.set_defaults(run=_sources)

    claims = sub.add_parser("claims", help="the project's claims, by grade")
    claims.add_argument("--project", required=True)
    claims.add_argument(
        "--grade", default=None, choices=["quarantined", "unverified", "design_grade"]
    )
    claims.set_defaults(run=_claims)

    worker = sub.add_parser("worker", help="run the Temporal worker for design sessions")
    worker.add_argument(
        "--address", default=None, help="Temporal (default: $ARCHITECT_TEMPORAL_ADDRESS)"
    )
    worker.add_argument("--task-queue", default=None, help="default: config/presets.yaml")
    worker.set_defaults(run=_worker)

    session = sub.add_parser("session", help="start, steer and inspect a design session")
    session.add_argument(
        "--address", default=None, help="Temporal (default: $ARCHITECT_TEMPORAL_ADDRESS)"
    )
    session.add_argument("--task-queue", default=None)
    session_sub = session.add_subparsers(dest="session_command", required=True)
    start = session_sub.add_parser("start", help="start a session from a brief")
    start.add_argument("--project", required=True)
    start.add_argument("--brief", required=True, type=Path, help="a markdown file")
    start.add_argument("--preset", default="quick", choices=["quick", "deep", "exhaustive"])
    start.add_argument("--source", action="append", default=[], help="a path or src_ id to include")
    start.add_argument(
        "--override",
        action="append",
        default=[],
        help="max_rounds|wall_clock_minutes|tokens|usd=value",
    )
    status = session_sub.add_parser("status", help="the live status query")
    status.add_argument("--session", required=True)
    show = session_sub.add_parser("show", help="timeline, rounds, gate, open risks, package key")
    show.add_argument("--project", required=True)
    show.add_argument("--session", required=True)
    for name in ("pause", "resume", "cancel", "approve", "reject"):
        signal = session_sub.add_parser(name, help=f"send the {name} signal")
        signal.add_argument("--session", required=True)
    steer = session_sub.add_parser("steer", help="send guidance the next context will include")
    steer.add_argument("--session", required=True)
    steer.add_argument("--text", required=True)
    session.set_defaults(run=_session)

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
