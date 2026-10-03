"""HTTP surface of the ledger and its read models. Every write goes through the Arbiter."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Body, FastAPI, File, Form, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from architect import __version__, ledger, projector, readmodel
from architect.arbiter import Arbiter
from architect.checks import runner
from architect.contracts import json_path, load_contracts
from architect.db import ensure_schema, open_pool
from architect.errors import Rejection
from architect.gateway.gateway import Gateway, default_providers
from architect.ingestion.config import load_ingest_config
from architect.ingestion.extract import PIPELINE_VERSION
from architect.ingestion.objectstore import LocalObjectStore
from architect.ingestion.pipeline import Pipeline
from architect.ingestion.sources import Ingestor
from architect.projector import Projector
from architect.sessions import service as session_service
from architect.sessions.config import load_session_config

MAX_PAGE = 1000


class ProjectIn(BaseModel):
    project_id: str = Field(min_length=1)


class SessionIn(BaseModel):
    brief: str = Field(min_length=1)
    preset: str = "quick"
    overrides: dict[str, Any] = Field(default_factory=dict)
    sources: list[str] = Field(default_factory=list)


class SteerIn(BaseModel):
    text: str = Field(min_length=1)


def create_app(
    database_url: str | None = None,
    gateway: Gateway | None = None,
    *,
    temporal_address: str | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        load_contracts()
        pool = open_pool(database_url)
        ensure_schema(pool)
        app.state.pool = pool
        app.state.arbiter = Arbiter(pool)
        app.state.gateway = gateway or Gateway(pool, providers=default_providers())
        app.state.object_store = LocalObjectStore()
        app.state.temporal_address = temporal_address
        app.state.temporal = None  # connected on the first session call
        try:
            yield
        finally:
            pool.close()

    async def temporal_client(app: FastAPI):
        if app.state.temporal is None:
            app.state.temporal = await session_service.connect(app.state.temporal_address)
        return app.state.temporal

    app = FastAPI(title="Yavin Architect ledger", version=__version__, lifespan=lifespan)

    @app.exception_handler(Rejection)
    def rejected(request: Request, exc: Rejection) -> JSONResponse:
        return JSONResponse(exc.body(), status_code=exc.http_status)

    @app.exception_handler(RequestValidationError)
    def malformed(request: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0]
        body = {
            "code": "MALFORMED_REQUEST",
            "detail": first["msg"],
            "json_path": json_path(first["loc"][1:]),
        }
        return JSONResponse(body, status_code=422)

    def require_project(request: Request, project_id: str) -> None:
        with request.app.state.pool.connection() as conn:
            if not ledger.project_exists(conn, project_id):
                raise Rejection("UNKNOWN_PROJECT", f"project {project_id!r} does not exist")

    @app.get("/healthz")
    def healthz(request: Request) -> dict[str, str]:
        with request.app.state.pool.connection() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}

    @app.post("/v1/projects", status_code=201)
    def create_project(request: Request, body: ProjectIn) -> dict[str, str]:
        if not ledger.create_project(request.app.state.pool, body.project_id):
            raise Rejection(
                "DUPLICATE_PROJECT", f"project {body.project_id!r} already exists", "$.project_id"
            )
        return {"project_id": body.project_id}

    @app.post("/v1/projects/{project_id}/events")
    def submit_event(
        request: Request, project_id: str, candidate: Annotated[dict[str, Any], Body()]
    ) -> JSONResponse:
        commit = request.app.state.arbiter.submit(project_id, candidate)
        return JSONResponse(
            {"event": commit.event, "replayed": commit.replayed},
            status_code=200 if commit.replayed else 201,
        )

    @app.get("/v1/projects/{project_id}/events")
    def list_events(
        request: Request,
        project_id: str,
        since_seq: Annotated[int | None, Query(ge=-1)] = None,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = 100,
    ) -> dict[str, Any]:
        """Events with seq > since_seq, in seq order. Pass the last seq seen to continue."""
        require_project(request, project_id)
        events = ledger.page(request.app.state.pool, project_id, since_seq, limit)
        return {"events": events, "next_since_seq": events[-1]["seq"] if events else since_seq}

    @app.get("/v1/projects/{project_id}/events/{event_id}")
    def get_event(request: Request, project_id: str, event_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        event = ledger.get_event(request.app.state.pool, project_id, event_id)
        if event is None:
            raise Rejection("UNKNOWN_EVENT", f"no event {event_id} in project {project_id!r}")
        return event

    @app.get("/v1/projects/{project_id}/head")
    def get_head(request: Request, project_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        return ledger.head(request.app.state.pool, project_id)

    # The read models (M2). These answer from proj_* only, so they are as fresh as the
    # projector; projections/status says how far behind that is.

    @app.get("/v1/projects/{project_id}/models/head")
    def get_head_model(request: Request, project_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        version = readmodel.head_model(request.app.state.pool, project_id)
        if version is None:
            raise Rejection(
                "MODEL_VERSION_NOT_FOUND", f"project {project_id!r} has no projected model version"
            )
        return version

    @app.get("/v1/projects/{project_id}/models/{version_id}")
    def get_model(request: Request, project_id: str, version_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        version = readmodel.model_version(request.app.state.pool, project_id, version_id)
        if version is None:
            raise Rejection(
                "MODEL_VERSION_NOT_FOUND",
                f"no projected model version {version_id} in project {project_id!r}",
            )
        return version

    @app.get("/v1/projects/{project_id}/claims")
    def list_claims(
        request: Request,
        project_id: str,
        status: str | None = None,
        load_bearing: bool | None = None,
        as_of_seq: Annotated[int | None, Query(ge=0)] = None,
        grade: str | None = None,
    ) -> dict[str, Any]:
        """Claims in commit order. as_of_seq answers "what did we believe once seq N landed";
        grade (quarantined | unverified | design_grade) lists by M5 grade."""
        require_project(request, project_id)
        if grade is not None:
            if grade not in ("quarantined", "unverified", "design_grade"):
                raise Rejection(
                    "MALFORMED_REQUEST",
                    "grade must be quarantined, unverified or design_grade",
                    "$.grade",
                )
            return {
                "claims": readmodel.claims_by_grade(request.app.state.pool, project_id, grade),
                "grade": grade,
            }
        statuses = load_contracts().schemas["claim.schema.json"]["$defs"]["EpistemicStatus"]["enum"]
        if status is not None and status not in statuses:
            raise Rejection(
                "MALFORMED_REQUEST", f"status must be one of {', '.join(statuses)}", "$.status"
            )
        claims = readmodel.list_claims(
            request.app.state.pool,
            project_id,
            status=status,
            load_bearing=load_bearing,
            as_of_seq=as_of_seq,
        )
        return {"claims": claims, "as_of_seq": as_of_seq}

    @app.get("/v1/projects/{project_id}/claims/{claim_id}")
    def get_claim(request: Request, project_id: str, claim_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        claim = readmodel.claim_detail(request.app.state.pool, project_id, claim_id)
        if claim is None:
            raise Rejection(
                "CLAIM_NOT_FOUND", f"no projected claim {claim_id} in project {project_id!r}"
            )
        return claim

    @app.get("/v1/projects/{project_id}/elements/{element_id}/why")
    def get_why(request: Request, project_id: str, element_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        trace = readmodel.why(request.app.state.pool, project_id, element_id)
        if trace is None:
            raise Rejection(
                "ELEMENT_NOT_FOUND",
                f"no element {element_id} in the projected head model of project {project_id!r}",
            )
        return trace

    @app.get("/v1/projects/{project_id}/projections/status")
    def get_projection_status(request: Request, project_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        return projector.status(request.app.state.pool, project_id)

    # Ingestion (M5). Sources in through the Arbiter; extraction through the gateway.

    @app.get("/v1/projects/{project_id}/sources")
    def list_sources(request: Request, project_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        Projector(request.app.state.pool).catch_up(project_id)
        return {"sources": readmodel.list_sources(request.app.state.pool, project_id)}

    @app.post("/v1/projects/{project_id}/sources", status_code=201)
    def add_source(
        request: Request,
        project_id: str,
        file: Annotated[UploadFile | None, File()] = None,
        origin: Annotated[str, Form()] = "user",
        github_url: Annotated[str | None, Form()] = None,
        ref: Annotated[str | None, Form()] = None,
    ) -> dict[str, Any]:
        """Multipart: a file (with an optional origin), or a github_url (with an optional ref)."""
        require_project(request, project_id)
        pool = request.app.state.pool
        ingestor = Ingestor(pool, request.app.state.object_store, load_ingest_config())
        if file is not None:
            import tempfile
            from pathlib import Path as _Path

            with tempfile.TemporaryDirectory() as tmp:
                path = _Path(tmp) / (file.filename or "upload.txt")
                path.write_bytes(file.file.read())
                source = ingestor.ingest_file(project_id, path, origin)
        elif github_url:
            source = ingestor.ingest_github(project_id, github_url, ref)
        else:
            raise Rejection("MALFORMED_REQUEST", "send a file or a github_url", "$")
        Projector(pool).catch_up(project_id)
        return source.__dict__

    @app.post("/v1/projects/{project_id}/sources/{source_id}/extract")
    def extract_source(
        request: Request,
        project_id: str,
        source_id: str,
        pipeline_version: Annotated[int, Query(ge=1)] = PIPELINE_VERSION,
    ) -> dict[str, Any]:
        require_project(request, project_id)
        pool = request.app.state.pool
        Projector(pool).catch_up(project_id)
        pipeline = Pipeline(
            pool, request.app.state.gateway, request.app.state.object_store, load_ingest_config()
        )
        try:
            report = pipeline.run(project_id, source_id, pipeline_version)
        except LookupError as missing:
            raise Rejection("SOURCE_NOT_FOUND", str(missing)) from missing
        Projector(pool).catch_up(project_id)
        body = dict(report.__dict__)
        body["quarantined"] = [list(x) for x in report.quarantined]
        return body

    # Design sessions (M6). Starts and signals go to Temporal; reads come from the session
    # read model and the package in the object store. The worker (`architect worker`) does
    # the work.

    @app.post("/v1/projects/{project_id}/sessions", status_code=201)
    async def start_session(request: Request, project_id: str, body: SessionIn) -> dict[str, Any]:
        require_project(request, project_id)
        config = load_session_config()
        try:
            session = session_service.build_input(
                config,
                project_id=project_id,
                brief=body.brief,
                preset=body.preset,
                overrides=body.overrides,
                sources=body.sources,
            )
        except ValueError as error:
            raise Rejection("MALFORMED_REQUEST", str(error), "$.preset") from error
        client = await temporal_client(request.app)
        await session_service.start(client, session, config.task_queue)
        return {
            "session_id": session.session_id,
            "preset": session.preset,
            "limits": session.limits,
        }

    @app.get("/v1/projects/{project_id}/sessions")
    def list_sessions(request: Request, project_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        return {"sessions": session_service.list_sessions(request.app.state.pool, project_id)}

    @app.get("/v1/projects/{project_id}/sessions/{session_id}")
    def get_session(request: Request, project_id: str, session_id: str) -> dict[str, Any]:
        require_project(request, project_id)
        pool = request.app.state.pool
        Projector(pool).catch_up(project_id)
        detail = session_service.show(pool, request.app.state.object_store, project_id, session_id)
        if detail is None:
            raise Rejection(
                "SESSION_NOT_FOUND", f"no session {session_id} in project {project_id!r}"
            )
        return detail

    @app.post("/v1/projects/{project_id}/sessions/{session_id}/{action}")
    async def signal_session(
        request: Request,
        project_id: str,
        session_id: str,
        action: str,
        body: SteerIn | None = None,
    ) -> dict[str, Any]:
        require_project(request, project_id)
        if action not in session_service.SIGNALS:
            raise Rejection(
                "MALFORMED_REQUEST",
                f"action must be one of {', '.join(session_service.SIGNALS)}",
                "$.action",
            )
        if action == "steer" and body is None:
            raise Rejection("MALFORMED_REQUEST", "steer needs a body with text", "$.text")
        if session_service.session_row(request.app.state.pool, project_id, session_id) is None:
            raise Rejection(
                "SESSION_NOT_FOUND", f"no session {session_id} in project {project_id!r}"
            )
        client = await temporal_client(request.app)
        await session_service.signal(client, session_id, action, body.text if body else None)
        return {"session_id": session_id, "signal": action}

    # The checks engine (M3). A run records its results through the Arbiter.

    @app.post("/v1/projects/{project_id}/models/{version_id}/checks")
    def run_checks(
        request: Request,
        project_id: str,
        version_id: str,
        as_of_seq: Annotated[int | None, Query(ge=0)] = None,
    ) -> dict[str, Any]:
        require_project(request, project_id)
        report = runner.run(request.app.state.pool, project_id, version_id, as_of_seq)
        return report.as_dict()

    @app.get("/v1/projects/{project_id}/models/{version_id}/checks")
    def get_checks(request: Request, project_id: str, version_id: str) -> dict[str, Any]:
        """The latest recorded result per check for the version."""
        require_project(request, project_id)
        pool = request.app.state.pool
        if readmodel.model_version(pool, project_id, version_id) is None:
            raise Rejection(
                "MODEL_VERSION_NOT_FOUND",
                f"no projected model version {version_id} in project {project_id!r}",
            )
        results = runner.recorded(pool, project_id, version_id)
        return {"model_version": version_id, "results": list(results.values())}

    @app.get("/v1/projects/{project_id}/models/{version_id}/gate")
    def get_gate(
        request: Request,
        project_id: str,
        version_id: str,
        as_of_seq: Annotated[int | None, Query(ge=0)] = None,
    ) -> dict[str, Any]:
        require_project(request, project_id)
        return runner.gate(request.app.state.pool, project_id, version_id, as_of_seq)

    return app
