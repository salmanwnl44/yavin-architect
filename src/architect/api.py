"""HTTP surface of the ledger. Every write goes through the Arbiter."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Body, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from architect import __version__, ledger
from architect.arbiter import Arbiter
from architect.contracts import json_path, load_contracts
from architect.db import ensure_schema, open_pool
from architect.errors import Rejection

MAX_PAGE = 1000


class ProjectIn(BaseModel):
    project_id: str = Field(min_length=1)


def create_app(database_url: str | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        load_contracts()
        pool = open_pool(database_url)
        ensure_schema(pool)
        app.state.pool = pool
        app.state.arbiter = Arbiter(pool)
        try:
            yield
        finally:
            pool.close()

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

    return app
