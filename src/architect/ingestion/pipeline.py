"""The extraction job: parse, pass A, pass B, commit. Plain, idempotent, resumable.

Each job is keyed by (project, content hash, pipeline version). Every stage leaves its work in
the ing_* tables and marks itself done, so a crash leaves finished stages finished and a re-run
continues; the Arbiter's idempotency keys make the commit stage safe to repeat. All model
calls go through the gateway under the scope {"session": "ingest:<job_id>"}.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from architect.gateway.gateway import Gateway
from architect.ingestion import commit as committing
from architect.ingestion.config import IngestConfig
from architect.ingestion.extract import (
    PIPELINE_VERSION,
    Candidate,
    agreement,
    data_block,
    extraction_request,
    normalize_candidates,
    pack,
)
from architect.ingestion.normalize import typed_id
from architect.ingestion.objectstore import ObjectStore
from architect.ingestion.parse import MEDIA_REPO, Segment, media_type_for, parse_file
from architect.ingestion.sources import Source, repo_files

STAGES = ("parse", "pass_a", "pass_b", "commit", "done")


@dataclass
class ExtractionReport:
    job_id: str
    source_id: str
    pipeline_version: int
    resumed_from: str
    metrics: dict[str, int] = field(default_factory=dict)
    committed: list[str] = field(default_factory=list)
    quarantined: list[tuple[str, str]] = field(default_factory=list)
    pass_b_excluded_families: list[str] = field(default_factory=list)


class Pipeline:
    def __init__(
        self, pool: ConnectionPool, gateway: Gateway, store: ObjectStore, config: IngestConfig
    ) -> None:
        self._pool = pool
        self._gateway = gateway
        self._store = store
        self._config = config
        # Test hook: called after each pass-B segment is stored. Raising simulates a crash.
        self.after_pass_b_segment: Callable[[str], None] | None = None

    # ---------------------------------------------------------------- sources and segments
    def source(self, project_id: str, source_id: str) -> Source:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT source_id, uri, content_hash, media_type, license, taint_origin "
                "FROM proj_sources WHERE project_id = %s AND source_id = %s",
                (project_id, source_id),
            ).fetchone()
        if row is None:
            raise LookupError(f"source {source_id} is not projected in project {project_id!r}")
        return Source(
            source_id=row["source_id"],
            content_hash=row["content_hash"],
            uri=row["uri"],
            media_type=row["media_type"],
            taint_origin=row["taint_origin"],
            license=row["license"],
            created=False,
        )

    def segments_for(self, source: Source) -> list[Segment]:
        """Parse the source's bytes from the object store. Deterministic."""
        data = self._store.get(source.content_hash)
        if source.media_type != MEDIA_REPO:
            # a single file's locators carry its name, taken from the uri's last path element
            name = source.uri.rsplit("/", 1)[-1].split("@", 1)[0] or "document"
            return parse_file(source.source_id, data, source.media_type, name)
        segments: list[Segment] = []
        for file in repo_files(data):
            parsed = parse_file(
                source.source_id, self._store.get(file.sha256), media_type_for(file.path), file.path
            )
            for segment in parsed:
                segments.append(
                    Segment(
                        segment.segment_id,
                        segment.locator,
                        segment.kind,
                        segment.text,
                        len(segments),
                    )
                )
        return segments

    def parse(self, source: Source, rebuild: bool = False) -> list[Segment]:
        """Store the segments (Stage 2). With rebuild, drop and re-parse."""
        segments = self.segments_for(source)
        with self._pool.connection() as conn, conn.transaction():
            if rebuild:
                conn.execute("DELETE FROM ing_segments WHERE source_id = %s", (source.source_id,))
            for segment in segments:
                conn.execute(
                    "INSERT INTO ing_segments (source_id, segment_id, locator, kind, text, "
                    "position) "
                    "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (source_id, segment_id) DO UPDATE "
                    "SET locator = EXCLUDED.locator, kind = EXCLUDED.kind, text = EXCLUDED.text, "
                    "position = EXCLUDED.position",
                    (
                        source.source_id,
                        segment.segment_id,
                        segment.locator,
                        segment.kind,
                        segment.text,
                        segment.position,
                    ),
                )
        return segments

    def stored_segments(self, source_id: str) -> list[Segment]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT segment_id, locator, kind, text, position FROM ing_segments "
                "WHERE source_id = %s ORDER BY position",
                (source_id,),
            ).fetchall()
        return [
            Segment(r["segment_id"], r["locator"], r["kind"], r["text"], r["position"])
            for r in rows
        ]

    # ---------------------------------------------------------------- the job
    def run(
        self, project_id: str, source_id: str, pipeline_version: int = PIPELINE_VERSION
    ) -> ExtractionReport:
        source = self.source(project_id, source_id)
        job_id = typed_id("job", project_id, source.content_hash, str(pipeline_version))
        stage = self._open_job(job_id, project_id, source, pipeline_version)
        report = ExtractionReport(job_id, source_id, pipeline_version, resumed_from=stage)
        session = f"ingest:{job_id}"
        # a job that resumes closes the calls an earlier run started and never recorded
        self._gateway.sweep_abandoned(scope={"session": session})

        if stage == "parse":
            self.parse(source)
            stage = self._advance(job_id, "pass_a")
        segments = self.stored_segments(source_id)
        by_locator = {s.locator: s for s in segments}
        by_id = {s.segment_id: s for s in segments}

        if stage == "pass_a":
            self._pass_a(job_id, source, segments, by_locator, session)
            stage = self._advance(job_id, "pass_b")
        pass_a = self._candidates(job_id, "A")
        # document order, so pass B and the commit walk the source the way a reader would
        pass_a.sort(key=lambda item: by_id[item[0].segment_id].position)

        if stage == "pass_b":
            report.pass_b_excluded_families = self._pass_b(job_id, source, pass_a, by_id, session)
            stage = self._advance(job_id, "commit")
        pass_b = self._candidates(job_id, "B")

        if stage == "commit":
            self._commit(
                job_id, project_id, source, pass_a, pass_b, by_id, pipeline_version, report
            )
            stage = self._advance(job_id, "done")
        else:
            report.committed = self._metric_list(job_id, "committed_ids")
            report.quarantined = [tuple(x) for x in self._metric_list(job_id, "quarantined")]
        report.metrics = self.metrics(job_id)
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT pass_b_excluded_families FROM ing_jobs WHERE job_id = %s", (job_id,)
            ).fetchone()
        report.pass_b_excluded_families = row["pass_b_excluded_families"] or []
        return report

    def progress(
        self, project_id: str, source_id: str, pipeline_version: int = PIPELINE_VERSION
    ) -> dict[str, Any]:
        """How far a job got: its stage, the segments of the source, those pass A found
        candidates in, and those pass B has answered."""
        source = self.source(project_id, source_id)
        job_id = typed_id("job", project_id, source.content_hash, str(pipeline_version))
        job = self.job(job_id)
        with self._pool.connection() as conn:
            total = conn.execute(
                "SELECT count(*) AS n FROM ing_segments WHERE source_id = %s", (source_id,)
            ).fetchone()["n"]
            with_candidates = conn.execute(
                "SELECT count(DISTINCT segment_id) AS n FROM ing_candidates "
                "WHERE job_id = %s AND pass = 'A'",
                (job_id,),
            ).fetchone()["n"]
            answered = conn.execute(
                "SELECT count(*) AS n FROM ing_pass_b WHERE job_id = %s", (job_id,)
            ).fetchone()["n"]
        return {
            "job_id": job_id,
            "stage": job["stage"] if job else None,
            "segments": total,
            "segments_with_candidates": with_candidates,
            "segments_answered": answered,
        }

    def commit_processed(
        self, project_id: str, source_id: str, pipeline_version: int = PIPELINE_VERSION
    ) -> ExtractionReport:
        """Commit what BOTH passes have finished so far, for a job that stopped part-way (its
        budget ran out in pass B). Only candidates of segments pass B has answered are judged:
        agreed ones are committed, disagreeing ones quarantined. Candidates of segments pass B
        has not reached are left alone, neither committed nor quarantined, until the job is
        resumed. Idempotent, and it does not move the job's stage: a later run() continues
        pass B and commits the rest."""
        source = self.source(project_id, source_id)
        job_id = typed_id("job", project_id, source.content_hash, str(pipeline_version))
        job = self.job(job_id)
        stage = job["stage"] if job else "parse"
        report = ExtractionReport(job_id, source_id, pipeline_version, resumed_from=stage)
        if job is None or stage in ("parse", "pass_a"):
            report.metrics = self.metrics(job_id) if job else {}
            return report  # nothing has been through both passes yet
        segments = self.stored_segments(source_id)
        by_id = {s.segment_id: s for s in segments}
        with self._pool.connection() as conn:
            answered = {
                r["segment_id"]
                for r in conn.execute(
                    "SELECT segment_id FROM ing_pass_b WHERE job_id = %s", (job_id,)
                ).fetchall()
            }
        pass_a = self._candidates(job_id, "A")
        pass_a.sort(key=lambda item: by_id[item[0].segment_id].position)
        ready = [item for item in pass_a if item[0].segment_id in answered]
        self._commit(
            job_id,
            project_id,
            source,
            ready,
            self._candidates(job_id, "B"),
            by_id,
            pipeline_version,
            report,
        )
        report.metrics = self.metrics(job_id)
        return report

    def _open_job(self, job_id: str, project_id: str, source: Source, version: int) -> str:
        with self._pool.connection() as conn, conn.transaction():
            conn.execute(
                "INSERT INTO ing_jobs (job_id, project_id, source_id, content_hash, "
                "pipeline_version, "
                "stage, status, attempts) VALUES (%s, %s, %s, %s, %s, 'parse', 'running', 0) "
                "ON CONFLICT (job_id) DO NOTHING",
                (job_id, project_id, source.source_id, source.content_hash, version),
            )
            row = conn.execute(
                "UPDATE ing_jobs SET attempts = attempts + 1, status = 'running' WHERE job_id = %s "
                "RETURNING stage",
                (job_id,),
            ).fetchone()
        return row["stage"]

    def _advance(self, job_id: str, stage: str) -> str:
        status = "done" if stage == "done" else "running"
        with self._pool.connection() as conn:
            conn.execute(
                "UPDATE ing_jobs SET stage = %s, status = %s WHERE job_id = %s",
                (stage, status, job_id),
            )
        return stage

    # ---------------------------------------------------------------- pass A
    def _pass_a(
        self,
        job_id: str,
        source: Source,
        segments: list[Segment],
        by_locator: dict[str, Segment],
        session: str,
    ) -> None:
        total = dropped = 0
        for batch in pack(segments, self._config.pack_chars):
            blocks = [
                data_block(s, source.source_id, self._config.max_segment_chars) for s in batch
            ]
            request = extraction_request(
                tier=self._config.pass_a_tier,
                purpose="extract-claims-a",
                blocks=blocks,
                taint_origin=source.taint_origin,
                session=session,
                exclude_families=[],
            )
            response = self._gateway.call(request)
            candidates, lost = normalize_candidates(response.parsed or {"claims": []}, by_locator)
            total += len(candidates)
            dropped += lost
            self._store_candidates(job_id, "A", candidates, response.call_id, response.family)
        self._metric(job_id, "pass_a_candidates", total)
        self._metric(job_id, "dropped_quote_a", dropped)

    # ---------------------------------------------------------------- pass B
    def _pass_b(
        self,
        job_id: str,
        source: Source,
        pass_a: list[tuple[Candidate, str, str]],
        by_id: dict[str, Segment],
        session: str,
    ) -> list[str]:
        families_a = {family for _, _, family in pass_a}
        tier_b = self._gateway.config.tiers.get(self._config.pass_b_tier, ())
        other_family = any(c.family not in families_a for c in tier_b)
        excluded = sorted(families_a) if other_family and families_a else []
        with self._pool.connection() as conn:
            conn.execute(
                "UPDATE ing_jobs SET pass_b_excluded_families = %s WHERE job_id = %s",
                (Jsonb(excluded), job_id),
            )
            done = {
                r["segment_id"]
                for r in conn.execute(
                    "SELECT segment_id FROM ing_pass_b WHERE job_id = %s", (job_id,)
                ).fetchall()
            }
        segment_ids = list(dict.fromkeys(c.segment_id for c, _, _ in pass_a))
        for metric in ("pass_b_calls", "pass_b_candidates", "dropped_quote_b"):
            self._metric(job_id, metric, 0, add=True)  # present even when nothing is asked
        for segment_id in segment_ids:
            if segment_id in done:
                continue
            segment = by_id[segment_id]
            request = extraction_request(
                tier=self._config.pass_b_tier,
                purpose="extract-claims-b",
                blocks=[data_block(segment, source.source_id, self._config.max_segment_chars)],
                taint_origin=source.taint_origin,
                session=session,
                exclude_families=excluded,
            )
            response = self._gateway.call(request)
            candidates, lost = normalize_candidates(
                response.parsed or {"claims": []}, {segment.locator: segment}
            )
            with self._pool.connection() as conn, conn.transaction():
                self._store_candidates(
                    job_id, "B", candidates, response.call_id, response.family, conn
                )
                conn.execute(
                    "INSERT INTO ing_pass_b (job_id, segment_id, call_id) VALUES (%s, %s, %s) "
                    "ON CONFLICT DO NOTHING",
                    (job_id, segment_id, response.call_id),
                )
            # counted per segment, so a job stopped part-way (a crash, its budget) has
            # counted exactly the segments it finished
            self._metric(job_id, "pass_b_calls", 1, add=True)
            self._metric(job_id, "pass_b_candidates", len(candidates), add=True)
            self._metric(job_id, "dropped_quote_b", lost, add=True)
            if self.after_pass_b_segment is not None:
                self.after_pass_b_segment(segment_id)
        return excluded

    # ---------------------------------------------------------------- commit
    def _commit(
        self,
        job_id: str,
        project_id: str,
        source: Source,
        pass_a: list[tuple[Candidate, str, str]],
        pass_b: list[tuple[Candidate, str, str]],
        by_id: dict[str, Segment],
        pipeline_version: int,
        report: ExtractionReport,
    ) -> None:
        recorded_at = self._source_ts(project_id, source.source_id)
        b_candidates = [c for c, _, _ in pass_b]
        seen: set[str] = set()
        agreed = quarantined = 0
        for candidate, call_id, _family in pass_a:
            claim = committing.build_claim(
                source,
                candidate,
                segment_kind=by_id[candidate.segment_id].kind,
                model_tier=self._config.pass_a_tier,
                prompt_hash=self._prompt_hash(call_id),
                pipeline_version=pipeline_version,
                recorded_at=recorded_at,
            )
            if claim["id"] in seen:
                continue
            seen.add(claim["id"])
            reason = agreement(candidate, b_candidates)
            committing.propose(self._pool, project_id, claim)
            if reason is None:
                committing.commit_claim(self._pool, project_id, claim)
                agreed += 1
                report.committed.append(claim["id"])
            else:
                quarantined += 1
                report.quarantined.append((claim["id"], reason))
                same_segment = [
                    c.as_dict() for c in b_candidates if c.segment_id == candidate.segment_id
                ]
                with self._pool.connection() as conn:
                    conn.execute(
                        "INSERT INTO ing_quarantine (job_id, claim_id, segment_id, reason, "
                        "pass_a, pass_b) "
                        "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (job_id, claim_id) DO NOTHING",
                        (
                            job_id,
                            claim["id"],
                            candidate.segment_id,
                            reason,
                            Jsonb(candidate.as_dict()),
                            Jsonb(same_segment),
                        ),
                    )
        self._metric(job_id, "agreed", agreed)
        self._metric(job_id, "quarantined", quarantined)
        self._metric(job_id, "committed", len(report.committed))
        self._metric_list_set(job_id, "committed_ids", report.committed)
        self._metric_list_set(job_id, "quarantined", [list(x) for x in report.quarantined])

    # ---------------------------------------------------------------- storage helpers
    def _store_candidates(
        self,
        job_id: str,
        pass_: str,
        candidates: list[Candidate],
        call_id: str,
        family: str,
        conn=None,
    ) -> None:
        rows = [
            (
                job_id,
                pass_,
                c.segment_id,
                typed_id(
                    "cand",
                    pass_,
                    c.segment_id,
                    c.spo,
                    json.dumps(c.magnitude, sort_keys=True),
                    json.dumps(c.conditions, sort_keys=True),
                ),
                Jsonb(c.as_dict()),
                call_id,
                family,
                i,
            )
            for i, c in enumerate(candidates)
        ]
        statement = (
            "INSERT INTO ing_candidates (job_id, pass, segment_id, candidate_id, candidate, "
            "call_id, family, position) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (job_id, pass, candidate_id) DO NOTHING"
        )
        if conn is not None:
            for row in rows:
                conn.execute(statement, row)
            return
        with self._pool.connection() as conn2, conn2.transaction():
            for row in rows:
                conn2.execute(statement, row)

    def _candidates(self, job_id: str, pass_: str) -> list[tuple[Candidate, str, str]]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT candidate, call_id, family FROM ing_candidates "
                "WHERE job_id = %s AND pass = %s "
                "ORDER BY segment_id, position",
                (job_id, pass_),
            ).fetchall()
        return [(Candidate.from_dict(r["candidate"]), r["call_id"], r["family"]) for r in rows]

    def _prompt_hash(self, call_id: str) -> str:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT prompt_hash FROM gw_calls WHERE call_id = %s", (call_id,)
            ).fetchone()
        return row["prompt_hash"] if row else "unrecorded"

    def _source_ts(self, project_id: str, source_id: str) -> str:
        """The source.ingested event's own timestamp: deterministic for the claim's recorded_at."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT ts_wire FROM events WHERE project_id = %s AND type = 'source.ingested' "
                "AND payload ->> 'source_id' = %s ORDER BY seq LIMIT 1",
                (project_id, source_id),
            ).fetchone()
        return row["ts_wire"]

    def _metric(self, job_id: str, metric: str, value: int, add: bool = False) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                "INSERT INTO ing_metrics (job_id, metric, value) VALUES (%s, %s, %s) "
                "ON CONFLICT (job_id, metric) DO UPDATE SET value = "
                + (
                    "to_jsonb((ing_metrics.value)::int + (EXCLUDED.value)::int)"
                    if add
                    else "EXCLUDED.value"
                ),
                (job_id, metric, Jsonb(value)),
            )

    def _metric_list_set(self, job_id: str, metric: str, value: list[Any]) -> None:
        with self._pool.connection() as conn:
            conn.execute(
                "INSERT INTO ing_metrics (job_id, metric, value) VALUES (%s, %s, %s) "
                "ON CONFLICT (job_id, metric) DO UPDATE SET value = EXCLUDED.value",
                (job_id, metric, Jsonb(value)),
            )

    def _metric_list(self, job_id: str, metric: str) -> list[Any]:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT value FROM ing_metrics WHERE job_id = %s AND metric = %s", (job_id, metric)
            ).fetchone()
        return list(row["value"]) if row else []

    def metrics(self, job_id: str) -> dict[str, int]:
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT metric, value FROM ing_metrics WHERE job_id = %s ORDER BY metric", (job_id,)
            ).fetchall()
        return {r["metric"]: r["value"] for r in rows if isinstance(r["value"], int)}

    def job(self, job_id: str) -> dict[str, Any] | None:
        with self._pool.connection() as conn:
            return conn.execute("SELECT * FROM ing_jobs WHERE job_id = %s", (job_id,)).fetchone()
