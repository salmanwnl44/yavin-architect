"""L3: extract from a real document with the real gateway. Manual, deselected in CI.

Put a short document (txt, md or pdf) at tests/live_docs/ (gitignored), set
ARCHITECT_ANTHROPIC_API_KEY in the shell, then:

    pytest -m live -v -k l3

It prints counts by grade, the drop counts and the total usd spent. The run is capped: a
budget.updated event on the extraction job's own gateway scope limits it to $1.00
(ARCHITECT_L3_USD_CAP overrides), so the gateway refuses the call that would cross the cap.
Pass B makes one call per segment with a surviving candidate, and a paper has many segments.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from architect import ledger, readmodel
from architect.arbiter import Arbiter
from architect.gateway.config import anthropic_api_key, load_config
from architect.gateway.errors import BudgetExceeded
from architect.gateway.gateway import Gateway, default_providers
from architect.ingestion.config import load_ingest_config
from architect.ingestion.extract import PIPELINE_VERSION
from architect.ingestion.normalize import typed_id
from architect.ingestion.objectstore import LocalObjectStore
from architect.ingestion.pipeline import Pipeline
from architect.ingestion.sources import Ingestor
from architect.projector import Projector
from conftest import PROJECT

pytestmark = pytest.mark.live

LIVE_DOCS = Path(__file__).parent / "live_docs"
USD_CAP_ENV = "ARCHITECT_L3_USD_CAP"
DEFAULT_USD_CAP = 1.00


def test_l3_extract_a_real_document(pool, tmp_path, capsys):
    if not anthropic_api_key():
        pytest.fail("set ARCHITECT_ANTHROPIC_API_KEY (or ANTHROPIC_API_KEY) to run the live tests")
    documents = sorted(p for p in LIVE_DOCS.glob("*") if p.suffix in (".txt", ".md", ".pdf"))
    if not documents:
        pytest.fail(f"place a short .txt, .md or .pdf document in {LIVE_DOCS}")
    ledger.create_project(pool, PROJECT)
    store = LocalObjectStore(tmp_path / "objects")
    config = load_ingest_config()
    gateway = Gateway(pool, load_config(), default_providers())
    source = Ingestor(pool, store, config).ingest_file(PROJECT, documents[0])
    Projector(pool).catch_up(PROJECT)

    # the per-test usd cap, set on the scope the pipeline charges this job to
    cap = float(os.environ.get(USD_CAP_ENV, DEFAULT_USD_CAP))
    job_id = typed_id("job", PROJECT, source.content_hash, str(PIPELINE_VERSION))
    scope = {"session": f"ingest:{job_id}"}
    Arbiter(pool).submit(
        PROJECT,
        {
            "actor": {"kind": "human", "id": "live-test"},
            "type": "budget.updated",
            "payload": {"scope": scope, "limits": {"usd": cap}},
            "idempotency_key": f"l3-usd-cap:{job_id}",
        },
    )
    Projector(pool).catch_up(PROJECT)
    try:
        report = Pipeline(pool, gateway, store, config).run(PROJECT, source.source_id)
    except BudgetExceeded as refused:
        spent = gateway.spend(scope) or {}
        with capsys.disabled():
            print(
                f"\n{documents[0].name}: stopped at the usd cap of ${cap:.2f} after "
                f"{spent.get('calls', 0)} calls and ${spent.get('usd', 0):.4f}: {refused}"
            )
        pytest.fail(
            f"L3 reached its usd cap of ${cap:.2f} before the extraction finished; raise "
            f"{USD_CAP_ENV} to let it complete (the job resumes where it stopped)"
        )
    assert report.job_id == job_id, "the cap sits on the scope the pipeline charged"
    Projector(pool).catch_up(PROJECT)
    by_grade = {}
    for row in readmodel.claims_by_grade(pool, PROJECT, None):
        by_grade[row["grade"]] = by_grade.get(row["grade"], 0) + 1
    by_grade["quarantined"] = len(readmodel.claims_by_grade(pool, PROJECT, "quarantined"))
    spend = gateway.spend(scope) or {}
    with capsys.disabled():
        usd = spend.get("usd", 0)
        print(
            f"\n{documents[0].name}: grades {by_grade}; metrics {report.metrics}; "
            f"usd {usd:.4f} of a ${cap:.2f} cap"
        )
    assert usd <= cap
    assert report.metrics.get("pass_a_candidates", 0) >= 0
