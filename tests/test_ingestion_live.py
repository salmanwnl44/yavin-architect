"""L3: extract from a real document with the real gateway. Manual, deselected in CI.

Put a short document (txt, md or pdf) at tests/live_docs/ (gitignored), set
ANTHROPIC_API_KEY in the shell, then:

    pytest -m live -v -k l3

It prints counts by grade, the drop counts and the total usd spent.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from architect import ledger, readmodel
from architect.gateway.config import load_config
from architect.gateway.gateway import Gateway, default_providers
from architect.ingestion.config import load_ingest_config
from architect.ingestion.objectstore import LocalObjectStore
from architect.ingestion.pipeline import Pipeline
from architect.ingestion.sources import Ingestor
from architect.projector import Projector
from conftest import PROJECT

pytestmark = pytest.mark.live

LIVE_DOCS = Path(__file__).parent / "live_docs"


def test_l3_extract_a_real_document(pool, tmp_path, capsys):
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.fail("set ANTHROPIC_API_KEY in the shell to run the live tests")
    documents = sorted(p for p in LIVE_DOCS.glob("*") if p.suffix in (".txt", ".md", ".pdf"))
    if not documents:
        pytest.fail(f"place a short .txt, .md or .pdf document in {LIVE_DOCS}")
    ledger.create_project(pool, PROJECT)
    store = LocalObjectStore(tmp_path / "objects")
    config = load_ingest_config()
    gateway = Gateway(pool, load_config(), default_providers())
    source = Ingestor(pool, store, config).ingest_file(PROJECT, documents[0])
    Projector(pool).catch_up(PROJECT)
    report = Pipeline(pool, gateway, store, config).run(PROJECT, source.source_id)
    Projector(pool).catch_up(PROJECT)
    by_grade = {}
    for row in readmodel.claims_by_grade(pool, PROJECT, None):
        by_grade[row["grade"]] = by_grade.get(row["grade"], 0) + 1
    by_grade["quarantined"] = len(readmodel.claims_by_grade(pool, PROJECT, "quarantined"))
    spend = gateway.spend({"session": f"ingest:{report.job_id}"}) or {}
    with capsys.disabled():
        usd = spend.get("usd", 0)
        print(f"\n{documents[0].name}: grades {by_grade}; metrics {report.metrics}; usd {usd:.4f}")
    assert report.metrics.get("pass_a_candidates", 0) >= 0
