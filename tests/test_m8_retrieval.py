"""M8, finding things: embeddings through the gateway (K4), the VectorIndex backends and their
parity (K2), the retrieval golden (K5), communities (K6), the Context Compiler on retrieval
(K7), the performance sanity check (K8) and the architecture rules (K9)."""

from __future__ import annotations

import ast
import json
import random
import statistics
import time
from pathlib import Path
from typing import Any

import httpx2 as httpx
import pytest

import architect
from architect import ledger, projector
from architect.arbiter import Arbiter
from architect.cli import main
from architect.gateway.errors import BudgetExceeded, NoEligibleModel, ProviderError, ReplayMiss
from architect.gateway.providers.mock import Failure, MockProvider
from architect.gateway.providers.openai_compat import OpenAICompatProvider
from architect.knowledge import communities, index, retrieval
from architect.knowledge.plane import KnowledgePlane
from architect.knowledge.vectors import (
    ExactVectorIndex,
    PgVectorIndex,
    cosine,
    pgvector_schema,
    stored_hashes,
    vector_index,
)
from architect.projector import Projector
from architect.sessions.compiler import CompileTask, compile, rank_claims
from builders import HUMAN, candidate, ident
from conftest import PROJECT
from knowledge_fixtures import (
    DIM,
    catch_up,
    commit_claim,
    commit_corpus,
    commit_source,
    knowledge_config,
    knowledge_gateway,
    load_corpus,
    make_claim,
    scripted_embedder,
)
from session_fixtures import SESSION_ID, ScriptedArchitect, make_gateway

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

SCOPE = index.knowledge_scope(PROJECT)


def gw_rows(pool, *purposes: str) -> list[dict[str, Any]]:
    with pool.connection() as conn:
        return conn.execute(
            "SELECT purpose, tier, status, state, request, response, tokens_in, usd, scope "
            "FROM gw_calls WHERE purpose = ANY(%s) ORDER BY ts, call_id",
            (list(purposes),),
        ).fetchall()


def small_project(pool) -> list[str]:
    """Three claims from a user's notes."""
    ledger.create_project(pool, PROJECT)
    commit_source(pool, PROJECT, "notes", "user", "file:///notes.md")
    ids = []
    for name, subject, predicate, obj in (
        ("s01", "lease-manager", "GRANTS", "shard-lease"),
        ("s02", "write-router", "ROUTES", "client-writes"),
        ("s03", "audit-log", "RECORDS", "privileged-actions"),
    ):
        claim = make_claim(
            name,
            {"entity_type": "component", "id": subject},
            predicate,
            {"entity_type": "property", "id": obj},
            source="notes",
            taint_origin="user",
        )
        ids.append(commit_claim(pool, PROJECT, claim))
    catch_up(pool, PROJECT)
    return ids


# --- K4: embeddings through the gateway ---------------------------------------------------------


def test_k4_embeddings_are_recorded_cached_and_made_once(pool):
    claim_ids = small_project(pool)
    provider = MockProvider("mock")
    gateway = knowledge_gateway(pool, provider)
    plane = KnowledgePlane(pool, gateway, knowledge_config(vector_backend="exact"))
    assert gateway.can_embed() and gateway.embedding_model() == ("mock-embed", DIM)

    state = plane.sync(PROJECT)
    assert (state["claims"], state["entities"], state["embedded"]) == (3, 6, 9)
    assert state["model"] == "mock-embed"
    # every embedding call is in the call log, written ahead like any other call
    rows = gw_rows(pool, "index-claim", "index-entity")
    assert [(r["purpose"], r["status"], r["state"]) for r in rows] == [
        ("index-claim", "started", "started"),
        ("index-claim", "ok", "completed"),
        ("index-entity", "started", "started"),
        ("index-entity", "ok", "completed"),
    ]
    assert all(r["tier"] == "embedding" and r["scope"] == SCOPE for r in rows)
    assert rows[1]["request"] == {"kind": "embedding", "texts": 3, "to_embed": 3}
    assert rows[1]["response"] == {"vectors": 3, "dim": DIM} and rows[1]["tokens_in"] > 0
    assert rows[1]["usd"] > 0, "embeddings are priced like any call"
    assert "lease manager" not in json.dumps([r["request"] for r in rows]), "counts, not texts"
    assert [len(batch) for batch in provider.embed_calls] == [3, 6]
    assert set(stored_hashes(pool, PROJECT, "claim", "mock-embed")) == set(claim_ids)
    spent = gateway.spend(SCOPE)
    assert spent["calls"] == 2 and spent["tokens"] == rows[1]["tokens_in"] + rows[3]["tokens_in"]

    # nothing changed: no provider call, not even a gateway call
    assert plane.sync(PROJECT)["fresh"] is True
    assert len(provider.embed_calls) == 2 and len(gw_rows(pool, "index-claim", "index-entity")) == 4

    # the vectors are lost: they come back from the gateway's cache, the provider is not asked
    with pool.connection() as conn:
        conn.execute("DELETE FROM emb_vectors")
    again = plane.sync(PROJECT, force=True)
    assert again["embedded"] == 9 and len(provider.embed_calls) == 2
    cached = gw_rows(pool, "index-claim", "index-entity")[4:]
    assert [(r["status"], r["tokens_in"], float(r["usd"])) for r in cached] == [
        ("cache_hit", 0, 0.0)
    ] * 2
    assert gateway.spend(SCOPE) == spent, "a cache hit costs nothing"

    # one more claim: only its text and its new entities are embedded
    extra = make_claim(
        "s04",
        {"entity_type": "component", "id": "lease-manager"},
        "RENEWS",
        {"entity_type": "property", "id": "fencing-epoch"},
        source="notes",
        taint_origin="user",
    )
    commit_claim(pool, PROJECT, extra)
    catch_up(pool, PROJECT)
    assert plane.sync(PROJECT)["embedded"] == 3  # the claim, the new entity, and lease-manager
    assert [len(batch) for batch in provider.embed_calls[2:]] == [1, 2]

    # the query is embedded once, then answered from the cache
    plane.search(PROJECT, "who grants the shard lease")
    plane.search(PROJECT, "who grants the shard lease")
    queries = gw_rows(pool, "search-query")
    assert [r["status"] for r in queries] == ["started", "ok", "cache_hit"]

    # replay mode never reaches a provider: cached texts are served, new ones are a miss
    rigged = MockProvider("mock")
    rigged.fail_if_called = True
    replaying = knowledge_gateway(pool, rigged, mode="replay")
    served = replaying.embed(["who grants the shard lease"], purpose="search-query", scope=SCOPE)
    assert served.cached == 1 and len(served.vectors[0]) == DIM
    with pytest.raises(ReplayMiss):
        replaying.embed(["a text nobody embedded"], purpose="search-query", scope=SCOPE)


def test_k4_embeddings_are_budget_scoped_and_search_degrades_without_them(pool):
    small_project(pool)
    provider = MockProvider("mock")
    gateway = knowledge_gateway(pool, provider)
    plane = KnowledgePlane(pool, gateway, knowledge_config(vector_backend="exact"))
    Arbiter(pool).submit(
        PROJECT,
        candidate("budget.updated", {"scope": SCOPE, "limits": {"tokens": 5}}, actor=HUMAN),
    )
    catch_up(pool, PROJECT)

    with pytest.raises(BudgetExceeded) as refused:
        plane.sync(PROJECT)
    assert refused.value.dimension == "tokens" and provider.embed_calls == []
    assert [r["status"] for r in gw_rows(pool, "index-claim")] == ["budget_refused"]
    assert (gateway.spend(SCOPE) or {"reserved_tokens": 0})["reserved_tokens"] == 0

    # search still answers, from the signals that need no embedding
    found = plane.search(PROJECT, "lease manager")
    assert found["embedding_model"] is None and found["signals"] == ["text", "graph"]
    assert found["hits"] and "vector" not in found["hits"][0]["signals"]

    # with the cap raised, the same plane embeds and the vector signal is back
    Arbiter(pool).submit(
        PROJECT,
        candidate("budget.updated", {"scope": SCOPE, "limits": {"tokens": 100000}}, actor=HUMAN),
    )
    catch_up(pool, PROJECT)
    found = plane.search(PROJECT, "lease manager")
    assert found["embedding_model"] == "mock-embed" and "vector" in found["signals"]
    assert gateway.spend(SCOPE)["tokens"] > 0


def test_k4_a_failing_embedding_provider_is_retried_and_a_malformed_one_is_refused(pool):
    ledger.create_project(pool, PROJECT)
    provider = MockProvider("mock")
    gateway = knowledge_gateway(pool, provider)
    provider.embed_failures.append(Failure(429))
    embedded = gateway.embed(["one", "two", "one"], purpose="test-embed")
    assert embedded.computed == 2 and embedded.vectors[0] == embedded.vectors[2]
    assert [r["status"] for r in gw_rows(pool, "test-embed")] == [
        "started",
        "error",
        "started",
        "ok",
    ]
    assert cosine(embedded.vectors[0], embedded.vectors[1]) < 0.9

    class WrongSize(MockProvider):
        def embed(self, model, texts, dim=None):
            return super().embed(model, texts, 8)

    with pytest.raises(Exception, match="malformed embeddings"):
        knowledge_gateway(pool, WrongSize("mock")).embed(["three"], purpose="test-embed")
    # a gateway whose model table has no embedding tier says so
    with pytest.raises(NoEligibleModel):
        make_gateway(pool, ScriptedArchitect({})).embed(["x"], purpose="test-embed")
    assert make_gateway(pool, ScriptedArchitect({})).can_embed() is False


def test_k4_the_openai_compatible_provider_embeds_over_http():
    seen: list[dict[str, Any]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append({"url": str(request.url), "body": json.loads(request.content)})
        texts = seen[-1]["body"]["input"]
        data = [{"index": i, "embedding": [float(i), 1.0]} for i in reversed(range(len(texts)))]
        return httpx.Response(200, json={"data": data, "usage": {"prompt_tokens": 7}})

    provider = OpenAICompatProvider("http://fake-llm:8000", transport=httpx.MockTransport(handle))
    result = provider.embed("local-embedding", ["a", "b", "c"], 2)
    assert seen[0]["url"] == "http://fake-llm:8000/v1/embeddings"
    assert seen[0]["body"] == {"model": "local-embedding", "input": ["a", "b", "c"]}
    assert result.vectors == [[0.0, 1.0], [1.0, 1.0], [2.0, 1.0]], "ordered by index"
    assert result.tokens == 7

    failing = OpenAICompatProvider(
        "http://fake-llm:8000", transport=httpx.MockTransport(lambda r: httpx.Response(503))
    )
    with pytest.raises(ProviderError) as error:
        failing.embed("local-embedding", ["a"])
    assert error.value.retryable is True


@pytest.mark.fastembed_smoke
def test_k4_fastembed_embeds_three_sentences_and_similar_ones_are_closer(pool, capsys):
    """The smoke test of the real local embedder (CI's smoke job; the model is cached). The
    model id comes from config/models.yaml, through the gateway, like any call."""
    from architect.gateway.gateway import Gateway, default_providers

    ledger.create_project(pool, PROJECT)
    gateway = Gateway(pool, providers=default_providers())
    assert gateway.can_embed(), "the embedding tier is served by the fastembed provider"
    model, dim = gateway.embedding_model()
    sentences = [
        "The write-ahead log makes committed writes survive a crash.",
        "A WAL guarantees durability of acknowledged writes after a failure.",
        "The cafeteria serves lunch between noon and two.",
    ]
    embedded = gateway.embed(sentences, purpose="smoke", scope=SCOPE)
    assert embedded.provider == "fastembed" and embedded.model == model
    assert embedded.dim == dim == 384 and embedded.computed == 3
    a, b, c = embedded.vectors
    similar, unrelated = cosine(a, b), max(cosine(a, c), cosine(b, c))
    with capsys.disabled():
        print(f"\nfastembed {model}: similar {similar:.3f}, unrelated {unrelated:.3f}")
    assert similar > unrelated + 0.2
    assert gateway.embed(sentences, purpose="smoke", scope=SCOPE).cached == 3


# --- K2: the vector index backends ---------------------------------------------------------------


def random_vectors(n: int, dim: int, seed: int) -> list[list[float]]:
    rng = random.Random(seed)
    return [[rng.gauss(0, 1) for _ in range(dim)] for _ in range(n)]


def brute_force(items: dict[str, list[float]], query: list[float], k: int) -> list[str]:
    scored = sorted(((-cosine(query, vector), item_id) for item_id, vector in items.items()))
    return [item_id for _, item_id in scored[:k]]


def load_vectors(index_: Any, n: int, dim: int, seed: int = 3) -> dict[str, list[float]]:
    items = {f"v{i:05d}": vector for i, vector in enumerate(random_vectors(n, dim, seed))}
    index_.upsert(PROJECT, "claim", "m", [(i, f"h-{i}", v) for i, v in items.items()])
    return items


def test_k2_the_exact_vector_index_agrees_with_brute_force(pool):
    ledger.create_project(pool, PROJECT)
    exact = ExactVectorIndex(pool)
    items = load_vectors(exact, 300, 32)
    for query in random_vectors(20, 32, seed=5):
        got = exact.search(PROJECT, "claim", "m", query, 10)
        assert [item_id for item_id, _ in got] == brute_force(items, query, 10)
        assert got[0][1] == pytest.approx(cosine(query, items[got[0][0]]), abs=1e-5)
    assert exact.search(PROJECT, "claim", "m", [0.0] * 32, 10) == [], "a zero query has no near"
    assert exact.search(PROJECT, "claim", "other-model", random_vectors(1, 32, 1)[0], 5) == []
    # a replaced vector and a deleted one are seen at once
    exact.upsert(PROJECT, "claim", "m", [("v00000", "h2", [1.0] + [0.0] * 31)])
    exact.delete(PROJECT, "claim", ["v00001"])
    top = exact.search(PROJECT, "claim", "m", [1.0] + [0.0] * 31, 300)
    assert top[0][0] == "v00000" and "v00001" not in {i for i, _ in top} and len(top) == 299
    assert vector_index(pool, knowledge_config(vector_backend="exact")).name == "exact"


@pytest.mark.needs_pgvector
def test_k2_pgvector_in_exact_mode_gives_the_same_top_k_as_numpy_and_hnsw_recall_is_recorded(
    pool, capsys
):
    ledger.create_project(pool, PROJECT)
    schema = pgvector_schema(pool)
    assert schema is not None
    pgv, exact = PgVectorIndex(pool, knowledge_config(), schema), ExactVectorIndex(pool)
    assert vector_index(pool, knowledge_config(vector_backend="auto")).name == "pgvector"
    items = load_vectors(pgv, 2000, DIM)  # writes emb_vectors too: both backends read the same
    queries = random_vectors(20, DIM, seed=5)
    recalls = []
    for query in queries:
        from_numpy = [i for i, _ in exact.search(PROJECT, "claim", "m", query, 10)]
        from_pgvector = pgv.search(PROJECT, "claim", "m", query, 10, exact=True)
        assert [i for i, _ in from_pgvector] == from_numpy == brute_force(items, query, 10)
        assert from_pgvector[0][1] == pytest.approx(cosine(query, items[from_numpy[0]]), abs=1e-5)
        approximate = [i for i, _ in pgv.search(PROJECT, "claim", "m", query, 10)]
        recalls.append(len(set(approximate) & set(from_numpy)) / 10)
    recall = statistics.mean(recalls)
    with capsys.disabled():
        print(f"\nK2 HNSW recall@10 over 2000 vectors of {DIM} d, 20 queries: {recall:.3f}")
    assert recall >= 0.9
    # deletes and replacements reach the mirror
    pgv.delete(PROJECT, "claim", ["v00000"])
    pgv.upsert(PROJECT, "claim", "m", [("v00001", "h2", [1.0] + [0.0] * (DIM - 1))])
    top = pgv.search(PROJECT, "claim", "m", [1.0] + [0.0] * (DIM - 1), 5, exact=True)
    assert top[0][0] == "v00001" and "v00000" not in {i for i, _ in top}
    assert top == [
        (i, pytest.approx(s, abs=1e-5))
        for i, s in exact.search(PROJECT, "claim", "m", [1.0] + [0.0] * (DIM - 1), 5)
    ]


# --- K5: the retrieval golden --------------------------------------------------------------------


@pytest.fixture
def golden(pool) -> dict[str, Any]:
    corpus = load_corpus()
    ids = commit_corpus(pool, PROJECT)
    provider = scripted_embedder(corpus)
    plane = KnowledgePlane(pool, knowledge_gateway(pool, provider), knowledge_config())
    return {"corpus": corpus, "ids": ids, "names": {v: k for k, v in ids.items()}, "plane": plane}


def names_of(golden: dict[str, Any], found: dict[str, Any]) -> list[str]:
    return [golden["names"][hit["claim_id"]] for hit in found["hits"]]


def test_k5_every_expected_claim_is_in_the_top_five_for_all_ten_queries(golden):
    plane, corpus = golden["plane"], golden["corpus"]
    assert len(corpus["claims"]) == 61 and len(corpus["queries"]) == 10
    for query in corpus["queries"]:
        found = plane.search(PROJECT, query["q"], k=5)
        top = names_of(golden, found)
        assert set(query["relevant"]) <= set(top), (query["q"], top)
        assert top[0] in query["relevant"], (query["q"], top)
        for hit in found["hits"]:
            # every hit is a claim, with what a reader needs to weigh it
            assert hit["claim_id"].startswith("clm_") and hit["signals"], hit
            assert set(hit["signals"]) <= set(retrieval.SIGNALS)
            assert set(hit["ranks"]) == set(hit["signals"])
            assert hit["status"] == "documented" and hit["grade"] in ("design_grade", "unverified")
            assert hit["taint"]["origin"] in ("external_untrusted", "external_trusted", "user")
            assert hit["evidence"] and {"source", "span", "kind"} == set(hit["evidence"][0])
            assert isinstance(hit["confidence"], float) and "conditions" in hit
    # the same question twice gives the same answer, in the same order
    first = plane.search(PROJECT, "cache stampede")
    assert plane.search(PROJECT, "cache stampede") == first
    assert first["hits"][0]["signals"] == ["text", "vector", "graph"]


def test_k5_one_query_only_vectors_answer_one_only_full_text_and_hybrid_answers_both(golden):
    plane, corpus = golden["plane"], golden["corpus"]
    by_signal = {q["only"]: q for q in corpus["queries"] if "only" in q}
    assert set(by_signal) == {"vector", "text"}

    meaning = by_signal["vector"]  # no word of the query is in the corpus
    assert names_of(golden, plane.search(PROJECT, meaning["q"], signals=["text"])) == []
    assert (
        names_of(golden, plane.search(PROJECT, meaning["q"], k=5, signals=["vector"]))[0] == "a04"
    )
    hybrid = plane.search(PROJECT, meaning["q"], k=5)
    assert names_of(golden, hybrid)[0] == "a04"
    assert "text" not in hybrid["hits"][0]["signals"] and "vector" in hybrid["hits"][0]["signals"]

    words = by_signal["text"]  # an identifier the embedder has no direction for
    assert names_of(golden, plane.search(PROJECT, words["q"], signals=["vector"])) == []
    assert names_of(golden, plane.search(PROJECT, words["q"], signals=["text"])) == ["c08"]
    hybrid = plane.search(PROJECT, words["q"], k=5)
    assert names_of(golden, hybrid)[0] == "c08"
    assert "vector" not in hybrid["hits"][0]["signals"] and "text" in hybrid["hits"][0]["signals"]


def test_k5_quarantined_claims_are_never_returned_unless_asked_for_and_filters_filter(golden):
    plane, ids = golden["plane"], golden["ids"]
    quarantined = {ids["q01"], ids["q02"]}
    for query in ("fencing token eliminates split brain", "cache layer guarantees read latency"):
        found = plane.search(PROJECT, query, k=50)
        assert not quarantined & {h["claim_id"] for h in found["hits"]}
        assert all(h["grade"] != "quarantined" for h in found["hits"])
    asked = plane.search(
        PROJECT, "fencing token eliminates split brain", grades=["quarantined", "unverified"]
    )
    shown = [h for h in asked["hits"] if h["claim_id"] == ids["q01"]]
    assert shown and shown[0]["grade"] == "quarantined" and shown[0]["confidence"] is None

    # taint: only the user's notes
    mine = plane.search(PROJECT, "throughput latency consumers", k=50, taints=["user"])
    assert mine["hits"] and {h["taint"]["origin"] for h in mine["hits"]} == {"user"}
    assert {golden["names"][h["claim_id"]][0] for h in mine["hits"]} <= {"d", "f"}
    # grade: the claim two sources state is design grade, and only it survives that filter
    strict = plane.search(PROJECT, "what prevents split brain", grades=["design_grade"])
    assert sorted(names_of(golden, strict)) == ["b03", "b03x"]
    # entity scope: only claims about that entity
    scoped = plane.search(PROJECT, "cache", k=50, entities=["ent:technique:cache-layer"])
    assert sorted(names_of(golden, scoped)) == ["a01", "a02"]
    # a refuted claim leaves the default results
    Arbiter(pool_of(plane)).submit(
        PROJECT,
        candidate("claim.retracted", {"claim_id": ids["c02"], "cause": "withdrawn"}, actor=HUMAN),
    )
    catch_up(pool_of(plane), PROJECT)
    after = plane.search(PROJECT, "group commit and fsync throughput", k=10)
    assert "c02" not in names_of(golden, after)
    kept = plane.search(PROJECT, "group commit and fsync throughput", statuses=["retracted"])
    assert names_of(golden, kept) == ["c02"]

    with pytest.raises(ValueError, match="scope must be one of"):
        plane.search(PROJECT, "x", scope="everywhere")
    with pytest.raises(ValueError, match="signals must be among"):
        plane.search(PROJECT, "x", signals=["luck"])


def pool_of(plane: KnowledgePlane):
    return plane.pool


def test_k5_reciprocal_rank_fusion_is_deterministic_and_breaks_ties_by_id():
    fused = retrieval.fuse({"text": ["b", "a", "c"], "vector": ["a", "b"], "graph": ["d"]}, 60)
    assert [item for item, _, _ in fused] == ["a", "b", "d", "c"]
    assert fused[0][1] == pytest.approx(1 / 62 + 1 / 61) and fused[0][1] == fused[1][1]
    assert fused[0][2] == {"text": 2, "vector": 1} and fused[2][2] == {"graph": 1}
    assert retrieval.fuse({}, 60) == []
    assert retrieval.query_terms("What is the WAL, and how does the WAL fsync?") == ["wal", "fsync"]


# --- K6: communities ------------------------------------------------------------------------------

CLUSTERS = {
    "lease": ["lease-manager", "fencing-epoch", "lease-table", "renewal-loop"],
    "ingest": ["ingest-api", "event-queue", "dedup-cache", "batch-writer"],
    "billing": ["invoice-job", "price-table", "ledger-export", "tax-rules"],
}


def community_project(pool) -> dict[str, str]:
    """Three clusters of four entities, each a ring with a chord, plus one weak link from
    lease to ingest: three level-0 communities. The lease cluster's claims come from an
    untrusted paper, the others from the user's notes."""
    ledger.create_project(pool, PROJECT)
    commit_source(pool, PROJECT, "paper", "external_untrusted", "https://example.org/p.pdf")
    commit_source(pool, PROJECT, "notes", "user", "file:///notes.md")
    ids: dict[str, str] = {}

    def link(name: str, a: str, b: str, source: str, taint: str) -> None:
        claim = make_claim(
            name,
            {"entity_type": "component", "id": a},
            "DEPENDS_ON",
            {"entity_type": "component", "id": b},
            source=source,
            taint_origin=taint,
        )
        ids[name] = commit_claim(pool, PROJECT, claim)

    for cluster, members in CLUSTERS.items():
        source, taint = ("paper", "external_untrusted") if cluster == "lease" else ("notes", "user")
        pairs = [(0, 1), (1, 2), (2, 3), (3, 0), (0, 2)]
        for n, (i, j) in enumerate(pairs):
            link(f"{cluster}{n}", members[i], members[j], source, taint)
    link("bridge0", "lease-manager", "ingest-api", "notes", "user")
    catch_up(pool, PROJECT)
    return ids


def summaries(pool) -> list[dict[str, Any]]:
    return [
        e["payload"]["claim"]
        for e in ledger.iter_events(pool, PROJECT)
        if e["type"] == "claim.committed"
        and e["payload"]["claim"]["subject"]["entity_type"] == "community"
    ]


def test_k6_the_partition_is_deterministic_and_summaries_are_inferred_claims(pool):
    ids = community_project(pool)
    provider = MockProvider("mock")
    gateway = knowledge_gateway(pool, provider)
    plane = KnowledgePlane(pool, gateway, knowledge_config())

    result = plane.rebuild_communities(PROJECT)
    level0 = [c for c in result["communities"] if c["level"] == 0]
    assert sorted(sorted(m.split(":")[-1] for m in c["members"]) for c in level0) == sorted(
        sorted(members) for members in CLUSTERS.values()
    )
    level1 = [c for c in result["communities"] if c["level"] == 1]
    assert all(c["parent_id"] in {p["community_id"] for p in level1} | {None} for c in level0)
    for parent in level1:
        children = [c for c in level0 if c["parent_id"] == parent["community_id"]]
        assert len(children) >= 2
        assert sorted(parent["members"]) == sorted(m for c in children for m in c["members"])
    assert len(result["written"]) == len(result["communities"]) == provider.call_count
    assert all(c["community_id"].startswith(f"comm_{c['level']}_") for c in result["communities"])

    # each summary is a committed, inferred claim that says what it was written from
    committed = summaries(pool)
    assert len(committed) == len(result["communities"])
    by_community = {c["community_id"]: c for c in result["communities"]}
    for claim in committed:
        community = by_community[claim["subject"]["id"]]
        assert claim["predicate"] == "SUMMARIZES" and claim["status"] == "inferred"
        assert claim["object"]["entity_type"] == "text" and claim["object"]["literal"]
        assert claim["id"] == community["summary_claim_id"]
        assert claim["provenance"]["derived_from"] == sorted(community["claim_ids"])
        assert set(claim["provenance"]["derived_from"]) <= set(ids.values())
        assert claim["provenance"]["extractor"]["model_tier"] == "tier-mid"
        assert community["summary"] == claim["object"]["literal"]
    # taint: the most restrictive of the member claims
    lease = next(c for c in level0 if "ent:component:lease-manager" in c["members"])
    billing = next(c for c in level0 if "ent:component:invoice-job" in c["members"])
    assert lease["taint_origin"] == "external_untrusted" and billing["taint_origin"] == "user"
    by_purpose = [c for c in provider.calls if "<<<UNTRUSTED-DATA" in c.messages[0]["content"]]
    assert len(by_purpose) == 1 + len(
        [
            p
            for p in level1
            if lease["community_id"]
            in {c["community_id"] for c in level0 if c["parent_id"] == p["community_id"]}
        ]
    ), "claims from the untrusted paper travel as untrusted data"
    rows = gw_rows(pool, "community-summary")
    assert {r["tier"] for r in rows} == {"tier-mid"} and rows[0]["scope"] == SCOPE

    # the same graph gives the same partition: a rebuild writes nothing and calls nothing
    again = plane.rebuild_communities(PROJECT)
    assert again["written"] == [] and sorted(again["kept"]) == sorted(by_community)
    assert again["communities"] == result["communities"] and provider.call_count == len(committed)
    with pool.connection() as conn:
        nodes, weights = communities.entity_graph(conn, PROJECT)
    assert communities.partition(nodes, weights, plane.config) == communities.partition(
        list(nodes), dict(weights), plane.config
    )
    # the operational table can be dropped: the summaries are in the ledger, nothing is asked
    with pool.connection() as conn:
        conn.execute("DELETE FROM kg_communities")
    rebuilt = plane.rebuild_communities(PROJECT)
    assert rebuilt["communities"] == result["communities"] and rebuilt["written"] == []
    assert provider.call_count == len(committed)

    # the projection rebuilds with the summaries in it: community nodes, ABOUT edges
    counted = plane.counts(PROJECT)
    assert counted["nodes"]["community"] == len(committed)
    hashed = projector.content_hash(pool, PROJECT)
    Projector(pool).rebuild(PROJECT)
    assert projector.content_hash(pool, PROJECT) == hashed


def test_k6_one_new_claim_regenerates_exactly_the_affected_summaries(pool):
    community_project(pool)
    provider = MockProvider("mock")
    plane = KnowledgePlane(pool, knowledge_gateway(pool, provider), knowledge_config())
    first = plane.rebuild_communities(PROJECT)
    before = {c["community_id"]: c for c in first["communities"]}
    calls = provider.call_count

    # one more claim inside the billing cluster
    extra = make_claim(
        "billingextra",
        {"entity_type": "component", "id": "price-table"},
        "FEEDS",
        {"entity_type": "component", "id": "tax-rules"},
        source="notes",
        taint_origin="user",
    )
    extra_id = commit_claim(pool, PROJECT, extra)
    catch_up(pool, PROJECT)
    second = plane.rebuild_communities(PROJECT)
    after = {c["community_id"]: c for c in second["communities"]}
    assert set(after) == set(before), "the partition did not move"
    affected = sorted(c for c in after if extra_id in after[c]["claim_ids"])
    billing = next(c for c in after.values() if "ent:component:invoice-job" in c["members"])
    assert billing["community_id"] in affected and billing["level"] == 0
    assert all("ent:component:price-table" in after[c]["members"] for c in affected), (
        "only communities the claim is about"
    )
    assert sorted(second["written"]) == affected
    assert provider.call_count - calls == len(affected), "one model call per affected community"
    for community_id, community in after.items():
        if community_id in affected:
            assert community["summary_claim_id"] != before[community_id]["summary_claim_id"]
        else:
            assert community == before[community_id], "an unchanged community is left alone"


def test_k6_a_global_query_returns_a_community_summary(pool):
    community_project(pool)
    provider = MockProvider("mock")
    plane = KnowledgePlane(pool, knowledge_gateway(pool, provider), knowledge_config())
    result = plane.rebuild_communities(PROJECT)
    summary_ids = {c["summary_claim_id"] for c in result["communities"]}

    found = plane.search(PROJECT, "what are the main themes of this system", scope="global")
    assert found["scope"] == "global" and "community" in found["signals"]
    returned = [h for h in found["hits"] if h["claim_id"] in summary_ids]
    assert {h["claim_id"] for h in returned} == summary_ids, "every current summary is returned"
    for hit in returned:
        assert hit["predicate"] == "SUMMARIZES" and "community" in hit["signals"]
        assert hit["status"] == "inferred" and hit["subject"]["entity_type"] == "community"
        assert hit["object"]["literal"] and hit["grade"] == "unverified"
    # a query that matches no entity is global by default; one that names an entity is local
    assert plane.search(PROJECT, "zzz qqq")["scope"] == "global"
    local = plane.search(PROJECT, "lease manager")
    assert local["scope"] == "local" and "community" not in local["signals"]
    assert not summary_ids & {h["claim_id"] for h in local["hits"]}, "summaries stay out of local"


# --- K7: the Context Compiler on retrieval --------------------------------------------------------


def test_k7_the_compiler_ranks_with_hybrid_retrieval_and_keeps_its_rules(pool):
    corpus = load_corpus()
    ids = commit_corpus(pool, PROJECT)
    provider = scripted_embedder(corpus)
    gateway = knowledge_gateway(pool, provider)
    requirement = make_claim(
        "req1",
        {"entity_type": "requirement", "id": "req_stampede"},
        "CONSTRAINS",
        {"entity_type": "property", "id": "cache-stampede"},
        source="notes",
        taint_origin="user",
    )
    commit_claim(pool, PROJECT, requirement)
    catch_up(pool, PROJECT)

    # by meaning: no word of this query is in the corpus, and the right claim is ranked first
    by_meaning = rank_claims(pool, PROJECT, "memoization freshness expiry", gateway)
    assert by_meaning[0]["claim_id"] == ids["a04"] and "vector" in by_meaning[0]["signals"]
    assert rank_claims(pool, PROJECT, "memoization freshness expiry") == [], "words alone: nothing"

    ranked = rank_claims(pool, PROJECT, "cache stampede and split brain", gateway)
    names = {v: k for k, v in ids.items()}
    top = [names.get(r["claim_id"], "?") for r in ranked[:6]]
    assert {"a06", "a07", "b03"} <= set(top), top
    listed = {r["claim_id"] for r in ranked}
    assert requirement["id"] not in listed, "requirements are the session's own statements"
    assert not {ids["q01"], ids["q02"]} & listed, "a quarantined proposal is never a fact"
    assert all(r["score"] > 0 and r["grade"] and r["first_seq"] >= 0 for r in ranked)

    compiled = compile(
        pool,
        CompileTask(
            project_id=PROJECT,
            session_id=SESSION_ID,
            goal="draft",
            phase="draft",
            round=1,
            token_target=700,
            research_claim_ids=[r["claim_id"] for r in ranked],
        ),
    )
    assert compiled.tokens <= 700 and requirement["id"] in compiled.manifest
    assert ids["a06"] in compiled.manifest and ids["b03"] in compiled.manifest
    dropped = {d["id"]: d["reason"] for d in compiled.dropped}
    assert dropped[ids["q01"]] == dropped[ids["q02"]] == "quarantined"
    assert "ELIMINATES" not in compiled.text and "confidence" not in compiled.text
    assert f"<<<UNTRUSTED-DATA source={ids['b03']}>>>" in compiled.text, "the paper is untrusted"
    assert "external_untrusted" in compiled.input_taints


# --- K8: performance sanity -----------------------------------------------------------------------


def bulk_claims(pool, n: int, seed: int = 13) -> list[str]:
    """`n` synthetic claims written straight into the read models (the ledger path would fold
    grades n times over): a test-only shortcut for a size the Arbiter is not the subject of."""
    rng = random.Random(seed)
    vocabulary = [f"w{i:04d}" for i in range(1500)]
    entities = [f"{rng.choice(vocabulary)}-{rng.choice(vocabulary)}" for _ in range(2500)]
    predicates = ["USES", "REQUIRES", "REDUCES", "CAUSES", "BOUNDS", "ENABLES"]
    ledger.create_project(pool, PROJECT)
    commit_source(pool, PROJECT, "bulk", "user", "file:///bulk.md")
    claims, nodes, edges = [], {}, []
    for i in range(n):
        subject, obj = rng.sample(entities, 2)
        claim = make_claim(
            f"bulk{i:06d}",
            {"entity_type": "component", "id": subject},
            rng.choice(predicates),
            {"entity_type": "property", "id": obj},
            source="bulk",
            taint_origin="user",
        )
        seq = 1000 + i
        claims.append((PROJECT, claim["id"], json.dumps(claim), seq, seq))
        a, b = f"ent:component:{subject}", f"ent:property:{obj}"
        nodes[a] = (PROJECT, a, "entity", "component", subject.replace("-", " "), "event", seq)
        nodes[b] = (PROJECT, b, "entity", "property", obj.replace("-", " "), "event", seq)
        edges.append((PROJECT, seq, 0, claim["predicate"], a, b, claim["id"]))
        edges.append((PROJECT, seq, 1, "ABOUT", claim["id"], a, claim["id"]))
        edges.append((PROJECT, seq, 2, "ABOUT", claim["id"], b, claim["id"]))
    with pool.connection() as conn, conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO proj_claims (project_id, claim_id, claim, status, load_bearing, "
            "taint_origin, derived_from, premise_compromised, first_seq, last_seq, grade, "
            "confidence) VALUES (%s, %s, %s::jsonb, 'documented', false, 'user', '{}', false, "
            "%s, %s, 'unverified', 0.5)",
            claims,
        )
        cur.executemany(
            "INSERT INTO proj_graph_nodes (project_id, node_id, node_type, entity_type, label, "
            "origin, seq) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            list(nodes.values()),
        )
        cur.executemany(
            "INSERT INTO proj_graph_edges (project_id, seq, ord, edge_type, src, dst, claim_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            edges,
        )
    return vocabulary


def timed_searches(plane: KnowledgePlane, vocabulary: list[str], n: int) -> list[float]:
    rng = random.Random(29)
    plane.search(PROJECT, "warm up")
    taken = []
    for _ in range(n):
        query = " ".join(rng.sample(vocabulary, 3))
        start = time.perf_counter()
        found = plane.search(PROJECT, query)
        taken.append((time.perf_counter() - start) * 1000)
        assert found["hits"] and "vector" in found["signals"]
    return taken


@pytest.mark.needs_pgvector
def test_k8_search_over_ten_thousand_claims_stays_under_half_a_second_at_p95(pool, capsys):
    vocabulary = bulk_claims(pool, 10_000)
    provider = MockProvider("mock")
    plane = KnowledgePlane(pool, knowledge_gateway(pool, provider), knowledge_config())
    assert plane.vectors.name == "pgvector"
    start = time.perf_counter()
    state = plane.sync(PROJECT, force=True)
    indexing = time.perf_counter() - start
    assert state["claims"] == 10_000 and state["embedded"] == 10_000 + state["entities"]
    taken = timed_searches(plane, vocabulary, 100)
    p50, p95 = statistics.median(taken), statistics.quantiles(taken, n=20)[18]
    with capsys.disabled():
        print(
            f"\nK8 search over 10,000 claims ({state['entities']} entities, pgvector HNSW, "
            f"{DIM} d): p50 {p50:.1f} ms, p95 {p95:.1f} ms, max {max(taken):.1f} ms over "
            f"{len(taken)} queries; indexing {indexing:.1f} s"
        )
    assert p95 < 500


def test_k8_the_fallback_backends_search_a_thousand_claims(pool, capsys):
    """The same path on the exact backend, at a size any machine handles: what runs locally."""
    vocabulary = bulk_claims(pool, 1_000)
    plane = KnowledgePlane(
        pool,
        knowledge_gateway(pool, MockProvider("mock")),
        knowledge_config(vector_backend="exact", graph_backend="sql"),
    )
    assert plane.backends() == {"graph": "sql", "vectors": "exact"}
    assert plane.sync(PROJECT, force=True)["claims"] == 1_000
    taken = timed_searches(plane, vocabulary, 30)
    with capsys.disabled():
        print(f"\nexact backend, 1,000 claims: p95 {statistics.quantiles(taken, n=20)[18]:.1f} ms")
    assert statistics.quantiles(taken, n=20)[18] < 2000


# --- K9: architecture -----------------------------------------------------------------------------

SRC = Path(architect.__file__).parent
PROVIDER_SDKS = {"anthropic", "openai", "httpx", "httpx2", "fastembed", "onnxruntime", "requests"}
HIT_KEYS = {
    "claim_id", "subject", "predicate", "object", "magnitude", "conditions", "status", "grade",
    "confidence", "taint", "evidence", "premise_compromised", "signals", "ranks", "score",
    "first_seq", "claim",
}  # fmt: skip


def imports_of(path: Path) -> set[str]:
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
    return found


def test_k9_the_knowledge_package_reaches_models_only_through_the_gateway():
    modules = sorted((SRC / "knowledge").glob("*.py"))
    assert {m.name for m in modules} >= {
        "graph.py", "graph_age.py", "vectors.py", "index.py", "retrieval.py", "resolution.py",
        "communities.py", "plane.py",
    }  # fmt: skip
    for module in modules:
        imported = imports_of(module)
        roots = {name.split(".")[0] for name in imported}
        assert not roots & PROVIDER_SDKS, f"{module.name} imports a provider SDK"
        assert not {i for i in imported if i.startswith("architect.gateway.providers")}, (
            f"{module.name} reaches past the gateway to a provider"
        )
    # the retrieval path writes no event: it never imports the Arbiter
    for name in ("retrieval.py", "index.py", "vectors.py", "graph.py", "graph_age.py"):
        assert "architect.arbiter" not in imports_of(SRC / "knowledge" / name), name
    # and the fold of the graph stays in the projector's module
    assert "INSERT INTO proj_graph" not in "".join(
        m.read_text(encoding="utf-8") for m in modules
    ), "only architect.projections writes the graph tables"


def test_k9_search_returns_claims_never_raw_segment_text(pool, tmp_path):
    from test_m8_graph import README_QUOTES, ingest_readme_corpus

    committed = ingest_readme_corpus(pool, tmp_path)
    plane = KnowledgePlane(pool, knowledge_gateway(pool, MockProvider("mock")), knowledge_config())
    # the quote is searchable ...
    found = plane.search(PROJECT, "partitioned former owner stale epoch")
    assert found["hits"] and found["hits"][0]["claim_id"] in committed
    for hit in found["hits"]:
        assert set(hit) == HIT_KEYS
        assert hit["claim_id"] in committed and hit["claim"]["id"] == hit["claim_id"]
        # ... but what comes back is the claim and its locator, not the segment's text
        assert hit["evidence"][0]["span"].startswith("README.md#")
        dumped = json.dumps(hit)
        for quote in README_QUOTES.values():
            assert " ".join(quote.split()) not in " ".join(dumped.split())
    with pool.connection() as conn:
        segments = conn.execute("SELECT text FROM ing_segments").fetchall()
    everything = json.dumps(found)
    assert segments and not any(
        s["text"][:60] in everything for s in segments if len(s["text"]) > 60
    )


# --- the API and the commands ---------------------------------------------------------------------


def test_search_graph_and_community_endpoints(client, pool):
    corpus = load_corpus()
    ids = commit_corpus(pool, PROJECT, create=False)  # the client fixture made the project
    base = f"/v1/projects/{PROJECT}"
    client.app.state.knowledge = KnowledgePlane(
        pool, knowledge_gateway(pool, scripted_embedder(corpus)), knowledge_config()
    )

    found = client.get(f"{base}/search", params={"q": "what prevents split brain", "k": 3})
    assert found.status_code == 200
    body = found.json()
    assert [h["claim_id"] for h in body["hits"]][:2] == [ids["b03"], ids["b03x"]]
    assert len(body["hits"]) == 3 and body["hits"][0]["signals"] == ["text", "vector", "graph"]
    strict = client.get(f"{base}/search", params={"q": "split brain", "grade": "design_grade"})
    assert {h["claim_id"] for h in strict.json()["hits"]} == {ids["b03"], ids["b03x"]}
    asked = client.get(
        f"{base}/search",
        params={"q": "fencing token eliminates split brain", "grade": ["quarantined"]},
    )
    assert [h["grade"] for h in asked.json()["hits"]] == ["quarantined"]
    for bad in ({"q": "x", "scope": "everywhere"}, {"q": "x", "grade": "excellent"}, {"q": ""}):
        refused = client.get(f"{base}/search", params=bad)
        assert refused.status_code == 422 and refused.json()["code"] == "MALFORMED_REQUEST"
    unknown = client.get("/v1/projects/nope/search", params={"q": "x"})
    assert unknown.status_code == 404 and unknown.json()["code"] == "UNKNOWN_PROJECT"

    graph = client.get(f"{base}/graph").json()
    assert graph["nodes"]["claim"] == 61 and graph["backends"]["graph"] in ("sql", "age")
    node = "ent:protocol:fencing-token"
    near = client.get(f"{base}/graph/nodes/{node}/neighbors", params={"depth": 2}).json()
    assert near["node"] == node and near["nodes"][0] == {
        "id": node, "type": "entity", "entity_type": "protocol", "label": "fencing token",
        "distance": 0,
    }  # fmt: skip
    assert {ids["b03"], ids["b03x"], "ent:property:split-brain"} <= {n["id"] for n in near["nodes"]}
    prevents = [e for e in near["edges"] if e["type"] == "PREVENTS"]
    assert len(prevents) == 2 and all(e["grade"] == "design_grade" for e in prevents)
    assert all(e["provenance"]["event_id"] and e["provenance"]["claim_id"] for e in prevents)
    assert (
        client.get(f"{base}/graph/nodes/{node}/neighbors", params={"depth": 4}).status_code == 422
    )
    path = client.get(
        f"{base}/graph/path", params={"from": node, "to": "ent:property:split-brain"}
    ).json()
    assert path["paths"] == [[node, "ent:property:split-brain"]]
    none = client.get(f"{base}/graph/path", params={"from": node, "to": "ent:property:nowhere"})
    assert none.json()["paths"] == []
    assert client.get(f"{base}/communities").json() == {"communities": [], "level": None}
    client.app.state.knowledge.rebuild_communities(PROJECT)
    listed = client.get(f"{base}/communities", params={"level": 0}).json()
    assert listed["communities"] and all(c["level"] == 0 for c in listed["communities"])


def test_search_graph_resolve_and_communities_commands(dsn, pool, monkeypatch, capsys):
    """The commands run on the shipped model table, where no embedding provider is registered
    in this environment: search answers from full text and the graph, and the two commands that
    need a model say so."""
    monkeypatch.setattr("architect.gateway.providers.fastembed.available", lambda: False)
    ids = commit_corpus(pool, PROJECT)

    def run(*args: str) -> tuple[int, str, str]:
        code = main(["--database-url", dsn, *args])
        captured = capsys.readouterr()
        return code, captured.out, captured.err

    code, out, _ = run("search", "--project", PROJECT, "what prevents split brain", "-k", "3")
    assert code == 0 and "3 hits  scope local  signals text, graph" in out
    assert out.splitlines()[1].startswith(f"{ids['b03']} [documented design_grade external_")
    assert "fencing-token PREVENTS split-brain" in out and "via text+graph" in out
    code, out, _ = run(
        "search", "--project", PROJECT, "cache stampede", "--json", "--taint", "user"
    )
    assert code == 0 and json.loads(out)["hits"] == []
    assert run("search", "--project", "nope", "x")[0] == 1

    code, out, _ = run("graph", "counts", "--project", PROJECT)
    assert code == 0 and json.loads(out)["nodes"] == {"claim": 61, "entity": 119, "source": 3}
    node = "ent:protocol:fencing-token"
    code, out, _ = run("graph", "neighbors", "--project", PROJECT, node, "--edge-type", "PREVENTS")
    assert code == 0 and [n["id"] for n in json.loads(out)["nodes"]] == [
        node,
        "ent:property:split-brain",
    ]
    code, out, _ = run("graph", "path", "--project", PROJECT, node, "ent:property:split-brain")
    assert code == 0 and json.loads(out)["paths"] == [[node, "ent:property:split-brain"]]
    code, _, err = run("graph", "neighbors", "--project", PROJECT, node, "--depth", "9")
    assert code == 1 and "depth must be between 1 and 3" in err

    code, _, err = run("resolve-entities", "--project", PROJECT)
    assert code == 1 and "needs an embedding provider" in err
    code, _, err = run("resolve-entities", "--project", PROJECT, "--revert", "evt_x")
    assert code == 1 and "--signer" in err
    code, out, _ = run("communities", "list", "--project", PROJECT)
    assert code == 0 and json.loads(out) == []
    code, _, err = run("communities", "rebuild", "--project", PROJECT)
    assert code == 1 and "communities stopped" in err, "no model answers tier-mid here"


def test_ident_is_what_the_corpus_ids_are_built_from():
    assert ident("clm", "a04") == "clm_0000000A04"
