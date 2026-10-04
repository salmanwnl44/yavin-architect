"""M8, the knowledge graph: the projection (K1), the two GraphStore backends and their parity
(K2), and entity resolution (K3)."""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any

import psycopg
import pytest

from architect import ledger, projector
from architect.gateway.config import from_mapping
from architect.gateway.gateway import Gateway
from architect.gateway.providers.mock import MockProvider
from architect.ingestion.config import IngestConfig
from architect.ingestion.objectstore import LocalObjectStore
from architect.ingestion.pipeline import Pipeline
from architect.ingestion.sources import Ingestor
from architect.knowledge import index, resolution
from architect.knowledge.graph import (
    ABOUT,
    SqlGraphStore,
    all_edges,
    all_nodes,
    counts,
    entity_node,
)
from architect.knowledge.graph_age import AgeGraphStore
from architect.knowledge.plane import KnowledgePlane, graph_store
from architect.knowledge.vectors import cosine
from architect.projector import Projector
from conftest import PROJECT
from ingest_fixtures import README
from knowledge_fixtures import (
    GRAPH_PREDICATES,
    catch_up,
    commit_claim,
    commit_random_graph,
    commit_source,
    knowledge_config,
    knowledge_gateway,
    make_claim,
    reference_distances,
    reference_paths,
)
from test_projections import FIX, commit_fixture

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

# --- K1: the graph projection -------------------------------------------------------------------

# The fixture ledger (phase0-contracts/fixture): what its graph holds. Pinned by hand from the
# ledger: 4 sources, 6 committed claims, the 6 entities they name, 2 requirement nodes (the
# subjects of the two requirement claims; 3 objects are literals and are no nodes), the 11
# elements of the head model, 1 ADR.
FIXTURE_GRAPH = {
    "nodes": {"adr": 1, "claim": 6, "element": 11, "entity": 6, "requirement": 2, "source": 4},
    "edges": {
        "ABOUT": 10,
        "CONSTRAINS": 1,
        "DECISION_EVIDENCE": 3,
        "DEPENDS_ON": 3,
        "EVIDENCES": 5,
        "HAS_FSYNC_P99": 1,
        "HAS_P99_PAYLOAD": 2,
        "SATISFIES": 5,
        "SUPERSEDES": 1,
    },
}

INGEST_MODELS: dict[str, Any] = {
    "tiers": {
        "tier-cheap": [{"provider": "mock", "model": "mock-a-small", "family": "mock-a"}],
        "tier-mid": [{"provider": "mock-b", "model": "mock-b-medium", "family": "mock-b"}],
        "tier-frontier": [{"provider": "mock", "model": "mock-a-large", "family": "mock-a"}],
    },
    "prices": {},
    "retries": {"max_attempts": 3, "base_delay_s": 0.0, "max_delay_s": 0.0},
    "structured": {"max_retries": 2},
}
# What the two extraction passes agree on in the M5 README (tests/ingest_fixtures.py): three
# claims, one per section, each with a verbatim quote.
README_CLAIMS = [
    ("grants ownership", "lease manager", "grants", "shard ownership", "component"),
    ("WAL rejects appends", "WAL", "rejects", "stale epoch appends", "component"),
    ("write router sustains", "write router", "sustains", "peak writes", "component"),
]
README_QUOTES = {
    "grants ownership": "The lease manager grants ownership of a shard for a bounded TTL.",
    "WAL rejects appends": "The WAL rejects appends\nwith a stale epoch",
    "write router sustains": "The write router sustains 2000 writes per second at peak",
}
INGESTION_GRAPH = {
    "nodes": {"claim": 3, "entity": 6, "source": 1},
    "edges": {"ABOUT": 6, "EVIDENCES": 3, "GRANTS": 1, "REJECTS": 1, "SUSTAINS": 1},
}


def ingest_readme_corpus(pool, tmp_path: Path) -> list[str]:
    """The M5 corpus through the real pipeline: ingest the README, two scripted passes that
    agree on three claims. Returns the committed claim ids."""
    ledger.create_project(pool, PROJECT)
    store = LocalObjectStore(tmp_path / "objects")
    config = IngestConfig()
    path = tmp_path / "README.md"
    path.write_text(README, encoding="utf-8")
    source = Ingestor(pool, store, config).ingest_file(PROJECT, path)
    catch_up(pool, PROJECT)
    mock, mock_b = MockProvider("mock"), MockProvider("mock-b")
    gateway = Gateway(
        pool, from_mapping(INGEST_MODELS), {"mock": mock, "mock-b": mock_b}, sleep=lambda s: 0
    )
    pipeline = Pipeline(pool, gateway, store, config)
    claims = []
    for marker, subject, predicate, obj, entity_type in README_CLAIMS:
        quote = README_QUOTES[marker]
        segment = next(
            s
            for s in pipeline.segments_for(pipeline.source(PROJECT, source.source_id))
            if " ".join(quote.split()) in " ".join(s.text.split())
        )
        claims.append(
            {
                "segment_locator": segment.locator,
                "subject": {"entity_type": entity_type, "name": subject},
                "predicate": predicate,
                "object": {"entity_type": "property", "name": obj},
                "quote": quote,
            }
        )
    mock.enqueue({"claims": claims})
    for claim in claims:  # pass B answers per segment, in document order
        mock_b.enqueue({"claims": [claim]})
    report = pipeline.run(PROJECT, source.source_id)
    assert len(report.committed) == 3 and report.quarantined == [], report
    catch_up(pool, PROJECT)
    return report.committed


def graph_rows(pool, project_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with pool.connection() as conn:
        return all_nodes(conn, project_id), all_edges(conn, project_id)


def assert_every_edge_has_provenance(pool, project_id: str) -> None:
    nodes, edges = graph_rows(pool, project_id)
    seqs = {e["seq"] for e in ledger.iter_events(pool, project_id)}
    claim_ids = {n["id"] for n in nodes if n["type"] == "claim"}
    assert edges
    for edge in edges:
        assert edge["seq"] in seqs, f"edge {edge} names no committed event"
    claim_edges = [e for e in edges if e["claim_id"] is not None]
    assert {e["claim_id"] for e in claim_edges} <= claim_ids
    assert all(e["claim_id"] for e in edges if e["edge_type"] == ABOUT)
    assert len({(e["seq"], e["ord"]) for e in edges}) == len(edges), "(seq, ord) names one edge"


def test_k1_the_fixture_graph_matches_the_pinned_table_and_a_rebuild_is_identical(pool):
    commit_fixture(pool)
    Projector(pool).catch_up()
    assert counts(pool, FIX) == FIXTURE_GRAPH
    assert_every_edge_has_provenance(pool, FIX)
    nodes, edges = graph_rows(pool, FIX)
    by_id = {n["id"]: n for n in nodes}
    # entities come from claim subjects and objects; a literal object is a value, not a node
    assert by_id["ent:technique:lease-ownership"]["type"] == "entity"
    assert by_id["ent:technique:lease-ownership"]["entity_type"] == "technique"
    assert by_id["req_FIXQPS001"]["type"] == "requirement", "a claim's subject and a model element"
    assert by_id["cmp_FIXWAL0001"] == {
        "id": "cmp_FIXWAL0001",
        "type": "element",
        "entity_type": "components",
        "label": "Write-Ahead Log",
        "seq": by_id["cmp_FIXWAL0001"]["seq"],
    }
    assert by_id["adr_FIXLEASE01"]["label"] == "Lease-based shard ownership with fencing epochs"
    predicate = next(e for e in edges if e["edge_type"] == "HAS_FSYNC_P99")
    assert (predicate["src"], predicate["dst"], predicate["claim_id"]) == (
        "ent:hardware_part:nvme-gen4-dc",
        "ent:metric:fsync-p99-latency",
        "clm_FIXFSYNC01",
    )
    # model links are the HEAD model's only: the fixture's head has 5 SATISFIES links
    with pool.connection() as conn:
        every_version = conn.execute(
            "SELECT count(*) AS n FROM proj_edges WHERE project_id = %s AND edge_type = %s",
            (FIX, "SATISFIES"),
        ).fetchone()["n"]
    assert every_version > FIXTURE_GRAPH["edges"]["SATISFIES"]

    before = (projector.content_hash(pool, FIX), counts(pool, FIX), graph_rows(pool, FIX))
    Projector(pool).rebuild(FIX)
    assert (projector.content_hash(pool, FIX), counts(pool, FIX), graph_rows(pool, FIX)) == before


def test_k1_the_ingestion_corpus_graph_matches_the_pinned_table(pool, tmp_path):
    committed = ingest_readme_corpus(pool, tmp_path)
    assert counts(pool, PROJECT) == INGESTION_GRAPH
    assert_every_edge_has_provenance(pool, PROJECT)
    nodes, edges = graph_rows(pool, PROJECT)
    assert {n["id"] for n in nodes if n["type"] == "entity"} == {
        "ent:component:lease-manager",
        "ent:property:shard-ownership",
        "ent:component:wal",
        "ent:property:stale-epoch-appends",
        "ent:component:write-router",
        "ent:property:peak-writes",
    }
    assert {e["claim_id"] for e in edges if e["claim_id"]} == set(committed)
    rejects = next(e for e in edges if e["edge_type"] == "REJECTS")
    assert (rejects["src"], rejects["dst"]) == (
        "ent:component:wal",
        "ent:property:stale-epoch-appends",
    )
    # the edge carries its claim's current status, grade and conditions when it is read
    seen = SqlGraphStore(pool).neighbors(PROJECT, "ent:component:wal")
    edge = next(e for e in seen["edges"] if e["type"] == "REJECTS")
    assert (edge["status"], edge["grade"], edge["conditions"]) == ("documented", "unverified", {})
    assert edge["provenance"]["claim_id"] == rejects["claim_id"]
    assert edge["provenance"]["event_id"] and edge["provenance"]["seq"] == rejects["seq"]
    # the quote each claim was committed with is part of its searchable text
    index.sync(pool, PROJECT, config=knowledge_config())
    with pool.connection() as conn:
        texts = [
            r["text"]
            for r in conn.execute("SELECT text FROM kg_claim_text ORDER BY claim_id").fetchall()
        ]
    assert any("wal rejects stale epoch appends" in t and "with a stale epoch" in t for t in texts)

    before = (projector.content_hash(pool, PROJECT), graph_rows(pool, PROJECT))
    Projector(pool).rebuild(PROJECT)
    assert (projector.content_hash(pool, PROJECT), graph_rows(pool, PROJECT)) == before


def test_k1_an_edge_without_provenance_cannot_be_stored(pool, dsn):
    ledger.create_project(pool, PROJECT)
    with psycopg.connect(dsn, autocommit=True) as conn:
        for seq, claim_id in ((None, "clm_NOSEQ00001"), (3, None)):
            with pytest.raises(psycopg.errors.NotNullViolation):
                conn.execute(
                    "INSERT INTO proj_graph_edges (project_id, seq, ord, edge_type, src, dst, "
                    "claim_id) VALUES (%s, %s, 0, 'USES', 'a', 'b', %s)",
                    (PROJECT, seq, claim_id),
                )
        with pytest.raises(psycopg.errors.NotNullViolation):
            conn.execute(
                "INSERT INTO proj_graph_nodes (project_id, node_id, node_type, label, origin, seq) "
                "VALUES (%s, 'x', 'entity', 'x', 'event', NULL)",
                (PROJECT,),
            )
    assert entity_node({"entity_type": "text", "literal": "a value"}) is None
    assert entity_node({"entity_type": "community", "id": "comm_0_abc"})[:2] == (
        "comm_0_abc",
        "community",
    )


def test_k1_only_the_head_model_contributes_element_nodes_and_links(pool):
    commit_fixture(pool)
    Projector(pool).catch_up()
    nodes, edges = graph_rows(pool, FIX)
    with pool.connection() as conn:
        head = conn.execute(
            "SELECT version_id, model FROM proj_model_versions WHERE project_id = %s "
            "ORDER BY committed_at_seq DESC LIMIT 1",
            (FIX,),
        ).fetchone()
    in_head = {
        element["id"] for elements in head["model"]["elements"].values() for element in elements
    }
    assert {n["id"] for n in nodes if n["type"] == "element"} == in_head
    # the fixture's requirements are claims, not model elements: their nodes come from the
    # claims' subjects, and the head's SATISFIES links point at them
    assert {n["id"] for n in nodes if n["type"] == "requirement"} == {
        "req_FIXQPS001",
        "req_FIXDUR001",
    }
    links = [e for e in edges if e["edge_type"] in ("SATISFIES", "DEPENDS_ON", "MITIGATES")]
    assert len(links) == sum(len(v) for v in head["model"]["links"].values())


# --- K2: the SQL backend against a reference, and the two backends against each other -----------


def seeded_queries(nodes: list[str], seed: int = 11) -> list[tuple[str, tuple]]:
    """Twenty traversal queries from a seeded generator: ten neighbors, ten paths."""
    rng = random.Random(seed)
    queries: list[tuple[str, tuple]] = []
    for n in range(10):
        types = None if n % 3 else sorted(rng.sample(GRAPH_PREDICATES, 2))
        queries.append(("neighbors", (rng.choice(nodes), 1 + n % 3, types)))
    for n in range(10):
        a, b = rng.sample(nodes, 2)
        types = None if n % 4 else sorted(rng.sample(GRAPH_PREDICATES, 3))
        queries.append(("paths", (a, b, 2 + n % 5, types)))
    return queries


def ask(store, project_id: str, kind: str, args: tuple) -> Any:
    if kind == "neighbors":
        node, depth, types = args
        return store.neighbors(project_id, node, depth=depth, edge_types=types)
    a, b, max_depth, types = args
    return store.paths(project_id, a, b, max_depth=max_depth, edge_types=types)


def test_k2_the_sql_graph_store_agrees_with_a_reference_traversal(pool):
    nodes = commit_random_graph(pool, PROJECT)
    store = SqlGraphStore(pool)
    with pool.connection() as conn:
        edges = [e for e in all_edges(conn, PROJECT) if e["edge_type"] in GRAPH_PREDICATES]
    answered_paths = 0
    for kind, args in seeded_queries(nodes):
        if kind == "neighbors":
            node, depth, types = args
            types = types if types is not None else list(GRAPH_PREDICATES)
            answer = store.neighbors(PROJECT, node, depth=depth, edge_types=types)
            expected = reference_distances(edges, node, depth, types)
            assert {n["id"]: n["distance"] for n in answer["nodes"]} == expected
            inside = set(expected)
            assert {(e["src"], e["dst"], e["type"]) for e in answer["edges"]} == {
                (e["src"], e["dst"], e["edge_type"])
                for e in edges
                if e["src"] in inside and e["dst"] in inside and e["edge_type"] in types
            }
            assert all(e["provenance"]["claim_id"] and e["status"] for e in answer["edges"])
        else:
            a, b, max_depth, types = args
            types = types if types is not None else list(GRAPH_PREDICATES)
            answer = store.paths(PROJECT, a, b, max_depth=max_depth, edge_types=types)
            assert answer == reference_paths(edges, a, b, max_depth, types)
            answered_paths += bool(answer)
    assert answered_paths >= 5, "the seeded queries exercise real paths"

    # the limits of the interface
    with pytest.raises(ValueError, match="depth must be between 1 and 3"):
        store.neighbors(PROJECT, nodes[0], depth=4)
    with pytest.raises(ValueError, match="max_depth must be between 1 and 6"):
        store.paths(PROJECT, nodes[0], nodes[1], max_depth=7)
    assert store.paths(PROJECT, nodes[0], nodes[0]) == [[nodes[0]]]
    assert store.paths(PROJECT, nodes[0], "ent:service:nowhere") == []
    sub = store.subgraph(PROJECT, nodes[:5])
    assert [n["id"] for n in sub["nodes"]] == sorted(nodes[:5])
    assert all(e["src"] in nodes[:5] and e["dst"] in nodes[:5] for e in sub["edges"])
    # a claim node is a node too: its neighbours are what it is about
    claim_id = edges[0]["claim_id"]
    about = store.neighbors(PROJECT, claim_id, depth=1, edge_types=[ABOUT])
    assert {n["id"] for n in about["nodes"]} == {claim_id, edges[0]["src"], edges[0]["dst"]}
    assert graph_store(pool, knowledge_config(graph_backend="sql")).name == "sql"


@pytest.mark.needs_age
def test_k2_the_age_backend_gives_the_same_answers_as_sql_on_twenty_seeded_queries(pool):
    nodes = commit_random_graph(pool, PROJECT)
    sql_store, age_store = SqlGraphStore(pool), AgeGraphStore(pool)
    assert graph_store(pool, knowledge_config(graph_backend="auto")).name == "age"
    try:
        queries = seeded_queries(nodes)
        assert len(queries) == 20
        nonempty = 0
        for kind, args in queries:
            from_sql, from_age = (
                ask(sql_store, PROJECT, kind, args),
                ask(age_store, PROJECT, kind, args),
            )
            assert from_age == from_sql, (kind, args)
            nonempty += bool(from_sql["edges"] if kind == "neighbors" else from_sql)
        assert nonempty >= 12, "the parity is over real answers, not empty ones"
        assert age_store.subgraph(PROJECT, nodes[:6]) == sql_store.subgraph(PROJECT, nodes[:6])

        # the AGE copy follows the ledger: a new claim, then a merge, then its revert
        new = make_claim(
            "ageextra001",
            {"entity_type": "service", "id": "node-00"},
            "USES",
            {"entity_type": "service", "id": "brand-new"},
            source="graph",
            taint_origin="user",
        )
        commit_claim(pool, PROJECT, new)
        catch_up(pool, PROJECT)
        for depth in (1, 2):
            assert age_store.neighbors(PROJECT, nodes[0], depth=depth) == sql_store.neighbors(
                PROJECT, nodes[0], depth=depth
            )
        merge = merge_event(pool, PROJECT, nodes[1], nodes[2])
        for store in (age_store, sql_store):
            assert store.neighbors(PROJECT, nodes[2])["node"] == nodes[1]
        assert age_store.neighbors(PROJECT, nodes[1], depth=2) == sql_store.neighbors(
            PROJECT, nodes[1], depth=2
        )
        resolution.revert_merge(pool, PROJECT, merge, signer="saumya")
        for kind, args in queries[:6]:
            assert ask(age_store, PROJECT, kind, args) == ask(sql_store, PROJECT, kind, args)
    finally:
        age_store.drop(PROJECT)


# --- K3: entity resolution ------------------------------------------------------------------------


def merge_event(pool, project_id: str, kept: str, merged: str, method: str = "human") -> str:
    from architect.arbiter import Arbiter
    from builders import HUMAN, candidate

    commit = Arbiter(pool).submit(
        project_id,
        candidate(
            "entity.merged",
            {"kept_id": kept, "merged_ids": [merged], "method": method},
            actor=HUMAN,
        ),
    )
    catch_up(pool, project_id)
    return commit.event["event_id"]


def resolution_project(pool) -> None:
    """Entities with an obvious duplicate (lease-manager / lease-managers), an ambiguous pair
    (write-router / write-routing-tier), a look-alike of ANOTHER type (a component and a
    technique both named cache) and unrelated ones."""
    ledger.create_project(pool, PROJECT)
    commit_source(pool, PROJECT, "notes", "user", "file:///notes.md")
    rows = [
        ("r01", "component", "lease-manager", "GRANTS", "shard-lease"),
        ("r02", "component", "lease-manager", "RENEWS", "shard-lease"),
        ("r03", "component", "lease-managers", "GRANTS", "epoch"),
        ("r04", "component", "write-router", "ROUTES", "writes"),
        ("r05", "component", "write-routing-tier", "ROUTES", "requests"),
        ("r06", "component", "cache", "STORES", "hot-keys"),
        ("r07", "technique", "cache", "REDUCES", "latency"),
        ("r08", "component", "audit-log", "RECORDS", "actions"),
    ]
    for name, entity_type, subject, predicate, obj in rows:
        commit_claim(
            pool,
            PROJECT,
            make_claim(
                name,
                {"entity_type": entity_type, "id": subject},
                predicate,
                {"entity_type": "property", "id": obj},
                source="notes",
                taint_origin="user",
            ),
        )
    catch_up(pool, PROJECT)


def script_entity_vectors(
    pool, provider: MockProvider, similar: dict[tuple[str, str], float]
) -> None:
    """Scripted embeddings for the entities: every entity gets its own direction, and each
    listed pair is placed at exactly the given cosine."""
    config = knowledge_config()
    index.sync(pool, PROJECT, config=config)  # the texts only: nothing is embedded yet
    with pool.connection() as conn:
        texts = {
            r["entity_id"]: r["text"]
            for r in conn.execute("SELECT entity_id, text FROM kg_entity_text").fetchall()
        }
    dim = 64
    basis = {entity: [0.0] * dim for entity in texts}
    for n, entity in enumerate(sorted(texts)):
        basis[entity][n] = 1.0
    for (a, b), wanted in similar.items():
        mixed = [
            wanted * x + (1 - wanted**2) ** 0.5 * y for x, y in zip(basis[a], basis[b], strict=True)
        ]
        basis[b] = mixed
        assert cosine(basis[a], basis[b]) == pytest.approx(wanted)
    for entity, text in texts.items():
        provider.vectors[text] = basis[entity]


LEASE, LEASES = "ent:component:lease-manager", "ent:component:lease-managers"
ROUTER, ROUTING = "ent:component:write-router", "ent:component:write-routing-tier"


def merges_of(pool) -> list[dict[str, Any]]:
    return [e for e in ledger.iter_events(pool, PROJECT) if e["type"] == "entity.merged"]


def test_k3_blocking_pairs_share_a_type_and_a_token_or_a_close_slug():
    entities = [
        {"entity_id": LEASE, "entity_type": "component", "name": "lease manager"},
        {"entity_id": LEASES, "entity_type": "component", "name": "lease managers"},
        {"entity_id": "ent:component:cache", "entity_type": "component", "name": "cache"},
        {"entity_id": "ent:technique:cache", "entity_type": "technique", "name": "cache"},
        {"entity_id": "ent:component:cachr", "entity_type": "component", "name": "cachr"},
        {"entity_id": "ent:component:audit-log", "entity_type": "component", "name": "audit log"},
    ]
    assert resolution.blocked_pairs(entities, 2) == [
        ("ent:component:cache", "ent:component:cachr"),  # edit distance 1, no shared token
        (LEASE, LEASES),  # a shared token
    ]
    assert resolution.edit_distance("kitten", "sitting", 3) == 3
    assert resolution.edit_distance("kitten", "sitting", 2) == 3, "limit + 1 once it is exceeded"
    assert resolution.edit_distance("lease-manager", "lease-managers", 2) == 1


def test_k3_duplicates_merge_ambiguous_pairs_are_adjudicated_and_types_never_mix(pool):
    resolution_project(pool)
    provider = MockProvider("mock")
    script_entity_vectors(pool, provider, {(LEASE, LEASES): 0.97, (ROUTER, ROUTING): 0.86})
    # the look-alike of another type would score 1.0 if it were ever compared
    provider.vectors["cache | reduces"] = provider.vectors["cache | stores"]
    provider.enqueue({"same": True, "reason": "a routing tier is the write router"})
    gateway = knowledge_gateway(pool, provider)
    plane = KnowledgePlane(pool, gateway, knowledge_config(graph_backend="sql"))

    report = plane.resolve_entities(PROJECT)
    assert [(m["kept_id"], m["merged_id"], m["method"]) for m in report["merged"]] == [
        (LEASE, LEASES, "embedding"),  # 0.97 >= 0.92: no model asked
        (ROUTER, ROUTING, "llm_adjudicated"),  # 0.86 is between 0.80 and 0.92
    ]
    assert [(a["a"], a["b"], a["same"]) for a in report["adjudicated"]] == [(ROUTER, ROUTING, True)]
    assert provider.call_count == 1, "one adjudication, for the one ambiguous pair"
    asked = provider.calls[0]
    assert "<<<UNTRUSTED-DATA" in asked.messages[0]["content"] and asked.output_schema
    # merges are events, through the Arbiter, with the method that decided them
    merges = merges_of(pool)
    assert [
        (e["payload"]["kept_id"], e["payload"]["merged_ids"], e["payload"]["method"])
        for e in merges
    ] == [
        (LEASE, [LEASES], "embedding"),
        (ROUTER, [ROUTING], "llm_adjudicated"),
    ]
    assert all(e["actor"]["id"] == "entity-resolver" for e in merges)
    with pool.connection() as conn:
        calls = conn.execute(
            "SELECT purpose, tier, status FROM gw_calls WHERE purpose = 'entity-adjudicate' "
            "AND status = 'ok'"
        ).fetchall()
    assert calls == [{"purpose": "entity-adjudicate", "tier": "tier-cheap", "status": "ok"}]

    # the graph answers to the kept id; the two caches of different types were never merged
    nodes, edges = graph_rows(pool, PROJECT)
    ids = {n["id"] for n in nodes}
    assert LEASE in ids and LEASES not in ids and ROUTER in ids and ROUTING not in ids
    assert {"ent:component:cache", "ent:technique:cache"} <= ids
    grants = sorted(e["dst"] for e in edges if e["src"] == LEASE and e["edge_type"] == "GRANTS")
    assert grants == ["ent:property:epoch", "ent:property:shard-lease"], "both names' claims"
    store = plane.graph
    assert store.neighbors(PROJECT, LEASES)["node"] == LEASE
    assert "ent:property:epoch" in {n["id"] for n in store.neighbors(PROJECT, LEASE)["nodes"]}
    assert plane.counts(PROJECT)["nodes"]["entity"] == 12, "fourteen entities, two merged away"

    # re-running proposes nothing new and asks no model again
    embeds, asks = len(provider.embed_calls), provider.call_count
    again = plane.resolve_entities(PROJECT)
    assert again["merged"] == [] and len(merges_of(pool)) == 2
    assert provider.call_count == asks and len(provider.embed_calls) == embeds


def test_k3_a_revert_restores_the_graph_and_the_pair_is_never_proposed_again(pool):
    resolution_project(pool)
    provider = MockProvider("mock")
    script_entity_vectors(pool, provider, {(LEASE, LEASES): 0.97})
    plane = KnowledgePlane(
        pool, knowledge_gateway(pool, provider), knowledge_config(graph_backend="sql")
    )
    before = graph_rows(pool, PROJECT)
    report = plane.resolve_entities(PROJECT)
    (merge,) = report["merged"]
    assert graph_rows(pool, PROJECT) != before
    assert report["distinct"] >= 1, "the unscripted write-router pair scored below the low bar"

    event = resolution.revert_merge(pool, PROJECT, merge["event_id"], signer="saumya")
    assert event["type"] == "entity.merge_reverted" and event["actor"]["kind"] == "human"
    assert graph_rows(pool, PROJECT) == before, "the graph is what it was before the merge"
    assert plane.graph.neighbors(PROJECT, LEASES)["node"] == LEASES

    again = plane.resolve_entities(PROJECT)
    assert again["merged"] == [] and again["skipped_reverted"] == 1
    assert len(merges_of(pool)) == 1, "the reverted pair was not proposed again"
    assert resolution.reverted_pairs(pool, PROJECT) == {frozenset((LEASE, LEASES))}

    # a rebuild of the projection replays merge and revert to the same graph
    hashed = projector.content_hash(pool, PROJECT)
    Projector(pool).rebuild(PROJECT)
    assert projector.content_hash(pool, PROJECT) == hashed and graph_rows(pool, PROJECT) == before


def test_k3_a_declined_adjudication_merges_nothing_and_merges_chain_to_the_kept_id(pool):
    resolution_project(pool)
    provider = MockProvider("mock")
    script_entity_vectors(pool, provider, {(ROUTER, ROUTING): 0.86})
    provider.enqueue({"same": False, "reason": "a tier is a deployment, a router is a component"})
    plane = KnowledgePlane(
        pool, knowledge_gateway(pool, provider), knowledge_config(graph_backend="sql")
    )
    report = plane.resolve_entities(PROJECT)
    assert report["merged"] == [] and report["adjudicated"][0]["same"] is False
    assert merges_of(pool) == []
    # asked again, the same question is answered from the gateway's cache: no provider call
    plane.resolve_entities(PROJECT)
    assert provider.call_count == 1

    # a -> b, then b -> c: everything answers to c
    first = merge_event(pool, PROJECT, ROUTER, ROUTING)
    merge_event(pool, PROJECT, LEASE, ROUTER)
    with pool.connection() as conn:
        aliases = {
            r["entity_id"]: r["canonical_id"]
            for r in conn.execute(
                "SELECT entity_id, canonical_id FROM proj_entity_alias"
            ).fetchall()
        }
    assert aliases == {ROUTING: LEASE, ROUTER: LEASE}
    resolution.revert_merge(pool, PROJECT, first, signer="saumya")
    with pool.connection() as conn:
        aliases = {
            r["entity_id"]: r["canonical_id"]
            for r in conn.execute(
                "SELECT entity_id, canonical_id FROM proj_entity_alias"
            ).fetchall()
        }
    assert aliases == {ROUTER: LEASE}
