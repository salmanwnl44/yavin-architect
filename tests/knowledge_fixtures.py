"""Shared pieces of the M8 (knowledge plane) tests: a model table with an embedding tier on
the mock provider, the labeled retrieval corpus, a scripted embedder, a seeded random graph,
and reference implementations the backends are compared against."""

from __future__ import annotations

import json
import random
import re
from collections import deque
from pathlib import Path
from typing import Any

from architect import ledger
from architect.arbiter import Arbiter
from architect.gateway.config import from_mapping
from architect.gateway.gateway import Gateway
from architect.gateway.providers.mock import MockProvider
from architect.knowledge.config import KnowledgeConfig, load_knowledge_config
from architect.projector import Projector
from builders import candidate, ident

CORPUS_PATH = Path(__file__).parent / "fixtures" / "retrieval" / "corpus.json"
DIM = 64

# every tier on the mock provider, and an embedding tier of 64-d vectors with a usage price
K_MODELS: dict[str, Any] = {
    "tiers": {
        "tier-cheap": [{"provider": "mock", "model": "mock-small", "family": "mock-a"}],
        "tier-mid": [{"provider": "mock", "model": "mock-medium", "family": "mock-a"}],
        "tier-frontier": [{"provider": "mock", "model": "mock-large", "family": "mock-a"}],
        "embedding": [
            {"provider": "mock", "model": "mock-embed", "family": "mock-embed", "dim": DIM}
        ],
    },
    "prices": {
        "mock-small": {"input": 1.0, "output": 5.0},
        "mock-medium": {"input": 2.0, "output": 10.0},
        "mock-large": {"input": 4.0, "output": 20.0},
        "mock-embed": {"input": 0.5, "output": 0.0},
    },
    "retries": {"max_attempts": 3, "base_delay_s": 0.0, "max_delay_s": 0.0},
    "structured": {"max_retries": 2},
}


def knowledge_gateway(pool, provider: MockProvider, mode: str = "live") -> Gateway:
    return Gateway(pool, from_mapping(K_MODELS), {"mock": provider}, mode=mode, sleep=lambda s: 0)


def knowledge_config(**overrides: Any) -> KnowledgeConfig:
    import dataclasses

    return dataclasses.replace(load_knowledge_config(), **overrides)


def catch_up(pool, project_id: str) -> None:
    Projector(pool).catch_up(project_id)


# --- claims ---------------------------------------------------------------------------------


def source_id(name: str) -> str:
    return ident("src", name)


def commit_source(pool, project_id: str, name: str, taint_origin: str, uri: str) -> str:
    Arbiter(pool).submit(
        project_id,
        candidate(
            "source.ingested",
            {
                "source_id": source_id(name),
                "uri": uri,
                "content_hash": f"sha256:{name}",
                "media_type": "text/markdown",
                "taint_origin": taint_origin,
            },
        ),
    )
    return source_id(name)


def make_claim(
    name: str,
    subject: dict[str, Any],
    predicate: str,
    obj: dict[str, Any],
    *,
    source: str,
    taint_origin: str,
    **extra: Any,
) -> dict[str, Any]:
    return {
        "id": ident("clm", name),
        "subject": subject,
        "predicate": predicate,
        "object": obj,
        "status": "documented",
        "evidence": [{"source": source_id(source), "span": f"{name} ¶1", "kind": "statement"}],
        "taint": {"origin": taint_origin},
        "recorded_at": "2026-10-04T09:00:00+05:30",
        "provenance": {
            "extractor": {"model_tier": "tier-cheap", "prompt_hash": "k5", "pipeline_version": 1}
        },
        **extra,
    }


def commit_claim(pool, project_id: str, claim: dict[str, Any]) -> str:
    Arbiter(pool).submit(
        project_id, candidate("claim.committed", {"claim_id": claim["id"], "claim": claim})
    )
    return claim["id"]


# --- the labeled retrieval corpus ---------------------------------------------------------------


def load_corpus() -> dict[str, Any]:
    return json.loads(CORPUS_PATH.read_text(encoding="utf-8"))


def commit_corpus(pool, project_id: str, *, create: bool = True) -> dict[str, str]:
    """Commit the corpus through the Arbiter. Returns name -> claim id (quarantined proposals
    included, under their names)."""
    corpus = load_corpus()
    if create:
        ledger.create_project(pool, project_id)
    for name, source in corpus["sources"].items():
        commit_source(pool, project_id, name, source["taint_origin"], source["uri"])
    ids: dict[str, str] = {}
    for entry in corpus["claims"]:
        taint = corpus["sources"][entry["source"]]["taint_origin"]
        claim = make_claim(
            entry["name"],
            entry["subject"],
            entry["predicate"],
            entry["object"],
            source=entry["source"],
            taint_origin=taint,
        )
        ids[entry["name"]] = commit_claim(pool, project_id, claim)
    for n, entry in enumerate(corpus["quarantined"]):
        taint = corpus["sources"][entry["source"]]["taint_origin"]
        claim = make_claim(
            entry["name"],
            entry["subject"],
            entry["predicate"],
            entry["object"],
            source=entry["source"],
            taint_origin=taint,
        )
        Arbiter(pool).submit(
            project_id,
            candidate("claim.proposed", {"proposal_id": f"prp-quarantined-{n}", "claim": claim}),
        )
        ids[entry["name"]] = claim["id"]
    catch_up(pool, project_id)
    return ids


_WORD = re.compile(r"[a-z0-9]+")


def scripted_embedder(corpus: dict[str, Any]) -> MockProvider:
    """The scripted embeddings of the retrieval golden: one direction per word of the corpus,
    synonyms sharing the direction of the word they mean, and no direction at all for the
    words the fixture lists as unknown. Deterministic; no model."""
    provider = MockProvider("mock")
    provider.known_concepts_only = True
    unknown = set(corpus["unknown_to_embedder"])
    for entry in corpus["claims"]:
        text = " ".join(
            [entry["subject"]["id"], entry["predicate"], str(entry["object"].get("id", ""))]
        )
        for word in _WORD.findall(text.lower().replace("_", " ")):
            if word not in unknown:
                provider.concepts[word] = word
    provider.concepts |= corpus["synonyms"]
    return provider


# --- a seeded random graph, and what a traversal of it must return ------------------------------

GRAPH_PREDICATES = ("USES", "REQUIRES", "CALLS", "REPLICATES")


def commit_random_graph(
    pool, project_id: str, *, entities: int = 30, claims: int = 70, seed: int = 7
) -> list[str]:
    """`claims` claims between `entities` entities, chosen by a seeded generator: the same
    graph every time. Returns the entity node ids."""
    rng = random.Random(seed)
    ledger.create_project(pool, project_id)
    commit_source(pool, project_id, "graph", "user", "file:///graph.md")
    names = [f"node-{n:02d}" for n in range(entities)]
    for n in range(claims):
        a, b = rng.sample(names, 2)
        claim = make_claim(
            f"edge{n:04d}",
            {"entity_type": "service", "id": a},
            rng.choice(GRAPH_PREDICATES),
            {"entity_type": "service", "id": b},
            source="graph",
            taint_origin="user",
        )
        commit_claim(pool, project_id, claim)
    catch_up(pool, project_id)
    return [f"ent:service:{name}" for name in names]


def adjacency(edges: list[dict[str, Any]], edge_types: list[str] | None) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for edge in edges:
        if edge_types is None or edge["edge_type"] in edge_types:
            out.setdefault(edge["src"], set()).add(edge["dst"])
            out.setdefault(edge["dst"], set()).add(edge["src"])
    return out


def reference_distances(
    edges: list[dict[str, Any]], start: str, depth: int, edge_types: list[str] | None
) -> dict[str, int]:
    """Breadth-first distances up to `depth`, straight from the edge list."""
    near = adjacency(edges, edge_types)
    distances = {start: 0}
    queue = deque([start])
    while queue:
        node = queue.popleft()
        if distances[node] == depth:
            continue
        for other in sorted(near.get(node, ())):
            if other not in distances:
                distances[other] = distances[node] + 1
                queue.append(other)
    return distances


def reference_paths(
    edges: list[dict[str, Any]],
    start: str,
    goal: str,
    max_depth: int,
    edge_types: list[str] | None,
) -> list[list[str]]:
    """Every shortest path of at most max_depth edges, by exhaustive search."""
    if start == goal:
        return [[start]]
    near = adjacency(edges, edge_types)
    found: list[list[str]] = []
    frontier = [[start]]
    for _ in range(max_depth):
        longer = []
        for path in frontier:
            for other in sorted(near.get(path[-1], ())):
                if other not in path:
                    longer.append(path + [other])
        found = [path for path in longer if path[-1] == goal]
        if found:
            break
        frontier = longer
    return sorted(found)
