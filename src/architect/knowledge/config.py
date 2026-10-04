"""config/knowledge.yaml, loaded and typed."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

GRAPH_BACKENDS = ("auto", "sql", "age")
VECTOR_BACKENDS = ("auto", "pgvector", "exact")


@dataclass(frozen=True)
class KnowledgeConfig:
    graph_backend: str = "auto"
    max_neighbor_depth: int = 3
    max_path_depth: int = 6
    vector_backend: str = "auto"
    hnsw: dict[str, int] = field(
        default_factory=lambda: {"m": 16, "ef_construction": 64, "ef_search": 100}
    )
    embedding_tier: str = "embedding"
    embedding_batch_size: int = 64
    # entity resolution
    high: float = 0.92
    low: float = 0.80
    max_edit_distance: int = 2
    context_predicates: int = 3
    adjudicate_tier: str = "tier-cheap"
    adjudicate_max_tokens: int = 200
    # retrieval
    k: int = 20
    rrf_k: int = 60
    candidates: int = 50
    entity_matches: int = 5
    entity_similarity: float = 0.60
    default_grades: tuple[str, ...] = ("design_grade", "unverified")
    # communities
    seed: int = 7
    resolutions: tuple[float, ...] = (1.0, 0.5)
    min_size: int = 2
    top_claims: int = 12
    summary_tier: str = "tier-mid"
    summary_max_tokens: int = 400


def from_mapping(data: dict[str, Any]) -> KnowledgeConfig:
    graph = data.get("graph", {})
    vectors = data.get("vectors", {})
    embedding = data.get("embedding", {})
    resolution = data.get("resolution", {})
    retrieval = data.get("retrieval", {})
    communities = data.get("communities", {})
    default = KnowledgeConfig()
    config = KnowledgeConfig(
        graph_backend=graph.get("backend", default.graph_backend),
        max_neighbor_depth=int(graph.get("max_neighbor_depth", default.max_neighbor_depth)),
        max_path_depth=int(graph.get("max_path_depth", default.max_path_depth)),
        vector_backend=vectors.get("backend", default.vector_backend),
        hnsw={k: int(v) for k, v in (vectors.get("hnsw") or default.hnsw).items()},
        embedding_tier=embedding.get("tier", default.embedding_tier),
        embedding_batch_size=int(embedding.get("batch_size", default.embedding_batch_size)),
        high=float(resolution.get("high", default.high)),
        low=float(resolution.get("low", default.low)),
        max_edit_distance=int(resolution.get("max_edit_distance", default.max_edit_distance)),
        context_predicates=int(resolution.get("context_predicates", default.context_predicates)),
        adjudicate_tier=resolution.get("adjudicate_tier", default.adjudicate_tier),
        adjudicate_max_tokens=int(
            resolution.get("adjudicate_max_tokens", default.adjudicate_max_tokens)
        ),
        k=int(retrieval.get("k", default.k)),
        rrf_k=int(retrieval.get("rrf_k", default.rrf_k)),
        candidates=int(retrieval.get("candidates", default.candidates)),
        entity_matches=int(retrieval.get("entity_matches", default.entity_matches)),
        entity_similarity=float(retrieval.get("entity_similarity", default.entity_similarity)),
        default_grades=tuple(retrieval.get("default_grades", default.default_grades)),
        seed=int(communities.get("seed", default.seed)),
        resolutions=tuple(float(r) for r in communities.get("resolutions", default.resolutions)),
        min_size=int(communities.get("min_size", default.min_size)),
        top_claims=int(communities.get("top_claims", default.top_claims)),
        summary_tier=communities.get("summary_tier", default.summary_tier),
        summary_max_tokens=int(communities.get("summary_max_tokens", default.summary_max_tokens)),
    )
    if config.graph_backend not in GRAPH_BACKENDS:
        raise ValueError(f"graph.backend must be one of {GRAPH_BACKENDS}")
    if config.vector_backend not in VECTOR_BACKENDS:
        raise ValueError(f"vectors.backend must be one of {VECTOR_BACKENDS}")
    if not 0 <= config.low <= config.high <= 1:
        raise ValueError("resolution thresholds must satisfy 0 <= low <= high <= 1")
    if len(config.resolutions) != 2:
        raise ValueError("communities.resolutions takes exactly two levels")
    return config


def _config_path() -> Path:
    for base in (Path.cwd().resolve(), *Path(__file__).resolve().parents):
        candidate = base / "config" / "knowledge.yaml"
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("config/knowledge.yaml not found")


def load_knowledge_config(path: Path | None = None) -> KnowledgeConfig:
    source = path or _config_path()
    return from_mapping(yaml.safe_load(source.read_text(encoding="utf-8")) or {})
