"""The knowledge plane in one object: a pool, the gateway, the configuration, and the two
backends it chose (graph store and vector index). What the API, the CLI and the Context
Compiler use."""

from __future__ import annotations

from typing import Any

from psycopg_pool import ConnectionPool

from architect.gateway.gateway import Gateway
from architect.knowledge import communities, index, resolution, retrieval
from architect.knowledge.config import KnowledgeConfig, load_knowledge_config
from architect.knowledge.graph import GraphStore, SqlGraphStore, counts
from architect.knowledge.graph_age import AgeGraphStore, age_installed
from architect.knowledge.vectors import VectorIndex, vector_index


def graph_store(pool: ConnectionPool, config: KnowledgeConfig) -> GraphStore:
    """The configured backend; `auto` is Apache AGE when the extension exists, else SQL."""
    if config.graph_backend == "sql":
        return SqlGraphStore(pool)
    if age_installed(pool):
        return AgeGraphStore(pool)
    if config.graph_backend == "age":
        raise RuntimeError("graph.backend is age, but the extension is not installed")
    return SqlGraphStore(pool)


class KnowledgePlane:
    def __init__(
        self,
        pool: ConnectionPool,
        gateway: Gateway | None = None,
        config: KnowledgeConfig | None = None,
        *,
        graph: GraphStore | None = None,
        vectors: VectorIndex | None = None,
    ) -> None:
        self.pool = pool
        self.gateway = gateway
        self.config = config or load_knowledge_config()
        self._graph = graph
        self._vectors = vectors

    @property
    def graph(self) -> GraphStore:
        if self._graph is None:
            self._graph = graph_store(self.pool, self.config)
        return self._graph

    @property
    def vectors(self) -> VectorIndex:
        if self._vectors is None:
            self._vectors = vector_index(self.pool, self.config)
        return self._vectors

    def backends(self) -> dict[str, str]:
        return {"graph": self.graph.name, "vectors": self.vectors.name}

    def sync(self, project_id: str, *, force: bool = False) -> dict[str, Any]:
        return index.sync(
            self.pool,
            project_id,
            config=self.config,
            gateway=self.gateway,
            vectors=self.vectors if self.gateway is not None else None,
            force=force,
        )

    def search(self, project_id: str, query: str, **options: Any) -> dict[str, Any]:
        return retrieval.search(
            self.pool,
            project_id,
            query,
            config=self.config,
            gateway=self.gateway,
            vectors=self.vectors if self.gateway is not None else None,
            **options,
        )

    def neighbors(self, project_id: str, node_id: str, **options: Any) -> dict[str, Any]:
        return self.graph.neighbors(project_id, node_id, **options)

    def paths(self, project_id: str, src: str, dst: str, **options: Any) -> list[list[str]]:
        return self.graph.paths(project_id, src, dst, **options)

    def counts(self, project_id: str) -> dict[str, dict[str, int]]:
        return counts(self.pool, project_id)

    def resolve_entities(self, project_id: str) -> dict[str, Any]:
        if self.gateway is None:
            raise RuntimeError("entity resolution needs the gateway")
        return resolution.resolve_entities(
            self.pool, project_id, gateway=self.gateway, vectors=self.vectors, config=self.config
        )

    def rebuild_communities(self, project_id: str) -> dict[str, Any]:
        if self.gateway is None:
            raise RuntimeError("community summaries need the gateway")
        return communities.rebuild(self.pool, project_id, gateway=self.gateway, config=self.config)

    def communities(self, project_id: str, level: int | None = None) -> list[dict[str, Any]]:
        return communities.list_communities(self.pool, project_id, level)
