"""Local CPU embeddings (ONNX, through the fastembed package): the on-prem default of the
`embedding` tier. Works offline once the model named in config/models.yaml is downloaded; the
download goes to ARCHITECT_FASTEMBED_CACHE when that is set. The package is optional
(`pip install -e ".[embeddings]"`); without it this provider is not registered."""

from __future__ import annotations

import importlib.util
import os
import threading
from typing import Any

from architect.gateway.errors import ProviderError
from architect.gateway.providers.base import EmbedResult, ProviderCall, ProviderResult

CACHE_ENV = "ARCHITECT_FASTEMBED_CACHE"


def available() -> bool:
    return importlib.util.find_spec("fastembed") is not None


class FastEmbedProvider:
    name = "fastembed"

    def __init__(self, cache_dir: str | None = None) -> None:
        self._cache_dir = cache_dir or os.environ.get(CACHE_ENV) or None
        self._engines: dict[str, Any] = {}
        self._lock = threading.Lock()

    def _engine(self, model: str) -> Any:
        with self._lock:
            if model not in self._engines:
                try:
                    from fastembed import TextEmbedding
                except ImportError as error:
                    raise ProviderError(
                        self.name, model, "the fastembed package is not installed", retryable=False
                    ) from error
                try:
                    self._engines[model] = TextEmbedding(
                        model_name=model, cache_dir=self._cache_dir
                    )
                except Exception as error:  # noqa: BLE001 - an unknown model, or no download
                    raise ProviderError(
                        self.name, model, f"cannot load the model: {error}", retryable=False
                    ) from error
            return self._engines[model]

    def embed(self, model: str, texts: list[str], dim: int | None = None) -> EmbedResult:
        engine = self._engine(model)
        try:
            vectors = [[float(x) for x in vector] for vector in engine.embed(list(texts))]
        except Exception as error:  # noqa: BLE001 - the runtime's errors are not typed
            raise ProviderError(
                self.name, model, f"embedding failed: {error}", retryable=False
            ) from error
        return EmbedResult(vectors=vectors, tokens=sum(max(1, len(t) // 4) for t in texts))

    def complete(self, call: ProviderCall) -> ProviderResult:
        raise ProviderError(self.name, call.model, "an embedding provider", retryable=False)
