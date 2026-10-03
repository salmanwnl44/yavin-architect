"""A deterministic provider for tests: scripted responses, scripted failures, counted calls."""

from __future__ import annotations

import hashlib
import json
from collections import deque
from dataclasses import dataclass
from typing import Any

from architect.gateway.errors import ProviderError
from architect.gateway.providers.base import ProviderCall, ProviderResult


def prompt_hash_of(call: ProviderCall) -> str:
    payload = {
        "system": call.system,
        "messages": call.messages,
        "output_schema": call.output_schema,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class Failure:
    """A scripted failure: an HTTP-like status (429, 500, 529), 'timeout', or 'bad_request'."""

    kind: int | str

    def raise_(self, provider: str, model: str) -> None:
        if self.kind == "timeout":
            raise ProviderError(provider, model, "timed out", retryable=True)
        if self.kind == "bad_request" or (
            isinstance(self.kind, int) and 400 <= self.kind < 500 and self.kind != 429
        ):
            raise ProviderError(
                provider,
                model,
                f"HTTP {self.kind}",
                retryable=False,
                status=self.kind if isinstance(self.kind, int) else 400,
            )
        raise ProviderError(
            provider, model, f"HTTP {self.kind}", retryable=True, status=int(self.kind)
        )


@dataclass(frozen=True)
class Scripted:
    """A scripted success. `text` may be a dict, which is serialized as JSON."""

    text: Any
    tokens_in: int = 100
    tokens_out: int = 20

    def result(self) -> ProviderResult:
        text = self.text if isinstance(self.text, str) else json.dumps(self.text, sort_keys=True)
        return ProviderResult(text=text, tokens_in=self.tokens_in, tokens_out=self.tokens_out)


class MockProvider:
    name = "mock"

    def __init__(self, name: str = "mock") -> None:
        self.name = name
        self.queue: deque[Scripted | Failure] = deque()
        self.by_hash: dict[str, deque[Scripted | Failure]] = {}
        self.calls: list[ProviderCall] = []
        self.fail_if_called = False

    # scripting
    def enqueue(self, *items: Scripted | Failure | str | dict) -> None:
        """Responses in order, for the next calls whatever their prompt."""
        for item in items:
            self.queue.append(item if isinstance(item, (Scripted, Failure)) else Scripted(item))

    def script(
        self, call_or_hash: ProviderCall | str, *items: Scripted | Failure | str | dict
    ) -> None:
        """Responses in order, for calls with this prompt hash."""
        key = call_or_hash if isinstance(call_or_hash, str) else prompt_hash_of(call_or_hash)
        bucket = self.by_hash.setdefault(key, deque())
        for item in items:
            bucket.append(item if isinstance(item, (Scripted, Failure)) else Scripted(item))

    @property
    def call_count(self) -> int:
        return len(self.calls)

    # the provider interface
    def complete(self, call: ProviderCall) -> ProviderResult:
        if self.fail_if_called:
            raise AssertionError("the mock provider was called, and this test forbids it")
        self.calls.append(call)
        bucket = self.by_hash.get(prompt_hash_of(call))
        if bucket:
            item = bucket.popleft()
        elif self.queue:
            item = self.queue.popleft()
        else:
            item = Scripted(
                self.default_text(call),
                tokens_in=max(
                    1, len(call.system + "".join(m["content"] for m in call.messages)) // 4
                ),
            )
        if isinstance(item, Failure):
            item.raise_(self.name, call.model)
        return item.result()

    @staticmethod
    def default_text(call: ProviderCall) -> str:
        """Deterministic text (or a schema-free JSON object) derived from the prompt."""
        digest = prompt_hash_of(call)[:12]
        if call.output_schema is not None:
            return json.dumps({"mock": digest})
        return f"mock:{call.model}:{digest}"
