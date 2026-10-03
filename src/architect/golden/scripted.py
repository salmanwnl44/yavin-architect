"""Mock mode: a gateway whose only provider answers the Architect's calls from a story, keyed
on the marker line the agent puts in every system prompt. Deterministic; no model, no key."""

from __future__ import annotations

import json
import re
from typing import Any

from psycopg_pool import ConnectionPool

from architect.gateway.config import GatewayConfig, from_mapping
from architect.gateway.errors import ProviderError
from architect.gateway.gateway import Gateway
from architect.gateway.providers.base import ProviderCall, ProviderResult

MARKER = re.compile(r"\[architect purpose=(\w+) round=(\d+)\]")

# every tier routes to the one scripted provider; the prices are made up so spend is non-zero
SCRIPTED_MODELS: dict[str, Any] = {
    "tiers": {
        tier: [{"provider": "mock", "model": "scripted-architect", "family": "scripted"}]
        for tier in ("tier-cheap", "tier-mid", "tier-frontier")
    },
    "prices": {"scripted-architect": {"input": 4.0, "output": 20.0}},
    "retries": {"max_attempts": 3, "base_delay_s": 0.0, "max_delay_s": 0.0},
    "structured": {"max_retries": 2},
}

Story = dict[tuple[str, int], list[dict[str, Any]]]


class ScriptedProvider:
    """Answers by (purpose, round); successive calls for one key take the next output and the
    last one repeats. A call the story does not cover is a non-retryable provider error."""

    name = "mock"

    def __init__(self, story: Story, *, tokens_in: int = 1000, tokens_out: int = 500) -> None:
        self.story = {key: list(outputs) for key, outputs in story.items()}
        self.served: dict[tuple[str, int], int] = {}
        self.tokens_in, self.tokens_out = tokens_in, tokens_out

    def complete(self, call: ProviderCall) -> ProviderResult:
        found = MARKER.search(call.system)
        key = (found.group(1), int(found.group(2))) if found else None
        outputs = self.story.get(key) if key else None
        if not outputs:
            raise ProviderError(
                self.name, call.model, f"the story has no output for {key}", retryable=False
            )
        n = self.served.get(key, 0)
        self.served[key] = n + 1
        output = outputs[min(n, len(outputs) - 1)]
        return ProviderResult(
            text=json.dumps(output, sort_keys=True),
            tokens_in=self.tokens_in,
            tokens_out=self.tokens_out,
        )


def scripted_config() -> GatewayConfig:
    return from_mapping(SCRIPTED_MODELS)


def scripted_gateway(pool: ConnectionPool, story: Story) -> Gateway:
    return Gateway(pool, scripted_config(), {"mock": ScriptedProvider(story)}, mode="live")
