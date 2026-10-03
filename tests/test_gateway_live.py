"""Live provider tests: real keys, real money, never in CI.

Deselected by default (pyproject addopts `-m "not live"`). Run them yourself with the key in
the shell, never in a file in the repo:

    export ARCHITECT_ANTHROPIC_API_KEY=...   # PowerShell: $env:ARCHITECT_ANTHROPIC_API_KEY = "..."
    pytest -m live -v

The app reads ARCHITECT_ANTHROPIC_API_KEY first and falls back to ANTHROPIC_API_KEY. L2 also
needs OPENAI_COMPAT_BASE_URL (and OPENAI_COMPAT_API_KEY if the server wants one); without the
URL it is deselected.
"""

from __future__ import annotations

import os

import pytest

from architect.gateway.config import anthropic_api_key, load_config
from architect.gateway.gateway import Gateway, default_providers
from architect.gateway.request import GatewayRequest

pytestmark = pytest.mark.live

SCHEMA = {
    "type": "object",
    "properties": {"city": {"type": "string"}, "country": {"type": "string"}},
    "required": ["city", "country"],
    "additionalProperties": False,
}


def live_request(tier: str, **overrides):
    base = {
        "role": "owner",
        "tier": tier,
        "purpose": "live-test",
        "system": "Answer in one short line.",
        "messages": [{"role": "user", "content": "What is the capital of France?"}],
        "max_tokens": 64,
        "cache": "off",
        "scope": {"session": "ses_LIVE000001"},
    }
    return GatewayRequest(**(base | overrides))


def test_l1_anthropic_tier_cheap_completion_and_structured_round_trip(pool):
    if not anthropic_api_key():
        pytest.fail("set ARCHITECT_ANTHROPIC_API_KEY (or ANTHROPIC_API_KEY) to run the live tests")
    gateway = Gateway(pool, load_config(), default_providers())
    plain = gateway.call(live_request("tier-cheap"))
    assert "Paris" in plain.text and plain.provider == "anthropic"
    assert plain.tokens_in > 0 and plain.tokens_out > 0 and plain.usd > 0

    structured = gateway.call(live_request("tier-cheap", output_schema=SCHEMA))
    assert structured.parsed["city"] == "Paris"
    assert gateway.spend({"session": "ses_LIVE000001"})["calls"] == 2


@pytest.mark.live_openai_compat
def test_l2_openai_compatible_server(pool):
    if not os.environ.get("OPENAI_COMPAT_BASE_URL"):
        pytest.fail("set OPENAI_COMPAT_BASE_URL to run the OpenAI-compatible live test")
    gateway = Gateway(pool, load_config(), default_providers())
    only_local = [
        f for f in {c.family for c in gateway.config.tiers["tier-cheap"]} if f != "local-vllm"
    ]
    plain = gateway.call(live_request("tier-cheap", exclude_families=only_local))
    assert plain.provider == "openai_compat" and plain.text
    structured = gateway.call(
        live_request("tier-cheap", exclude_families=only_local, output_schema=SCHEMA)
    )
    assert "city" in structured.parsed
