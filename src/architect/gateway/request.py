"""What a caller sends the gateway and what it gets back."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Tier = Literal["tier-cheap", "tier-mid", "tier-frontier"]
CacheMode = Literal["auto", "force", "off"]
TaintOrigin = Literal["external_untrusted", "external_trusted", "internal", "user"]


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["user", "assistant"]
    content: str


class Scope(BaseModel):
    """What a call is charged to. Budgets apply to any subset of these."""

    model_config = ConfigDict(extra="forbid")
    tenant: str | None = None
    session: str | None = None
    phase: str | None = None

    def present(self) -> dict[str, str]:
        return {k: v for k, v in self.model_dump().items() if v is not None}


class GatewayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: str = Field(min_length=1, description="the agent role making the call")
    tier: Tier
    purpose: str = Field(min_length=1, description="a short tag, e.g. extract-claims")
    system: str = ""
    messages: list[Message] = Field(min_length=1)
    output_schema: dict[str, Any] | None = None
    max_tokens: int = Field(default=1024, ge=1)
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    scope: Scope = Field(default_factory=Scope)
    input_taints: list[TaintOrigin] = Field(default_factory=list)
    exclude_families: list[str] = Field(default_factory=list)
    cache: CacheMode = "auto"


class GatewayResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    text: str
    parsed: Any | None = None
    call_id: str
    provider: str
    model: str
    family: str
    tokens_in: int
    tokens_out: int
    usd: float
    latency_ms: int
    cache_hit: bool
    attempts: int
