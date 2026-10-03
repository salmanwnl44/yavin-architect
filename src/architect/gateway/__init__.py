"""The model gateway (M4): the only path from the platform to any LLM.

Spec principle P8: models are replaceable workers. A caller names a tier and a purpose, never
a model; the gateway routes, forces and validates structured output, caches, enforces
budgets, retries and falls back, keeps families apart when asked, and records every call so
a run can be replayed exactly (P10). No module outside `gateway/providers` imports an LLM SDK,
and no model id appears outside `config/models.yaml`.
"""

from architect.gateway.gateway import Gateway
from architect.gateway.request import GatewayRequest, GatewayResponse

__all__ = ["Gateway", "GatewayRequest", "GatewayResponse"]
