"""config/models.yaml, loaded and typed. The only reader of model ids and prices."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

CONFIG_ENV = "ARCHITECT_MODELS_CONFIG"
OPENAI_COMPAT_URL_ENV = "OPENAI_COMPAT_BASE_URL"


@dataclass(frozen=True)
class Candidate:
    provider: str
    model: str
    family: str
    sampling: bool = True  # whether the model accepts a temperature


@dataclass(frozen=True)
class Price:
    input: float  # usd per million input tokens
    output: float  # usd per million output tokens


@dataclass(frozen=True)
class GatewayConfig:
    tiers: dict[str, tuple[Candidate, ...]]
    prices: dict[str, Price]
    max_attempts: int = 3
    base_delay_s: float = 0.5
    max_delay_s: float = 8.0
    structured_retries: int = 2
    concurrency: dict[str, int] = field(default_factory=dict)

    def price(self, model: str) -> Price:
        return self.prices.get(model, Price(0.0, 0.0))

    def usd(self, model: str, tokens_in: int, tokens_out: int) -> float:
        price = self.price(model)
        return (tokens_in * price.input + tokens_out * price.output) / 1_000_000


def default_config_path() -> Path:
    env = os.environ.get(CONFIG_ENV)
    if env:
        return Path(env)
    for base in (Path.cwd().resolve(), *Path(__file__).resolve().parents):
        candidate = base / "config" / "models.yaml"
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"config/models.yaml not found; set {CONFIG_ENV}")


def from_mapping(data: dict[str, Any], *, openai_compat_url: str | None = None) -> GatewayConfig:
    compat = data.get("openai_compat")
    tiers: dict[str, tuple[Candidate, ...]] = {}
    for tier, entries in data["tiers"].items():
        candidates = [Candidate(**entry) for entry in entries]
        if openai_compat_url and compat:
            candidates.append(Candidate(provider="openai_compat", **compat))
        tiers[tier] = tuple(candidates)
    prices = {
        model: Price(float(p["input"]), float(p["output"]))
        for model, p in data.get("prices", {}).items()
    }
    retries = data.get("retries", {})
    return GatewayConfig(
        tiers=tiers,
        prices=prices,
        max_attempts=int(retries.get("max_attempts", 3)),
        base_delay_s=float(retries.get("base_delay_s", 0.5)),
        max_delay_s=float(retries.get("max_delay_s", 8.0)),
        structured_retries=int(data.get("structured", {}).get("max_retries", 2)),
        concurrency={k: int(v) for k, v in data.get("concurrency", {}).items()},
    )


def load_config(path: Path | None = None) -> GatewayConfig:
    source = path or default_config_path()
    data = yaml.safe_load(source.read_text(encoding="utf-8"))
    return from_mapping(data, openai_compat_url=os.environ.get(OPENAI_COMPAT_URL_ENV))
