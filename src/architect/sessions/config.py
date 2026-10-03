"""config/presets.yaml, loaded and typed: presets, the architect's tier and retry bound, the
Context Compiler's token target, the checkpoint cadence and the Temporal task queue."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from architect.sessions.types import HUMAN_GATES

TEMPORAL_ADDRESS_ENV = "ARCHITECT_TEMPORAL_ADDRESS"
DEFAULT_TEMPORAL_ADDRESS = "localhost:7233"
TASK_QUEUE_ENV = "ARCHITECT_TEMPORAL_TASK_QUEUE"

OVERRIDABLE = ("max_rounds", "wall_clock_minutes", "tokens", "usd")


@dataclass(frozen=True)
class Preset:
    name: str
    max_rounds: int
    wall_clock_minutes: int
    tokens: int | None
    usd: float | None
    human_gates: tuple[str, ...] = ("end",)

    def limits(self, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
        """The resolved limits the workflow and the budget event carry. Overrides may change
        max_rounds, wall_clock_minutes, tokens and usd; `null` means uncapped."""
        limits: dict[str, Any] = {
            "preset": self.name,
            "k": 1,
            "max_rounds": self.max_rounds,
            "wall_clock_minutes": self.wall_clock_minutes,
            "tokens": self.tokens,
            "usd": self.usd,
            "human_gates": list(self.human_gates),
        }
        for key, value in (overrides or {}).items():
            if key not in OVERRIDABLE:
                raise ValueError(f"{key} is not overridable; choose from {OVERRIDABLE}")
            limits[key] = value
        return limits


@dataclass(frozen=True)
class SessionConfig:
    presets: dict[str, Preset]
    architect_tier: str = "tier-frontier"
    architect_max_tokens: int = 4096
    rejection_retries: int = 3
    token_target: int = 6000
    checkpoint_minutes: int = 5
    task_queue: str = "architect-sessions"
    extra: dict[str, Any] = field(default_factory=dict)

    def preset(self, name: str) -> Preset:
        if name not in self.presets:
            raise ValueError(f"unknown preset {name!r}; choose from {sorted(self.presets)}")
        return self.presets[name]


def _config_path() -> Path:
    for base in (Path.cwd().resolve(), *Path(__file__).resolve().parents):
        candidate = base / "config" / "presets.yaml"
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("config/presets.yaml not found")


def from_mapping(data: dict[str, Any]) -> SessionConfig:
    presets: dict[str, Preset] = {}
    for name, entry in data.get("presets", {}).items():
        gates = tuple(entry.get("human_gates", ["end"]))
        for gate in gates:
            if gate not in HUMAN_GATES:
                raise ValueError(f"preset {name}: unknown human gate {gate!r}")
        presets[name] = Preset(
            name=name,
            max_rounds=int(entry["max_rounds"]),
            wall_clock_minutes=int(entry["wall_clock_minutes"]),
            tokens=None if entry.get("tokens") is None else int(entry["tokens"]),
            usd=None if entry.get("usd") is None else float(entry["usd"]),
            human_gates=gates,
        )
    architect = data.get("architect", {})
    return SessionConfig(
        presets=presets,
        architect_tier=architect.get("tier", "tier-frontier"),
        architect_max_tokens=int(architect.get("max_tokens", 4096)),
        rejection_retries=int(architect.get("rejection_retries", 3)),
        token_target=int(data.get("context", {}).get("token_target", 6000)),
        checkpoint_minutes=int(data.get("checkpoint_minutes", 5)),
        task_queue=os.environ.get(TASK_QUEUE_ENV)
        or data.get("temporal", {}).get("task_queue", "architect-sessions"),
    )


def load_session_config(path: Path | None = None) -> SessionConfig:
    data = yaml.safe_load((path or _config_path()).read_text(encoding="utf-8"))
    return from_mapping(data)


def temporal_address() -> str:
    return os.environ.get(TEMPORAL_ADDRESS_ENV, DEFAULT_TEMPORAL_ADDRESS)
