"""config/ingest.yaml and config/confidence.yaml, loaded and typed."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def _config_dir() -> Path:
    for base in (Path.cwd().resolve(), *Path(__file__).resolve().parents):
        if (base / "config" / "ingest.yaml").is_file():
            return base / "config"
    raise FileNotFoundError("config/ingest.yaml not found")


@dataclass(frozen=True)
class IngestConfig:
    max_file_bytes: int = 524288
    skip_dirs: tuple[str, ...] = ("vendor", "node_modules", "third_party", "dist", "build", ".git")
    trusted_domains: tuple[str, ...] = ()
    pass_a_tier: str = "tier-cheap"
    pass_b_tier: str = "tier-mid"
    pack_chars: int = 12000
    max_segment_chars: int = 6000


@dataclass(frozen=True)
class ConfidenceWeights:
    version: int = 1
    base_by_source_tier: dict[str, float] = field(default_factory=dict)
    per_corroboration: float = 0.1
    max_corroboration: float = 0.3
    two_pass_agreement: float = 0.1
    per_verification_event: float = 0.1
    max_verification: float = 0.2
    recency_decay: float = 1.0


def load_ingest_config(path: Path | None = None) -> IngestConfig:
    data: dict[str, Any] = yaml.safe_load(
        (path or _config_dir() / "ingest.yaml").read_text(encoding="utf-8")
    )
    sources, extraction = data.get("sources", {}), data.get("extraction", {})
    return IngestConfig(
        max_file_bytes=int(sources.get("max_file_bytes", 524288)),
        skip_dirs=tuple(sources.get("skip_dirs", IngestConfig.skip_dirs)),
        trusted_domains=tuple(sources.get("trusted_domains", ())),
        pass_a_tier=extraction.get("pass_a_tier", "tier-cheap"),
        pass_b_tier=extraction.get("pass_b_tier", "tier-mid"),
        pack_chars=int(extraction.get("pack_chars", 12000)),
        max_segment_chars=int(extraction.get("max_segment_chars", 6000)),
    )


def load_confidence_weights(path: Path | None = None) -> ConfidenceWeights:
    data: dict[str, Any] = yaml.safe_load(
        (path or _config_dir() / "confidence.yaml").read_text(encoding="utf-8")
    )
    return ConfidenceWeights(
        version=int(data.get("version", 1)),
        base_by_source_tier={k: float(v) for k, v in data.get("base_by_source_tier", {}).items()},
        per_corroboration=float(data.get("per_corroboration", 0.1)),
        max_corroboration=float(data.get("max_corroboration", 0.3)),
        two_pass_agreement=float(data.get("two_pass_agreement", 0.1)),
        per_verification_event=float(data.get("per_verification_event", 0.1)),
        max_verification=float(data.get("max_verification", 0.2)),
        recency_decay=float(data.get("recency_decay", 1.0)),
    )
