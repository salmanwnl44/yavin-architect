"""A golden task on disk: goldens/<task>/{brief.md, seed.json, expected.yaml, mock/*.json}."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from architect.model_fold import apply_patch
from architect.sessions.activities import seed_ops

GOLDENS_ENV = "ARCHITECT_GOLDENS_DIR"
MODES = ("review", "design")

Story = dict[tuple[str, int], list[dict[str, Any]]]


def goldens_dir() -> Path:
    """$ARCHITECT_GOLDENS_DIR, else the nearest goldens/ above the working directory or the
    package."""
    env = os.environ.get(GOLDENS_ENV)
    if env:
        return Path(env)
    for base in (Path.cwd().resolve(), *Path(__file__).resolve().parents):
        candidate = base / "goldens"
        if (candidate / "scorecard.schema.json").is_file():
            return candidate
    raise FileNotFoundError(f"goldens/ not found; set {GOLDENS_ENV}")


@dataclass(frozen=True)
class GoldenTask:
    task_id: str
    root: Path
    brief: str
    seed: dict[str, Any]
    expected: dict[str, Any]

    def mock(self, name: str) -> dict[str, Any]:
        return json.loads((self.root / "mock" / f"{name}.json").read_text(encoding="utf-8"))

    def story(self, mode: str) -> Story:
        """What the scripted architect answers in mock mode. Review: the frame, then one
        repair that fixes every planted flaw. Design: the frame, then a draft that is the
        seed with that repair already applied (so the first attack finds nothing)."""
        frame, repair = self.mock("frame"), self.mock("repair-1")
        if mode == "review":
            return {("frame", 0): [frame], ("repair", 1): [repair]}
        clean = apply_patch(self.seed, repair)
        draft = {"rationale": "the reference design for this task", "ops": seed_ops(clean)}
        return {("frame", 0): [frame], ("draft", 1): [draft]}


def load_task(task_id: str) -> GoldenTask:
    root = goldens_dir() / task_id
    if not (root / "brief.md").is_file():
        known = sorted(p.name for p in goldens_dir().iterdir() if (p / "brief.md").is_file())
        raise FileNotFoundError(f"no golden task {task_id!r}; known: {known}")
    return GoldenTask(
        task_id=task_id,
        root=root,
        brief=(root / "brief.md").read_text(encoding="utf-8"),
        seed=json.loads((root / "seed.json").read_text(encoding="utf-8")),
        expected=yaml.safe_load((root / "expected.yaml").read_text(encoding="utf-8")),
    )
