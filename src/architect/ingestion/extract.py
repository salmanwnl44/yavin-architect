"""Stages 3 and 4: two independent extraction passes through the gateway, and their agreement.

Pass A (tier-cheap) reads packed segments; pass B (tier-mid, another family when one is
configured) re-derives from one segment at a time and never sees pass A's output. A model
proposes only subject, predicate, object, magnitude, conditions and a verbatim quote (R1);
every segment reaches it inside an untrusted data block (R3); a candidate whose quote is not
in its segment is dropped before anything else happens (R2).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from architect.gateway.request import GatewayRequest
from architect.gateway.untrusted import wrap_untrusted
from architect.ingestion import normalize
from architect.ingestion.parse import Segment

PIPELINE_VERSION = 1

PROMPT_VERSION = "extract-v1"

SYSTEM_PROMPT = (
    "You extract factual technical claims from documents. A claim states what a technique, "
    "system, component or artifact does, requires, guarantees, measures or constrains, with "
    "its conditions and numbers when the text gives them. Report only what the text states; "
    "never infer, summarize or evaluate. Each claim names the segment it came from and quotes "
    "the exact words that support it, copied verbatim from that segment. Return JSON only."
)

OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["claims"],
    "properties": {
        "claims": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["segment_locator", "subject", "predicate", "object", "quote"],
                "properties": {
                    "segment_locator": {"type": "string"},
                    "subject": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["entity_type", "name"],
                        "properties": {
                            "entity_type": {"type": "string"},
                            "name": {"type": "string"},
                        },
                    },
                    "predicate": {"type": "string"},
                    "object": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["entity_type"],
                        "properties": {
                            "entity_type": {"type": "string"},
                            "name": {"type": "string"},
                            "literal": {"type": ["string", "number", "boolean"]},
                        },
                    },
                    "magnitude": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["value", "unit"],
                        "properties": {"value": {"type": "number"}, "unit": {"type": "string"}},
                    },
                    "conditions": {
                        "type": "object",
                        "additionalProperties": {"type": ["string", "number", "boolean"]},
                    },
                    "quote": {"type": "string"},
                },
            },
        }
    },
}

REASONS = ("pass_b_missing", "spo_mismatch", "magnitude_mismatch", "condition_conflict")


@dataclass(frozen=True)
class Candidate:
    """A normalized extraction candidate bound to its segment."""

    segment_id: str
    locator: str
    subject: dict[str, Any]
    predicate: str
    object: dict[str, Any]
    magnitude: dict[str, Any] | None
    conditions: dict[str, Any]
    quote: str
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def spo(self) -> str:
        return normalize.spo_key(self.subject, self.predicate, self.object)

    def as_dict(self) -> dict[str, Any]:
        return {
            "segment_id": self.segment_id,
            "locator": self.locator,
            "subject": self.subject,
            "predicate": self.predicate,
            "object": self.object,
            "magnitude": self.magnitude,
            "conditions": self.conditions,
            "quote": self.quote,
            "raw": self.raw,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Candidate:
        return cls(**data)


def data_block(segment: Segment, source_id: str, max_chars: int) -> str:
    text = segment.text if len(segment.text) <= max_chars else segment.text[:max_chars]
    return wrap_untrusted(text, f"{source_id}:{segment.locator}")


def pack(segments: list[Segment], pack_chars: int) -> list[list[Segment]]:
    """Consecutive segments grouped so one call stays under the character target."""
    batches: list[list[Segment]] = []
    current: list[Segment] = []
    size = 0
    for segment in segments:
        length = len(segment.text) + 200
        if current and size + length > pack_chars:
            batches.append(current)
            current, size = [], 0
        current.append(segment)
        size += length
    if current:
        batches.append(current)
    return batches


def user_message(blocks: list[str]) -> str:
    return (
        "Extract the factual technical claims from the data blocks below. For each claim, set "
        "segment_locator to the source locator written in the block's delimiter (the part after "
        "the colon) and quote the supporting words verbatim.\n\n" + "\n\n".join(blocks)
    )


def extraction_request(
    *,
    tier: str,
    purpose: str,
    blocks: list[str],
    taint_origin: str,
    session: str,
    exclude_families: list[str],
) -> GatewayRequest:
    return GatewayRequest(
        role="extractor",
        tier=tier,
        purpose=purpose,
        system=(
            f"{SYSTEM_PROMPT}\n\nPrompt version: {PROMPT_VERSION}; "
            f"pipeline version: {PIPELINE_VERSION}."
        ),
        messages=[{"role": "user", "content": user_message(blocks)}],
        output_schema=OUTPUT_SCHEMA,
        max_tokens=4096,
        temperature=0.0,
        scope={"session": session},
        input_taints=[taint_origin],
        exclude_families=exclude_families,
        cache="auto",
    )


def normalize_candidates(
    parsed: dict[str, Any], segments: dict[str, Segment]
) -> tuple[list[Candidate], int]:
    """Validated model output -> normalized candidates bound to known segments, and the number
    dropped by the verbatim-quote filter (R2). Candidates naming an unknown locator are
    dropped too."""
    out: list[Candidate] = []
    dropped = 0
    for item in parsed.get("claims", []):
        segment = segments.get(item["segment_locator"])
        if segment is None:
            dropped += 1
            continue
        if not normalize.quote_found(item["quote"], segment.text):
            dropped += 1
            continue
        out.append(
            Candidate(
                segment_id=segment.segment_id,
                locator=segment.locator,
                subject=normalize.entity(item["subject"]),
                predicate=normalize.predicate(item["predicate"]),
                object=normalize.entity(item["object"]),
                magnitude=normalize.magnitude(item.get("magnitude")),
                conditions=normalize.conditions(item.get("conditions")),
                quote=normalize.whitespace(item["quote"]),
                raw=item,
            )
        )
    return out, dropped


def agreement(candidate: Candidate, pass_b: list[Candidate]) -> str | None:
    """None when pass B agrees, else the reason it does not."""
    same_segment = [c for c in pass_b if c.segment_id == candidate.segment_id]
    if not same_segment:
        return "pass_b_missing"
    matching = [c for c in same_segment if c.spo == candidate.spo]
    if not matching:
        return "spo_mismatch"
    for other in matching:
        if other.magnitude != candidate.magnitude:
            continue
        shared = set(other.conditions) & set(candidate.conditions)
        if any(other.conditions[k] != candidate.conditions[k] for k in shared):
            continue
        return None
    if all(other.magnitude != candidate.magnitude for other in matching):
        return "magnitude_mismatch"
    return "condition_conflict"
