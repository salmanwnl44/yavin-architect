"""A structural diff between two System Models: elements added, removed and changed (field by
field), links added and removed. Pure functions over model dicts: no database, no I/O.

An element is identified by (element type, id). A link has no id: links are compared as
values, per link type, as multisets.
"""

from __future__ import annotations

import json
from typing import Any

Model = dict[str, Any]
Diff = dict[str, Any]


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _elements(model: Model) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for element_type, elements in (model.get("elements") or {}).items():
        for element in elements:
            out.setdefault((element_type, str(element.get("id"))), element)
    return out


def _field_changes(before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
    """One entry per field that differs: {field, from?, to?}. `from` is absent for a field
    the later version added, `to` for one it removed."""
    changes: list[dict[str, Any]] = []
    for name in sorted(set(before) | set(after)):
        if name in before and name in after:
            if before[name] != after[name]:
                changes.append({"field": name, "from": before[name], "to": after[name]})
        elif name in after:
            changes.append({"field": name, "to": after[name]})
        else:
            changes.append({"field": name, "from": before[name]})
    return changes


def diff_models(before: Model, after: Model) -> Diff:
    """What turns `before` into `after`."""
    old, new = _elements(before), _elements(after)
    added = [
        {"element_type": t, "id": i, "element": new[(t, i)]} for (t, i) in new if (t, i) not in old
    ]
    removed = [
        {"element_type": t, "id": i, "element": old[(t, i)]} for (t, i) in old if (t, i) not in new
    ]
    changed = []
    for key in old:
        if key in new and old[key] != new[key]:
            changed.append(
                {
                    "element_type": key[0],
                    "id": key[1],
                    "fields": _field_changes(old[key], new[key]),
                }
            )
    links_added: list[dict[str, Any]] = []
    links_removed: list[dict[str, Any]] = []
    old_links, new_links = before.get("links") or {}, after.get("links") or {}
    for link_type in sorted(set(old_links) | set(new_links)):
        remaining = [_canonical(link) for link in old_links.get(link_type, [])]
        for link in new_links.get(link_type, []):
            key = _canonical(link)
            if key in remaining:
                remaining.remove(key)
            else:
                links_added.append({"link_type": link_type, "link": link})
        for key in remaining:
            links_removed.append({"link_type": link_type, "link": json.loads(key)})
    return {
        "elements": {"added": added, "removed": removed, "changed": changed},
        "links": {"added": links_added, "removed": links_removed},
    }


def is_empty(diff: Diff) -> bool:
    return not any(diff["elements"].values()) and not any(diff["links"].values())


def summary(diff: Diff) -> str:
    elements, links = diff["elements"], diff["links"]
    if is_empty(diff):
        return "no structural change"
    return (
        f"elements +{len(elements['added'])} -{len(elements['removed'])} "
        f"~{len(elements['changed'])}; links +{len(links['added'])} -{len(links['removed'])}"
    )


def format_diff(diff: Diff) -> list[str]:
    """Human-readable lines: + added, - removed, ~ changed with one line per field."""
    lines: list[str] = []
    for item in diff["elements"]["added"]:
        lines.append(f"+ {item['element_type']} {item['id']}")
    for item in diff["elements"]["removed"]:
        lines.append(f"- {item['element_type']} {item['id']}")
    for item in diff["elements"]["changed"]:
        lines.append(f"~ {item['element_type']} {item['id']}")
        for change in item["fields"]:
            if "from" in change and "to" in change:
                lines.append(
                    f"    {change['field']}: {_canonical(change['from'])} -> "
                    f"{_canonical(change['to'])}"
                )
            elif "to" in change:
                lines.append(f"    {change['field']}: (unset) -> {_canonical(change['to'])}")
            else:
                lines.append(f"    {change['field']}: {_canonical(change['from'])} -> (unset)")
    for item in diff["links"]["added"]:
        lines.append(f"+ link {item['link_type']} {_canonical(item['link'])}")
    for item in diff["links"]["removed"]:
        lines.append(f"- link {item['link_type']} {_canonical(item['link'])}")
    return lines
