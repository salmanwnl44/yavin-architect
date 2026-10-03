"""Shared, pure helpers over a System Model: element lists, the flow graph, capacity params."""

from __future__ import annotations

from typing import Any

Model = dict[str, Any]
Element = dict[str, Any]


def elements(model: Model, element_type: str) -> list[Element]:
    return model.get("elements", {}).get(element_type, [])


def links(model: Model, link_type: str) -> list[dict[str, Any]]:
    return model.get("links", {}).get(link_type, [])


def by_id(model: Model, element_type: str) -> dict[str, Element]:
    """Elements of one type by id, first one wins."""
    out: dict[str, Element] = {}
    for element in elements(model, element_type):
        out.setdefault(element["id"], element)
    return out


def inbound(model: Model, component_id: str) -> list[Element]:
    """Flows into a component."""
    return [flow for flow in elements(model, "flows") if flow["to"] == component_id]


def ingress_flows(model: Model) -> list[Element]:
    """Flows whose source is an external component."""
    components = by_id(model, "components")
    return [
        flow
        for flow in elements(model, "flows")
        if flow["from"] in components and components[flow["from"]]["kind"] == "external"
    ]


def sync_dependencies(model: Model) -> dict[str, list[str]]:
    """component id -> the components it depends on synchronously."""
    out: dict[str, list[str]] = {}
    for link in links(model, "depends_on"):
        if link.get("kind") == "sync":
            out.setdefault(link["from"], []).append(link["to"])
    return out


def reachable_from_ingress(model: Model) -> list[str]:
    """Non-external components on the request path: start at the targets of the ingress
    flows, then follow flows and sync depends_on links. In model order."""
    components = by_id(model, "components")
    sync = sync_dependencies(model)
    reached = {flow["to"] for flow in ingress_flows(model)}
    frontier = list(reached)
    while frontier:
        source = frontier.pop()
        targets = [flow["to"] for flow in elements(model, "flows") if flow["from"] == source]
        for target in targets + sync.get(source, []):
            if target not in reached:
                reached.add(target)
                frontier.append(target)
    return [cid for cid, c in components.items() if cid in reached and c["kind"] != "external"]


def sync_closure(model: Model, component_id: str) -> list[str]:
    """The component plus everything it depends on synchronously, transitively. In model
    order, the component itself first."""
    sync = sync_dependencies(model)
    reached = {component_id}
    frontier = [component_id]
    while frontier:
        source = frontier.pop()
        for target in sync.get(source, []):
            if target not in reached:
                reached.add(target)
                frontier.append(target)
    ordered = [cid for cid in by_id(model, "components") if cid in reached]
    return [component_id] + [cid for cid in ordered if cid != component_id]


def replicas(model: Model, component: Element) -> tuple[int, bool]:
    """(replica count, assumed): from the component's deployment unit, else 1 and assumed."""
    unit_id = component.get("deployment_unit")
    units = by_id(model, "deployment_units")
    if unit_id in units and "replicas" in units[unit_id]:
        return int(units[unit_id]["replicas"]), False
    return 1, True


# contracts v1.0 gives CapacityParam no applies_to (contracts-PROPOSALS.md P-9); the
# convention `name = "<element_id>.<metric>"` binds a param to an element, here only.
def capacity_param(model: Model, element_id: str, metric: str) -> Element | None:
    wanted = f"{element_id}.{metric}"
    for param in elements(model, "capacity_params"):
        if param["name"] == wanted:
            return param
    return None
