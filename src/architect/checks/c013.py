"""C-013 · L0 · critical · Referential integrity.

Every reference in the model resolves: flows to components and interfaces, components to
interfaces and deployment units, requirement refs and SATISFIES links to requirements in the
context, links to components, and the element refs of trust boundaries, state machines,
failure modes, SLOs, cost models, observability specs and MITIGATES controls to any element.
No id names two elements. An empty model passes.
"""

from __future__ import annotations

from typing import Any

from architect.checks.context import CheckContext, index_elements
from architect.checks.graph import elements, links
from architect.checks.outcome import CheckOutcome, failed, passed

CHECK_ID = "C-013"
USES = ("requirements",)


def check(model: dict[str, Any], ctx: CheckContext, params: dict[str, Any]) -> CheckOutcome:
    index = index_elements(model)
    components = {i for i, (t, _) in index.items() if t == "components"}
    interfaces = {i for i, (t, _) in index.items() if t == "interfaces"}
    units = {i for i, (t, _) in index.items() if t == "deployment_units"}
    requirements = set(ctx.requirements)
    dangling: list[dict[str, str]] = []

    def expect(holder: str, field: str, ref: str | None, pool: set[str]) -> None:
        if ref is not None and ref not in pool:
            dangling.append({"element": holder, "field": field, "missing": ref})

    for flow in elements(model, "flows"):
        expect(flow["id"], "from", flow["from"], components)
        expect(flow["id"], "to", flow["to"], components)
        expect(flow["id"], "via_interface", flow.get("via_interface"), interfaces)
    for component in elements(model, "components"):
        for ref in component.get("interfaces", []):
            expect(component["id"], "interfaces", ref, interfaces)
        expect(component["id"], "deployment_unit", component.get("deployment_unit"), units)
        for ref in component.get("requirement_refs", []):
            expect(component["id"], "requirement_refs", ref, requirements)
    for link in links(model, "satisfies"):
        expect(link["component"], "links.satisfies.component", link["component"], components)
        expect(link["component"], "links.satisfies.requirement", link["requirement"], requirements)
    for link in links(model, "depends_on"):
        expect(link["from"], "links.depends_on.from", link["from"], components)
        expect(link["from"], "links.depends_on.to", link["to"], components)
    for link in links(model, "mitigates"):
        expect(link["control"], "links.mitigates.control", link["control"], set(index))
    for boundary in elements(model, "trust_boundaries"):
        for ref in boundary["member_elements"]:
            expect(boundary["id"], "member_elements", ref, set(index))
    for element_type in ("state_machines", "failure_modes"):
        for element in elements(model, element_type):
            expect(element["id"], "element_ref", element["element_ref"], set(index))
    for element_type in ("slos", "cost_models", "observability_specs"):
        for element in elements(model, element_type):
            expect(element["id"], "applies_to", element["applies_to"], set(index))

    seen: dict[str, str] = {}
    duplicates: list[str] = []
    for element_type, items in model.get("elements", {}).items():
        for element in items:
            if element["id"] in seen and element["id"] not in duplicates:
                duplicates.append(element["id"])
            seen.setdefault(element["id"], element_type)

    holders = list(dict.fromkeys([d["element"] for d in dangling] + duplicates))
    evidence = {"dangling": dangling, "duplicate_ids": duplicates}
    return failed(holders, **evidence) if holders else passed(**evidence)
