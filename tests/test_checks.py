"""The checks engine without a database: the catalog, every check, waivers, units, purity.

M3 exit tests E1, E2 and E9 live here; E3's verdict table is also reproduced here on the
model replay.py folds, with the context built from the fixture ledger in memory.
"""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from architect.checks import (
    c001,
    c002,
    c003,
    c004,
    c005,
    c006,
    c007,
    c008,
    c009,
    c010,
    c011,
    c012,
    c013,
    graph,
    units,
)
from architect.checks.catalog import (
    REGISTRY,
    Catalog,
    CatalogError,
    evaluate,
    implementation,
    inputs_hash,
    load_catalog,
)
from architect.checks.context import CheckContext, index_elements
from architect.checks.outcome import CheckOutcome, settle
from architect.checks.waivers import check_waiver, claim_waiver, requirement_waiver
from architect.contracts import first_error, load_contracts
from replay_reference import fixture_events, reference

# --- tiny model builders ----------------------------------------------------------------


def component(cid: str, kind: str = "service", **extra: Any) -> dict[str, Any]:
    return {
        "id": cid,
        "name": cid,
        "kind": kind,
        "stateful": False,
        "requirement_refs": [],
    } | extra


def flow(fid: str, source: str, target: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": fid,
        "from": source,
        "to": target,
        "data_class": "internal",
        "rate": {"peak_qps": 100, "payload_bytes": 64},
    } | extra


def interface(iid: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": iid,
        "contract_ref": "openapi:svc.yaml#/op",
        "style": "sync",
        "idempotent": True,
        "authn": "service_identity",
    } | extra


def model(**parts: Any) -> dict[str, Any]:
    links = {k: parts.pop(k) for k in ("satisfies", "depends_on", "mitigates") if k in parts}
    return {
        "version_id": "mv_TEST000001",
        "project_id": "p",
        "elements": {k: list(v) for k, v in parts.items()},
        "links": links,
    }


def context(
    requirements: tuple[str, ...] = (),
    waivers: dict[str, str] | None = None,
    claims: dict[str, dict[str, Any]] | None = None,
    as_of_seq: int = 0,
) -> CheckContext:
    return CheckContext(
        as_of_seq=as_of_seq,
        requirements=requirements,
        claims=claims or {},
        waivers=waivers or {},
    )


def claim_view(status: str, load_bearing: bool = True) -> dict[str, Any]:
    return {"claim": {}, "status": status, "load_bearing": load_bearing, "first_seq": 0}


def run(module: Any, m: dict[str, Any], ctx: CheckContext | None = None, **params: Any):
    return module.check(m, ctx or context(), params)


# --- E1: the catalog --------------------------------------------------------------------


def test_the_bundled_catalog_validates_against_the_contract():
    catalog = load_catalog()
    data = {"catalog_version": catalog.version, "checks": list(catalog.checks)}
    assert first_error(load_contracts().catalog, data) is None
    assert catalog.version == "1.0.0"


def test_registry_and_catalog_name_the_same_checks():
    catalog = load_catalog()
    implemented = {module.CHECK_ID for module in REGISTRY.values()}
    listed = {entry["id"] for entry in catalog.checks}
    assert implemented == listed
    for entry in catalog.checks:
        module = implementation(entry)
        assert module is not None and module.CHECK_ID == entry["id"]
        assert entry["implementation"]["ref"] == module.__name__


def fixture_proposed_check() -> dict[str, Any]:
    for event in fixture_events():
        if event["type"] == "objection.raised":
            return event["payload"]["objection"]["detecting_check"]["proposed_check"]
    raise AssertionError("the fixture raises no objection")


def test_a_proposed_check_loads_and_records_not_implemented(tmp_path):
    proposed = fixture_proposed_check()
    assert proposed["id"] == "C-031"
    assert first_error(load_contracts().check, proposed) is None

    data = {"catalog_version": "1.0.1", "checks": [*load_catalog().checks, proposed]}
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    catalog = load_catalog(path)
    entry = catalog.entry("C-031")
    assert entry is not None and implementation(entry) is None
    outcome = evaluate(entry, model(), context())
    assert outcome.status == "skipped" and outcome.evidence["reason"] == "not_implemented"


def test_an_invalid_catalog_is_refused(tmp_path):
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps({"catalog_version": "1", "checks": []}), encoding="utf-8")
    with pytest.raises(CatalogError):
        load_catalog(path)


# --- E2: the checks ---------------------------------------------------------------------


def test_c001_requirement_coverage():
    m = model(components=[component("a")], satisfies=[{"component": "a", "requirement": "req_1"}])
    assert run(c001, m, context(("req_1",))).status == "pass"

    out = run(c001, m, context(("req_1", "req_2", "req_3")))
    assert out.status == "fail" and out.element_refs == ["req_2", "req_3"]
    assert out.evidence["satisfied"] == ["req_1"]

    waived = run(c001, m, context(("req_1", "req_2"), {"req_2": "wvr_1"}))
    assert waived.status == "pass" and waived.evidence["waived"] == {"req_2": "wvr_1"}

    assert run(c001, m, context()).status == "skipped"


def test_c002_no_orphans():
    m = model(
        components=[component("a"), component("b"), component("x", kind="external")],
        satisfies=[{"component": "a", "requirement": "req_1"}],
    )
    out = run(c002, m)
    assert out.status == "fail" and out.element_refs == ["b"]
    assert run(c002, m, context(waivers={"C-002:b": "wvr_1"})).status == "pass"
    m["links"]["satisfies"].append({"component": "b", "requirement": "req_1"})
    assert run(c002, m).status == "pass"
    assert run(c002, model(components=[component("x", kind="external")])).status == "skipped"
    assert run(c002, model()).status == "skipped"


def test_c003_interface_binding():
    ok = model(
        components=[component("svc", interfaces=["if_1"]), component("db", kind="datastore")],
        interfaces=[interface("if_1")],
        flows=[flow("f1", "db", "svc", via_interface="if_1"), flow("f2", "svc", "db")],
    )
    out = run(c003, ok)
    assert out.status == "pass" and out.evidence["limitations"]

    bad = copy.deepcopy(ok)
    bad["elements"]["flows"][0]["via_interface"] = "if_ghost"  # (a) does not exist
    bad["elements"]["flows"].append(flow("f3", "db", "svc"))  # (b) service without via
    bad["elements"]["interfaces"].append(interface("if_2", contract_ref="kv.yaml"))  # (c)
    bad["elements"]["components"][0]["interfaces"] = []
    out = run(c003, bad)
    assert out.status == "fail" and out.element_refs == ["f1", "f3", "if_2"]
    assert {v["rule"] for v in out.evidence["violations"]} == {"a", "b", "c"}

    unlisted = copy.deepcopy(ok)
    unlisted["elements"]["components"][0]["interfaces"] = []
    out = run(c003, unlisted)
    assert out.status == "fail" and out.element_refs == ["f1"]

    assert run(c003, model(components=[component("a")])).status == "skipped"


def test_c004_requirement_refs_match_links():
    m = model(
        components=[component("a", requirement_refs=["req_1"]), component("b")],
        satisfies=[{"component": "a", "requirement": "req_1"}],
    )
    assert run(c004, m).status == "pass"
    m["links"]["satisfies"].append({"component": "b", "requirement": "req_2"})
    out = run(c004, m)
    assert out.status == "fail" and out.element_refs == ["b"]
    assert out.evidence["mismatches"]["b"] == {"requirement_refs": [], "satisfies": ["req_2"]}
    assert run(c004, model()).status == "skipped"


def capacity(cid: str, value: float, unit: str = "qps") -> dict[str, Any]:
    return {"id": f"cap-{cid}", "name": f"{cid}.max_qps", "value": value, "unit": unit}


def test_c005_capacity_headroom():
    m = model(
        components=[component("x", kind="external"), component("a"), component("b")],
        flows=[flow("f1", "x", "a"), flow("f2", "a", "b"), flow("f3", "x", "b")],
        capacity_params=[capacity("a", 150), capacity("b", 400, "req/s")],
    )
    out = run(c005, m)
    assert out.status == "pass"
    assert out.evidence["components"]["b"] == {
        "inbound_qps": 200.0,
        "required": 300.0,
        "capacity": 400.0,
        "unit": "req/s",
    }

    tight = run(c005, m, headroom=2.0)
    assert tight.status == "fail" and tight.element_refs == ["a"]

    missing = copy.deepcopy(m)
    missing["elements"]["capacity_params"].pop(0)
    out = run(c005, missing)
    assert out.status == "error" and out.element_refs == ["a"]
    assert out.evidence["missing"] == ["a: no capacity param a.max_qps"]

    unknown = copy.deepcopy(m)
    unknown["elements"]["capacity_params"][0]["unit"] = "furlongs"
    out = run(c005, unknown)
    assert out.status == "error" and "furlongs" in out.evidence["missing"][0]

    both = copy.deepcopy(unknown)
    both["elements"]["capacity_params"][1]["value"] = 10
    out = run(c005, both)
    assert out.status == "fail" and out.element_refs == ["b"]
    assert out.evidence["errors"]["element_refs"] == ["a"]

    assert run(c005, model(components=[component("a")])).status == "skipped"
    assert run(c005, m, context(waivers={"C-005": "wvr_1"})).evidence["waived"] == {
        "a": "wvr_1",
        "b": "wvr_1",
    }


def availability(cid: str, value: float, unit: str = "ratio") -> dict[str, Any]:
    return {"id": f"av-{cid}", "name": f"{cid}.availability", "value": value, "unit": unit}


def test_c006_availability_composition():
    m = model(
        components=[component("a", deployment_unit="du_a"), component("b")],
        deployment_units=[{"id": "du_a", "name": "a", "runtime": "k8s", "replicas": 2}],
        depends_on=[{"from": "a", "to": "b", "kind": "sync"}],
        slos=[
            {
                "id": "slo_1",
                "applies_to": "a",
                "metric": "availability",
                "target": 99.9,
                "unit": "%",
            }
        ],
        capacity_params=[availability("a", 0.99), availability("b", 99.95, "%")],
    )
    out = run(c006, m)
    detail = out.evidence["slos"]["slo_1"]
    assert out.status == "pass"
    assert detail["components"]["a"] == {"replicas": 2, "a": 0.99, "a_eff": 1 - 0.01**2}
    assert detail["components"]["b"]["assumed"] == "replicas undeclared, assumed 1"
    assert detail["composite"] == pytest.approx((1 - 0.01**2) * 0.9995)
    assert detail["target"] == pytest.approx(0.999)

    weak = copy.deepcopy(m)
    weak["elements"]["capacity_params"][1]["value"] = 0.9
    out = run(c006, weak)
    assert out.status == "fail" and out.element_refs == ["slo_1"]

    missing = copy.deepcopy(m)
    missing["elements"]["capacity_params"].pop(1)
    out = run(c006, missing)
    assert out.status == "error" and out.element_refs == ["slo_1"]
    assert out.evidence["missing"] == ["slo_1: no capacity param b.availability"]

    unknown = copy.deepcopy(m)
    unknown["elements"]["slos"][0]["unit"] = "nines"
    out = run(c006, unknown)
    assert out.status == "error" and "nines" in out.evidence["missing"][0]

    via_flow = copy.deepcopy(m)
    via_flow["elements"]["flows"] = [flow("f1", "a", "b")]
    via_flow["elements"]["slos"][0]["applies_to"] = "f1"
    assert list(run(c006, via_flow).evidence["slos"]["slo_1"]["components"]) == ["b"]

    assert run(c006, model(components=[component("a")])).status == "skipped"


def test_c007_stateful_durability():
    m = model(
        components=[
            component("a"),
            component(
                "d",
                stateful=True,
                durability_class="durable",
                recovery={"rpo_s": 0, "rto_s": 30, "path": "promote"},
            ),
            component(
                "r",
                stateful=True,
                durability_class="rebuildable",
                recovery={"rto_s": 10, "path": "replay"},
            ),
            component("e", stateful=True, durability_class="ephemeral"),
        ]
    )
    out = run(c007, m)
    assert out.status == "pass"
    assert out.evidence["components"]["e"]["note"] == "state loss accepted"

    bad = copy.deepcopy(m)
    bad["elements"]["components"][1]["recovery"] = {"rpo_s": 0}
    bad["elements"]["components"][2].pop("recovery")
    bad["elements"]["components"].append(component("n", stateful=True))
    out = run(c007, bad)
    assert out.status == "fail" and out.element_refs == ["d", "r", "n"]
    assert out.evidence["components"]["d"]["missing"] == ["recovery.rto_s", "recovery.path"]
    assert out.evidence["components"]["n"]["missing"] == ["durability_class"]

    assert run(c007, model(components=[component("a")])).status == "skipped"


def test_c008_trust_boundaries_and_sensitive_data():
    m = model(
        components=[
            component("x", kind="external"),
            component("a", interfaces=["if_1"]),
            component("b"),
        ],
        interfaces=[interface("if_1")],
        flows=[
            flow(
                "in",
                "x",
                "a",
                via_interface="if_1",
                input_validation="schema",
                encryption_in_transit=True,
            ),
            flow("inner", "a", "b"),
        ],
        trust_boundaries=[{"id": "tb", "name": "cluster", "member_elements": ["a", "b"]}],
    )
    out = run(c008, m)
    assert out.status == "pass" and out.evidence["flows"]["in"]["boundaries"] == ["tb"]
    assert "inner" not in out.evidence["flows"]

    bare = copy.deepcopy(m)
    bare["elements"]["flows"][0] = flow("in", "x", "a")
    out = run(c008, bare)
    assert out.status == "fail" and out.element_refs == ["in"]
    assert out.evidence["flows"]["in"]["missing"] == [
        "input_validation",
        "encryption_in_transit",
        "authn_undeclared",
    ]

    anonymous = copy.deepcopy(m)
    anonymous["elements"]["interfaces"][0]["authn"] = "none"
    assert run(c008, anonymous).evidence["flows"]["in"]["missing"] == ["authn_none"]

    secret = model(
        components=[component("a"), component("b")],
        flows=[flow("s", "a", "b", data_class="pii")],
    )
    out = run(c008, secret)
    assert out.status == "fail" and out.evidence["flows"]["s"]["missing"] == [
        "encryption_in_transit"
    ]
    secret["elements"]["flows"][0]["encryption_in_transit"] = True
    assert run(c008, secret).status == "pass"

    plain = model(components=[component("a"), component("b")], flows=[flow("f", "a", "b")])
    assert run(c008, plain).status == "skipped"


def test_c009_open_load_bearing_assumptions():
    claims = {
        "clm_a": claim_view("assumed"),
        "clm_b": claim_view("measured"),
        "clm_c": claim_view("assumed", load_bearing=False),
    }
    out = run(c009, model(), context(claims=claims))
    assert out.status == "fail" and out.element_refs == ["clm_a"]
    assert out.evidence["load_bearing"] == {"clm_a": "assumed", "clm_b": "measured"}

    out = run(c009, model(), context(claims=claims, waivers={"clm_a": "wvr_1"}))
    assert out.status == "pass" and out.evidence["waived"] == {"clm_a": "wvr_1"}

    assert run(c009, model(), context()).status == "pass"


def test_c010_single_points_of_failure():
    m = model(
        components=[
            component("x", kind="external"),
            component("a", deployment_unit="du_a"),
            component("b"),
            component("c"),
            component("off"),
        ],
        deployment_units=[{"id": "du_a", "name": "a", "runtime": "k8s", "replicas": 3}],
        flows=[flow("in", "x", "a"), flow("ab", "a", "b")],
        depends_on=[
            {"from": "b", "to": "c", "kind": "sync"},
            {"from": "c", "to": "off", "kind": "async"},
        ],
    )
    out = run(c010, m)
    assert out.status == "fail" and out.element_refs == ["b", "c"]
    assert out.evidence["request_path"]["a"] == {"replicas": 3}
    assert out.evidence["request_path"]["b"]["assumed"] == "replicas undeclared, assumed 1"
    assert "off" not in out.evidence["request_path"]

    out = run(c010, m, context(waivers={"C-010:b": "wvr_1", "C-010:c": "wvr_2"}))
    assert out.status == "pass" and out.evidence["waived"] == {"b": "wvr_1", "c": "wvr_2"}

    assert run(c010, model(components=[component("a")])).status == "skipped"


def test_c011_idempotent_async_interfaces():
    m = model(
        interfaces=[interface("s", style="sync", idempotent=False), interface("q", style="async")]
    )
    assert run(c011, m).status == "pass"
    m["elements"]["interfaces"][1]["idempotent"] = False
    out = run(c011, m)
    assert out.status == "fail" and out.element_refs == ["q"]
    assert run(c011, model(interfaces=[interface("s")])).status == "skipped"


def test_c012_backpressure():
    m = model(
        components=[component("a"), component("b"), component("q", kind="queue"), component("c")],
        interfaces=[interface("stream", style="stream")],
        flows=[
            flow("to_queue", "a", "q", backpressure_ref="queue:q"),
            flow("fan1", "a", "c", backpressure_ref="queue:c"),
            flow("fan2", "b", "c", backpressure_ref="queue:c"),
            flow("streamed", "a", "b", via_interface="stream", backpressure_ref="credits"),
            flow("plain", "b", "a"),
        ],
    )
    out = run(c012, m)
    assert out.status == "pass"
    assert set(out.evidence["flows"]) == {"to_queue", "fan1", "fan2", "streamed"}
    for f in m["elements"]["flows"][:4]:
        f.pop("backpressure_ref")
    out = run(c012, m)
    assert out.status == "fail" and out.element_refs == ["to_queue", "fan1", "fan2", "streamed"]
    assert run(c012, model(flows=[flow("plain", "b", "a")])).status == "skipped"


def test_c013_referential_integrity():
    m = model(
        components=[
            component("a", interfaces=["if_1"], deployment_unit="du", requirement_refs=["req_1"])
        ],
        interfaces=[interface("if_1")],
        deployment_units=[{"id": "du", "name": "du", "runtime": "k8s"}],
        flows=[flow("f", "a", "a", via_interface="if_1")],
        satisfies=[{"component": "a", "requirement": "req_1"}],
        trust_boundaries=[{"id": "tb", "name": "tb", "member_elements": ["a"]}],
        state_machines=[{"id": "sm", "element_ref": "a", "states": ["s"], "transitions": []}],
    )
    assert run(c013, m, context(("req_1",))).status == "pass"
    assert run(c013, model()).status == "pass"

    bad = copy.deepcopy(m)
    bad["elements"]["flows"][0]["to"] = "ghost"
    bad["elements"]["components"][0]["requirement_refs"] = ["req_9"]
    bad["elements"]["trust_boundaries"][0]["member_elements"] = ["nobody"]
    bad["elements"]["interfaces"].append(interface("if_1"))
    out = run(c013, bad, context(("req_1",)))
    assert out.status == "fail" and out.element_refs == ["f", "a", "tb", "if_1"]
    assert {"element": "f", "field": "to", "missing": "ghost"} in out.evidence["dangling"]
    assert out.evidence["duplicate_ids"] == ["if_1"]


# --- waivers, units, graph ---------------------------------------------------------------


def test_waiver_target_ref_forms():
    ctx = context(
        waivers={"C-002": "wvr_whole", "C-010:b": "wvr_b", "req_1": "wvr_req", "clm_1": "wvr_clm"}
    )
    assert check_waiver(ctx, "C-002", "anything") == "wvr_whole"
    assert check_waiver(ctx, "C-010", "b") == "wvr_b"
    assert check_waiver(ctx, "C-010", "c") is None and check_waiver(ctx, "C-010") is None
    assert (
        requirement_waiver(ctx, "req_1") == "wvr_req" and requirement_waiver(ctx, "req_2") is None
    )
    assert claim_waiver(ctx, "clm_1") == "wvr_clm" and claim_waiver(ctx, "clm_2") is None
    # a requirement waiver is not a claim waiver, and neither waives another check
    assert claim_waiver(ctx, "req_1") == "wvr_req", "forms are by id shape; the context decides"
    assert check_waiver(ctx, "C-001", "req_1") is None


def test_units():
    assert units.throughput(3, "msg/s") == 3.0 and units.throughput(2, "writes/s") == 2.0
    assert units.ratio(99.5, "%") == pytest.approx(0.995) and units.ratio(0.5, "ratio") == 0.5
    assert units.seconds(1500, "ms") == 1.5 and units.seconds(2, "us") == pytest.approx(2e-6)
    with pytest.raises(units.UnknownUnit, match="not a known throughput unit"):
        units.throughput(1, "ratio")
    with pytest.raises(units.UnknownUnit):
        units.ratio(1, "qps")


def test_graph_helpers():
    m = model(
        components=[
            component("x", kind="external"),
            component("a"),
            component("b"),
            component("c"),
        ],
        flows=[flow("in", "x", "a"), flow("ab", "a", "b")],
        depends_on=[
            {"from": "a", "to": "c", "kind": "sync"},
            {"from": "c", "to": "b", "kind": "async"},
        ],
    )
    assert [f["id"] for f in graph.ingress_flows(m)] == ["in"]
    assert [f["id"] for f in graph.inbound(m, "b")] == ["ab"]
    assert graph.reachable_from_ingress(m) == ["a", "b", "c"]
    assert graph.sync_closure(m, "a") == ["a", "c"] and graph.sync_closure(m, "b") == ["b"]
    assert graph.replicas(m, component("a")) == (1, True)
    assert graph.capacity_param(m, "a", "max_qps") is None
    assert index_elements(m)["ab"] == ("flows", m["elements"]["flows"][1])


def test_outcomes_say_what_they_must():
    with pytest.raises(ValueError):
        CheckOutcome("error", ["a"], {})
    with pytest.raises(ValueError):
        CheckOutcome("skipped")
    with pytest.raises(ValueError):
        CheckOutcome("maybe")
    assert settle([], [], []).status == "pass"
    assert settle([], ["a"], ["a lacks x"]).evidence["missing"] == ["a lacks x"]
    assert settle(["b"], ["a"], ["a lacks x"]).evidence["errors"]["element_refs"] == ["a"]


# --- E3 on the reference model, and E9 purity --------------------------------------------

EXPECTED_ON_V3 = {
    "C-001": ("pass", []),
    "C-002": ("pass", []),
    "C-003": ("pass", []),
    "C-004": ("pass", []),
    "C-005": ("error", ["cmp_FIXROUTER1", "cmp_FIXSHARD01", "cmp_FIXWAL0001"]),
    "C-006": ("skipped", []),
    "C-007": ("fail", ["cmp_FIXLEASE01"]),
    "C-008": ("pass", []),
    "C-009": ("pass", []),
    "C-010": ("fail", ["cmp_FIXROUTER1", "cmp_FIXSHARD01", "cmp_FIXWAL0001"]),
    "C-011": ("skipped", []),
    "C-012": ("skipped", []),
    "C-013": ("pass", []),
}


def fixture_context(as_of_seq: int) -> CheckContext:
    """The fixture's knowledge as of a seq, folded in memory the way the runner folds it."""
    claims: dict[str, dict[str, Any]] = {}
    waivers: dict[str, str] = {}
    for event in fixture_events():
        if event["seq"] > as_of_seq:
            break
        payload = event["payload"]
        if event["type"] == "claim.committed":
            claim = payload["claim"]
            claims[claim["id"]] = claim_view(claim["status"], claim.get("load_bearing", False))
            claims[claim["id"]]["claim"] = claim
        elif event["type"] == "claim.status_changed":
            claims[payload["claim_id"]]["status"] = payload["to"]
        elif event["type"] == "waiver.signed":
            waivers[payload["target_ref"]] = payload["waiver_id"]
    requirements = tuple(
        view["claim"]["subject"]["id"]
        for view in claims.values()
        if view["claim"]["subject"].get("entity_type") == "requirement"
        and view["status"] not in ("refuted", "retracted")
    )
    return CheckContext(as_of_seq, requirements, claims, waivers)


def test_the_catalog_on_the_reference_model_gives_the_expected_table():
    final_model = reference()["final_model"]
    ctx = fixture_context(39)
    catalog = load_catalog()
    table = {}
    for entry in catalog.checks:
        outcome = evaluate(entry, final_model, ctx)
        table[entry["id"]] = (outcome.status, outcome.element_refs)
    assert table == EXPECTED_ON_V3
    c010 = evaluate(catalog.entry("C-010"), final_model, ctx)
    assert c010.evidence["waived"] == {"cmp_FIXLEASE01": "wvr_FIXSPOF001"}
    c009 = evaluate(catalog.entry("C-009"), final_model, ctx)
    assert c009.evidence["load_bearing"] == {"clm_FIXPAYLOAD1": "measured"}


def test_running_every_check_twice_on_the_same_inputs_is_identical():
    final_model = reference()["final_model"]
    ctx = fixture_context(39)
    catalog = load_catalog()
    first = [
        (evaluate(e, final_model, ctx), inputs_hash(e, final_model, ctx)) for e in catalog.checks
    ]
    second = [
        (evaluate(e, final_model, ctx), inputs_hash(e, final_model, ctx)) for e in catalog.checks
    ]
    assert first == second
    # The hash covers the inputs, not the check: checks that read the same inputs with the
    # same params and version share it; the runner keys results by check id on top of it.
    distinct = {
        (
            tuple(REGISTRY[e["implementation"]["ref"]].USES),
            json.dumps(e.get("params", {}), sort_keys=True),
            e["version"],
        )
        for e in catalog.checks
    }
    assert len({digest for _, digest in first}) == len(distinct)


def test_inputs_hash_follows_what_a_check_reads():
    final_model = reference()["final_model"]
    catalog = load_catalog()
    c009, c013 = catalog.entry("C-009"), catalog.entry("C-013")
    before, after = fixture_context(31), fixture_context(39)
    assert inputs_hash(c009, final_model, before) != inputs_hash(c009, final_model, after)
    assert inputs_hash(c013, final_model, before) == inputs_hash(c013, final_model, after)
    bumped = c009 | {"version": 2}
    assert inputs_hash(bumped, final_model, after) != inputs_hash(c009, final_model, after)
    assert isinstance(Catalog("1.0.0", (c009,)).entry("C-009"), dict)
