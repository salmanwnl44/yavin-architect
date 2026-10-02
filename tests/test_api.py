"""The HTTP surface: projects, submission, reads, head."""

from __future__ import annotations

from builders import as_candidate, ident, sample_ledger, source
from conftest import PROJECT

EVENTS = f"/v1/projects/{PROJECT}/events"


def ingest_sample(client) -> list[dict]:
    committed = []
    for event in sample_ledger(PROJECT):
        response = client.post(EVENTS, json=as_candidate(event))
        assert response.status_code == 201, response.text
        committed.append(response.json()["event"])
    return committed


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_create_project(client):
    created = client.post("/v1/projects", json={"project_id": "p2"})
    assert created.status_code == 201 and created.json() == {"project_id": "p2"}

    again = client.post("/v1/projects", json={"project_id": "p2"})
    assert again.status_code == 409 and again.json()["code"] == "DUPLICATE_PROJECT"

    empty = client.post("/v1/projects", json={"project_id": ""})
    assert empty.status_code == 422
    assert empty.json()["code"] == "MALFORMED_REQUEST"
    assert empty.json()["json_path"] == "$.project_id"


def test_submit_returns_201_then_200_replayed(client):
    sent = source()
    first = client.post(EVENTS, json=sent)
    second = client.post(EVENTS, json=sent)
    assert first.status_code == 201 and first.json()["replayed"] is False
    assert second.status_code == 200 and second.json()["replayed"] is True
    assert second.json()["event"] == first.json()["event"]
    assert second.json()["event"]["seq"] == 0


def test_the_whole_sample_session_commits(client):
    committed = ingest_sample(client)
    assert [e["seq"] for e in committed] == list(range(24))
    assert {e["type"] for e in committed} == {e["type"] for e in sample_ledger()}
    assert len({e["type"] for e in committed}) == 19


def test_events_page_in_seq_order(client):
    ingest_sample(client)

    first = client.get(EVENTS, params={"limit": 10}).json()
    assert [e["seq"] for e in first["events"]] == list(range(10))
    assert first["next_since_seq"] == 9

    second = client.get(EVENTS, params={"since_seq": first["next_since_seq"], "limit": 10}).json()
    assert [e["seq"] for e in second["events"]] == list(range(10, 20))

    rest = client.get(EVENTS, params={"since_seq": 19}).json()
    assert [e["seq"] for e in rest["events"]] == [20, 21, 22, 23]

    done = client.get(EVENTS, params={"since_seq": 23}).json()
    assert done == {"events": [], "next_since_seq": 23}


def test_page_parameters_are_validated(client):
    assert client.get(EVENTS, params={"limit": 0}).status_code == 422
    assert client.get(EVENTS, params={"limit": 1001}).status_code == 422
    assert client.get(EVENTS, params={"since_seq": "x"}).json()["code"] == "MALFORMED_REQUEST"


def test_get_event_by_id(client):
    committed = ingest_sample(client)
    target = committed[16]
    assert client.get(f"{EVENTS}/{target['event_id']}").json() == target

    missing = client.get(f"{EVENTS}/{ident('evt', 'ghost')}")
    assert missing.status_code == 404 and missing.json()["code"] == "UNKNOWN_EVENT"


def test_head_summarizes_the_arbiter_state(client):
    empty = client.get(f"/v1/projects/{PROJECT}/head").json()
    assert empty == {
        "last_seq": None,
        "model_head_version": None,
        "open_objections": 0,
        "claim_counts_by_status": {},
    }

    ingest_sample(client)
    assert client.get(f"/v1/projects/{PROJECT}/head").json() == {
        "last_seq": 23,
        "model_head_version": ident("mv", "v3"),
        "open_objections": 0,
        # latency: documented; ttl: assumed -> measured; probe: measured; derived: retracted
        "claim_counts_by_status": {"documented": 1, "measured": 2, "retracted": 1},
    }


def test_head_counts_open_objections(client):
    for event in sample_ledger(PROJECT)[:12]:  # up to and including objection.raised
        assert client.post(EVENTS, json=as_candidate(event)).status_code == 201
    head = client.get(f"/v1/projects/{PROJECT}/head").json()
    assert head["open_objections"] == 1
    assert head["model_head_version"] == ident("mv", "v2")


def test_unknown_project_is_404_everywhere(client):
    for path in ("events", f"events/{ident('evt', 'x')}", "head"):
        response = client.get(f"/v1/projects/nope/{path}")
        assert response.status_code == 404 and response.json()["code"] == "UNKNOWN_PROJECT"
    posted = client.post("/v1/projects/nope/events", json=source())
    assert posted.status_code == 404 and posted.json()["code"] == "UNKNOWN_PROJECT"
