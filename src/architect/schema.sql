-- Yavin Architect ledger schema (PostgreSQL 16). Idempotent: safe to run on every startup.

CREATE TABLE IF NOT EXISTS projects (
    project_id text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- The truth plane. Rows are written by the Arbiter and never changed afterwards.
-- ts_wire keeps the timestamp exactly as committed (offset and precision included) so a
-- dump reproduces the event and the hash chain can be recomputed; ts is the same instant
-- for querying.
CREATE TABLE IF NOT EXISTS events (
    project_id      text        NOT NULL REFERENCES projects (project_id),
    seq             bigint      NOT NULL CHECK (seq >= 0),
    event_id        text        NOT NULL UNIQUE,
    ts              timestamptz NOT NULL,
    ts_wire         text        NOT NULL,
    actor           jsonb       NOT NULL,
    session_id      text,
    task_id         text,
    type            text        NOT NULL,
    payload         jsonb       NOT NULL,
    idempotency_key text        NOT NULL,
    prev_hash       text,
    PRIMARY KEY (project_id, seq),
    UNIQUE (project_id, idempotency_key)
);

CREATE OR REPLACE FUNCTION events_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'events is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END
$$;

CREATE OR REPLACE TRIGGER events_no_update_delete
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_append_only();

CREATE OR REPLACE TRIGGER events_no_truncate
    BEFORE TRUNCATE ON events
    FOR EACH STATEMENT EXECUTE FUNCTION events_append_only();

-- Arbiter lookup state. A projection of events: maintained in the commit transaction and
-- rebuildable from the ledger alone (architect rebuild-state).
CREATE TABLE IF NOT EXISTS arb_sources (
    project_id text NOT NULL REFERENCES projects (project_id),
    source_id  text NOT NULL,
    PRIMARY KEY (project_id, source_id)
);

CREATE TABLE IF NOT EXISTS arb_claims (
    project_id   text    NOT NULL REFERENCES projects (project_id),
    claim_id     text    NOT NULL,
    status       text    NOT NULL,
    load_bearing boolean NOT NULL,
    claim        jsonb   NOT NULL,
    PRIMARY KEY (project_id, claim_id)
);

-- Claim proposals and model patch proposals share one id namespace per project.
CREATE TABLE IF NOT EXISTS arb_proposals (
    project_id  text NOT NULL REFERENCES projects (project_id),
    proposal_id text NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('claim', 'model_patch')),
    PRIMARY KEY (project_id, proposal_id)
);

CREATE TABLE IF NOT EXISTS arb_model_heads (
    project_id   text PRIMARY KEY REFERENCES projects (project_id),
    head_version text NOT NULL
);

-- Every model version committed so far, by model.version_created or model.patch_committed.
CREATE TABLE IF NOT EXISTS arb_model_versions (
    project_id text NOT NULL REFERENCES projects (project_id),
    version_id text NOT NULL,
    PRIMARY KEY (project_id, version_id)
);

CREATE TABLE IF NOT EXISTS arb_objections (
    project_id   text    NOT NULL REFERENCES projects (project_id),
    objection_id text    NOT NULL,
    severity     text    NOT NULL,
    open         boolean NOT NULL,
    PRIMARY KEY (project_id, objection_id)
);

-- Read models (M2). Projections of events, like arb_*, but shaped for queries rather than
-- for the Arbiter's rules. Written only by the projector (architect.projector), each event's
-- rows in the same transaction as its cursor, and rebuildable from the ledger alone
-- (architect rebuild-projections). Nothing here holds a wall-clock value or a generated id.
CREATE TABLE IF NOT EXISTS proj_cursors (
    projection text   NOT NULL,
    project_id text   NOT NULL REFERENCES projects (project_id),
    last_seq   bigint NOT NULL,  -- seq of the last event folded; -1 before the first
    PRIMARY KEY (projection, project_id)
);

CREATE TABLE IF NOT EXISTS proj_sources (
    project_id   text   NOT NULL REFERENCES projects (project_id),
    source_id    text   NOT NULL,
    uri          text   NOT NULL,
    content_hash text   NOT NULL,
    media_type   text   NOT NULL,
    license      text,
    taint_origin text   NOT NULL,
    seq          bigint NOT NULL,
    PRIMARY KEY (project_id, source_id)
);

-- claim is the claim as committed; status is the current one, which the claim's own status
-- field stops matching once a claim.status_changed or claim.retracted lands.
CREATE TABLE IF NOT EXISTS proj_claims (
    project_id          text    NOT NULL REFERENCES projects (project_id),
    claim_id            text    NOT NULL,
    claim               jsonb   NOT NULL,
    status              text    NOT NULL,
    load_bearing        boolean NOT NULL,
    taint_origin        text    NOT NULL,
    supersedes          text,
    derived_from        text[]  NOT NULL,
    premise_compromised boolean NOT NULL,  -- a refuted or retracted claim is in its premise chain
    first_seq           bigint  NOT NULL,
    last_seq            bigint  NOT NULL,  -- last event that committed it or changed its status
    PRIMARY KEY (project_id, claim_id)
);

-- One row per status a claim has held: [from_seq, to_seq), to_seq NULL while current.
CREATE TABLE IF NOT EXISTS proj_claim_status_history (
    project_id  text   NOT NULL REFERENCES projects (project_id),
    claim_id    text   NOT NULL,
    from_seq    bigint NOT NULL,
    to_seq      bigint,
    status      text   NOT NULL,
    cause_event text   NOT NULL,
    PRIMARY KEY (project_id, claim_id, from_seq)
);

-- model is the whole System Model at that version, not the patch that produced it.
CREATE TABLE IF NOT EXISTS proj_model_versions (
    project_id       text   NOT NULL REFERENCES projects (project_id),
    version_id       text   NOT NULL,
    parent_version   text,
    committed_at_seq bigint NOT NULL,
    model            jsonb  NOT NULL,
    PRIMARY KEY (project_id, version_id),
    UNIQUE (project_id, committed_at_seq)
);

-- The graph as an edge list. ord numbers the edges one event produces. version_id is set
-- on the model edges (SATISFIES, DEPENDS_ON, MITIGATES), which are written once per version.
CREATE TABLE IF NOT EXISTS proj_edges (
    project_id text    NOT NULL REFERENCES projects (project_id),
    seq        bigint  NOT NULL,
    ord        integer NOT NULL,
    edge_type  text    NOT NULL,
    src        text    NOT NULL,
    dst        text    NOT NULL,
    version_id text,
    PRIMARY KEY (project_id, seq, ord)
);
CREATE INDEX IF NOT EXISTS proj_edges_by_src ON proj_edges (project_id, edge_type, src);
CREATE INDEX IF NOT EXISTS proj_edges_by_dst ON proj_edges (project_id, edge_type, dst);

CREATE TABLE IF NOT EXISTS proj_objections (
    project_id     text    NOT NULL REFERENCES projects (project_id),
    objection_id   text    NOT NULL,
    severity       text    NOT NULL,
    open           boolean NOT NULL,
    objection      jsonb   NOT NULL,
    raised_seq     bigint  NOT NULL,
    resolved_seq   bigint,
    resolution     text,
    resolution_ref text,
    PRIMARY KEY (project_id, objection_id)
);

CREATE TABLE IF NOT EXISTS proj_decisions (
    project_id text   NOT NULL REFERENCES projects (project_id),
    seq        bigint NOT NULL,
    adr_id     text   NOT NULL,
    decision   jsonb  NOT NULL,
    PRIMARY KEY (project_id, seq)
);

CREATE TABLE IF NOT EXISTS proj_waivers (
    project_id text   NOT NULL REFERENCES projects (project_id),
    seq        bigint NOT NULL,
    waiver_id  text   NOT NULL,
    target_ref text   NOT NULL,
    risk       text   NOT NULL,
    signer     text   NOT NULL,
    PRIMARY KEY (project_id, seq)
);

-- version_id is the model head when the result was committed (NULL before any version).
CREATE TABLE IF NOT EXISTS proj_checks (
    project_id   text   NOT NULL REFERENCES projects (project_id),
    seq          bigint NOT NULL,
    result_id    text   NOT NULL,
    check_id     text   NOT NULL,
    status       text   NOT NULL,
    element_refs jsonb  NOT NULL,
    evidence     jsonb,
    version_id   text,
    PRIMARY KEY (project_id, seq)
);

-- Phase changes and checkpoints in commit order. ts is the event's own ts, verbatim.
CREATE TABLE IF NOT EXISTS proj_session_timeline (
    project_id text   NOT NULL REFERENCES projects (project_id),
    seq        bigint NOT NULL,
    session_id text   NOT NULL,
    kind       text   NOT NULL CHECK (kind IN ('phase_changed', 'checkpoint')),
    from_phase text,
    phase      text   NOT NULL,
    detail     jsonb,
    ts         text   NOT NULL,
    PRIMARY KEY (project_id, seq)
);
