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
