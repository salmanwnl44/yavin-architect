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

-- Every model version committed so far, by model.version_created or model.patch_committed,
-- with its materialized System Model. The Arbiter applies each patch to the head's model
-- before committing it, and a version created from a parent starts as the parent's model.
CREATE TABLE IF NOT EXISTS arb_model_versions (
    project_id text  NOT NULL REFERENCES projects (project_id),
    version_id text  NOT NULL,
    model      jsonb NOT NULL,
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
    -- M5: computed, never in the ledger. grade: unverified | design_grade; confidence per
    -- config/confidence.yaml with the inputs it was computed from.
    two_pass_agreement  boolean NOT NULL DEFAULT false,
    grade               text    NOT NULL DEFAULT 'unverified',
    confidence          double precision NOT NULL DEFAULT 0,
    confidence_inputs   jsonb   NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (project_id, claim_id)
);

-- Every claim proposal, and whether a commit followed. A proposal never committed is a
-- quarantined claim (M5).
CREATE TABLE IF NOT EXISTS proj_claim_proposals (
    project_id  text   NOT NULL REFERENCES projects (project_id),
    proposal_id text   NOT NULL,
    claim_id    text   NOT NULL,
    claim       jsonb  NOT NULL,
    seq         bigint NOT NULL,
    committed   boolean NOT NULL DEFAULT false,
    PRIMARY KEY (project_id, proposal_id)
);

-- Recorded experiments: the verification events a claim's grade and confidence count.
CREATE TABLE IF NOT EXISTS proj_experiments (
    project_id    text   NOT NULL REFERENCES projects (project_id),
    seq           bigint NOT NULL,
    experiment_id text   NOT NULL,
    result_claims jsonb  NOT NULL,
    PRIMARY KEY (project_id, seq)
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

-- Budget limits as set by budget.updated, by the scope they were set on. null = uncapped.
CREATE TABLE IF NOT EXISTS proj_budgets (
    project_id text   NOT NULL REFERENCES projects (project_id),
    seq        bigint NOT NULL,
    scope      jsonb  NOT NULL,
    limits     jsonb  NOT NULL,
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

-- The knowledge graph (M8), a projection like the tables above. Nodes: sources, claims,
-- entities (the subjects and objects of claims), the elements and requirements of the HEAD
-- model, ADRs and communities. `seq` is the event that introduced the node; `origin` says
-- whether an event or the head model put it there (model nodes are replaced with each head).
CREATE TABLE IF NOT EXISTS proj_graph_nodes (
    project_id  text   NOT NULL REFERENCES projects (project_id),
    node_id     text   NOT NULL,
    node_type   text   NOT NULL CHECK (node_type IN
        ('source', 'claim', 'entity', 'element', 'requirement', 'adr', 'community')),
    entity_type text,
    label       text   NOT NULL,
    origin      text   NOT NULL CHECK (origin IN ('event', 'model')),
    seq         bigint NOT NULL,
    PRIMARY KEY (project_id, node_id)
);

-- The edges derived from claims: (subject) -[PREDICATE]-> (object) and (claim) -[ABOUT]->
-- (entity). The other edges of the graph are proj_edges. P2: an edge without provenance is
-- un-storable, so seq (the committing event) and claim_id are both NOT NULL.
CREATE TABLE IF NOT EXISTS proj_graph_edges (
    project_id text    NOT NULL REFERENCES projects (project_id),
    seq        bigint  NOT NULL,
    ord        integer NOT NULL,
    edge_type  text    NOT NULL,
    src        text    NOT NULL,
    dst        text    NOT NULL,
    claim_id   text    NOT NULL,
    PRIMARY KEY (project_id, seq, ord)
);
CREATE INDEX IF NOT EXISTS proj_graph_edges_by_src ON proj_graph_edges (project_id, src);
CREATE INDEX IF NOT EXISTS proj_graph_edges_by_dst ON proj_graph_edges (project_id, dst);

-- entity.merged events, and whether an entity.merge_reverted undid them.
CREATE TABLE IF NOT EXISTS proj_entity_merges (
    project_id   text   NOT NULL REFERENCES projects (project_id),
    seq          bigint NOT NULL,
    event_id     text   NOT NULL,
    kept_id      text   NOT NULL,
    merged_ids   text[] NOT NULL,
    method       text   NOT NULL,
    reverted_seq bigint,
    PRIMARY KEY (project_id, seq)
);

-- entity -> the id it answers to now, from the merges that are not reverted.
CREATE TABLE IF NOT EXISTS proj_entity_alias (
    project_id   text NOT NULL REFERENCES projects (project_id),
    entity_id    text NOT NULL,
    canonical_id text NOT NULL,
    PRIMARY KEY (project_id, entity_id)
);

-- The model gateway (M4). Not projections of the ledger: the call log is its own append-only
-- record, the cache and the spend table are operational state.
CREATE OR REPLACE FUNCTION append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % is not allowed', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END
$$;

-- Every model call: each attempt, failure, cache hit and replay. Never changed afterwards.
CREATE TABLE IF NOT EXISTS gw_calls (
    call_id      text        PRIMARY KEY,
    ts           timestamptz NOT NULL DEFAULT now(),
    scope        jsonb       NOT NULL,
    role         text        NOT NULL,
    purpose      text        NOT NULL,
    tier         text        NOT NULL,
    provider     text,
    model        text,
    family       text,
    prompt_hash  text        NOT NULL,
    request      jsonb       NOT NULL,
    response     jsonb,
    error        text,
    tokens_in    integer     NOT NULL DEFAULT 0,
    tokens_out   integer     NOT NULL DEFAULT 0,
    usd          numeric     NOT NULL DEFAULT 0,
    latency_ms   integer     NOT NULL DEFAULT 0,
    cache_hit    boolean     NOT NULL DEFAULT false,
    attempt      integer     NOT NULL,
    status       text        NOT NULL,
    input_taints jsonb       NOT NULL
);
CREATE INDEX IF NOT EXISTS gw_calls_by_prompt ON gw_calls (prompt_hash, ts);

-- Write-ahead call records (M8 step 0). A provider attempt is two rows: `started` before the
-- provider is called (with what was reserved for it), then the row that closes it, which
-- names it in started_id. A started row nothing closed is closed by the sweep as
-- `abandoned`. `state` is derived from `status`.
ALTER TABLE gw_calls ADD COLUMN IF NOT EXISTS started_id text;
ALTER TABLE gw_calls ADD COLUMN IF NOT EXISTS reserved_tokens bigint NOT NULL DEFAULT 0;
ALTER TABLE gw_calls ADD COLUMN IF NOT EXISTS reserved_usd numeric NOT NULL DEFAULT 0;
ALTER TABLE gw_calls ADD COLUMN IF NOT EXISTS state text GENERATED ALWAYS AS (
    CASE status
        WHEN 'started' THEN 'started'
        WHEN 'abandoned' THEN 'abandoned'
        WHEN 'error' THEN 'failed'
        WHEN 'budget_refused' THEN 'failed'
        ELSE 'completed'
    END
) STORED;
ALTER TABLE gw_calls DROP CONSTRAINT IF EXISTS gw_calls_status_check;
ALTER TABLE gw_calls ADD CONSTRAINT gw_calls_status_check CHECK (status IN (
    'started', 'ok', 'error', 'invalid_output', 'cache_hit', 'replay', 'budget_refused',
    'abandoned'
)) NOT VALID;
-- a started row is closed at most once by its own attempt and at most once by the sweep
CREATE UNIQUE INDEX IF NOT EXISTS gw_calls_one_close
    ON gw_calls (started_id) WHERE started_id IS NOT NULL AND status <> 'abandoned';
CREATE UNIQUE INDEX IF NOT EXISTS gw_calls_one_abandon
    ON gw_calls (started_id) WHERE status = 'abandoned';
CREATE INDEX IF NOT EXISTS gw_calls_open ON gw_calls (ts) WHERE status = 'started';

CREATE OR REPLACE TRIGGER gw_calls_no_update_delete
    BEFORE UPDATE OR DELETE ON gw_calls
    FOR EACH ROW EXECUTE FUNCTION append_only();

CREATE OR REPLACE TRIGGER gw_calls_no_truncate
    BEFORE TRUNCATE ON gw_calls
    FOR EACH STATEMENT EXECUTE FUNCTION append_only();

-- Responses by request key; a hit never reaches a provider.
CREATE TABLE IF NOT EXISTS gw_cache (
    key        text        PRIMARY KEY,
    response   jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Spend per scope (every non-empty subset of {tenant, session, phase} a call carried).
CREATE TABLE IF NOT EXISTS gw_spend (
    scope_key       text    PRIMARY KEY,
    scope           jsonb   NOT NULL,
    tokens          bigint  NOT NULL DEFAULT 0,
    usd             numeric NOT NULL DEFAULT 0,
    reserved_tokens bigint  NOT NULL DEFAULT 0,
    reserved_usd    numeric NOT NULL DEFAULT 0,
    calls           integer NOT NULL DEFAULT 0
);
-- Abandoned calls (M8 step 0): their reservations, charged. Included in tokens, usd and calls.
ALTER TABLE gw_spend ADD COLUMN IF NOT EXISTS abandoned_tokens bigint NOT NULL DEFAULT 0;
ALTER TABLE gw_spend ADD COLUMN IF NOT EXISTS abandoned_usd numeric NOT NULL DEFAULT 0;
ALTER TABLE gw_spend ADD COLUMN IF NOT EXISTS abandoned_calls integer NOT NULL DEFAULT 0;

-- Ingestion (M5): derived, operational state. Segments are rebuildable from the object store;
-- jobs, candidates and quarantine from the gateway's call log.
CREATE TABLE IF NOT EXISTS ing_segments (
    source_id  text    NOT NULL,
    segment_id text    NOT NULL,
    locator    text    NOT NULL,
    kind       text    NOT NULL CHECK (kind IN ('statement', 'table', 'code')),
    text       text    NOT NULL,
    position   integer NOT NULL,
    PRIMARY KEY (source_id, segment_id)
);

CREATE TABLE IF NOT EXISTS ing_jobs (
    job_id                   text    PRIMARY KEY,
    project_id               text    NOT NULL REFERENCES projects (project_id),
    source_id                text    NOT NULL,
    content_hash             text    NOT NULL,
    pipeline_version         integer NOT NULL,
    stage                    text    NOT NULL CHECK (stage IN ('parse', 'pass_a', 'pass_b', 'commit', 'done')),
    status                   text    NOT NULL CHECK (status IN ('running', 'done')),
    attempts                 integer NOT NULL DEFAULT 0,
    pass_b_excluded_families jsonb
);

CREATE TABLE IF NOT EXISTS ing_candidates (
    job_id       text    NOT NULL REFERENCES ing_jobs (job_id),
    pass         text    NOT NULL CHECK (pass IN ('A', 'B')),
    segment_id   text    NOT NULL,
    candidate_id text    NOT NULL,
    candidate    jsonb   NOT NULL,
    call_id      text    NOT NULL,
    family       text    NOT NULL,
    position     integer NOT NULL,
    PRIMARY KEY (job_id, pass, candidate_id)
);

CREATE TABLE IF NOT EXISTS ing_pass_b (
    job_id     text NOT NULL REFERENCES ing_jobs (job_id),
    segment_id text NOT NULL,
    call_id    text NOT NULL,
    PRIMARY KEY (job_id, segment_id)
);

CREATE TABLE IF NOT EXISTS ing_quarantine (
    job_id     text  NOT NULL REFERENCES ing_jobs (job_id),
    claim_id   text  NOT NULL,
    segment_id text  NOT NULL,
    reason     text  NOT NULL CHECK (reason IN ('pass_b_missing', 'spo_mismatch', 'magnitude_mismatch', 'condition_conflict')),
    pass_a     jsonb NOT NULL,
    pass_b     jsonb NOT NULL,
    PRIMARY KEY (job_id, claim_id)
);

CREATE TABLE IF NOT EXISTS ing_metrics (
    job_id text  NOT NULL REFERENCES ing_jobs (job_id),
    metric text  NOT NULL,
    value  jsonb NOT NULL,
    PRIMARY KEY (job_id, metric)
);

-- Design sessions (M6): operational state of the session engine. The ledger holds the
-- session's phase changes, checkpoints, budget, claims, versions, checks and decisions; these
-- tables hold what the frozen event types cannot (contracts-PROPOSALS P-11): the session's
-- status and outcome, the human approve/reject decision, the package key, and the agent
-- messages (which reference gw_calls rows, never duplicate them).
CREATE TABLE IF NOT EXISTS ses_sessions (
    project_id      text        NOT NULL REFERENCES projects (project_id),
    session_id      text        NOT NULL,
    preset          text        NOT NULL,
    limits          jsonb       NOT NULL,
    status          text        NOT NULL,
    outcome         text,
    phase           text,
    round           integer     NOT NULL DEFAULT 0,
    best_version    text,
    open_risks      jsonb       NOT NULL DEFAULT '[]'::jsonb,
    spend           jsonb       NOT NULL DEFAULT '{"tokens": 0, "usd": 0}'::jsonb,
    package_key     text,
    brief_source_id text,
    started_at      text        NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, session_id)
);

-- Every message between the harness and an agent (agent_protocol.schema.json), append-only.
CREATE TABLE IF NOT EXISTS ag_messages (
    project_id       text  NOT NULL REFERENCES projects (project_id),
    msg_id           text  NOT NULL,
    session_id       text  NOT NULL,
    task_id          text  NOT NULL,
    parent_task      text,
    type             text  NOT NULL,
    agent            jsonb NOT NULL,
    ts               text  NOT NULL,
    body             jsonb NOT NULL,
    depends_on       jsonb NOT NULL,
    cost             jsonb NOT NULL,
    call_ids         jsonb NOT NULL,
    context_manifest jsonb,
    context_dropped  jsonb,
    PRIMARY KEY (project_id, msg_id)
);
CREATE INDEX IF NOT EXISTS ag_messages_by_session ON ag_messages (project_id, session_id, ts);

CREATE OR REPLACE TRIGGER ag_messages_no_update_delete
    BEFORE UPDATE OR DELETE ON ag_messages
    FOR EACH ROW EXECUTE FUNCTION append_only();

CREATE OR REPLACE TRIGGER ag_messages_no_truncate
    BEFORE TRUNCATE ON ag_messages
    FOR EACH STATEMENT EXECUTE FUNCTION append_only();

-- M7: what a human gate needs beside the status. failure: why a failed session failed;
-- last_refusal: the last decision the workflow refused and why; gate_verdict: the package's;
-- waivers: what approve_with_risks signed.
ALTER TABLE ses_sessions ADD COLUMN IF NOT EXISTS failure text;
ALTER TABLE ses_sessions ADD COLUMN IF NOT EXISTS last_refusal jsonb;
ALTER TABLE ses_sessions ADD COLUMN IF NOT EXISTS gate_verdict text;
ALTER TABLE ses_sessions ADD COLUMN IF NOT EXISTS waivers jsonb NOT NULL DEFAULT '[]'::jsonb;

-- M7: the order messages were recorded in. `ts` is the session clock's origin (the same for
-- every message of a session, so a replay is identical), so it cannot order them.
ALTER TABLE ag_messages ADD COLUMN IF NOT EXISTS n bigserial;

-- The knowledge plane (M8). Derived, operational state: all of it can be dropped and built
-- again from the read models and the gateway.

-- Embeddings by (model, text): a hit never reaches a provider.
CREATE TABLE IF NOT EXISTS gw_embed_cache (
    model      text        NOT NULL,
    text_hash  text        NOT NULL,
    dim        integer     NOT NULL,
    vector     real[]      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model, text_hash)
);

-- The vectors of a project's claims and entities. text_hash says what text was embedded, so
-- a claim or entity is embedded again only when its text or the model changes.
CREATE TABLE IF NOT EXISTS emb_vectors (
    project_id text    NOT NULL REFERENCES projects (project_id),
    kind       text    NOT NULL CHECK (kind IN ('claim', 'entity')),
    id         text    NOT NULL,
    model      text    NOT NULL,
    dim        integer NOT NULL,
    text_hash  text    NOT NULL,
    vector     real[]  NOT NULL,
    PRIMARY KEY (project_id, kind, id, model)
);

-- The verbatim quote an extracted claim was committed with (the claim itself carries only
-- the locator). Part of the claim's searchable text.
CREATE TABLE IF NOT EXISTS ing_claim_quotes (
    project_id text NOT NULL REFERENCES projects (project_id),
    claim_id   text NOT NULL,
    quote      text NOT NULL,
    PRIMARY KEY (project_id, claim_id)
);

-- The canonical text of every committed claim (subject, predicate, object, magnitude,
-- conditions, quote), for full-text search.
CREATE TABLE IF NOT EXISTS kg_claim_text (
    project_id   text NOT NULL REFERENCES projects (project_id),
    claim_id     text NOT NULL,
    subject_type text NOT NULL,
    text         text NOT NULL,
    text_hash    text NOT NULL,
    tsv          tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, text)) STORED,
    PRIMARY KEY (project_id, claim_id)
);
CREATE INDEX IF NOT EXISTS kg_claim_text_tsv ON kg_claim_text USING gin (tsv);

-- The name of every entity (with the predicates it appears in), for matching a query to
-- entities.
CREATE TABLE IF NOT EXISTS kg_entity_text (
    project_id  text NOT NULL REFERENCES projects (project_id),
    entity_id   text NOT NULL,
    entity_type text NOT NULL,
    name        text NOT NULL,
    text        text NOT NULL,
    text_hash   text NOT NULL,
    tsv         tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, name)) STORED,
    PRIMARY KEY (project_id, entity_id)
);
CREATE INDEX IF NOT EXISTS kg_entity_text_tsv ON kg_entity_text USING gin (tsv);

-- How far the search index of a project has followed its read models.
CREATE TABLE IF NOT EXISTS kg_index_state (
    project_id text   PRIMARY KEY REFERENCES projects (project_id),
    seq        bigint NOT NULL,
    model      text
);

-- The current communities of a project's entity graph (Leiden, two levels) and the summary
-- claim of each. input_hash decides whether a summary must be written again.
CREATE TABLE IF NOT EXISTS kg_communities (
    project_id       text    NOT NULL REFERENCES projects (project_id),
    community_id     text    NOT NULL,
    level            integer NOT NULL,
    parent_id        text,
    members          text[]  NOT NULL,
    claim_ids        text[]  NOT NULL,
    input_hash       text    NOT NULL,
    summary_claim_id text,
    PRIMARY KEY (project_id, community_id)
);

-- Which ledger seq the Apache AGE copy of a project's graph was loaded at (age backend only).
CREATE TABLE IF NOT EXISTS kg_age_sync (
    project_id text   PRIMARY KEY REFERENCES projects (project_id),
    graph      text   NOT NULL,
    seq        bigint NOT NULL
);

-- What this schema was last brought up to: the sha256 of schema.sql. ensure_schema applies
-- the DDL above only when it differs (or a table is missing), so a starting process takes no
-- table locks on a schema that is already current.
CREATE TABLE IF NOT EXISTS schema_meta (
    key   text PRIMARY KEY,
    value text NOT NULL
);
