-- =============================================================================
-- BACKGROUND JOB ENGINE v2.0
-- Schema: bck
-- =============================================================================
-- File 01: DDL — Schema, Types, Tables, Indexes, Default Config
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bck;

-- ---------------------------------------------------------------------------
-- ENUM: Process lifecycle status
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE bck.process_status AS ENUM (
        'PENDING',      -- Registered, waiting to be picked up by orchestrator
        'RUNNING',      -- Currently executing in a pg_background worker
        'DONE',         -- Finished successfully
        'FAILED',       -- Exhausted all retry attempts
        'CANCELLED',    -- Manually cancelled before execution
        'RETRYING'      -- Failed once, waiting for next retry window
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ---------------------------------------------------------------------------
-- ENUM: Orchestrator lifecycle status
-- ---------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE bck.orchestrator_status AS ENUM (
        'ACTIVE',    -- Loop is running and processing jobs
        'STOPPING',  -- Graceful shutdown requested, finishing current cycle
        'STOPPED',   -- Cleanly stopped (queue empty or manual request)
        'CRASHED'    -- Died unexpectedly (detected via heartbeat threshold)
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- =============================================================================
-- TABLE: bck.config
-- Dynamic configuration for the orchestrator engine.
-- Changes here take effect on the next orchestrator cycle (no restart needed).
-- =============================================================================
CREATE TABLE IF NOT EXISTS bck.config (
    key          TEXT        NOT NULL,
    value        TEXT        NOT NULL,
    data_type    TEXT        NOT NULL DEFAULT 'text'
                                CONSTRAINT chk_config_type
                                CHECK (data_type IN ('integer','numeric','boolean','text')),
    description  TEXT,
    update_time  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    update_by    TEXT        NOT NULL DEFAULT current_user,

    CONSTRAINT pk_config PRIMARY KEY (key)
);

COMMENT ON TABLE  bck.config             IS 'Dynamic orchestrator configuration. All changes apply on the next cycle without restart.';
COMMENT ON COLUMN bck.config.key         IS 'Unique parameter name';
COMMENT ON COLUMN bck.config.value       IS 'Parameter value (always stored as text, cast on read)';
COMMENT ON COLUMN bck.config.data_type   IS 'Expected type for safe casting: integer, numeric, boolean, text';
COMMENT ON COLUMN bck.config.description IS 'Human-readable explanation of what this parameter controls';

-- Default configuration values
INSERT INTO bck.config (key, value, data_type, description) VALUES
    ('max_workers',           '5',    'integer', 'Maximum number of concurrent background workers allowed'),
    ('cycle_interval_ms',     '2000', 'integer', 'Milliseconds to sleep between orchestrator cycles. Set 0 to disable sleep'),
    ('max_orch_instances',    '1',    'integer', 'Maximum simultaneous orchestrator instances allowed (singleton = 1)'),
    ('default_timeout_sec',   '600',  'integer', 'Default query execution timeout in seconds. Set 0 to disable timeout'),
    ('max_failed_attempts',   '3',    'integer', 'Maximum failed execution attempts before marking process as FAILED'),
    ('retry_delay_sec',       '60',   'integer', 'Seconds to wait before retrying a failed process'),
    ('heartbeat_crash_sec',   '30',   'integer', 'Seconds without heartbeat before orchestrator is considered crashed')
ON CONFLICT (key) DO NOTHING;





CREATE OR REPLACE FUNCTION bck.fn_trg_config_safety_check()
RETURNS TRIGGER AS $$
DECLARE
    v_sys_max_workers INT;
    v_new_val_int     INT;
    v_safe_limit      INT;
BEGIN
    -- Only apply logic if the key being inserted/updated is 'max_workers'
    IF NEW.key = 'max_workers' THEN
        
        -- 1. Fetch system-level limit from GUC (Grand Unified Configuration)
        v_sys_max_workers := current_setting('max_worker_processes')::INT;
        v_safe_limit      := v_sys_max_workers - 5;
        
        -- 2. Validate that the value is a proper integer
        BEGIN
            v_new_val_int := NEW.value::INT;
        EXCEPTION WHEN others THEN
            RAISE EXCEPTION 'Configuration error: "max_workers" value must be a valid integer.';
        END;

        -- 3. Safety boundary validation
        IF v_new_val_int > v_safe_limit THEN
            RAISE EXCEPTION 
                E'\n--- ORCHESTRATOR SAFETY ALERT ---\n'
                'CAUSE: Requested max_workers (%) exceeds the safety threshold.\n'
                'SERVER CONFIGURATION: max_worker_processes = %\n'
                'ALLOWED LIMIT: % (System limit minus 5 for critical background tasks).\n'
                'RECOMMENDATION: Increase "max_worker_processes" in postgresql.conf and restart the service '
                'or decrease the value in this bck.config table.',
                v_new_val_int, v_sys_max_workers, v_safe_limit;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------------------
-- TRIGGER: trg_bck_config_max_workers_safety
-- Ensures that the orchestrator never claims all available worker slots,
-- leaving room for autovacuum, replication, and parallel queries.
-- ---------------------------------------------------------------------------
CREATE TRIGGER trg_bck_config_max_workers_safety
    BEFORE INSERT OR UPDATE ON bck.config
    FOR EACH ROW
    EXECUTE FUNCTION bck.fn_trg_config_safety_check();








-- =============================================================================
-- TABLE: bck.query_catalog
-- Stores reusable query definitions with optional parameter overrides.
-- Queries registered here can be referenced when creating process batches.
-- =============================================================================
CREATE TABLE IF NOT EXISTS bck.query_catalog (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY,
    query_sql    TEXT        NOT NULL,
    description  TEXT,
    params       JSONB       NOT NULL DEFAULT '{}',  -- Optional param overrides (merged with config on execution)
    insert_time  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    update_time  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    insert_by    TEXT        NOT NULL DEFAULT current_user,

    CONSTRAINT pk_query_catalog PRIMARY KEY (id)
);

COMMENT ON TABLE  bck.query_catalog            IS 'Catalog of reusable SQL queries. Each entry can carry parameter overrides that take precedence over bck.config defaults.';
COMMENT ON COLUMN bck.query_catalog.id         IS 'Sequential surrogate key';
COMMENT ON COLUMN bck.query_catalog.query_sql  IS 'SQL statement to execute. Can be any valid PostgreSQL statement or function call';
COMMENT ON COLUMN bck.query_catalog.description IS 'Optional human-readable description of what this query does';
COMMENT ON COLUMN bck.query_catalog.params     IS 'JSON key-value pairs that override bck.config values at execution time. Example: {"default_timeout_sec": 120}';

-- =============================================================================
-- TABLE: bck.process
-- Core process queue. Each row is one unit of work (one query execution).
-- Parent rows group related children. Children are the actual background workers.
-- =============================================================================
CREATE TABLE IF NOT EXISTS bck.process (
    -- Identity
    id           BIGINT      GENERATED ALWAYS AS IDENTITY,
    id_parent    BIGINT,                          -- NULL = this IS the parent batch record
    pid          INT         DEFAULT 0,           -- pg_background PID while running

    -- Process metadata
    name         TEXT,                            -- Human-readable label for this process/batch
    query_sql    TEXT        NOT NULL,            -- SQL to execute (copied from catalog or provided inline)
    params       JSONB       NOT NULL DEFAULT '{}', -- Effective params at registration time

    -- Lifecycle
    status       bck.process_status NOT NULL DEFAULT 'PENDING',

    -- Retry control
    attempts     SMALLINT    NOT NULL DEFAULT 0,  -- Total execution attempts made
    max_attempts SMALLINT    NOT NULL DEFAULT 3,  -- Maximum allowed attempts (from config at registration)

    -- Timing
    start_time   TIMESTAMPTZ,                     -- When execution began (last attempt)
    end_time     TIMESTAMPTZ,                     -- When execution finished (last attempt)
    insert_time  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    update_time  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    -- Error tracking
    error_msg    TEXT,                            -- Last error message (if any)

    -- Audit
    insert_by    TEXT        NOT NULL DEFAULT current_user,

    CONSTRAINT pk_process PRIMARY KEY (id),
    CONSTRAINT fk_process_parent FOREIGN KEY (id_parent)
        REFERENCES bck.process (id)
        ON DELETE CASCADE
);

COMMENT ON TABLE  bck.process             IS 'Core process queue. Parent rows (id_parent IS NULL) represent batch groups. Child rows are individual query executions.';
COMMENT ON COLUMN bck.process.id          IS 'Sequential surrogate key';
COMMENT ON COLUMN bck.process.id_parent   IS 'References parent batch row. NULL means this row IS the parent';
COMMENT ON COLUMN bck.process.pid         IS 'PID of the pg_background worker currently executing this process. 0 = not running';
COMMENT ON COLUMN bck.process.name        IS 'Optional descriptive label for the process or batch';
COMMENT ON COLUMN bck.process.query_sql   IS 'SQL statement to be executed by the background worker';
COMMENT ON COLUMN bck.process.params      IS 'Effective configuration params at registration time (merged from catalog + config defaults)';
COMMENT ON COLUMN bck.process.status      IS 'Current lifecycle status of this process';
COMMENT ON COLUMN bck.process.attempts    IS 'How many execution attempts have been made (including the current one)';
COMMENT ON COLUMN bck.process.max_attempts IS 'Maximum attempts allowed. Sourced from config.max_failed_attempts at registration time';
COMMENT ON COLUMN bck.process.start_time  IS 'Timestamp when the last execution attempt began';
COMMENT ON COLUMN bck.process.end_time    IS 'Timestamp when the last execution attempt finished (success or failure)';
COMMENT ON COLUMN bck.process.error_msg   IS 'Error message from the last failed attempt';

-- Indexes
-- Fast lookup for orchestrator: find PENDING/RETRYING children ready to run
CREATE INDEX IF NOT EXISTS idx_process_pending
    ON bck.process (id_parent, status, insert_time ASC)
    WHERE status IN ('PENDING', 'RETRYING');

-- Fast lookup by parent for status aggregation
CREATE INDEX IF NOT EXISTS idx_process_parent
    ON bck.process (id_parent);

-- Fast lookup by status for monitoring views
CREATE INDEX IF NOT EXISTS idx_process_status
    ON bck.process (status);

-- Fast lookup by PID for heartbeat checks
CREATE INDEX IF NOT EXISTS idx_process_pid
    ON bck.process (pid)
    WHERE pid > 0;

-- =============================================================================
-- TABLE: bck.orchestrator
-- Tracks every orchestrator instance launched via pg_background.
-- Used for: singleton enforcement, crash detection, and metrics.
-- =============================================================================
CREATE TABLE IF NOT EXISTS bck.orchestrator (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY,
    pid             INT,                              -- pg_background PID of the orchestrator loop
    status          bck.orchestrator_status NOT NULL DEFAULT 'ACTIVE',

    -- Timing
    start_time      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    end_time        TIMESTAMPTZ,

    -- Activity metrics
    total_cycles    BIGINT      NOT NULL DEFAULT 0,   -- Number of completed loop cycles
    total_launched  BIGINT      NOT NULL DEFAULT 0,   -- Total worker processes dispatched
    launch_count    INT         NOT NULL DEFAULT 1,   -- How many times THIS session was re-launched (self-heal counter)

    -- Graceful stop control
    stop_req_time   TIMESTAMPTZ,                      -- When stop was requested
    stop_req_by     TEXT,                             -- Who requested the stop

    -- Snapshot of config at launch time (for audit/debugging)
    config_snapshot JSONB       NOT NULL DEFAULT '{}',

    -- Audit
    insert_by       TEXT        NOT NULL DEFAULT current_user,
    insert_time     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT pk_orchestrator PRIMARY KEY (id)
);

COMMENT ON TABLE  bck.orchestrator                IS 'Registry of all orchestrator instances. Used for singleton control, crash detection, and operational metrics.';
COMMENT ON COLUMN bck.orchestrator.id             IS 'Sequential surrogate key for this orchestrator session';
COMMENT ON COLUMN bck.orchestrator.pid            IS 'pg_background PID of the orchestrator loop process';
COMMENT ON COLUMN bck.orchestrator.status         IS 'Current status of this orchestrator instance';
COMMENT ON COLUMN bck.orchestrator.last_heartbeat IS 'Updated every cycle. If stale beyond heartbeat_crash_sec, instance is considered CRASHED';
COMMENT ON COLUMN bck.orchestrator.total_cycles   IS 'Counter of completed loop cycles since last start';
COMMENT ON COLUMN bck.orchestrator.total_launched IS 'Total number of child worker processes dispatched by this instance';
COMMENT ON COLUMN bck.orchestrator.launch_count   IS 'Incremented each time this orchestrator was self-healed and re-launched by a dying worker';
COMMENT ON COLUMN bck.orchestrator.config_snapshot IS 'Copy of bck.config at the moment this orchestrator was started';



 
