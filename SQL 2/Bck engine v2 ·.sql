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
-- =============================================================================
-- BACKGROUND JOB ENGINE v2.0
-- =============================================================================
-- File 02: Internal Helper Functions
-- =============================================================================

-- ---------------------------------------------------------------------------
-- HELPER: fn_cfg
-- Reads a single config value from bck.config with a safe fallback default.
-- Usage: bck.fn_cfg('max_workers', '5')::INT
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bck.fn_cfg(
    p_key      TEXT,
    p_default  TEXT DEFAULT NULL
)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
SET search_path TO bck, public, pg_temp
AS $$
BEGIN
    RETURN COALESCE(
        (SELECT value FROM bck.config WHERE key = p_key),
        p_default
    );
EXCEPTION WHEN OTHERS THEN
    RETURN p_default;
END;
$$;

COMMENT ON FUNCTION bck.fn_cfg IS
    'Reads a config value from bck.config. Returns p_default if the key does not exist or on any error.
     Example: bck.fn_cfg(''max_workers'', ''5'')::INT';

-- ---------------------------------------------------------------------------
-- HELPER: fn_server_ok
-- Checks whether server capacity allows launching more workers right now.
-- Returns TRUE if safe to launch, FALSE if server is under pressure.
-- Also returns current metrics for logging purposes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bck.fn_server_ok(
    p_workers_to_launch  INT DEFAULT 1,
    OUT can_launch        BOOLEAN,
    OUT active_conns      INT,
    OUT max_conns         INT,
    OUT free_conns        INT,
    OUT running_workers   INT,
    OUT load_pct          NUMERIC
)
RETURNS RECORD
LANGUAGE plpgsql
STABLE
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_max_workers  INT;
BEGIN
    -- Count active (non-idle) connections, excluding our own session
    SELECT COUNT(*)
    INTO active_conns
    FROM pg_stat_activity
    WHERE state <> 'idle'
      AND pid <> pg_backend_pid();

    max_conns  := current_setting('max_connections')::INT;
    free_conns := max_conns - active_conns;
    load_pct   := ROUND((active_conns::NUMERIC / NULLIF(max_conns, 0)) * 100, 2);

    -- Count workers currently running in our process table
    SELECT COUNT(*)
    INTO running_workers
    FROM bck.process
    WHERE status = 'RUNNING';

    v_max_workers := bck.fn_cfg('max_workers', '5')::INT;

    -- Safe to launch only if:
    --   1. We have not hit the max worker limit
    --   2. There are enough free connections for the workers we want to launch
    --      (plus 2 reserved for orchestrator + safety margin)
    can_launch := (running_workers < v_max_workers)
              AND (free_conns >= (p_workers_to_launch + 2));
END;
$$;

COMMENT ON FUNCTION bck.fn_server_ok IS
    'Checks if the server has capacity to launch p_workers_to_launch more background workers.
     Validates both the max_workers limit and available connections.
     Returns metrics useful for logging.';

-- ---------------------------------------------------------------------------
-- HELPER: fn_resolve_params
-- Merges query-level params (from catalog or registration) with bck.config
-- defaults. Query-level params take precedence over config table values.
-- Returns the effective JSONB params for a given process.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bck.fn_resolve_params(
    p_override_params  JSONB DEFAULT '{}'
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_config_json  JSONB := '{}';
    v_row          RECORD;
BEGIN
    -- Build a JSONB object from the entire config table
    FOR v_row IN SELECT key, value FROM bck.config LOOP
        v_config_json := jsonb_set(v_config_json, ARRAY[v_row.key], to_jsonb(v_row.value));
    END LOOP;

    -- Merge: config defaults first, then override with query-level params
    -- jsonb || operator: right side wins on key conflicts
    RETURN v_config_json || COALESCE(p_override_params, '{}');
END;
$$;

COMMENT ON FUNCTION bck.fn_resolve_params IS
    'Merges bck.config defaults with query-level param overrides.
     Query-level params always win over config table values.
     Used at registration time to snapshot effective params for each process.';

-- ---------------------------------------------------------------------------
-- HELPER: fn_fix_stuck
-- Detects and resolves processes that are stuck in RUNNING state but
-- whose pg_background PID no longer exists in pg_stat_activity.
-- Called at the start of each orchestrator cycle as a safety net.
-- Returns the number of processes fixed.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bck.fn_fix_stuck()
RETURNS INT
LANGUAGE plpgsql
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_rec    RECORD;
    v_count  INT := 0;
    v_max    SMALLINT;
BEGIN
    v_max := bck.fn_cfg('max_failed_attempts', '3')::SMALLINT;

    -- Find processes marked RUNNING whose PID is gone from pg_stat_activity
    FOR v_rec IN
        SELECT p.id, p.pid, p.attempts, p.id_parent
        FROM bck.process p
        WHERE p.status = 'RUNNING'
          AND p.pid > 0
          AND NOT EXISTS (
              SELECT 1 FROM pg_stat_activity
              WHERE pid = p.pid
                AND backend_type = 'pg_background'
          )
        FOR UPDATE SKIP LOCKED
    LOOP
        -- Decide: retry or fail permanently
        IF v_rec.attempts < v_max THEN
            UPDATE bck.process SET
                status      = 'RETRYING',
                pid         = 0,
                end_time    = clock_timestamp(),
                error_msg   = FORMAT(
                    '[STUCK] Worker PID %s vanished from pg_stat_activity. Attempt %s/%s. Scheduled for retry.',
                    v_rec.pid, v_rec.attempts, v_max
                ),
                update_time = clock_timestamp()
            WHERE id = v_rec.id;
        ELSE
            UPDATE bck.process SET
                status      = 'FAILED',
                pid         = 0,
                end_time    = clock_timestamp(),
                error_msg   = FORMAT(
                    '[STUCK] Worker PID %s vanished. Exhausted %s/%s attempts. Marked as FAILED.',
                    v_rec.pid, v_rec.attempts, v_max
                ),
                update_time = clock_timestamp()
            WHERE id = v_rec.id;
        END IF;

        v_count := v_count + 1;
    END LOOP;

    -- Also un-claim processes stuck in PENDING with a stale PID (edge case)
    UPDATE bck.process SET
        pid         = 0,
        update_time = clock_timestamp()
    WHERE status = 'PENDING'
      AND pid > 0;

    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION bck.fn_fix_stuck IS
    'Safety net: finds RUNNING processes whose pg_background PID is gone and resets them
     to RETRYING (if attempts remain) or FAILED (if exhausted). Called each orchestrator cycle.';

-- ---------------------------------------------------------------------------
-- HELPER: fn_orch_is_alive
-- Checks if there is at least one ACTIVE orchestrator instance with a
-- recent heartbeat. Used by workers for self-heal logic.
-- Returns TRUE if a live orchestrator exists.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bck.fn_orch_is_alive()
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_threshold  INT;
    v_found      BOOLEAN;
BEGIN
    v_threshold := bck.fn_cfg('heartbeat_crash_sec', '30')::INT;

    SELECT EXISTS (
        SELECT 1
        FROM bck.orchestrator
        WHERE status = 'ACTIVE'
          AND last_heartbeat > clock_timestamp() - (v_threshold || ' seconds')::INTERVAL
    ) INTO v_found;

    RETURN v_found;
END;
$$;

COMMENT ON FUNCTION bck.fn_orch_is_alive IS
    'Returns TRUE if at least one orchestrator instance has sent a heartbeat
     within the last heartbeat_crash_sec seconds. Used by workers for self-heal.';
-- =============================================================================
-- BACKGROUND JOB ENGINE v2.0
-- =============================================================================
-- File 03: fn_register_batch — Register queries as a process batch
-- =============================================================================
--
-- PURPOSE:
--   Receives a JSON array of queries (with optional per-query param overrides),
--   creates one PARENT process row for the batch, and N CHILD process rows
--   (one per query). Returns the parent ID for tracking.
--
-- JSON INPUT FORMAT:
--   [
--     {
--       "query_sql":    "UPDATE users SET active = true WHERE id = 1",  -- REQUIRED
--       "name":         "Activate user 1",                               -- optional
--       "params":       { "default_timeout_sec": 120 }                  -- optional overrides
--     },
--     {
--       "query_sql":    "SELECT bck.rebuild_index()",
--       "name":         "Rebuild index"
--     }
--   ]
--
-- PARAM PRECEDENCE (highest to lowest):
--   Per-query "params" JSON  >  bck.config table defaults
--
-- =============================================================================

CREATE OR REPLACE FUNCTION bck.fn_register_batch(
    p_queries   JSONB,          -- JSON array of query objects (see format above)
    p_name      TEXT DEFAULT NULL   -- Optional batch label shown in monitoring
)
RETURNS TABLE (
    id_parent    BIGINT,
    total_queued INT,
    message      TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_id_parent      BIGINT;
    v_item           JSONB;
    v_query_sql      TEXT;
    v_item_name      TEXT;
    v_item_params    JSONB;
    v_eff_params     JSONB;   -- Effective params after merge with config
    v_max_attempts   SMALLINT;
    v_count          INT := 0;
    v_arr_len        INT;
BEGIN
    -- -------------------------------------------------------------------------
    -- VALIDATIONS
    -- -------------------------------------------------------------------------
    IF p_queries IS NULL OR jsonb_typeof(p_queries) <> 'array' THEN
        RAISE EXCEPTION '[REGISTER] p_queries must be a non-null JSON array'
            USING ERRCODE = 'JE001';
    END IF;

    v_arr_len := jsonb_array_length(p_queries);

    IF v_arr_len = 0 THEN
        RAISE EXCEPTION '[REGISTER] p_queries array cannot be empty'
            USING ERRCODE = 'JE002';
    END IF;

    -- Read default max_attempts from config (will be stamped on every child)
    v_max_attempts := bck.fn_cfg('max_failed_attempts', '3')::SMALLINT;

    -- -------------------------------------------------------------------------
    -- CREATE PARENT ROW
    -- The parent represents the batch as a whole. It holds no query itself,
    -- but is used as the grouping key for all child processes.
    -- -------------------------------------------------------------------------
    INSERT INTO bck.process (
        id_parent,
        name,
        query_sql,
        status,
        max_attempts,
        params,
        insert_by
    ) VALUES (
        NULL,                          -- NULL = this IS the parent
        COALESCE(TRIM(p_name), 'Batch ' || TO_CHAR(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS')),
        '-- PARENT BATCH ROW --',      -- Placeholder; parent is never executed
        'PENDING',
        v_max_attempts,
        bck.fn_resolve_params('{}'),   -- Snapshot of full config at registration time
        current_user
    )
    RETURNING id INTO v_id_parent;

    -- -------------------------------------------------------------------------
    -- CREATE CHILD ROWS (one per query in the array)
    -- -------------------------------------------------------------------------
    FOR v_item IN SELECT jsonb_array_elements(p_queries) LOOP

        -- Extract fields from JSON item
        v_query_sql   := TRIM(v_item ->> 'query_sql');
        v_item_name   := TRIM(v_item ->> 'name');
        v_item_params := COALESCE(v_item -> 'params', '{}');

        -- Validate: query_sql is required
        IF v_query_sql IS NULL OR v_query_sql = '' THEN
            -- Rollback parent and all children inserted so far
            RAISE EXCEPTION '[REGISTER] Item % has an empty or missing "query_sql" field',
                v_count + 1
                USING ERRCODE = 'JE003';
        END IF;

        -- Resolve effective params: config defaults merged with item-level overrides
        v_eff_params := bck.fn_resolve_params(v_item_params);

        -- Derive per-item max_attempts (item params may override the config default)
        v_max_attempts := COALESCE(
            (v_eff_params ->> 'max_failed_attempts')::SMALLINT,
            bck.fn_cfg('max_failed_attempts', '3')::SMALLINT
        );

        INSERT INTO bck.process (
            id_parent,
            name,
            query_sql,
            status,
            max_attempts,
            params,
            insert_by
        ) VALUES (
            v_id_parent,
            COALESCE(v_item_name, 'Process ' || (v_count + 1)),
            v_query_sql,
            'PENDING',
            v_max_attempts,
            v_eff_params,
            current_user
        );

        v_count := v_count + 1;
    END LOOP;

    -- -------------------------------------------------------------------------
    -- RETURN CONFIRMATION
    -- -------------------------------------------------------------------------
    RETURN QUERY
    SELECT
        v_id_parent,
        v_count,
        FORMAT(
            'Batch registered successfully. Parent ID: %s | Processes queued: %s | Use fn_start_orchestrator() to begin execution.',
            v_id_parent,
            v_count
        )::TEXT;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION '[REGISTER] Failed to register batch: % — SQLSTATE: %',
            SQLERRM, SQLSTATE;
END;
$$;

COMMENT ON FUNCTION bck.fn_register_batch IS
    'Registers a JSON array of queries as a background process batch.
     Creates one parent row (for tracking) and one child row per query.
     Returns the parent ID needed to start the orchestrator.

     Parameters:
       p_queries  - JSON array of objects with fields:
                    "query_sql" (required), "name" (optional), "params" (optional overrides)
       p_name     - Optional label for the whole batch

     Returns:
       id_parent    - Parent process ID for use with fn_start_orchestrator()
       total_queued - Number of child processes created
       message      - Confirmation text

     Example:
       SELECT * FROM bck.fn_register_batch(
         p_queries => ''[
           {"query_sql": "UPDATE accounts SET synced = true", "name": "Sync accounts"},
           {"query_sql": "CALL rebuild_stats()", "params": {"default_timeout_sec": 1800}}
         ]''::JSONB,
         p_name => ''Nightly sync batch''
       );';

-- Permissions
REVOKE ALL  ON FUNCTION bck.fn_register_batch(JSONB, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bck.fn_register_batch(JSONB, TEXT) TO PUBLIC;
-- =============================================================================
-- BACKGROUND JOB ENGINE v2.0
-- =============================================================================
-- File 04: fn_worker — Background worker with self-heal orchestrator logic
-- =============================================================================
--
-- PURPOSE:
--   Executes a single child process in a pg_background session.
--   After finishing, checks if the orchestrator is still alive.
--   If the orchestrator is DEAD and there are still pending processes,
--   this worker self-heals by launching a new orchestrator.
--
-- SELF-HEAL FLOW:
--   Worker finishes execution
--       │
--       ├─ Check pg_stat_activity for orchestrator PID  (fn_orch_is_alive)
--       │
--       ├─ ALIVE  → close normally
--       │
--       └─ DEAD + pending work remains → launch new orchestrator via pg_background
--
-- =============================================================================

CREATE OR REPLACE FUNCTION bck.fn_worker(
    p_id         BIGINT,    -- Child process ID to execute
    p_orch_id    BIGINT     -- Orchestrator session ID that launched this worker
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
SET statement_timeout = 0
SET lock_timeout = 0
SET log_min_messages = 'panic'
AS $$
DECLARE
    v_proc          RECORD;
    v_rows          BIGINT  := 0;
    v_success       BOOLEAN := FALSE;
    v_err_msg       TEXT;
    v_err_detail    TEXT;
    v_pending_count INT;
    v_id_parent     BIGINT;
    v_new_orch_pid  INT;
BEGIN
    -- =========================================================================
    -- STEP 1: Lock and claim the process row
    -- SKIP LOCKED prevents two workers from running the same process
    -- =========================================================================
    SELECT * INTO v_proc
    FROM bck.process
    WHERE id = p_id
      AND status IN ('PENDING', 'RETRYING')
    FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
        -- Already taken by another worker or status changed — exit silently
        RAISE LOG '[WORKER] Process % not available (already claimed or wrong status). Exiting.', p_id;
        RETURN;
    END IF;

    v_id_parent := v_proc.id_parent;

    -- =========================================================================
    -- STEP 2: Mark as RUNNING
    -- =========================================================================
    UPDATE bck.process SET
        status      = 'RUNNING',
        pid         = pg_backend_pid(),
        attempts    = attempts + 1,
        start_time  = clock_timestamp(),
        end_time    = NULL,
        error_msg   = NULL,
        update_time = clock_timestamp()
    WHERE id = p_id;

    -- =========================================================================
    -- STEP 3: Execute the query
    -- =========================================================================
    BEGIN
        EXECUTE v_proc.query_sql;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_success := TRUE;

    EXCEPTION WHEN OTHERS THEN
        v_err_msg    := SQLERRM;
        GET STACKED DIAGNOSTICS v_err_detail := PG_EXCEPTION_DETAIL;
    END;

    -- =========================================================================
    -- STEP 4A: SUCCESS — mark DONE
    -- =========================================================================
    IF v_success THEN
        UPDATE bck.process SET
            status      = 'DONE',
            pid         = 0,
            end_time    = clock_timestamp(),
            error_msg   = NULL,
            update_time = clock_timestamp()
        WHERE id = p_id;

        RAISE LOG '[WORKER] Process % (%) finished successfully. Rows affected: %',
            p_id, v_proc.name, v_rows;

    -- =========================================================================
    -- STEP 4B: FAILURE — decide retry or final fail
    -- =========================================================================
    ELSE
        -- attempts was already incremented in STEP 2
        -- Re-read the current attempt count
        SELECT attempts INTO v_proc.attempts FROM bck.process WHERE id = p_id;

        IF v_proc.attempts < v_proc.max_attempts THEN
            -- Schedule a retry: status RETRYING, delay sourced from effective params
            UPDATE bck.process SET
                status      = 'RETRYING',
                pid         = 0,
                end_time    = clock_timestamp(),
                error_msg   = FORMAT(
                    '[Attempt %s/%s] %s | Detail: %s',
                    v_proc.attempts, v_proc.max_attempts,
                    v_err_msg,
                    COALESCE(v_err_detail, 'none')
                ),
                -- Shift insert_time forward so it surfaces after the retry delay
                -- The orchestrator picks up processes ordered by insert_time ASC
                insert_time = clock_timestamp() + (
                    COALESCE(
                        (v_proc.params ->> 'retry_delay_sec'),
                        bck.fn_cfg('retry_delay_sec', '60')
                    )::INT || ' seconds'
                )::INTERVAL,
                update_time = clock_timestamp()
            WHERE id = p_id;

            RAISE WARNING '[WORKER] Process % FAILED (attempt %/%s). Retry scheduled in %ss. Error: %',
                p_id, v_proc.attempts, v_proc.max_attempts,
                COALESCE((v_proc.params ->> 'retry_delay_sec'), bck.fn_cfg('retry_delay_sec', '60')),
                v_err_msg;
        ELSE
            -- Exhausted all attempts — mark as permanently FAILED
            UPDATE bck.process SET
                status      = 'FAILED',
                pid         = 0,
                end_time    = clock_timestamp(),
                error_msg   = FORMAT(
                    '[FINAL FAILURE after %s/%s attempts] %s | Detail: %s',
                    v_proc.attempts, v_proc.max_attempts,
                    v_err_msg,
                    COALESCE(v_err_detail, 'none')
                ),
                update_time = clock_timestamp()
            WHERE id = p_id;

            RAISE WARNING '[WORKER] Process % PERMANENTLY FAILED after %s attempts. Error: %',
                p_id, v_proc.attempts, v_err_msg;
        END IF;
    END IF;

    -- =========================================================================
    -- STEP 5: SELF-HEAL CHECK
    -- After finishing, verify the orchestrator is still alive.
    -- If it is dead AND there is still pending work → launch a new orchestrator.
    -- This prevents the entire queue from stalling if the orchestrator crashes.
    -- =========================================================================

    IF NOT bck.fn_orch_is_alive() THEN
        -- Check if there are still processes waiting to run under the same parent
        SELECT COUNT(*) INTO v_pending_count
        FROM bck.process
        WHERE id_parent = v_id_parent
          AND status IN ('PENDING', 'RETRYING');

        IF v_pending_count > 0 THEN
            RAISE WARNING '[WORKER] Orchestrator is DEAD. % processes still pending for parent %. Self-healing: launching new orchestrator.',
                v_pending_count, v_id_parent;

            -- Launch a fresh orchestrator in background
            -- It will pick up remaining work on its first cycle
            v_new_orch_pid := pg_background_launch(
                'SELECT bck.fn_start_orchestrator()'
            );

            RAISE LOG '[WORKER] Self-heal: new orchestrator launched with PID %.', v_new_orch_pid;
        ELSE
            RAISE LOG '[WORKER] Orchestrator is gone but no pending work remains for parent %. Nothing to self-heal.', v_id_parent;
        END IF;
    END IF;

EXCEPTION WHEN OTHERS THEN
    -- Critical unexpected error inside the worker itself (outside the EXECUTE block)
    RAISE WARNING '[WORKER] Critical worker error for process %: % — %', p_id, SQLSTATE, SQLERRM;

    -- Emergency cleanup: ensure process is not left in RUNNING state
    BEGIN
        UPDATE bck.process SET
            status      = 'FAILED',
            pid         = 0,
            end_time    = clock_timestamp(),
            error_msg   = FORMAT('[WORKER CRASH] %s: %s', SQLSTATE, LEFT(SQLERRM, 500)),
            update_time = clock_timestamp()
        WHERE id = p_id
          AND status = 'RUNNING';
    EXCEPTION WHEN OTHERS THEN
        NULL; -- Never propagate errors from emergency cleanup
    END;
END;
$$;

COMMENT ON FUNCTION bck.fn_worker IS
    'Background worker that executes one child process.
     Handles: execution, error capture, retry scheduling, and final failure.
     After finishing, performs a self-heal check: if the orchestrator is dead
     and pending work remains, automatically launches a new orchestrator.
     This function is invoked by the orchestrator and should NOT be called directly.';

-- Permissions
REVOKE ALL    ON FUNCTION bck.fn_worker(BIGINT, BIGINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bck.fn_worker(BIGINT, BIGINT) TO PUBLIC;
ALTER FUNCTION bck.fn_worker(BIGINT, BIGINT) SET search_path TO bck, public, pg_temp;
-- =============================================================================
-- BACKGROUND JOB ENGINE v2.0
-- =============================================================================
-- File 05: Orchestrator — Loop + fn_start_orchestrator + fn_stop_orchestrator
-- =============================================================================
--
-- DESIGN DECISIONS:
--
--   1. FIRE & FORGET: fn_start_orchestrator() returns immediately.
--      The client can close their connection. The loop lives in pg_background.
--
--   2. SELF-CLOSING: The orchestrator loop exits on its own when:
--        a) No more PENDING/RETRYING processes exist for the batch
--        b) A STOPPING/STOPPED status is set via fn_stop_orchestrator()
--
--   3. NO INFINITE IDLE LOOP: The orchestrator does NOT spin forever.
--      It only runs while there is work. This avoids wasting a connection
--      and CPU cycles when the queue is empty.
--
--   4. SINGLETON: Only one active instance is allowed by default
--      (configurable via max_orch_instances in bck.config).
--
-- =============================================================================

-- ---------------------------------------------------------------------------
-- INTERNAL: fn_orchestrator_loop
-- Runs inside a pg_background session. Never call directly.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bck.fn_orchestrator_loop(
    p_orch_id    BIGINT,   -- Row ID in bck.orchestrator for this session
    p_id_parent  BIGINT    -- Parent process batch ID to process
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
SET statement_timeout = 0
SET lock_timeout = 0
SET log_min_messages = 'panic'
AS $$
DECLARE
    v_max_workers     INT;
    v_batch_size      INT;
    v_interval_ms     INT;
    v_orch_status     bck.orchestrator_status;
    v_metrics         RECORD;
    v_running         INT;
    v_slots           INT;
    v_job             RECORD;
    v_bg_pid          INT;
    v_stuck_fixed     INT;
    v_pending_exists  BOOLEAN;
BEGIN
    RAISE LOG '[ORCHESTRATOR %] Loop started. Parent batch: %', p_orch_id, p_id_parent;

    -- =========================================================================
    -- MAIN LOOP — runs until queue is empty or stop is requested
    -- =========================================================================
    LOOP

        -- ---------------------------------------------------------------------
        -- CYCLE 1: Read dynamic config (refreshed every cycle)
        -- ---------------------------------------------------------------------
        v_max_workers := bck.fn_cfg('max_workers', '5')::INT;
        v_interval_ms := bck.fn_cfg('cycle_interval_ms', '2000')::INT;

        -- ---------------------------------------------------------------------
        -- CYCLE 2: Heartbeat + check for stop request
        -- ---------------------------------------------------------------------
        UPDATE bck.orchestrator SET
            last_heartbeat = clock_timestamp(),
            total_cycles   = total_cycles + 1
        WHERE id = p_orch_id
        RETURNING status INTO v_orch_status;

        IF v_orch_status IN ('STOPPING', 'STOPPED') THEN
            RAISE LOG '[ORCHESTRATOR %] Stop requested. Exiting loop cleanly.', p_orch_id;
            EXIT;
        END IF;

        -- ---------------------------------------------------------------------
        -- CYCLE 3: Fix any stuck/zombie processes from previous runs
        -- ---------------------------------------------------------------------
        v_stuck_fixed := bck.fn_fix_stuck();
        IF v_stuck_fixed > 0 THEN
            RAISE LOG '[ORCHESTRATOR %] Fixed % stuck process(es).', p_orch_id, v_stuck_fixed;
        END IF;

        -- ---------------------------------------------------------------------
        -- CYCLE 4: Check if any work remains for this batch
        -- If nothing is PENDING or RETRYING → queue is drained → EXIT
        -- ---------------------------------------------------------------------
        SELECT EXISTS (
            SELECT 1 FROM bck.process
            WHERE id_parent = p_id_parent
              AND status IN ('PENDING', 'RETRYING')
              AND insert_time <= clock_timestamp()  -- Respect retry delay window
        ) INTO v_pending_exists;

        IF NOT v_pending_exists THEN
            -- Double-check: also ensure nothing is still RUNNING
            -- (workers in flight might produce RETRYING items on failure)
            SELECT NOT EXISTS (
                SELECT 1 FROM bck.process
                WHERE id_parent = p_id_parent
                  AND status = 'RUNNING'
            ) INTO v_pending_exists;  -- Reusing var: TRUE = all done

            IF v_pending_exists THEN
                RAISE LOG '[ORCHESTRATOR %] Queue empty. All processes finished. Closing orchestrator.', p_orch_id;
                EXIT;
            END IF;
            -- Still have RUNNING workers → wait for them to finish
            PERFORM pg_sleep(v_interval_ms::FLOAT / 1000);
            CONTINUE;
        END IF;

        -- ---------------------------------------------------------------------
        -- CYCLE 5: Check server capacity before launching any workers
        -- ---------------------------------------------------------------------
        SELECT * INTO v_metrics
        FROM bck.fn_server_ok(1);  -- Check if at least 1 worker can be launched

        v_running := v_metrics.running_workers;
        v_slots   := GREATEST(0, v_max_workers - v_running);

        IF NOT v_metrics.can_launch OR v_slots = 0 THEN
            RAISE LOG '[ORCHESTRATOR %] Capacity wait — Running: %/% | Load: %.1f%% | Free conns: %',
                p_orch_id, v_running, v_max_workers, v_metrics.load_pct, v_metrics.free_conns;
            PERFORM pg_sleep(v_interval_ms::FLOAT / 1000);
            CONTINUE;
        END IF;

        -- ---------------------------------------------------------------------
        -- CYCLE 6: Dispatch workers up to available slots
        -- FOR UPDATE SKIP LOCKED is critical: prevents two orchestrator cycles
        -- from claiming the same process if cycles overlap.
        -- ---------------------------------------------------------------------
        FOR v_job IN
            SELECT id, name
            FROM bck.process
            WHERE id_parent = p_id_parent
              AND status IN ('PENDING', 'RETRYING')
              AND insert_time <= clock_timestamp()
            ORDER BY insert_time ASC   -- FIFO within same parent; retries go last (delayed insert_time)
            LIMIT v_slots
            FOR UPDATE SKIP LOCKED
        LOOP
            -- Claim: mark PENDING → RUNNING pre-emptively to reserve the slot
            -- (fn_worker will re-claim with FOR UPDATE SKIP LOCKED as well)
            UPDATE bck.process SET
                status      = 'PENDING',   -- Keep PENDING, worker will flip to RUNNING
                pid         = -1,          -- -1 = "claimed, worker not yet started"
                update_time = clock_timestamp()
            WHERE id = v_job.id;

            -- Launch the worker in a background session (Fire & Forget)
            BEGIN
                v_bg_pid := pg_background_launch(
                    FORMAT('SELECT bck.fn_worker(%s, %s)', v_job.id, p_orch_id)
                );

                RAISE LOG '[ORCHESTRATOR %] Launched worker for process % (%) — bg PID: %',
                    p_orch_id, v_job.id, v_job.name, v_bg_pid;

                -- Update metrics counter
                UPDATE bck.orchestrator SET
                    total_launched = total_launched + 1
                WHERE id = p_orch_id;

            EXCEPTION WHEN OTHERS THEN
                -- Launch failed: release the claim so the next cycle retries
                RAISE WARNING '[ORCHESTRATOR %] Failed to launch worker for process %: %',
                    p_orch_id, v_job.id, SQLERRM;

                UPDATE bck.process SET
                    pid         = 0,
                    update_time = clock_timestamp()
                WHERE id = v_job.id
                  AND pid = -1;
            END;
        END LOOP;

        -- ---------------------------------------------------------------------
        -- CYCLE 7: Sleep until next cycle
        -- ---------------------------------------------------------------------
        IF v_interval_ms > 0 THEN
            PERFORM pg_sleep(v_interval_ms::FLOAT / 1000);
        END IF;

    END LOOP;
    -- END MAIN LOOP

    -- =========================================================================
    -- CLEAN EXIT: Update orchestrator status
    -- =========================================================================
    UPDATE bck.orchestrator SET
        status   = 'STOPPED',
        end_time = clock_timestamp()
    WHERE id = p_orch_id;

    RAISE LOG '[ORCHESTRATOR %] Session closed cleanly.', p_orch_id;

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '[ORCHESTRATOR %] Fatal loop error: % — %', p_orch_id, SQLSTATE, SQLERRM;
    BEGIN
        UPDATE bck.orchestrator SET
            status   = 'CRASHED',
            end_time = clock_timestamp()
        WHERE id = p_orch_id;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
END;
$$;

COMMENT ON FUNCTION bck.fn_orchestrator_loop IS
    'Internal orchestrator loop. Runs in a pg_background session.
     Exits automatically when the queue is empty. DO NOT call directly.
     Use fn_start_orchestrator() instead.';


-- =============================================================================
-- PUBLIC: fn_start_orchestrator
-- Fire & Forget — returns immediately, client can close connection.
-- =============================================================================
CREATE OR REPLACE FUNCTION bck.fn_start_orchestrator(
    p_id_parent  BIGINT DEFAULT NULL   -- NULL = pick the oldest pending parent batch
)
RETURNS TABLE (
    orch_id     BIGINT,
    bg_pid      INT,
    status      TEXT,
    message     TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_max_instances   INT;
    v_crash_sec       INT;
    v_active_count    INT;
    v_active_id       BIGINT;
    v_id_parent       BIGINT;
    v_orch_id         BIGINT;
    v_bg_pid          INT;
    v_config_snap     JSONB;
    v_pending_count   INT;
BEGIN
    v_max_instances := bck.fn_cfg('max_orch_instances', '1')::INT;
    v_crash_sec     := bck.fn_cfg('heartbeat_crash_sec', '30')::INT;

    -- -------------------------------------------------------------------------
    -- Mark crashed orchestrators (stale heartbeat)
    -- -------------------------------------------------------------------------
    UPDATE bck.orchestrator SET
        status   = 'CRASHED',
        end_time = clock_timestamp()
    WHERE status = 'ACTIVE'
      AND last_heartbeat < clock_timestamp() - (v_crash_sec || ' seconds')::INTERVAL;

    -- -------------------------------------------------------------------------
    -- Singleton check: is there already an active orchestrator?
    -- -------------------------------------------------------------------------
    SELECT COUNT(*), MAX(id)
    INTO v_active_count, v_active_id
    FROM bck.orchestrator
    WHERE status = 'ACTIVE';

    IF v_active_count >= v_max_instances THEN
        RETURN QUERY SELECT
            v_active_id,
            NULL::INT,
            'ALREADY_RUNNING'::TEXT,
            FORMAT('An orchestrator (ID: %s) is already active. Use monitoring views to track progress.', v_active_id)::TEXT;
        RETURN;
    END IF;

    -- -------------------------------------------------------------------------
    -- Resolve which parent batch to process
    -- -------------------------------------------------------------------------
    IF p_id_parent IS NOT NULL THEN
        v_id_parent := p_id_parent;

        -- Validate: parent must exist and have pending children
        SELECT COUNT(*) INTO v_pending_count
        FROM bck.process
        WHERE id_parent = v_id_parent
          AND status IN ('PENDING', 'RETRYING');

        IF v_pending_count = 0 THEN
            RETURN QUERY SELECT
                NULL::BIGINT, NULL::INT,
                'NO_WORK'::TEXT,
                FORMAT('Parent batch %s has no PENDING or RETRYING processes.', v_id_parent)::TEXT;
            RETURN;
        END IF;
    ELSE
        -- Auto-detect: pick oldest parent batch with pending work
        SELECT p.id_parent INTO v_id_parent
        FROM bck.process p
        WHERE p.id_parent IS NOT NULL
          AND p.status IN ('PENDING', 'RETRYING')
        ORDER BY p.insert_time ASC
        LIMIT 1;

        IF v_id_parent IS NULL THEN
            RETURN QUERY SELECT
                NULL::BIGINT, NULL::INT,
                'NO_WORK'::TEXT,
                'No pending processes found in any batch.'::TEXT;
            RETURN;
        END IF;
    END IF;

    -- -------------------------------------------------------------------------
    -- Snapshot current config for audit
    -- -------------------------------------------------------------------------
    SELECT jsonb_object_agg(key, value)
    INTO v_config_snap
    FROM bck.config;

    -- -------------------------------------------------------------------------
    -- Create orchestrator session record
    -- -------------------------------------------------------------------------
    INSERT INTO bck.orchestrator (
        status,
        config_snapshot,
        insert_by
    ) VALUES (
        'ACTIVE',
        v_config_snap,
        current_user
    )
    RETURNING id INTO v_orch_id;

    -- -------------------------------------------------------------------------
    -- Launch the loop in background — Fire & Forget
    -- Client can close connection immediately after this returns.
    -- -------------------------------------------------------------------------
    v_bg_pid := pg_background_launch(
        FORMAT('SELECT bck.fn_orchestrator_loop(%s, %s)', v_orch_id, v_id_parent)
    );

    -- Save PID
    UPDATE bck.orchestrator SET pid = v_bg_pid WHERE id = v_orch_id;

    RAISE LOG '[START] Orchestrator % launched. PID: % | Parent batch: %',
        v_orch_id, v_bg_pid, v_id_parent;

    -- -------------------------------------------------------------------------
    -- Return immediately — do not wait for any processing
    -- -------------------------------------------------------------------------
    RETURN QUERY SELECT
        v_orch_id,
        v_bg_pid,
        'STARTED'::TEXT,
        FORMAT(
            'Orchestrator started in background. ID: %s | PID: %s | Batch: %s | You may close your connection safely.',
            v_orch_id, v_bg_pid, v_id_parent
        )::TEXT;

EXCEPTION WHEN OTHERS THEN
    -- Cleanup: remove orphan orchestrator row if launch failed
    BEGIN
        DELETE FROM bck.orchestrator WHERE id = v_orch_id AND pid IS NULL;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
    RAISE EXCEPTION '[START] Failed to start orchestrator: % — %', SQLSTATE, SQLERRM;
END;
$$;

COMMENT ON FUNCTION bck.fn_start_orchestrator IS
    'Starts the background orchestrator. Returns IMMEDIATELY — client can close their connection.
     The orchestrator runs autonomously and closes itself when the queue is empty.
     Parameters:
       p_id_parent - Target parent batch ID. NULL = auto-picks oldest pending batch.
     Returns: orch_id, bg_pid, status, message';

-- Permissions
REVOKE ALL    ON FUNCTION bck.fn_start_orchestrator(BIGINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bck.fn_start_orchestrator(BIGINT) TO PUBLIC;
ALTER FUNCTION bck.fn_start_orchestrator(BIGINT) SET search_path TO bck, public, pg_temp;


-- =============================================================================
-- PUBLIC: fn_stop_orchestrator
-- Requests a graceful shutdown. Workers in flight finish normally.
-- =============================================================================
CREATE OR REPLACE FUNCTION bck.fn_stop_orchestrator(
    p_orch_id  BIGINT DEFAULT NULL   -- NULL = stop all active instances
)
RETURNS TABLE (
    orch_id       BIGINT,
    prev_status   TEXT,
    new_status    TEXT,
    message       TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_row   RECORD;
    v_count INT := 0;
BEGIN
    FOR v_row IN
        SELECT id, status::TEXT AS prev_st
        FROM bck.orchestrator
        WHERE status = 'ACTIVE'
          AND (p_orch_id IS NULL OR id = p_orch_id)
        FOR UPDATE SKIP LOCKED
    LOOP
        UPDATE bck.orchestrator SET
            status       = 'STOPPING',
            stop_req_time = clock_timestamp(),
            stop_req_by   = current_user
        WHERE id = v_row.id;

        v_count := v_count + 1;

        RETURN QUERY SELECT
            v_row.id,
            v_row.prev_st,
            'STOPPING'::TEXT,
            FORMAT('Stop requested for orchestrator %s. It will finish the current cycle and exit gracefully. Running workers will complete normally.', v_row.id)::TEXT;
    END LOOP;

    IF v_count = 0 THEN
        RETURN QUERY SELECT
            p_orch_id,
            'NOT_FOUND'::TEXT,
            'NOT_FOUND'::TEXT,
            COALESCE(
                FORMAT('No active orchestrator found with ID: %s', p_orch_id),
                'No active orchestrator instances found.'
            )::TEXT;
    END IF;
END;
$$;

COMMENT ON FUNCTION bck.fn_stop_orchestrator IS
    'Requests graceful stop of the orchestrator. The loop exits after its current cycle.
     Workers already in flight finish normally — they are NOT killed.
     p_orch_id = NULL stops all active instances.';

-- Permissions
REVOKE ALL    ON FUNCTION bck.fn_stop_orchestrator(BIGINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bck.fn_stop_orchestrator(BIGINT) TO PUBLIC;
ALTER FUNCTION bck.fn_stop_orchestrator(BIGINT) SET search_path TO bck, public, pg_temp;
-- =============================================================================
-- BACKGROUND JOB ENGINE v2.0
-- =============================================================================
-- File 06: Admin Functions + Monitoring Views
-- =============================================================================


-- =============================================================================
-- ADMIN: fn_cancel_process
-- Cancels a PENDING or RETRYING process before it executes.
-- RUNNING processes cannot be cancelled (use fn_stop_orchestrator first).
-- =============================================================================
CREATE OR REPLACE FUNCTION bck.fn_cancel_process(
    p_id      BIGINT,
    p_reason  TEXT DEFAULT 'Manually cancelled'
)
RETURNS TABLE (
    id          BIGINT,
    name        TEXT,
    old_status  TEXT,
    new_status  TEXT,
    message     TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_proc  RECORD;
BEGIN
    SELECT id, name, status::TEXT AS st
    INTO v_proc
    FROM bck.process
    WHERE process.id = p_id
    FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
        RETURN QUERY SELECT p_id, NULL::TEXT, 'NOT_FOUND'::TEXT, 'NOT_FOUND'::TEXT,
            'Process not found or currently locked by another session.'::TEXT;
        RETURN;
    END IF;

    IF v_proc.st NOT IN ('PENDING', 'RETRYING') THEN
        RETURN QUERY SELECT p_id, v_proc.name, v_proc.st, v_proc.st,
            FORMAT('Cannot cancel: process is in "%s" status. Only PENDING/RETRYING can be cancelled.', v_proc.st)::TEXT;
        RETURN;
    END IF;

    UPDATE bck.process SET
        status      = 'CANCELLED',
        end_time    = clock_timestamp(),
        error_msg   = p_reason,
        update_time = clock_timestamp()
    WHERE id = p_id;

    RETURN QUERY SELECT p_id, v_proc.name, v_proc.st, 'CANCELLED'::TEXT,
        FORMAT('Process %s cancelled. Reason: %s', p_id, p_reason)::TEXT;
END;
$$;

COMMENT ON FUNCTION bck.fn_cancel_process IS
    'Cancels a PENDING or RETRYING process. RUNNING processes cannot be cancelled here.';

REVOKE ALL    ON FUNCTION bck.fn_cancel_process(BIGINT, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bck.fn_cancel_process(BIGINT, TEXT) TO PUBLIC;


-- =============================================================================
-- ADMIN: fn_cancel_batch
-- Cancels all PENDING/RETRYING processes under a parent batch.
-- =============================================================================
CREATE OR REPLACE FUNCTION bck.fn_cancel_batch(
    p_id_parent  BIGINT,
    p_reason     TEXT DEFAULT 'Batch manually cancelled'
)
RETURNS TABLE (
    cancelled_count  INT,
    message          TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_count  INT;
BEGIN
    UPDATE bck.process SET
        status      = 'CANCELLED',
        end_time    = clock_timestamp(),
        error_msg   = p_reason,
        update_time = clock_timestamp()
    WHERE id_parent = p_id_parent
      AND status IN ('PENDING', 'RETRYING');

    GET DIAGNOSTICS v_count = ROW_COUNT;

    RETURN QUERY SELECT v_count,
        FORMAT('%s process(es) cancelled in batch %s. Reason: %s', v_count, p_id_parent, p_reason)::TEXT;
END;
$$;

COMMENT ON FUNCTION bck.fn_cancel_batch IS
    'Cancels all PENDING and RETRYING processes in a batch. Running processes finish normally.';

REVOKE ALL    ON FUNCTION bck.fn_cancel_batch(BIGINT, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bck.fn_cancel_batch(BIGINT, TEXT) TO PUBLIC;


-- =============================================================================
-- ADMIN: fn_retry_failed
-- Re-queues a permanently FAILED process for another attempt.
-- Resets attempt counters and status to PENDING.
-- =============================================================================
CREATE OR REPLACE FUNCTION bck.fn_retry_failed(
    p_id  BIGINT
)
RETURNS TABLE (
    id       BIGINT,
    name     TEXT,
    message  TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_proc  RECORD;
BEGIN
    SELECT id, name INTO v_proc
    FROM bck.process
    WHERE process.id = p_id AND status = 'FAILED'
    FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
        RETURN QUERY SELECT p_id, NULL::TEXT,
            'Process not found in FAILED status (or currently locked).'::TEXT;
        RETURN;
    END IF;

    UPDATE bck.process SET
        status      = 'PENDING',
        attempts    = 0,
        pid         = 0,
        start_time  = NULL,
        end_time    = NULL,
        error_msg   = NULL,
        insert_time = clock_timestamp(),   -- Reset so it goes to end of queue
        update_time = clock_timestamp()
    WHERE id = p_id;

    RETURN QUERY SELECT p_id, v_proc.name,
        FORMAT('Process %s re-queued as PENDING. Start orchestrator to execute.', p_id)::TEXT;
END;
$$;

COMMENT ON FUNCTION bck.fn_retry_failed IS
    'Re-queues a permanently FAILED process. Resets all attempt counters.
     The orchestrator must be running (or started) to pick it up.';

REVOKE ALL    ON FUNCTION bck.fn_retry_failed(BIGINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bck.fn_retry_failed(BIGINT) TO PUBLIC;


-- =============================================================================
-- ADMIN: fn_cleanup_history
-- Removes DONE/CANCELLED process rows older than N days.
-- =============================================================================
CREATE OR REPLACE FUNCTION bck.fn_cleanup_history(
    p_days  INT DEFAULT 30
)
RETURNS TABLE (
    deleted_count  BIGINT,
    message        TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_count  BIGINT;
BEGIN
    -- Delete child rows first (FK constraint), then orphaned parents
    DELETE FROM bck.process
    WHERE status IN ('DONE', 'CANCELLED')
      AND end_time < clock_timestamp() - (p_days || ' days')::INTERVAL;

    GET DIAGNOSTICS v_count = ROW_COUNT;

    -- Cleanup old stopped orchestrator records
    DELETE FROM bck.orchestrator
    WHERE status IN ('STOPPED', 'CRASHED')
      AND end_time < clock_timestamp() - (p_days || ' days')::INTERVAL;

    RETURN QUERY SELECT v_count,
        FORMAT('Cleanup complete: %s process rows deleted (older than %s days).', v_count, p_days)::TEXT;
END;
$$;

COMMENT ON FUNCTION bck.fn_cleanup_history IS
    'Purges DONE and CANCELLED process rows older than p_days. Also cleans up old orchestrator records.';

REVOKE ALL    ON FUNCTION bck.fn_cleanup_history(INT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION bck.fn_cleanup_history(INT) TO PUBLIC;


-- =============================================================================
-- VIEW: vw_orchestrator
-- Real-time status of all orchestrator instances
-- =============================================================================
CREATE OR REPLACE VIEW bck.vw_orchestrator AS
SELECT
    o.id,
    o.pid,
    o.status,
    o.start_time,
    o.last_heartbeat,
    o.end_time,
    AGE(clock_timestamp(), o.start_time)                               AS uptime,
    ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - o.last_heartbeat))::NUMERIC, 1)
                                                                       AS secs_since_heartbeat,
    CASE
        WHEN o.status <> 'ACTIVE'                                      THEN '⚪ INACTIVE'
        WHEN EXTRACT(EPOCH FROM (clock_timestamp() - o.last_heartbeat))
             > bck.fn_cfg('heartbeat_crash_sec','30')::INT             THEN '🔴 UNRESPONSIVE'
        WHEN EXTRACT(EPOCH FROM (clock_timestamp() - o.last_heartbeat)) > 10
                                                                       THEN '🟡 SLOW HEARTBEAT'
        ELSE                                                                '🟢 HEALTHY'
    END                                                                AS health,
    o.total_cycles,
    o.total_launched,
    o.launch_count                                                     AS self_heal_count,
    o.stop_req_time,
    o.stop_req_by,
    o.insert_by                                                        AS started_by,
    o.insert_time
FROM bck.orchestrator o
ORDER BY o.insert_time DESC;

COMMENT ON VIEW bck.vw_orchestrator IS
    'Real-time status of all orchestrator instances. Watch health column for crash detection.';


-- =============================================================================
-- VIEW: vw_batch_summary
-- One row per parent batch — high-level progress overview
-- =============================================================================
CREATE OR REPLACE VIEW bck.vw_batch_summary AS
SELECT
    parent.id                                                          AS id_parent,
    parent.name                                                        AS batch_name,

    -- Counts by status
    COUNT(child.id)                                                    AS total,
    COUNT(child.id) FILTER (WHERE child.status = 'PENDING')           AS pending,
    COUNT(child.id) FILTER (WHERE child.status = 'RETRYING')          AS retrying,
    COUNT(child.id) FILTER (WHERE child.status = 'RUNNING')           AS running,
    COUNT(child.id) FILTER (WHERE child.status = 'DONE')              AS done,
    COUNT(child.id) FILTER (WHERE child.status = 'FAILED')            AS failed,
    COUNT(child.id) FILTER (WHERE child.status = 'CANCELLED')         AS cancelled,

    -- Progress
    CASE WHEN COUNT(child.id) = 0 THEN '0%'
         ELSE ROUND(
             COUNT(child.id) FILTER (WHERE child.status = 'DONE')::NUMERIC
             / COUNT(child.id) * 100
         )::TEXT || '%'
    END                                                                AS pct_done,

    -- Visual progress bar (20 chars)
    '[' || RPAD(
        REPEAT('█',
            LEAST(
                COALESCE(ROUND(
                    COUNT(child.id) FILTER (WHERE child.status = 'DONE')::NUMERIC
                    / NULLIF(COUNT(child.id),0) * 20
                )::INT, 0),
                20
            )
        ), 20, '·'
    ) || ']'                                                           AS progress_bar,

    -- Error rate
    CASE WHEN COUNT(child.id) FILTER (WHERE child.status IN ('DONE','FAILED')) = 0 THEN 0
         ELSE ROUND(
             COUNT(child.id) FILTER (WHERE child.status = 'FAILED')::NUMERIC
             / NULLIF(COUNT(child.id) FILTER (WHERE child.status IN ('DONE','FAILED')), 0) * 100,
             2
         )
    END                                                                AS error_rate_pct,

    -- Overall batch status
    CASE
        WHEN COUNT(child.id) = COUNT(child.id) FILTER (WHERE child.status IN ('DONE','FAILED','CANCELLED'))
            THEN '✅ FINISHED'
        WHEN COUNT(child.id) FILTER (WHERE child.status = 'RUNNING') > 0
            THEN '🔥 RUNNING'
        WHEN COUNT(child.id) FILTER (WHERE child.status IN ('PENDING','RETRYING')) > 0
            THEN '⏳ PENDING'
        ELSE '⚙️ IN PROGRESS'
    END                                                                AS batch_status,

    parent.insert_time,
    parent.insert_by

FROM bck.process parent
LEFT JOIN bck.process child ON child.id_parent = parent.id
WHERE parent.id_parent IS NULL          -- Only parent rows
GROUP BY parent.id, parent.name, parent.insert_time, parent.insert_by
ORDER BY parent.insert_time DESC;

COMMENT ON VIEW bck.vw_batch_summary IS
    'One row per batch. Shows progress bar, counts by status, and error rate.';


-- =============================================================================
-- VIEW: vw_process_monitor
-- All child processes with full timing and retry detail
-- =============================================================================
CREATE OR REPLACE VIEW bck.vw_process_monitor AS
SELECT
    p.id,
    p.id_parent,
    parent.name                                                        AS batch_name,
    p.name,
    p.status,

    -- Timing
    p.start_time,
    p.end_time,
    p.insert_time,
    p.update_time,

    -- Elapsed time (running or finished)
    CASE
        WHEN p.status = 'RUNNING'  THEN AGE(clock_timestamp(), p.start_time)
        WHEN p.status IN ('DONE','FAILED','CANCELLED')
                                   THEN AGE(p.end_time, p.start_time)
        ELSE NULL
    END                                                                AS elapsed,

    -- Timeout countdown (while running)
    CASE
        WHEN p.status = 'RUNNING' AND (p.params ->> 'default_timeout_sec')::INT > 0
        THEN (p.start_time + ((p.params ->> 'default_timeout_sec')::INT || ' seconds')::INTERVAL)
             - clock_timestamp()
        ELSE NULL
    END                                                                AS time_to_timeout,

    -- Retry info
    p.attempts,
    p.max_attempts,
    p.attempts::TEXT || '/' || p.max_attempts::TEXT                   AS attempt_display,

    -- Background worker PID
    p.pid,

    -- Error
    p.error_msg,

    -- Effective params
    p.params,

    -- Query (preview)
    LEFT(p.query_sql, 150)                                             AS query_preview,

    p.insert_by

FROM bck.process p
LEFT JOIN bck.process parent ON parent.id = p.id_parent
WHERE p.id_parent IS NOT NULL           -- Only child rows
ORDER BY
    CASE p.status
        WHEN 'RUNNING'   THEN 1
        WHEN 'RETRYING'  THEN 2
        WHEN 'PENDING'   THEN 3
        WHEN 'DONE'      THEN 4
        WHEN 'FAILED'    THEN 5
        ELSE 6
    END,
    p.insert_time ASC;

COMMENT ON VIEW bck.vw_process_monitor IS
    'Full detail of all child processes. Ordered by status (RUNNING first) then insert_time.
     Use with WHERE id_parent = X to focus on a specific batch.';


-- =============================================================================
-- VIEW: vw_running_workers
-- Only processes currently executing — real-time worker dashboard
-- =============================================================================
CREATE OR REPLACE VIEW bck.vw_running_workers AS
SELECT
    p.id,
    p.id_parent,
    p.name,
    p.pid                                                              AS bg_pid,
    p.start_time,
    AGE(clock_timestamp(), p.start_time)                               AS running_for,

    -- Timeout info
    (p.params ->> 'default_timeout_sec')::INT                         AS timeout_sec,
    CASE
        WHEN (p.params ->> 'default_timeout_sec')::INT > 0
        THEN p.start_time + ((p.params ->> 'default_timeout_sec')::INT || ' seconds')::INTERVAL
        ELSE NULL
    END                                                                AS timeout_at,

    -- Timeout warning
    CASE
        WHEN (p.params ->> 'default_timeout_sec')::INT <= 0          THEN '⚪ No timeout'
        WHEN EXTRACT(EPOCH FROM (clock_timestamp() - p.start_time)) /
             NULLIF((p.params ->> 'default_timeout_sec')::INT, 0) >= 0.8
                                                                      THEN '🔴 TIMEOUT IMMINENT'
        WHEN EXTRACT(EPOCH FROM (clock_timestamp() - p.start_time)) /
             NULLIF((p.params ->> 'default_timeout_sec')::INT, 0) >= 0.5
                                                                      THEN '🟡 HALFWAY'
        ELSE                                                               '🟢 NORMAL'
    END                                                                AS timeout_alert,

    p.attempts,
    LEFT(p.query_sql, 200)                                             AS query_preview

FROM bck.process p
WHERE p.status = 'RUNNING'
ORDER BY p.start_time ASC;

COMMENT ON VIEW bck.vw_running_workers IS
    'Real-time view of all currently executing background workers with timeout alerts.';


-- =============================================================================
-- VIEW: vw_failed_processes
-- All permanently failed processes with full error detail for diagnosis
-- =============================================================================
CREATE OR REPLACE VIEW bck.vw_failed_processes AS
SELECT
    p.id,
    p.id_parent,
    parent.name                           AS batch_name,
    p.name,
    p.attempts,
    p.max_attempts,
    p.error_msg,
    p.start_time,
    p.end_time,
    AGE(clock_timestamp(), p.end_time)    AS time_since_failure,
    p.query_sql,
    p.params,
    p.insert_by
FROM bck.process p
LEFT JOIN bck.process parent ON parent.id = p.id_parent
WHERE p.status = 'FAILED'
ORDER BY p.end_time DESC;

COMMENT ON VIEW bck.vw_failed_processes IS
    'All permanently failed processes. Use fn_retry_failed(id) to re-queue any of these.';
-- =============================================================================
-- BACKGROUND JOB ENGINE v2.0
-- =============================================================================
-- File 07: Usage Guide & Examples
-- =============================================================================

/*
╔══════════════════════════════════════════════════════════════════════════════╗
║               BACKGROUND JOB ENGINE v2.0 — COMPLETE USAGE GUIDE             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Schema  : bck                                                               ║
║  Requires: pg_background extension                                           ║
╚══════════════════════════════════════════════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 OBJECT REFERENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

 TABLES
   bck.config           — Dynamic configuration (editable at runtime)
   bck.query_catalog    — Reusable query library with param overrides
   bck.process          — Core queue: parent batch + child process rows
   bck.orchestrator     — Orchestrator session registry

 PUBLIC FUNCTIONS
   bck.fn_register_batch()    — Register a JSON batch of queries → returns id_parent
   bck.fn_start_orchestrator()— Fire & Forget: start processing in background
   bck.fn_stop_orchestrator() — Request graceful orchestrator shutdown
   bck.fn_cancel_process()    — Cancel a single PENDING/RETRYING process
   bck.fn_cancel_batch()      — Cancel all pending processes in a batch
   bck.fn_retry_failed()      — Re-queue a permanently FAILED process
   bck.fn_cleanup_history()   — Purge old DONE/CANCELLED rows

 INTERNAL FUNCTIONS (do not call directly)
   bck.fn_orchestrator_loop() — Background loop, called by fn_start_orchestrator
   bck.fn_worker()            — Background worker, called by the orchestrator
   bck.fn_cfg()               — Config reader helper
   bck.fn_server_ok()         — Server capacity check
   bck.fn_resolve_params()    — Param merge helper
   bck.fn_fix_stuck()         — Zombie process cleanup
   bck.fn_orch_is_alive()     — Orchestrator health check (used by self-heal)

 MONITORING VIEWS
   bck.vw_orchestrator        — Orchestrator health, heartbeat, and metrics
   bck.vw_batch_summary       — Per-batch progress bar and status counts
   bck.vw_process_monitor     — All child processes with full detail
   bck.vw_running_workers     — Only currently running workers (real-time)
   bck.vw_failed_processes    — All failed processes with error detail


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 STEP 1: REGISTER A BATCH (simple)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
*/

-- Basic batch: two queries, default settings
SELECT * FROM bck.fn_register_batch(
    p_queries => '[
        {"query_sql": "UPDATE accounts SET synced = true WHERE region = ''US''",  "name": "Sync US accounts"},
        {"query_sql": "UPDATE accounts SET synced = true WHERE region = ''EU''",  "name": "Sync EU accounts"},
        {"query_sql": "CALL rebuild_account_stats()",                             "name": "Rebuild stats"}
    ]'::JSONB,
    p_name => 'Account Sync — 2024-12'
);
-- Returns: id_parent (save this for monitoring)

/*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 STEP 1B: REGISTER WITH PARAM OVERRIDES
 Per-query params override bck.config for that specific process only.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
*/

SELECT * FROM bck.fn_register_batch(
    p_queries => '[
        {
            "query_sql": "CALL nightly_full_report()",
            "name":      "Full report — long timeout",
            "params": {
                "default_timeout_sec": 3600,
                "max_failed_attempts": 1
            }
        },
        {
            "query_sql": "DELETE FROM tmp_staging WHERE created_at < NOW() - INTERVAL ''1 day''",
            "name":      "Cleanup staging",
            "params": {
                "retry_delay_sec": 10
            }
        }
    ]'::JSONB,
    p_name => 'Nightly maintenance'
);

/*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 STEP 2: START THE ORCHESTRATOR (Fire & Forget)
 Returns immediately. Close your connection — processing continues in background.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
*/

-- Option A: auto-detect oldest pending batch
SELECT * FROM bck.fn_start_orchestrator();

-- Option B: target a specific batch by id_parent
SELECT * FROM bck.fn_start_orchestrator(p_id_parent => 42);

/*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 STEP 3: MONITOR (reconnect anytime — no waiting required)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
*/

-- Is the orchestrator alive?
SELECT id, pid, status, health, secs_since_heartbeat, total_cycles, total_launched, uptime
FROM bck.vw_orchestrator;

-- Batch-level progress summary
SELECT id_parent, batch_name, batch_status, total, done, running,
       pending, failed, pct_done, progress_bar, error_rate_pct
FROM bck.vw_batch_summary;

-- Full process detail for a specific batch
SELECT id, name, status, elapsed, attempt_display, time_to_timeout, error_msg
FROM bck.vw_process_monitor
WHERE id_parent = 42;

-- Workers running right now
SELECT id, name, bg_pid, running_for, timeout_alert, query_preview
FROM bck.vw_running_workers;

-- Failed processes that need attention
SELECT id, name, batch_name, attempts, error_msg, time_since_failure
FROM bck.vw_failed_processes;

/*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 ADMINISTRATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
*/

-- Cancel a single process (must be PENDING or RETRYING)
SELECT * FROM bck.fn_cancel_process(p_id => 15, p_reason => 'No longer needed');

-- Cancel all pending processes in a batch
SELECT * FROM bck.fn_cancel_batch(p_id_parent => 42, p_reason => 'Batch aborted by admin');

-- Re-queue a permanently failed process
SELECT * FROM bck.fn_retry_failed(p_id => 17);

-- Stop the orchestrator gracefully (finishes current cycle, workers complete normally)
SELECT * FROM bck.fn_stop_orchestrator();            -- All active instances
SELECT * FROM bck.fn_stop_orchestrator(p_orch_id => 3);  -- Specific instance

-- Clean up old completed records
SELECT * FROM bck.fn_cleanup_history(p_days => 30);

/*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 TUNE CONFIGURATION (changes apply on next orchestrator cycle)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
*/

-- See all current config
SELECT * FROM bck.config ORDER BY key;

-- Increase max parallel workers
UPDATE bck.config SET value = '10', update_time = clock_timestamp()
WHERE key = 'max_workers';

-- Reduce cycle sleep (more responsive, more CPU)
UPDATE bck.config SET value = '500', update_time = clock_timestamp()
WHERE key = 'cycle_interval_ms';

-- Reduce max load threshold (more conservative)
UPDATE bck.config SET value = '120', update_time = clock_timestamp()
WHERE key = 'retry_delay_sec';

/*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 QUERY CATALOG — Reusable queries
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
*/

-- Store a reusable query in the catalog
INSERT INTO bck.query_catalog (query_sql, description, params) VALUES
    ('CALL rebuild_all_indexes()', 'Full index rebuild — run during low traffic',
     '{"default_timeout_sec": 7200, "max_failed_attempts": 1}'),
    ('UPDATE stats_cache SET dirty = true', 'Invalidate stats cache', '{}');

-- View catalog
SELECT id, description, LEFT(query_sql, 80) AS query_preview, params FROM bck.query_catalog;

-- Register batch using catalog entries
SELECT * FROM bck.fn_register_batch(
    p_queries => (
        SELECT jsonb_agg(
            jsonb_build_object(
                'query_sql', query_sql,
                'name',      description,
                'params',    params
            )
        )
        FROM bck.query_catalog
        WHERE id IN (1, 2)
    ),
    p_name => 'Scheduled maintenance from catalog'
);

/*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SELF-HEAL BEHAVIOR (automatic — no action needed)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

 If the orchestrator crashes unexpectedly:
   1. Any running worker will detect the crash at completion (fn_orch_is_alive)
   2. If pending work remains, the worker calls fn_start_orchestrator() itself
   3. The new orchestrator picks up where the old one left off
   4. The launch_count column in bck.orchestrator tracks self-heal events

 This means 1000 queued processes will complete even if the orchestrator
 crashes mid-way — the last worker to finish always checks and self-heals.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 RETRY DELAY MECHANISM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

 When a process fails and has retries remaining:
   - Status is set to RETRYING
   - insert_time is shifted forward by retry_delay_sec seconds
   - The orchestrator picks processes ORDER BY insert_time ASC
   - This naturally places retries at the END of the current queue
   - Effectively: all fresh PENDING work runs first, retries come last

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 pg_cron INTEGRATION (recommended for scheduled batches)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

 -- Run nightly maintenance at 2 AM every day
 SELECT cron.schedule(
     'nightly-maintenance',
     '0 2 * * *',
     $$
     SELECT bck.fn_start_orchestrator(
         (SELECT id FROM bck.process
          WHERE id_parent IS NULL
            AND name = 'Nightly maintenance'
          ORDER BY insert_time DESC LIMIT 1)
     )
     $$
 );

*/
