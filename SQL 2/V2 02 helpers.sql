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
