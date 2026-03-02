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
DECLARE 
  v_value TEXT;
BEGIN
    SELECT value into v_value::text FROM bck.config WHERE key = p_key;
  IF v_value = '0' THEN 
    RETURN COALESCE(p_default,v_value );
	END IF;
    RETURN v_value;
END;
$$;


COMMENT ON FUNCTION bck.fn_cfg IS
    'Reads a config value from bck.config. Returns p_default if the key does not exist or on any error.
     Example: bck.fn_cfg(''max_workers'', ''5'')::INT';

-- select  bck.fn_cfg('max_workers')::INT;
-- select  bck.fn_cfg('max_workers','5')::INT;



-- ---------------------------------------------------------------------------
-- HELPER: fn_server_ok
-- Checks server capacity based on physical limits and logical configuration.
-- RETURNS: 
--   * can_launch: Boolean decision.
--   * workers_capacity: Real number of slots available.
--   * pg_background_active: Specifically processes of type pg_background.
--   * active_workers: All non-client processes (system + workers).
--   * max_worker_processes: The GUC limit from postgresql.conf.
--   * max_workers_cfg: The logical limit from bck.config.
-- ---------------------------------------------------------------------------

-- drop FUNCTION bck.fn_server_ok;
CREATE OR REPLACE FUNCTION bck.fn_server_ok(
    p_workers_to_launch    INT DEFAULT NULL, -- Cambiado a NULL por defecto
    OUT can_launch          BOOLEAN,
    OUT workers_capacity    INT,
    OUT pg_background_active INT,
    OUT active_workers      INT,
    OUT max_worker_processes INT,
    OUT max_workers_cfg     INT
)
RETURNS RECORD
LANGUAGE plpgsql
STABLE
SET search_path TO bck, public, pg_temp
AS $$
DECLARE
    v_logical_free     INT;
    v_physical_free    INT;
    v_running_internal INT;
BEGIN
    -- Actualizar estadísticas
    PERFORM pg_stat_clear_snapshot();	

    -- 1. Límites Físicos (Nivel PostgreSQL)
    max_worker_processes := current_setting('max_worker_processes')::INT;
    
    SELECT COUNT(*) INTO active_workers
    FROM pg_stat_activity
    WHERE backend_type <> 'client backend';

    SELECT COUNT(*) INTO pg_background_active
    FROM pg_stat_activity
    WHERE backend_type = 'pg_background';

    -- 2. Límites Lógicos (Nivel Orquestador)
    max_workers_cfg := bck.fn_cfg('max_workers', current_setting('max_worker_processes')::text)::INT;

    SELECT COUNT(*) INTO v_running_internal
    FROM bck.process
    WHERE status = 'RUNNING';

    -- 3. Cálculo de Capacidad
    -- Usamos LEAST para tomar el límite más estricto
    v_logical_free  := max_workers_cfg - v_running_internal;
    v_physical_free := max_worker_processes - active_workers - 1;

    -- GREATEST(0, ...) asegura que no devolvamos números negativos si el servidor está saturado
    workers_capacity := GREATEST(0, LEAST(v_logical_free, v_physical_free));

    -- 4. Decisión Final
    -- Si p_workers_to_launch es NULL, can_launch será TRUE si hay al menos 1 espacio.
    -- O podrías ajustarlo para que sea siempre TRUE si solo quieres consultar.
    can_launch := (workers_capacity >= COALESCE(p_workers_to_launch, 1));

END;
$$;


COMMENT ON FUNCTION bck.fn_server_ok IS 
    'Checks server capacity including max_connections AND max_worker_processes for all backend types.';
	
	
-- update bck.config set value = '45' where key = 'max_workers'; 
-- Select * from bck.fn_server_ok(50);




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


-- select * from bck.fn_resolve_params();
-- {"max_workers": "45", "retry_delay_sec": "60", "cycle_interval_ms": "2000", "max_orch_instances": "1", "default_timeout_sec": "600", "heartbeat_crash_sec": "30", "max_failed_attempts": "3"} 


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

	-- UPDATE STATS
    PERFORM pg_stat_clear_snapshot();	

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




-- ---------------------------------------------------------------------------
-- HELPER: validate_pids
-- Verifies the operational status of a given list of Process IDs (PIDs).
-- Cross-references input PIDs with pg_stat_activity to identify orphans.
-- Returns a summary of active/inactive counts and their respective arrays.
-- ---------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION bck.validate_pids(p_pids BIGINT[])
RETURNS TABLE (
    active_count   INT,
    active_pids    BIGINT[],
    inactive_count INT,
    inactive_pids  BIGINT[]
) AS $$
BEGIN
    -- Forzamos la actualización de las estadísticas de actividad
    PERFORM pg_stat_clear_snapshot();

    RETURN QUERY
    WITH input_pids AS (
        -- Convertimos el array de entrada en una tabla virtual
        SELECT unnest(p_pids) AS pid_to_check
    ),
    status_check AS (
        -- Cruzamos los PIDs enviados con la vista de actividad de Postgres
        SELECT 
            i.pid_to_check,
            CASE WHEN s.pid IS NOT NULL THEN TRUE ELSE FALSE END as is_active
        FROM input_pids i
        LEFT JOIN pg_stat_activity s ON i.pid_to_check = s.pid
    )
    SELECT 
        COUNT(*) FILTER (WHERE is_active)::INT as active_count,
        COALESCE(array_agg(pid_to_check) FILTER (WHERE is_active), '{}') as active_pids,
        COUNT(*) FILTER (WHERE NOT is_active)::INT as inactive_count,
        COALESCE(array_agg(pid_to_check) FILTER (WHERE NOT is_active), '{}') as inactive_pids
    FROM status_check;
END;
$$ LANGUAGE plpgsql;


--- SELECT * FROM bck.validate_pids(ARRAY[pg_backend_pid(), 99999, 88888 ]);
