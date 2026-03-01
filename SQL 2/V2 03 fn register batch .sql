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
