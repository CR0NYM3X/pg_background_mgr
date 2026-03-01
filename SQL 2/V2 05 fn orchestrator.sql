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
