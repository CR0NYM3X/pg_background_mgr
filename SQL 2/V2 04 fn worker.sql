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
