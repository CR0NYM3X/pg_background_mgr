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
