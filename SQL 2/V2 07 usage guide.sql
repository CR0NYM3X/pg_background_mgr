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
