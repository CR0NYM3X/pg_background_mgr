# 🐘 BCK Engine — Background Job Queue for PostgreSQL

> Execute hundreds of SQL queries in parallel, in the background, without blocking your session — built entirely inside PostgreSQL using `pg_background`.

---

## Table of Contents

- [What is BCK Engine?](#what-is-bck-engine)
- [How it Works — Architecture](#how-it-works--architecture)
- [Workflow](#workflow)
- [Object Reference](#object-reference)
- [Usage Examples](#usage-examples)
- [Monitoring](#monitoring)
- [Administration](#administration)
- [Configuration](#configuration)
- [Self-Heal System](#self-heal-system)
- [Anti-Saturation Strategy](#anti-saturation-strategy)
- [Retry Mechanism](#retry-mechanism)
- [Comparison with Alternatives](#comparison-with-alternatives)
- [Key Advantages](#key-advantages)
- [Requirements & Installation](#requirements--installation)

---

## What is BCK Engine?

**BCK Engine** is a production-grade background job queue built entirely inside PostgreSQL. It allows you to register any number of SQL queries as a batch, start a background orchestrator with a single function call, and immediately close your connection. PostgreSQL handles the rest autonomously.

**No Node.js. No Python. No Redis. No external services. Just PostgreSQL.**

```sql
-- 1. Register your queries
SELECT * FROM bck.fn_register_batch(
    p_queries => '[
        {"query_sql": "UPDATE accounts SET synced = true"},
        {"query_sql": "CALL rebuild_stats()"},
        {"query_sql": "DELETE FROM tmp_cache WHERE expired_at < NOW()"}
    ]'::JSONB,
    p_name => 'Nightly maintenance'
);

-- 2. Start the orchestrator — returns immediately
SELECT * FROM bck.fn_start_orchestrator();

-- 3. Close your connection. Everything runs in the background.
-- Come back later to check progress:
SELECT * FROM bck.vw_batch_summary;
```

---

## How it Works — Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLIENT SESSION                               │
│                                                                     │
│  fn_register_batch(JSON)  →  Returns id_parent                     │
│  fn_start_orchestrator()  →  Returns immediately ✓                 │
│                              Client can close connection            │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ pg_background_launch()
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│              ORCHESTRATOR LOOP  (pg_background process)             │
│                                                                     │
│  ┌─ Each cycle: ──────────────────────────────────────────────┐    │
│  │  1. Update heartbeat                                        │    │
│  │  2. Fix stuck/zombie processes (fn_fix_stuck)               │    │
│  │  3. Check: any PENDING/RETRYING work remaining?             │    │
│  │     └─ NO → EXIT (queue empty, orchestrator closes itself)  │    │
│  │  4. Check server capacity (fn_server_ok)                    │    │
│  │     └─ Overloaded → sleep, retry next cycle                 │    │
│  │  5. Claim available slots (FOR UPDATE SKIP LOCKED)          │    │
│  │  6. Launch workers via pg_background_launch()               │    │
│  │  7. Sleep cycle_interval_ms                                 │    │
│  └─────────────────────────────────────────────────────────────┘    │
└──────────────┬─────────────────┬──────────────────┬─────────────────┘
               │                 │                  │
               ▼                 ▼                  ▼
     ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
     │  WORKER #1   │  │  WORKER #2   │  │  WORKER #N   │
     │ pg_background│  │ pg_background│  │ pg_background│
     │              │  │              │  │              │
     │ EXECUTE sql  │  │ EXECUTE sql  │  │ EXECUTE sql  │
     │ → DONE       │  │ → DONE       │  │ → RETRYING   │
     │              │  │              │  │              │
     │ Self-heal ✓  │  │ Self-heal ✓  │  │ Self-heal ✓  │
     └──────────────┘  └──────────────┘  └──────────────┘
               │                 │                  │
               └─────────────────┼──────────────────┘
                                 ▼
                     ┌──────────────────────┐
                     │    bck.process       │
                     │    bck.orchestrator  │
                     │    (status updates)  │
                     └──────────────────────┘
```

### Core Components

| Component | Role |
|---|---|
| `bck.process` | The queue table. Holds parent batch rows and individual child process rows |
| `bck.orchestrator` | Registry of orchestrator sessions: tracks heartbeat, metrics, and status |
| `bck.config` | Dynamic configuration — changes apply on the next cycle without restart |
| `bck.query_catalog` | Optional reusable query library with per-query parameter overrides |

### Process Lifecycle

```
PENDING ──► (orchestrator claims it) ──► RUNNING ──► DONE
                                              │
                                              └──► FAILED (error)
                                                       │
                                              attempts < max? ──► RETRYING ──► PENDING (after delay)
                                                       │
                                              attempts >= max? ──► FAILED (permanent)

PENDING / RETRYING ──► (admin cancels) ──► CANCELLED
```

---

## Workflow

### Step 1 — Register a Batch

The client calls `fn_register_batch()` with a JSON array of queries. The function creates:
- **One parent row** — represents the batch as a whole, used for tracking and grouping.
- **N child rows** — one per query, these are the actual units of work.

Each child row snapshots its effective parameters at registration time (merged from per-query overrides and `bck.config` defaults), so configuration changes later won't affect already-registered processes.

### Step 2 — Start the Orchestrator

`fn_start_orchestrator()` does the following and **returns in milliseconds**:

1. Marks any crashed orchestrators (stale heartbeat) as `CRASHED`
2. Checks the singleton limit (`max_orch_instances`)
3. Creates a new row in `bck.orchestrator`
4. Calls `pg_background_launch()` to start the loop in a background session
5. Returns `orch_id`, `bg_pid`, and a confirmation message

The client connection is no longer needed after this point.

### Step 3 — Orchestrator Loop (background, autonomous)

Every cycle the orchestrator:

1. Updates its heartbeat timestamp (crash detection)
2. Checks for a stop request
3. Calls `fn_fix_stuck()` to recover zombie processes whose PID vanished
4. Checks if any work remains — **exits automatically when the queue is empty**
5. Evaluates server capacity (connection load + active worker count)
6. Claims available slots with `FOR UPDATE SKIP LOCKED` (race-condition safe)
7. Launches a `fn_worker()` per slot via `pg_background_launch()`

### Step 4 — Worker Execution (background, per-process)

Each worker:
1. Locks its row with `FOR UPDATE SKIP LOCKED`
2. Marks status as `RUNNING`, records its PID and start time
3. Executes the SQL with `EXECUTE`
4. On success: marks `DONE`
5. On failure: checks attempt count → schedules `RETRYING` or marks `FAILED`
6. **Performs self-heal check** (see below)

### Step 5 — Monitor Anytime

The client reconnects at any point and queries the monitoring views. No waiting, no blocking.

---

## Object Reference

### Tables

| Table | Description |
|---|---|
| `bck.config` | Dynamic engine configuration. All keys editable at runtime |
| `bck.query_catalog` | Reusable SQL library with optional per-query param overrides |
| `bck.process` | Core queue: parent batch rows + child process rows |
| `bck.orchestrator` | Orchestrator session registry (heartbeat, metrics, status) |

### Public Functions

| Function | Description |
|---|---|
| `bck.fn_register_batch(p_queries JSONB, p_name TEXT)` | Register a JSON array of queries as a batch. Returns `id_parent` |
| `bck.fn_start_orchestrator(p_id_parent BIGINT)` | Fire & Forget: starts the orchestrator. Returns immediately |
| `bck.fn_stop_orchestrator(p_orch_id BIGINT)` | Request graceful shutdown. Workers in flight finish normally |
| `bck.fn_cancel_process(p_id BIGINT, p_reason TEXT)` | Cancel a single PENDING/RETRYING process |
| `bck.fn_cancel_batch(p_id_parent BIGINT, p_reason TEXT)` | Cancel all pending processes in a batch |
| `bck.fn_retry_failed(p_id BIGINT)` | Re-queue a permanently FAILED process from scratch |
| `bck.fn_cleanup_history(p_days INT)` | Purge DONE/CANCELLED rows older than N days |

### Monitoring Views

| View | Description |
|---|---|
| `bck.vw_orchestrator` | Orchestrator health, heartbeat age, cycles, and worker count |
| `bck.vw_batch_summary` | Per-batch progress bar, status counts, and error rate |
| `bck.vw_process_monitor` | All child processes with timing, retry detail, and timeout countdown |
| `bck.vw_running_workers` | Only currently executing workers with timeout alerts |
| `bck.vw_failed_processes` | All failed processes with full error history for diagnosis |

---

## Usage Examples

### Basic Batch

```sql
SELECT * FROM bck.fn_register_batch(
    p_queries => '[
        {"query_sql": "UPDATE users SET last_sync = NOW() WHERE active = true",
         "name": "Sync active users"},
        {"query_sql": "CALL rebuild_search_index()",
         "name": "Rebuild search index"},
        {"query_sql": "DELETE FROM session_log WHERE created_at < NOW() - INTERVAL ''30 days''",
         "name": "Purge old sessions"}
    ]'::JSONB,
    p_name => 'Daily maintenance'
);
-- Returns: id_parent = 1, total_queued = 3
```

### Batch with Per-Query Parameter Overrides

Different queries can have different timeouts, retry counts, and delays:

```sql
SELECT * FROM bck.fn_register_batch(
    p_queries => '[
        {
            "query_sql": "CALL generate_annual_report(2024)",
            "name": "Annual report — needs 1 hour",
            "params": {
                "default_timeout_sec": 3600,
                "max_failed_attempts": 1
            }
        },
        {
            "query_sql": "UPDATE cache SET dirty = true",
            "name": "Invalidate cache — fast, retry quickly",
            "params": {
                "retry_delay_sec": 5,
                "max_failed_attempts": 5
            }
        }
    ]'::JSONB,
    p_name => 'End of year processing'
);
```

### Start Orchestrator (Fire & Forget)

```sql
-- Auto-detect oldest pending batch
SELECT * FROM bck.fn_start_orchestrator();

-- Target a specific batch
SELECT * FROM bck.fn_start_orchestrator(p_id_parent => 42);

-- Returns immediately:
-- orch_id | bg_pid |  status  | message
--       1 |  18432 | STARTED  | Orchestrator started in background. ID: 1 | PID: 18432...
```

### Using the Query Catalog

Store reusable queries with preset parameters:

```sql
-- Store frequently used queries
INSERT INTO bck.query_catalog (query_sql, description, params) VALUES
    ('CALL vacuum_all_tables()',     'Full vacuum pass',         '{"default_timeout_sec": 7200}'),
    ('CALL reindex_all()',           'Full reindex',             '{"default_timeout_sec": 3600, "max_failed_attempts": 1}'),
    ('UPDATE stats SET stale = true','Invalidate stats cache',   '{}');

-- Create a batch from catalog entries
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
        WHERE id IN (1, 2, 3)
    ),
    p_name => 'Weekly maintenance from catalog'
);
```

### Integration with pg_cron (Recommended for Scheduled Batches)

```sql
-- Schedule nightly batch registration + execution at 2 AM
SELECT cron.schedule(
    'nightly-maintenance',
    '0 2 * * *',
    $$
    DO $$
    DECLARE v_id BIGINT;
    BEGIN
        SELECT id_parent INTO v_id
        FROM bck.fn_register_batch(
            p_queries => '[
                {"query_sql": "CALL nightly_cleanup()", "name": "Cleanup"},
                {"query_sql": "CALL update_materialized_views()", "name": "Refresh MVs"}
            ]'::JSONB,
            p_name => 'Nightly — ' || TO_CHAR(NOW(), 'YYYY-MM-DD')
        );
        PERFORM bck.fn_start_orchestrator(v_id);
    END;
    $$
    $$
);
```

---

## Monitoring

All views are non-blocking and can be queried at any time from any session.

### Is the Orchestrator Alive?

```sql
SELECT id, pid, status, health, secs_since_heartbeat,
       total_cycles, total_launched, uptime
FROM bck.vw_orchestrator;

-- health column values:
-- 🟢 HEALTHY        — heartbeat is recent
-- 🟡 SLOW HEARTBEAT — more than 10s since last heartbeat
-- 🔴 UNRESPONSIVE   — exceeded heartbeat_crash_sec threshold
-- ⚪ INACTIVE       — stopped or crashed
```

### Batch Progress

```sql
SELECT id_parent, batch_name, batch_status,
       total, done, running, pending, failed,
       pct_done, progress_bar, error_rate_pct
FROM bck.vw_batch_summary;

-- Example output:
-- id_parent | batch_name         | batch_status | total | done | running | pct_done | progress_bar
--         1 | Daily maintenance  | 🔥 RUNNING   |  100  |  73  |    5    |   73%    | [███████████████·····]
```

### Workers Running Right Now

```sql
SELECT id, name, bg_pid, running_for, timeout_alert, query_preview
FROM bck.vw_running_workers;

-- timeout_alert values:
-- 🟢 NORMAL           — less than 50% of timeout used
-- 🟡 HALFWAY          — 50-80% of timeout used
-- 🔴 TIMEOUT IMMINENT — over 80% of timeout used
```

### Full Process Detail for a Specific Batch

```sql
SELECT id, name, status, elapsed, attempt_display, time_to_timeout, error_msg
FROM bck.vw_process_monitor
WHERE id_parent = 1;
```

### Failed Processes — Diagnose Errors

```sql
SELECT id, name, batch_name, attempts, error_msg, time_since_failure
FROM bck.vw_failed_processes;
```

---

## Administration

### Cancel a Single Process

```sql
-- Only works on PENDING or RETRYING processes
SELECT * FROM bck.fn_cancel_process(
    p_id     => 15,
    p_reason => 'No longer needed'
);
```

### Cancel an Entire Batch

```sql
-- Cancels all PENDING/RETRYING. Running processes finish normally.
SELECT * FROM bck.fn_cancel_batch(
    p_id_parent => 1,
    p_reason    => 'Batch aborted by admin'
);
```

### Re-Queue a Failed Process

```sql
-- Resets attempt counter and puts it back to PENDING
SELECT * FROM bck.fn_retry_failed(p_id => 17);
-- Then start (or let a running orchestrator pick it up):
SELECT * FROM bck.fn_start_orchestrator();
```

### Stop the Orchestrator Gracefully

```sql
-- Stop all active instances (finishes current cycle, workers complete normally)
SELECT * FROM bck.fn_stop_orchestrator();

-- Stop a specific instance
SELECT * FROM bck.fn_stop_orchestrator(p_orch_id => 1);
```

### Clean Up Old History

```sql
-- Delete DONE/CANCELLED rows older than 30 days
SELECT * FROM bck.fn_cleanup_history(p_days => 30);
```

---

## Configuration

All parameters are stored in `bck.config` and apply on the **next orchestrator cycle** without any restart:

```sql
-- View all current settings
SELECT * FROM bck.config ORDER BY key;
```

| Key | Default | Description |
|---|---|---|
| `max_workers` | `5` | Maximum concurrent background workers |
| `cycle_interval_ms` | `2000` | Milliseconds between orchestrator cycles. `0` = no sleep |
| `max_orch_instances` | `1` | Max simultaneous orchestrator instances (singleton = 1) |
| `default_timeout_sec` | `600` | Default query timeout in seconds. `0` = disabled |
| `max_failed_attempts` | `3` | Max failed attempts before marking as permanently FAILED |
| `retry_delay_sec` | `60` | Seconds to wait before retrying a failed process |
| `heartbeat_crash_sec` | `30` | Seconds without heartbeat before declaring orchestrator crashed |

### Change Configuration at Runtime

```sql
-- Allow more parallel workers
UPDATE bck.config SET value = '10', update_time = clock_timestamp()
WHERE key = 'max_workers';

-- Faster cycles (higher CPU usage)
UPDATE bck.config SET value = '500', update_time = clock_timestamp()
WHERE key = 'cycle_interval_ms';

-- More conservative retry delay
UPDATE bck.config SET value = '300', update_time = clock_timestamp()
WHERE key = 'retry_delay_sec';
```

> **Note:** Per-query `params` in `fn_register_batch()` take precedence over these global config values for individual processes.

---

## Self-Heal System

One of BCK Engine's most important features is the **self-healing orchestrator**.

### The Problem

Imagine you queue 1,000 processes and the orchestrator crashes halfway through (OOM, server restart, fatal error). Without self-heal, the remaining 500 processes would sit in `PENDING` forever — waiting for a human to manually restart the orchestrator.

### The Solution

After every worker finishes its execution (success or failure), it performs this check:

```
Worker finishes
      │
      ▼
Is there an ACTIVE orchestrator with a recent heartbeat?
      │
      ├─── YES → Close normally. Orchestrator is handling the rest.
      │
      └─── NO  → Are there still PENDING/RETRYING processes in this batch?
                      │
                      ├─── NO  → Nothing to do. Close normally.
                      │
                      └─── YES → Launch a new orchestrator via pg_background_launch()
                                 The queue continues automatically.
```

This means **as long as at least one worker finishes**, the queue will never stall. The `launch_count` column in `bck.orchestrator` tracks how many times self-heal was triggered.

---

## Anti-Saturation Strategy

Before launching any worker, the orchestrator evaluates two conditions:

### Condition 1 — Worker Limit

```
running_workers < max_workers
```

The count of processes currently in `RUNNING` status must be below the configured maximum.

### Condition 2 — Connection Availability

```
free_connections >= workers_to_launch + 2
```

There must be enough free PostgreSQL connections to accommodate the new workers, plus a safety margin of 2 (reserved for the orchestrator itself and emergencies).

If either condition fails, the orchestrator skips launching in that cycle, sleeps for `cycle_interval_ms`, and re-evaluates on the next cycle.

**This means the engine is self-throttling** — it never overwhelms the server regardless of how many processes are queued.

---

## Retry Mechanism

### Retry Delay via Insert Time Shifting

When a process fails and has remaining attempts, BCK Engine does something elegant: instead of adding a `scheduled_at` column, it shifts the process's `insert_time` forward by `retry_delay_sec` seconds.

Since the orchestrator picks processes ordered by `insert_time ASC`, this naturally pushes retries **to the end of the current queue**:

```
Queue at time T:
  Process A — insert_time: 10:00:01  → PENDING  (fresh)
  Process B — insert_time: 10:00:02  → PENDING  (fresh)
  Process C — insert_time: 10:05:00  → RETRYING (failed, retrying in 5 min)

Orchestrator picks A and B first.
C becomes eligible at 10:05:00, after fresh work is done.
```

This ensures retries never starve fresh work.

---

## Comparison with Alternatives

There are several tools that solve background processing in PostgreSQL. Here is how BCK Engine compares:

### pg-boss (Node.js)
pg-boss is a job queue built in Node.js on top of PostgreSQL for background processing and reliable asynchronous execution. It is an excellent tool — but it requires a **Node.js runtime** running alongside your database. BCK Engine requires nothing outside of PostgreSQL itself.

### PGQueuer (Python)
PGQueuer turns your PostgreSQL database into a fast, reliable background job processor with no separate message broker required. Again, it requires a **Python application layer** to run consumers. BCK Engine has no external runtime dependency.

### Graphile Worker (Node.js)
Graphile Worker is a job queue for PostgreSQL running on Node.js that allows running jobs in the background so HTTP response/application code is not held up. Excellent for Node.js teams, but again requires an external process.

### pg_cron
pg_cron is a cron-based job scheduler for PostgreSQL that runs inside the database as an extension and allows the execution of database tasks directly from the database. It is great for scheduled recurring tasks, but is not designed to execute dynamic batches of arbitrary queries with parallelism, retries, and capacity management. BCK Engine complements pg_cron — pg_cron triggers the batch, BCK Engine executes it.

### Raw pg_background
pg_background enables PostgreSQL to execute SQL commands asynchronously in dedicated background worker processes. It is not a full-blown scheduler. It is not a queueing platform. It is a sharp tool: "run this SQL over there, and let me decide how to interact with it." BCK Engine is the complete queueing and orchestration layer built on top of `pg_background`.

### Summary Table

| Feature | BCK Engine | pg-boss | PGQueuer | Graphile Worker | pg_cron |
|---|---|---|---|---|---|
| Lives entirely in PostgreSQL | ✅ | ❌ | ❌ | ❌ | ✅ |
| No external runtime needed | ✅ | ❌ (Node) | ❌ (Python) | ❌ (Node) | ✅ |
| Parallel execution | ✅ | ✅ | ✅ | ✅ | ⚠️ Limited |
| Self-closing orchestrator | ✅ | N/A | N/A | N/A | N/A |
| Self-heal on crash | ✅ | ❌ | ❌ | ❌ | N/A |
| Per-process param overrides | ✅ | ✅ | ✅ | ✅ | ❌ |
| Anti-saturation control | ✅ | ❌ | ❌ | ❌ | ❌ |
| Dynamic config without restart | ✅ | ❌ | ❌ | ❌ | ❌ |
| Fire & Forget (close session) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Built-in monitoring views | ✅ | ❌ | ✅ | ❌ | ❌ |

---

## Key Advantages

### 1. Zero External Dependencies
Everything lives inside a single PostgreSQL schema. No Node.js, no Python, no Redis, no message brokers. If your database is running, BCK Engine is running.

### 2. True Fire & Forget
`fn_start_orchestrator()` returns in milliseconds. Your application sends the batch, starts the orchestrator, and disconnects. No connection held open, no polling required.

### 3. Self-Closing Orchestrator
The orchestrator process only exists while there is work to do. When the queue empties, it exits cleanly — consuming no connections or CPU at idle. This is fundamentally different from tools that run a permanent daemon.

### 4. Crash-Resilient Self-Heal
If the orchestrator dies unexpectedly, the next worker to finish will detect it and launch a replacement automatically. No human intervention required for a queue of any size.

### 5. Anti-Saturation by Design
The capacity check happens before every dispatch cycle. The engine always validates that free connections are available before launching workers, preventing it from ever contributing to a database overload.

### 6. Dynamic Configuration
All engine parameters can be changed in `bck.config` while the orchestrator is running. No restart needed. Changes take effect on the next cycle.

### 7. Per-Process Parameter Overrides
Each query in a batch can override global settings (timeout, retry count, retry delay). A long-running report can have a 2-hour timeout while a cache invalidation in the same batch uses a 5-second timeout.

### 8. ACID-Compliant State
Every status update, heartbeat, and log entry is a regular PostgreSQL transaction. Queue state is always consistent — it benefits from the same ACID guarantees as your application data.

### 9. Atomic Job Claiming
`FOR UPDATE SKIP LOCKED` ensures that even if multiple orchestrators run simultaneously (allowed by configuration), no process is ever executed twice.

---

## Requirements & Installation

### Requirements

- PostgreSQL 14 or higher
- `pg_background` extension installed
- `uuid-ossp` extension (included with PostgreSQL)

### Install pg_background

```bash
git clone https://github.com/vibhorkum/pg_background.git
cd pg_background
make
sudo make install
```

### Install BCK Engine

```bash
# Connect to your database and run the single install file:
psql -U postgres -d your_database -f bck_engine_v2.sql
```

Or file by file in order:

```bash
psql -U postgres -d your_database -f v2_01_ddl.sql
psql -U postgres -d your_database -f v2_02_helpers.sql
psql -U postgres -d your_database -f v2_03_fn_register_batch.sql
psql -U postgres -d your_database -f v2_04_fn_worker.sql
psql -U postgres -d your_database -f v2_05_fn_orchestrator.sql
psql -U postgres -d your_database -f v2_06_admin_and_views.sql
```

### Verify Installation

```sql
-- Check all objects were created
SELECT routine_name
FROM information_schema.routines
WHERE routine_schema = 'bck'
ORDER BY routine_name;

-- Check default config loaded
SELECT * FROM bck.config;

-- Quick smoke test
SELECT * FROM bck.fn_register_batch(
    '[{"query_sql": "SELECT pg_sleep(1)", "name": "Test process"}]'::JSONB,
    'Smoke test'
);
SELECT * FROM bck.fn_start_orchestrator();
SELECT * FROM bck.vw_batch_summary;
```

---

## License

MIT License. See `LICENSE` for details.

---

> Built on top of [`pg_background`](https://github.com/vibhorkum/pg_background) by Vibhor Kumar.
