

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
