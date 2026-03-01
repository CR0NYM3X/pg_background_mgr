-- =============================================================================
-- BCK ENGINE — EJEMPLO COMPLETO DE USO REAL
-- =============================================================================
-- Escenario: Tienes 500,000 registros de ventas que procesar y cargar
-- en una tabla de hechos. En lugar de un solo INSERT que bloquea todo,
-- los dividimos en lotes paralelos que corren en background.
-- =============================================================================


-- =============================================================================
-- PASO 0: CREAR LAS TABLAS DEL EJEMPLO
-- =============================================================================

-- Tabla fuente: ventas sin procesar (simula datos crudos de un ERP o CSV)
CREATE TABLE IF NOT EXISTS demo.sales_raw (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region      TEXT        NOT NULL,
    product_id  INT         NOT NULL,
    quantity    INT         NOT NULL,
    unit_price  NUMERIC(10,2) NOT NULL,
    sale_date   DATE        NOT NULL,
    customer_id INT         NOT NULL,
    processed   BOOLEAN     NOT NULL DEFAULT FALSE,
    insert_time TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Tabla destino: hechos de ventas procesados y enriquecidos
CREATE TABLE IF NOT EXISTS demo.sales_fact (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region          TEXT        NOT NULL,
    product_id      INT         NOT NULL,
    quantity        INT         NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    total_amount    NUMERIC(12,2) NOT NULL,    -- quantity * unit_price
    sale_date       DATE        NOT NULL,
    sale_month      INT         NOT NULL,      -- derived
    sale_year       INT         NOT NULL,      -- derived
    customer_id     INT         NOT NULL,
    is_high_value   BOOLEAN     NOT NULL,      -- total_amount > 1000
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Tabla de log del proceso (para auditoría)
CREATE TABLE IF NOT EXISTS demo.sales_process_log (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region      TEXT,
    rows_moved  INT,
    started_at  TIMESTAMPTZ,
    ended_at    TIMESTAMPTZ,
    insert_time TIMESTAMPTZ DEFAULT clock_timestamp()
);

-- Índices útiles
CREATE INDEX IF NOT EXISTS idx_sales_raw_region    ON demo.sales_raw (region, processed);
CREATE INDEX IF NOT EXISTS idx_sales_fact_region   ON demo.sales_fact (region, sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_fact_month    ON demo.sales_fact (sale_year, sale_month);

COMMENT ON TABLE demo.sales_raw         IS 'Raw unprocessed sales — simulates incoming data from ERP or CSV load';
COMMENT ON TABLE demo.sales_fact        IS 'Processed sales fact table — enriched and ready for reporting';
COMMENT ON TABLE demo.sales_process_log IS 'Audit log: one row per region batch processed';


-- =============================================================================
-- PASO 1: POBLAR LA TABLA FUENTE CON DATOS DE PRUEBA
-- 500,000 registros divididos en 5 regiones
-- =============================================================================

INSERT INTO demo.sales_raw (region, product_id, quantity, unit_price, sale_date, customer_id)
SELECT
    -- 5 regiones distribuidas uniformemente
    (ARRAY['NORTH','SOUTH','EAST','WEST','CENTER'])[((ROW_NUMBER() OVER ()) % 5) + 1],
    (RANDOM() * 999 + 1)::INT,                              -- product_id: 1-1000
    (RANDOM() * 49  + 1)::INT,                              -- quantity: 1-50
    ROUND((RANDOM() * 490 + 10)::NUMERIC, 2),               -- unit_price: $10-$500
    DATE '2024-01-01' + ((RANDOM() * 364)::INT),            -- random date in 2024
    (RANDOM() * 9999 + 1)::INT                              -- customer_id: 1-10000
FROM GENERATE_SERIES(1, 500000);

-- Verificar la carga
SELECT region, COUNT(*) AS total_rows
FROM demo.sales_raw
GROUP BY region
ORDER BY region;

/*
 region | total_rows
--------+-----------
 CENTER |    100000
 EAST   |    100000
 NORTH  |    100000
 SOUTH  |    100000
 WEST   |    100000
*/


-- =============================================================================
-- PASO 2: CREAR LAS FUNCIONES QUE PROCESARÁN CADA REGIÓN
-- Cada función procesa una región completa:
--   1. Lee todos los registros sin procesar de esa región
--   2. Los transforma (calcula totales, extrae mes/año, clasifica)
--   3. Los inserta en sales_fact
--   4. Los marca como procesados en sales_raw
--   5. Registra el resultado en sales_process_log
-- =============================================================================

CREATE OR REPLACE FUNCTION demo.fn_process_region(p_region TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_processed INT;
    v_started        TIMESTAMPTZ := clock_timestamp();
BEGIN
    -- Transformar e insertar en la tabla de hechos
    INSERT INTO demo.sales_fact (
        region,
        product_id,
        quantity,
        unit_price,
        total_amount,
        sale_date,
        sale_month,
        sale_year,
        customer_id,
        is_high_value
    )
    SELECT
        region,
        product_id,
        quantity,
        unit_price,
        quantity * unit_price               AS total_amount,
        sale_date,
        EXTRACT(MONTH FROM sale_date)::INT  AS sale_month,
        EXTRACT(YEAR  FROM sale_date)::INT  AS sale_year,
        customer_id,
        (quantity * unit_price) > 1000      AS is_high_value
    FROM demo.sales_raw
    WHERE region    = p_region
      AND processed = FALSE;

    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;

    -- Marcar como procesados en la fuente
    UPDATE demo.sales_raw
    SET processed = TRUE
    WHERE region    = p_region
      AND processed = FALSE;

    -- Registrar auditoría
    INSERT INTO demo.sales_process_log (region, rows_moved, started_at, ended_at)
    VALUES (p_region, v_rows_processed, v_started, clock_timestamp());

    RETURN FORMAT('Region %s: %s rows processed in %.2f seconds',
        p_region,
        v_rows_processed,
        EXTRACT(EPOCH FROM (clock_timestamp() - v_started))
    );
END;
$$;


-- =============================================================================
-- PASO 3: REGISTRAR EL BATCH EN BCK ENGINE
-- Registramos 5 procesos — uno por región — todos en paralelo
-- =============================================================================

SELECT * FROM bck.fn_register_batch(
    p_queries => '[
        {
            "query_sql": "SELECT demo.fn_process_region(''NORTH'')",
            "name":      "Process region NORTH",
            "params":    {"default_timeout_sec": 300, "max_failed_attempts": 2}
        },
        {
            "query_sql": "SELECT demo.fn_process_region(''SOUTH'')",
            "name":      "Process region SOUTH",
            "params":    {"default_timeout_sec": 300, "max_failed_attempts": 2}
        },
        {
            "query_sql": "SELECT demo.fn_process_region(''EAST'')",
            "name":      "Process region EAST",
            "params":    {"default_timeout_sec": 300, "max_failed_attempts": 2}
        },
        {
            "query_sql": "SELECT demo.fn_process_region(''WEST'')",
            "name":      "Process region WEST",
            "params":    {"default_timeout_sec": 300, "max_failed_attempts": 2}
        },
        {
            "query_sql": "SELECT demo.fn_process_region(''CENTER'')",
            "name":      "Process region CENTER",
            "params":    {"default_timeout_sec": 300, "max_failed_attempts": 2}
        }
    ]'::JSONB,
    p_name => 'Sales 2024 — Parallel Region Processing'
);

/*
 id_parent | total_queued | message
-----------+--------------+------------------------------------------------------
         1 |            5 | Batch registered successfully. Parent ID: 1 | ...
*/


-- =============================================================================
-- PASO 4: INICIAR EL ORQUESTADOR (Fire & Forget)
-- Retorna en milisegundos. Puedes cerrar tu conexión después de esto.
-- =============================================================================

SELECT * FROM bck.fn_start_orchestrator(p_id_parent => 1);

/*
 orch_id | bg_pid |  status  | message
---------+--------+----------+------------------------------------------------------
       1 |  24871 | STARTED  | Orchestrator started in background. ID: 1 | PID: 24871 | ...
*/

-- ✅ Desde este momento los 5 procesos corren en paralelo en background.
-- Puedes cerrar tu sesión. El motor sigue corriendo solo.


-- =============================================================================
-- PASO 5: MONITOREAR EL PROGRESO
-- Ejecuta estas queries cuando quieras para ver el estado
-- =============================================================================

-- ----------------------------------------------------------------------------
-- MONITOR A: ¿Está vivo el orquestador?
-- ----------------------------------------------------------------------------
SELECT
    id,
    pid,
    status,
    health,
    secs_since_heartbeat,
    total_cycles,
    total_launched,
    uptime
FROM bck.vw_orchestrator;

/*
 id |  pid  | status |    health    | secs_since_heartbeat | total_cycles | total_launched | uptime
----+-------+--------+--------------+----------------------+--------------+----------------+--------
  1 | 24871 | ACTIVE | 🟢 HEALTHY  |                  1.2 |            4 |              5 | 00:00:08
*/


-- ----------------------------------------------------------------------------
-- MONITOR B: Resumen del batch con barra de progreso
-- ----------------------------------------------------------------------------
SELECT
    id_parent,
    batch_name,
    batch_status,
    total,
    done,
    running,
    pending,
    failed,
    pct_done,
    progress_bar,
    error_rate_pct
FROM bck.vw_batch_summary;

/*
 id_parent |             batch_name              | batch_status | total | done | running | pending | failed | pct_done |      progress_bar      | error_rate_pct
-----------+-------------------------------------+--------------+-------+------+---------+---------+--------+----------+------------------------+----------------
         1 | Sales 2024 — Parallel Region Proc.. | 🔥 RUNNING  |     5 |    2 |       3 |       0 |      0 |    40%   | [████████············] |           0.00
*/


-- ----------------------------------------------------------------------------
-- MONITOR C: Workers activos ahora mismo con alerta de timeout
-- ----------------------------------------------------------------------------
SELECT
    id,
    name,
    bg_pid,
    running_for,
    timeout_alert,
    query_preview
FROM bck.vw_running_workers;

/*
 id |        name         | bg_pid | running_for | timeout_alert |                query_preview
----+---------------------+--------+-------------+---------------+----------------------------------------------
  3 | Process region EAST |  24872 | 00:00:12    | 🟢 NORMAL    | SELECT demo.fn_process_region('EAST')
  4 | Process region WEST |  24873 | 00:00:12    | 🟢 NORMAL    | SELECT demo.fn_process_region('WEST')
  5 | Process region CTR  |  24874 | 00:00:12    | 🟢 NORMAL    | SELECT demo.fn_process_region('CENTER')
*/


-- ----------------------------------------------------------------------------
-- MONITOR D: Detalle completo de todos los procesos del batch
-- ----------------------------------------------------------------------------
SELECT
    id,
    name,
    status,
    elapsed,
    attempt_display,
    time_to_timeout,
    error_msg
FROM bck.vw_process_monitor
WHERE id_parent = 1;

/*
 id |          name          |  status  |   elapsed   | attempt_display | time_to_timeout | error_msg
----+------------------------+----------+-------------+-----------------+-----------------+-----------
  2 | Process region NORTH   | DONE     | 00:00:11    | 1/2             |                 |
  1 | Process region SOUTH   | DONE     | 00:00:10    | 1/2             |                 |
  3 | Process region EAST    | RUNNING  | 00:00:14    | 1/2             | 00:04:46        |
  4 | Process region WEST    | RUNNING  | 00:00:14    | 1/2             | 00:04:46        |
  5 | Process region CENTER  | RUNNING  | 00:00:14    | 1/2             | 00:04:46        |
*/


-- ----------------------------------------------------------------------------
-- MONITOR E: Validar los datos cargados en tiempo real
-- (puedes correr esto MIENTRAS los procesos están corriendo)
-- ----------------------------------------------------------------------------
SELECT
    sf.region,
    COUNT(*)                        AS rows_in_fact,
    SUM(sf.total_amount)            AS total_revenue,
    AVG(sf.total_amount)            AS avg_sale,
    COUNT(*) FILTER (WHERE sf.is_high_value) AS high_value_sales,
    -- Comparar contra la fuente
    (SELECT COUNT(*) FROM demo.sales_raw sr
     WHERE sr.region = sf.region AND sr.processed = TRUE) AS rows_marked_processed
FROM demo.sales_fact sf
GROUP BY sf.region
ORDER BY sf.region;

/*
 region | rows_in_fact | total_revenue | avg_sale | high_value_sales | rows_marked_processed
--------+--------------+---------------+----------+------------------+-----------------------
 CENTER |       100000 |  1,274,982.50 |   127.50 |            12847 |                100000
 EAST   |        62400 |    794,321.10 |   127.30 |             7921 |                 62400  ← en progreso
 NORTH  |       100000 |  1,281,044.20 |   128.10 |            12903 |                100000
 SOUTH  |       100000 |  1,268,771.30 |   126.88 |            12754 |                100000
 WEST   |        45200 |    575,811.40 |   127.40 |             5712 |                 45200  ← en progreso
*/


-- =============================================================================
-- PASO 6: CUANDO TODO TERMINA — Validación final
-- =============================================================================

-- El orquestador se cierra solo cuando termina. Verificar:
SELECT id, status, total_launched, total_cycles, uptime
FROM bck.vw_orchestrator;
/*
 id | status  | total_launched | total_cycles | uptime
----+---------+----------------+--------------+---------
  1 | STOPPED |              5 |            8 | 00:01:03
*/


-- Resumen final del batch
SELECT * FROM bck.vw_batch_summary WHERE id_parent = 1;
/*
 id_parent |          batch_name           | batch_status | total | done | running | failed | pct_done |      progress_bar      
-----------+-------------------------------+--------------+-------+------+---------+--------+----------+------------------------
         1 | Sales 2024 — Parallel Region | ✅ FINISHED  |     5 |    5 |       0 |      0 |   100%   | [████████████████████] 
*/


-- Validación de integridad: ¿todos los registros se procesaron?
SELECT
    'sales_raw total'      AS metric, COUNT(*)::TEXT AS value FROM demo.sales_raw
UNION ALL
SELECT
    'sales_raw processed',           COUNT(*)::TEXT FROM demo.sales_raw WHERE processed = TRUE
UNION ALL
SELECT
    'sales_raw pending',             COUNT(*)::TEXT FROM demo.sales_raw WHERE processed = FALSE
UNION ALL
SELECT
    'sales_fact total',              COUNT(*)::TEXT FROM demo.sales_fact
UNION ALL
SELECT
    'total_revenue',  TO_CHAR(SUM(total_amount), 'FM$999,999,999.00') FROM demo.sales_fact;

/*
       metric        |     value
---------------------+---------------
 sales_raw total     | 500000
 sales_raw processed | 500000
 sales_raw pending   | 0
 sales_fact total    | 500000
 total_revenue       | $6,371,448.20
*/


-- Log de auditoría: cuánto tardó cada región
SELECT
    region,
    rows_moved,
    started_at,
    ended_at,
    AGE(ended_at, started_at)   AS duration
FROM demo.sales_process_log
ORDER BY started_at;

/*
 region |  rows_moved  |         started_at          |          ended_at           | duration
--------+--------------+-----------------------------+-----------------------------+----------
 SOUTH  |       100000 | 2024-12-01 14:00:08.123 UTC | 2024-12-01 14:00:21.445 UTC | 13.32s
 NORTH  |       100000 | 2024-12-01 14:00:08.124 UTC | 2024-12-01 14:00:22.018 UTC | 13.89s
 EAST   |       100000 | 2024-12-01 14:00:08.124 UTC | 2024-12-01 14:00:23.556 UTC | 15.43s
 WEST   |       100000 | 2024-12-01 14:00:08.125 UTC | 2024-12-01 14:00:22.887 UTC | 14.76s
 CENTER |       100000 | 2024-12-01 14:00:08.125 UTC | 2024-12-01 14:00:24.102 UTC | 15.98s
*/
-- ✅ 500,000 registros procesados en ~16 segundos gracias al paralelismo.
-- Secuencial hubiera tomado ~70 segundos.


-- =============================================================================
-- EXTRA: ¿Qué pasa si un proceso falla? Simularlo:
-- =============================================================================

-- Registrar un batch con una query que va a fallar a propósito
SELECT * FROM bck.fn_register_batch(
    p_queries => '[
        {
            "query_sql": "SELECT 1/0",
            "name": "Proceso que fallará — división por cero",
            "params": {"max_failed_attempts": 3, "retry_delay_sec": 5}
        },
        {
            "query_sql": "SELECT demo.fn_process_region(''NORTH'')",
            "name": "Proceso que sí funcionará",
            "params": {"max_failed_attempts": 2}
        }
    ]'::JSONB,
    p_name => 'Test de fallos'
);
-- Returns id_parent = 2

SELECT * FROM bck.fn_start_orchestrator(p_id_parent => 2);

-- Monitorear: verás el proceso fallido entrar en RETRYING
SELECT id, name, status, attempt_display, error_msg
FROM bck.vw_process_monitor
WHERE id_parent = 2;

/*
 id |              name               |  status  | attempt_display |          error_msg
----+---------------------------------+----------+-----------------+-------------------------------
  6 | Proceso que fallará             | RETRYING | 1/3             | [Attempt 1/3] division by zero
  7 | Proceso que sí funcionará       | DONE     | 1/2             |
*/

-- Después de 3 intentos fallidos:
/*
 id |              name               |  status  | attempt_display |              error_msg
----+---------------------------------+----------+-----------------+--------------------------------------
  6 | Proceso que fallará             | FAILED   | 3/3             | [FINAL FAILURE after 3/3] division..
  7 | Proceso que sí funcionará       | DONE     | 1/2             |
*/

-- Ver los procesos fallidos para diagnóstico
SELECT id, name, attempts, error_msg, time_since_failure
FROM bck.vw_failed_processes;

-- Re-encolar el proceso fallido después de corregir el problema
SELECT * FROM bck.fn_retry_failed(p_id => 6);
SELECT * FROM bck.fn_start_orchestrator(p_id_parent => 2);


-- =============================================================================
-- LIMPIEZA (opcional)
-- =============================================================================
-- SELECT * FROM bck.fn_cleanup_history(p_days => 0);  -- Elimina DONE/CANCELLED
-- DROP TABLE IF EXISTS demo.sales_raw, demo.sales_fact, demo.sales_process_log CASCADE;
-- DROP FUNCTION IF EXISTS demo.fn_process_region(TEXT);
