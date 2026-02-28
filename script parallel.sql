pg_background

/*
orquestar el paralelismo mediante `pg_background`, permitiendo el control de ciclos de vida de procesos "workers" que se alimentan de una cola de tareas.

Aquí tienes la propuesta arquitectónica y el diseño de la estructura de datos bajo la **plantilla corporativa de Jorge**.

---

## 1. Resumen Técnico del Diseño

El sistema se basará en un modelo de **Orquestador-Worker**.

* **Inventario:** Controla el "presupuesto" de procesos disponibles para un lote.
* **Procesos:** Es la cola de tareas (queries) vinculadas al inventario.
* **Estado y Reciclaje:** Los procesos pasan por estados controlados para permitir que un worker de `pg_background` tome la siguiente tarea pendiente si el límite de paralelismo real es menor al total de tareas.

### Diagrama de Flujo Lógico


\c postgres
drop database test;
create database test;
\c test 
create extension pg_background;


----------------------- Agregar validacion de PARALLEL
	and execution_mode = 'PARALLEL';

	
*/
 

---------------- ESTRUCTURA CORE ----------------
CREATE SCHEMA IF NOT EXISTS bck;


-- truncate table bck.background_config RESTART IDENTITY ;
CREATE TABLE bck.background_config (
    config_key   text PRIMARY KEY,
    config_value text NOT NULL,
    description  text,
    date_update  timestamptz DEFAULT clock_timestamp()
);

-- Insertamos el valor por defecto solicitado
INSERT INTO bck.background_config (config_key, config_value, description)
VALUES ('max_attempts', '3', 'Número por defecto de reintentos para procesos secuenciales y paralelos.');


-- truncate table bck.background_inventory RESTART IDENTITY ;
CREATE TABLE IF NOT EXISTS bck.background_inventory (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uuid_parent     uuid DEFAULT gen_random_uuid() UNIQUE,
    cnt_total_bck   integer NOT NULL ,
    cnt_used_bck        integer DEFAULT 0,
    force_cnt       boolean DEFAULT false,
    date_insert     timestamptz DEFAULT clock_timestamp()
);

---------------- STRUCTURE ----------------

-- truncate table bck.background_process RESTART IDENTITY ;
---- Se podria tomar un priority  podría lanzar primero las tareas críticas
---- Agregarle altun tipo de statment_timeout por proceso o permitirle agregar parametros extras como lock_timeout , etc, ec
--- Agregar Sistema de Cola Dinámica si quiero ejecutar 100 procesos entonces que siempre se ejecuten solo 20 procesos y en cuando se libere alguno de esos 20 que se vaya ejecutando otro proceos esto solo funciona en modo alatorio. no en paralelo o secuencial


/**
PARALLEL :  Esto siempre se les indica  a los procesos que incie al mismo tiempo  a fuerzas todos los procesos tienen que estar activados en caso de que uno falle se cancela todo

RANDOM :  Este elige a alatoreamente consultas y las ejecuta, obvio siempre primero lanza el proceso, despues valida si se ejecuto el proceso y si se ejecuto entonces le dice que ya se puede ejecutar la intruccion esto por seguridad en casos de que el proceso no se ejecute por x o Y motivo

SEQUENTIAL o Priority : Esto se ejecuta de forma secuencial o prioridad donde el orden importa o la prioridad importa esto permite hacer alguna actividad de forma secuencia . esto los procesos estaran validando siempre el proceso si fallo o se esta ejecutando, y en caso de que falle uno los pendientes se cancelaran 
por ejemplo se tienen que ejcutar 5 proceos , se lanzo los 5 procesos despues se habilita el primero , se espera a que termine y si una vez que se acomplete se ejecutara el segundo , siempre el proceso que termine dara la orden a su proceso siguiente para que inicie, y en caso si falla por ejemplo el rpceso 
3  entonces el proceso 4 y 5 se cancelaran por si solos esto debido a que ya se rompera el orden, este comportamiento se puede

**/


CREATE TABLE IF NOT EXISTS bck.background_process (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uuid_parent     uuid NOT NULL REFERENCES bck.background_inventory(uuid_parent) ON DELETE CASCADE,
    uuid_child     uuid DEFAULT gen_random_uuid() UNIQUE,
    pid             integer DEFAULT 0,
    status          text DEFAULT 'REGISTRADO' 
                    CHECK (status IN ('REGISTRADO', 'LANZADO', 'EJECUTANDO', 'COMPLETADO', 'FALLIDO')),
					-- CHECK (status IN ('REGISTRADO', 'LANZADO', 'EJECUTANDO', 'COMPLETADO', 'FALLIDO')),
    query_exec      text NOT NULL,
    
    -- Control de reintentos (Basado en tu idea original)
    attempts        integer DEFAULT 0,
    failed_attempts integer DEFAULT 0,
    max_attempts    integer DEFAULT 3,
    
    -- Trazabilidad de tiempos y errores (Necesario para run_task)
    start_time      timestamptz,
    end_time        timestamptz,
    error_msg       text,

	process_name text,
	execution_mode text CHECK (execution_mode IN ('PARALLEL', 'SEQUENTIAL', 'RANDOM')) DEFAULT 'PARALLEL',
	
    -- Auditoría
    date_insert     timestamptz DEFAULT clock_timestamp(),
    date_update     timestamptz DEFAULT clock_timestamp()
);


---------------- INDEXES ----------------
-- Indice para que el worker encuentre tareas rápidas sin bloquear toda la tabla
CREATE INDEX IF NOT EXISTS idx_bck_proc_lookup ON bck.background_process (uuid_parent, status, pid) WHERE status = 'REGISTRADO';

CREATE INDEX IF NOT EXISTS idx_bck_proc_uuid ON bck.background_process(uuid_parent);
CREATE INDEX IF NOT EXISTS idx_bck_proc_status ON bck.background_process(status);

 
 


CREATE OR REPLACE VIEW bck.vw_status_progreso AS
WITH cte_real_global AS (
    -- Esta es la verdad del motor: ¿Cuántos workers de fondo hay en total?
    SELECT count(*) as total_activos_motor
    FROM pg_stat_activity 
    WHERE backend_type = 'pg_background'
),
cte_metricas AS (
    SELECT 
        p.uuid_parent,
        count(*) as total_tareas,
        count(*) FILTER (WHERE p.status = 'COMPLETADO') as completados,
        count(*) FILTER (WHERE p.status = 'FALLIDO') as fallidos,
        count(*) FILTER (WHERE p.status = 'REGISTRADO') as en_espera,
        count(*) FILTER (WHERE p.status = 'LANZADO') as lanzado_cola,
        -- Mantenemos el conteo de cuántos de NUESTROS procesos están marcados como EJECUTANDO
        count(*) FILTER (WHERE p.status = 'EJECUTANDO') as ejecucion_en_tabla
    FROM bck.background_process p
    GROUP BY p.uuid_parent
)
SELECT 
    m.uuid_parent,
    CASE 
        WHEN m.total_tareas = (m.completados + m.fallidos) AND m.total_tareas > 0 THEN '✅ FINALIZADO'
        WHEN r.total_activos_motor > 0 THEN '🔥 EJECUTANDO'
        WHEN m.en_espera = m.total_tareas THEN '⏳ PENDIENTE'
        ELSE '⚙️ EN PROCESO'
    END as "Estatus Global",
    m.total_tareas as "Total",
    m.completados as "Hechos",
    m.fallidos as "Errores",
    -- AQUÍ ESTÁ TU CAMBIO: Directo del motor, sin filtros de tabla
    r.total_activos_motor as "Activos Real", 
    m.en_espera as "En Espera",
    CASE 
        WHEN m.total_tareas = 0 THEN '0%'
        ELSE round((m.completados::float / m.total_tareas::float) * 100)::text || '%' 
    END as "Avance",
    '[' || 
    rpad(
        repeat('█', (COALESCE(round((m.completados::float / NULLIF(m.total_tareas,0)::float) * 20),0))::int), 
        20, 
        ' '
    ) || 
    ']' as "Progreso"
FROM cte_metricas m, cte_real_global r; -- Cross join para tener la métrica real en todas las filas


-- Select * from bck.vw_status_progreso ;


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




-- 3. Función 1: Inicialización de Inventario
CREATE OR REPLACE FUNCTION bck.fn_crear_inventario(p_cantidad integer)
RETURNS uuid
LANGUAGE plpgsql
AS $func$
DECLARE
    v_uuid          uuid    := gen_random_uuid();
    v_max_system    integer; -- Capacidad total del servidor
    v_max_safe      integer; -- Capacidad permitida (max - 2)
BEGIN

	/*
    -- 1. Obtener límites dinámicos del servidor
    v_max_system := current_setting('max_worker_processes')::integer;
    v_max_safe   := v_max_system - 2;

    -- Garantizar que v_max_safe no sea negativo en servidores muy pequeños
    IF v_max_safe < 1 THEN v_max_safe := 1; END IF;

    -- 2. Validación de parámetros de entrada
    IF p_cantidad IS NULL OR p_cantidad <= 0 THEN
        RAISE EXCEPTION 'La cantidad de procesos debe ser mayor a 0.';
    ELSIF p_cantidad > v_max_safe THEN
        RAISE EXCEPTION 'Capacidad insuficiente: Solicitado %, Máximo seguro permitido % Ajuste (max_worker_processes: %)', 
            p_cantidad, v_max_safe, v_max_system;
    END IF;
	*/
	
    -- 3. Registro en tabla de control interna
    INSERT INTO bck.background_inventory (
        uuid_parent, 
        cnt_total_bck, 
        cnt_used_bck, -- Ajustado al nombre de columna estándar
        date_insert
    )
    VALUES (
        v_uuid, 
        p_cantidad, 
        0, 
        clock_timestamp()
    );

    RETURN v_uuid;

EXCEPTION 
    WHEN unique_violation THEN
        RAISE EXCEPTION 'Error crítico: Colisión de UUID detectada. Reintente la operación.';
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error al crear inventario: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
END;
$func$;


select * from bck.fn_crear_inventario(10);

-- truncate table bck.background_inventory  RESTART IDENTITY ;
select * from bck.background_inventory where uuid_parent = '75916486-2a9f-41b1-a369-42b339d97d53';

+----+--------------------------------------+---------------+--------------+-----------+-------------------------------+
| id |             uuid_parent              | cnt_total_bck | cnt_used_bck | force_cnt |          date_insert          |
+----+--------------------------------------+---------------+--------------+-----------+-------------------------------+
|  1 | 453d77bc-6e4b-42e0-911f-7a3016470b0b |             4 |            0 | f         | 2026-02-23 18:35:52.353918-07 |
+----+--------------------------------------+---------------+--------------+-----------+-------------------------------+
(1 row)





-- ## 4. Función 2: Registro de Tareas (Queries)


---------------- EXAMPLE USAGE ----------------
CREATE OR REPLACE FUNCTION bck.fn_registrar_proceso(
    p_uuid_parent  uuid,
    p_queries      text[],
    p_process_name text    DEFAULT NULL,
    p_mode         text    DEFAULT 'PARALLEL',
    p_repeat       integer DEFAULT 1
)
RETURNS text[]
LANGUAGE plpgsql
AS $func$
DECLARE
    v_cnt_total     integer;
    v_cnt_used      integer;
    v_query_item    text;
    v_num_queries   integer;
    v_total_inserts integer;
    v_i             integer;
    -- Variables para límites de sistema
    v_max_system    integer;
    v_max_safe      integer;
    v_mode_upper    text;
    -- Variable para configuración dinámica
    v_default_max_attempts integer;
BEGIN
    -- 0. Normalizar modo
    v_mode_upper := upper(p_mode);

    -- 0.1 Leer configuración dinámica (NUEVO)
    -- Si no existe la llave, por seguridad ponemos 3 como respaldo
    SELECT COALESCE(config_value::integer, 3) 
    INTO v_default_max_attempts 
    FROM bck.background_config 
    WHERE config_key = 'max_attempts';

    -- 1. Validación de parámetros iniciales
    IF p_uuid_parent IS NULL OR p_queries IS NULL OR array_length(p_queries, 1) = 0 THEN
        RAISE EXCEPTION 'Parámetros inválidos: El UUID y al menos una Query son obligatorios.';
    END IF;

    IF p_repeat <= 0 THEN
        RAISE EXCEPTION 'El parámetro p_repeat debe ser mayor a 0.';
    END IF;

    -- Cálculo de carga total solicitado
    v_num_queries   := array_length(p_queries, 1);
    v_total_inserts := v_num_queries * p_repeat;

    -- 2. VALIDACIÓN DE LÍMITES SEGÚN MODO
    IF v_mode_upper = 'PARALLEL' THEN
        v_max_system := current_setting('max_worker_processes')::integer;
        v_max_safe   := v_max_system - 2;

        IF v_max_safe < 1 THEN v_max_safe := 1; END IF;

        IF v_total_inserts > v_max_safe THEN
            RAISE EXCEPTION 'Capacidad insuficiente para PARALELO: Solicitado %, Máximo seguro % (Ajuste max_worker_processes: % - 2)', 
                v_total_inserts, v_max_safe, v_max_system;
        END IF;
    END IF;

    -- 3. Bloqueo de inventario para control de capacidad interna
    SELECT cnt_total_bck, cnt_used_bck
      INTO v_cnt_total, v_cnt_used
    FROM bck.background_inventory
    WHERE uuid_parent = p_uuid_parent
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Error: El UUID de inventario % no existe.', p_uuid_parent;
    END IF;

    IF (v_cnt_used + v_total_inserts) > v_cnt_total THEN
        RAISE EXCEPTION 'Capacidad insuficiente en inventario: Disponible %, solicitado % (Queries: % x Repeticiones: %).', 
            (v_cnt_total - v_cnt_used), v_total_inserts, v_num_queries, p_repeat;
    END IF;

    -- 4. Recorrer el array y aplicar repeticiones
    FOREACH v_query_item IN ARRAY p_queries
    LOOP
        IF v_query_item IS NULL OR trim(v_query_item) = '' THEN
            RAISE EXCEPTION 'Error: Se encontró una query vacía o nula en el array.';
        END IF;

        FOR v_i IN 1..p_repeat LOOP
            INSERT INTO bck.background_process (
                uuid_parent, 
                query_exec, 
                status, 
                process_name,
                execution_mode,
                max_attempts, -- USAMOS EL VALOR LEÍDO DE LA TABLA
                date_insert
            )
            VALUES (
                p_uuid_parent, 
                v_query_item, 
                'REGISTRADO', 
                p_process_name,
                v_mode_upper,
                v_default_max_attempts, -- VALOR DINÁMICO
                clock_timestamp()
            );
        END LOOP;
    END LOOP;

    -- 5. Actualización masiva del contador en el padre
    UPDATE bck.background_inventory 
    SET cnt_used_bck = cnt_used_bck + v_total_inserts 
    WHERE uuid_parent = p_uuid_parent;

    RETURN p_queries;

EXCEPTION 
    WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'Violación de integridad: El UUID padre no es válido.';
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Error en bck.fn_registrar_proceso: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
END;
$func$;


--- Llenado colocando varias querys 
SELECT bck.fn_registrar_proceso(
    p_uuid_parent       := '75916486-2a9f-41b1-a369-42b339d97d53', 
    p_queries           := ARRAY['SELECT pg_sleep(1)' , 'SELECT pg_sleep(2)' , 'SELECT pg_sleep(3)' , 'SELECT pg_sleep(4)' ], 
    p_process_name      := 'TEST_REPETITIVO', 
    p_mode              := 'PARALLEL', 
    p_repeat            := 1
);


-- llenado multiplicando las querys 
SELECT bck.fn_registrar_proceso(
    p_uuid_parent       := '75916486-2a9f-41b1-a369-42b339d97d53', 
    p_queries           := ARRAY['SELECT pg_sleep(3)'], 
    p_process_name      := 'TEST_INDIVIDUAL', 
    p_mode              := 'PARALLEL', 
    p_repeat            := 10
);


select * from bck.background_inventory where uuid_parent = '75916486-2a9f-41b1-a369-42b339d97d53';
--  update bck.background_inventory set cnt_used_bck = 0  where uuid_parent = '75916486-2a9f-41b1-a369-42b339d97d53';

-- truncate TABLE bck.background_process RESTART IDENTITY ;
select id,uuid_child,pid,execution_mode,status,query_exec, attempts,failed_attempts ,max_attempts , date_update  from bck.background_process where uuid_parent = '75916486-2a9f-41b1-a369-42b339d97d53';
+----+--------------------------------------+-----+----------------+--------+--------------------+----------+-----------------+--------------+-------------------------------+
| id |              uuid_child              | pid | execution_mode | status |     query_exec     | attempts | failed_attempts | max_attempts |          date_update          |
+----+--------------------------------------+-----+----------------+--------+--------------------+----------+-----------------+--------------+-------------------------------+
|  1 | 8349d89c-6d3a-4eaf-aea9-53cf76109f4b |   0 | PARALLEL       | REGISTRADO  | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:25:11.447487-07 |
|  2 | ac0fc0c6-b19f-45f8-8607-cecf47c37a83 |   0 | PARALLEL       | REGISTRADO  | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:25:11.448472-07 |
|  3 | 25647917-0c01-4e95-b03a-53d87436f708 |   0 | PARALLEL       | REGISTRADO  | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:25:11.448604-07 |
|  4 | b2e41db4-ddfa-4555-894c-632e6c78e241 |   0 | PARALLEL       | REGISTRADO  | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:25:11.448701-07 |
+----+--------------------------------------+-----+----------------+--------+--------------------+----------+-----------------+--------------+-------------------------------+
(4 rows)





---------------------------------------------- EJECUTOR ----------------------------------------------


 
---------------- EXAMPLE USAGE ----------------
/**
Esta es la funcion que va utilizar cada proceso para ejecutar las instrucciones.
	agregarle un parametro que force el usao de un uuid padre, esto restablece los valores como failed_attempts = 0  y status = REGISTRADO
**/
	

CREATE OR REPLACE FUNCTION bck.run_task_parallel(
    p_child_uuid uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY INVOKER
AS $func$
DECLARE
    -- Variables para el EXCEPTION
    ex_message      text;
    ex_context      text;
    
    -- Variables de control
    v_process_id    bigint;
    v_parent_uuid   uuid;
    v_sql_to_run    text;
    v_status        text;
BEGIN
    -- 1. Validación de parámetros de entrada
    IF p_child_uuid IS NULL THEN
        RAISE EXCEPTION 'Parámetros inválidos: UUID hijo es obligatorio.';
    END IF;

    -- 2. Localizar y Validar el registro asignado
    SELECT id, uuid_parent, status, query_exec 
      INTO v_process_id, v_parent_uuid, v_status, v_sql_to_run
    FROM bck.background_process
    WHERE uuid_child = p_child_uuid;

    IF v_process_id IS NULL THEN
        RETURN false;
    END IF;

    -- 3. Bucle de Sincronización (Polling)
    WHILE v_status != 'EJECUTANDO' LOOP
        IF v_status NOT IN ('REGISTRADO', 'LANZADO', 'EJECUTANDO') THEN
            RETURN false;
        END IF;

        PERFORM pg_sleep(0.1); 
        
        SELECT status INTO v_status 
        FROM bck.background_process 
        WHERE id = v_process_id;
    END LOOP;

    -- 4. Fase de Ejecución
    UPDATE bck.background_process 
    SET start_time = clock_timestamp(),
        date_update = clock_timestamp()
    WHERE id = v_process_id;

    -- Ejecución del comando SQL dinámico
    IF v_sql_to_run IS NOT NULL AND trim(v_sql_to_run) != '' THEN
        EXECUTE v_sql_to_run;
    ELSE
        RAISE EXCEPTION 'La instrucción SQL (query_exec) está vacía.';
    END IF;

    -- 5. Registro de éxito (Completado)
    UPDATE bck.background_process 
    SET status = 'COMPLETADO',
        end_time = clock_timestamp()
    WHERE id = v_process_id;

    -- 6. LIBERACIÓN DE SLOT EN INVENTARIO (Ruta Feliz)
    UPDATE bck.background_inventory 
    SET cnt_used_bck = cnt_used_bck - 1 
    WHERE uuid_parent = v_parent_uuid;

    RETURN true;

-- MANEJO DE ERRORES
EXCEPTION 
    WHEN QUERY_CANCELED THEN
        -- Registro de fallo por cancelación
        UPDATE bck.background_process 
        SET status = 'FALLIDO',
            end_time = clock_timestamp(),
            failed_attempts = failed_attempts + 1,
            error_msg = format('CANCELADO: Interrupción externa. uuid_child: %s', p_child_uuid)
        WHERE id = v_process_id;
        
        -- LIBERACIÓN DE SLOT EN INVENTARIO (Ruta Cancelación)
        UPDATE bck.background_inventory 
        SET cnt_used_bck = cnt_used_bck - 1 
        WHERE uuid_parent = v_parent_uuid;

        RAISE NOTICE 'Proceso cancelado para uuid_child: %', p_child_uuid;
        RETURN false;

    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS 
            ex_message = MESSAGE_TEXT,
            ex_context = PG_EXCEPTION_CONTEXT;

        -- Registro de fallo por error
        UPDATE bck.background_process 
        SET status = 'FALLIDO',
            end_time = clock_timestamp(),
            failed_attempts = failed_attempts + 1,
            error_msg = format('ERROR: %s | CTX: %s', ex_message, ex_context)
        WHERE id = v_process_id;
        
        -- LIBERACIÓN DE SLOT EN INVENTARIO (Ruta Error)
        UPDATE bck.background_inventory 
        SET cnt_used_bck = cnt_used_bck - 1 
        WHERE uuid_parent = v_parent_uuid;

        RAISE NOTICE 'Error en worker: %', ex_message;
        RETURN false;
END;
$func$;


-- Revocar público
REVOKE ALL ON FUNCTION bck.run_task_parallel(uuid, text) FROM PUBLIC;

-- Dar permisos al rol que ejecutará los workers
GRANT EXECUTE ON FUNCTION bck.run_task_parallel(uuid, text) TO PUBLIC;

-- Seguridad adicional: Forzar search_path para evitar ataques de shadowing
ALTER FUNCTION bck.run_task_parallel(uuid, text) SET search_path TO bck, public, pg_temp;
 

 
---------------- EXAMPLE USAGE ----------------

SELECT bck.run_task_parallel('dab7cbd6-5f2a-492c-9b37-1de4abccd957');

select * from pg_stat_activity where  query ilike '%run_task%';
select pg_terminate_backend(2407201);
select SELECT pg_cancel_backend(16569);

 update bck.background_process set status = 'EJECUTANDO' where uuid_child = 'db6dfc08-55e1-4b71-8a64-78a4477e0d64';


select id,uuid_child,pid,execution_mode,status,query_exec, attempts,failed_attempts ,max_attempts , date_update  from bck.background_process where uuid_parent = '75916486-2a9f-41b1-a369-42b339d97d53';
+----+--------------------------------------+-----+----------------+------------+--------------------+----------+-----------------+--------------+-------------------------------+
| id |              uuid_child              | pid | execution_mode |   status   |     query_exec     | attempts | failed_attempts | max_attempts |          date_update          |
+----+--------------------------------------+-----+----------------+------------+--------------------+----------+-----------------+--------------+-------------------------------+
|  1 | dab7cbd6-5f2a-492c-9b37-1de4abccd957 |   0 | PARALLEL       | REGISTRADO      | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:29:48.340886-07 |
|  2 | c4c3a8ad-902d-46e0-89d7-3fc4a1ebc1a1 |   0 | PARALLEL       | REGISTRADO      | SELECT pg_sleep(2) |        0 |               0 |            3 | 2026-02-25 00:29:48.341405-07 |
|  4 | e253b5dc-8764-4eae-9b4d-808dff91198a |   0 | PARALLEL       | REGISTRADO      | SELECT pg_sleep(4) |        0 |               0 |            3 | 2026-02-25 00:29:48.341599-07 |
|  3 | db6dfc08-55e1-4b71-8a64-78a4477e0d64 |   0 | PARALLEL       | COMPLETADO | SELECT pg_sleep(3) |        0 |               0 |            3 | 2026-02-25 00:43:42.64628-07  |
+----+--------------------------------------+-----+----------------+------------+--------------------+----------+-----------------+--------------+-------------------------------+
(4 rows)



	
------------------------------------------
 

CREATE OR REPLACE FUNCTION bck.fn_launch_parallel_sync(
    p_uuid_parent uuid
)
RETURNS text
LANGUAGE plpgsql
AS $func$
DECLARE
    -- Configuración
    v_max_safe      integer := (current_setting('max_worker_processes')::int - 5);
    v_wait_min      integer := 30;
    
    -- Control
    v_needed        integer;
    v_current_bck   integer;
    v_success_pids  int[] := '{}';
    v_start_wait    timestamptz := clock_timestamp();
    v_rec           record;
    v_pid           integer;
    v_cnt           integer := 0;
    -- Estado de Aborto
    v_abort_now     boolean := false;
    v_culprit_uuid  uuid;
    v_error_msg     text;
BEGIN
    -- 1. Validación de Capacidad Inicial
    SELECT count(*) INTO v_needed FROM bck.background_process 
    WHERE uuid_parent = p_uuid_parent AND status = 'REGISTRADO';

    IF v_needed = 0 THEN 
        RAISE NOTICE '[INFO] No se encontraron procesos con estatus REGISTRADO para el UUID: %', p_uuid_parent;
        RETURN 'INFO: Sin procesos pendientes.'; 
    END IF;
    
    RAISE NOTICE '[STEP 1] Iniciando orquestación para % procesos. Límite de seguridad: %', v_needed, v_max_safe;

    -- 2. Bucle de Espera de Slots
    LOOP
        PERFORM pg_stat_clear_snapshot();
        SELECT count(*) INTO v_current_bck FROM pg_stat_activity WHERE backend_type = 'pg_background';
        
        IF (v_current_bck + v_needed) <= v_max_safe THEN
            RAISE NOTICE '[STEP 2] Capacidad confirmada. Slots ocupados: %, Disponibles para este lote: %', v_current_bck, v_needed;
            EXIT;
        END IF;
        
        IF clock_timestamp() > v_start_wait + (v_wait_min || ' minutes')::interval THEN
            RAISE NOTICE '[ERROR] Timeout alcanzado (30 min). Abortando por falta de slots.';
            RAISE EXCEPTION 'Timeout 30m: Capacidad insuficiente en el servidor.';
        END IF;
        
        RAISE NOTICE '[WAIT] Slots insuficientes (Ocupados: %). Reintentando en 10s...', v_current_bck;
        PERFORM pg_sleep(10);
    END LOOP;

    -- 3. Bucle de Lanzamiento Principal
    RAISE NOTICE '[STEP 3] Comenzando fase de lanzamiento individual...';
    
    FOR v_rec IN SELECT uuid_child, max_attempts FROM bck.background_process WHERE uuid_parent = p_uuid_parent AND status = 'REGISTRADO' LOOP
        DECLARE
            v_success_this boolean := false;
            v_fail_count   integer := 0;
        BEGIN
			v_cnt := v_cnt + 1; 
			
            RAISE NOTICE ' -> Procesando #% hijo: % (Intentos permitidos: %)',v_cnt, v_rec.uuid_child, v_rec.max_attempts;

            WHILE v_fail_count < v_rec.max_attempts AND NOT v_success_this LOOP
                BEGIN
					
                    -- A. Lanzamiento
                    v_pid := pg_background_launch(format($_bck_$ SELECT bck.run_task_parallel(%L) $_bck_$, v_rec.uuid_child));
                    
					PERFORM pg_sleep(0.1);
                    -- B. Validación física
                    PERFORM pg_stat_clear_snapshot();
                    IF EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = v_pid) THEN
                        
                        RAISE NOTICE '    [OK] Lanzado PID: %. Ejecutando detach...', v_pid;
                        
                        -- C. DETACH
                        PERFORM pg_background_detach(v_pid);
                        
                        UPDATE bck.background_process 
                        SET pid = v_pid, 
                            status = 'LANZADO', 
                            attempts = attempts + 1, 
                            date_update = clock_timestamp() 
                        WHERE uuid_child = v_rec.uuid_child;
                        
                        v_success_pids := array_append(v_success_pids, v_pid);
                        v_success_this := true;
                    ELSE
                        RAISE EXCEPTION 'PID % no fue visible en pg_stat_activity', v_pid;
                    END IF;

                EXCEPTION WHEN OTHERS THEN
					v_cnt := v_cnt - 1; 
                    v_fail_count := v_fail_count + 1;
                    RAISE NOTICE '    [RETRY %/%] Falló lanzamiento de %. Motivo: %', v_fail_count, v_rec.max_attempts, v_rec.uuid_child, SQLERRM;
                    
                    UPDATE bck.background_process 
                    SET failed_attempts = v_fail_count, 
                        error_msg = SQLERRM, 
                        date_update = clock_timestamp() 
                    WHERE uuid_child = v_rec.uuid_child;
                    
                    PERFORM pg_sleep(0.2);
                END;
				
				
            END LOOP;

            -- Verificación de fracaso tras reintentos
            IF NOT v_success_this THEN
                RAISE NOTICE '[FATAL] El proceso % falló todos sus intentos.', v_rec.uuid_child;
                v_abort_now    := true;
                v_culprit_uuid := v_rec.uuid_child;
                v_error_msg    := 'Fallo definitivo tras agotar reintentos de lanzamiento.';
                EXIT; -- Sale del FOR principal
            END IF;
        END;
    END LOOP;

    -- 4. Gestión de Aborto Masivo o Sincronización Final
    IF v_abort_now THEN
        RAISE NOTICE '[STEP 4] INICIANDO KILL SWITCH. Culpable: %', v_culprit_uuid;
        
        IF array_length(v_success_pids, 1) > 0 THEN
            RAISE NOTICE ' -> Terminando % procesos hermanos que ya estaban lanzados...', array_length(v_success_pids, 1);
            PERFORM pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = ANY(v_success_pids);
        END IF;

        -- Actualización masiva de estatus por fallo
        UPDATE bck.background_process 
        SET status = 'FALLIDO', error_msg = 'ORIGEN DEL ABORTO: ' || v_error_msg 
        WHERE uuid_child = v_culprit_uuid;

        UPDATE bck.background_process 
        SET status = 'FALLIDO', error_msg = 'CANCELADO: Abortado por fallo en worker ' || v_culprit_uuid 
        WHERE uuid_parent = p_uuid_parent AND status IN ('LANZADO', 'REGISTRADO') AND uuid_child <> v_culprit_uuid;

        RAISE NOTICE '[DONE] El lote ha sido cancelado y los PIDs han sido terminados.';
        RETURN 'ERROR: Ejecución cancelada.';
    ELSE
        -- 5. SEÑAL DE INICIO (EJECUTANDO)
        RAISE NOTICE '[STEP 5] Todos los workers listos. Enviando señal de inicio masivo...';
        
        UPDATE bck.background_process 
        SET status = 'EJECUTANDO', date_update = clock_timestamp() 
        WHERE uuid_parent = p_uuid_parent AND status = 'LANZADO';
        
        RAISE NOTICE '[SUCCESS] Lote de procesos en ejecución.';
        RETURN format('ÉXITO: %s%% procesos lanzados en paralelo.', array_length(v_success_pids, 1));
    END IF;

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '[CRITICAL ERROR] Fallo en orquestador: %', SQLERRM;

	IF array_length(v_success_pids, 1) > 0 THEN
		RAISE NOTICE ' -> Terminando % procesos hermanos que ya estaban lanzados...', array_length(v_success_pids, 1);
		PERFORM pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = ANY(v_success_pids);
	END IF;
	
    UPDATE bck.background_process 
    SET status = 'FALLIDO', error_msg = 'FALLO INESPERADO: ' || SQLERRM 
    WHERE uuid_parent = p_uuid_parent;
	
    RETURN 'FALLO INESPERADO: ' || SQLERRM;
END;
$func$;
 


-- Seguridad y Path
ALTER FUNCTION bck.fn_launch_parallel_sync(uuid) SET search_path TO bck, public, pg_temp;

REVOKE ALL ON FUNCTION bck.fn_launch_parallel_sync(uuid) FROM PUBLIC;


------------------------------------------------------------------------------------------------------------------------

-- 1. Crear el inventario para 3 procesos
SELECT bck.fn_crear_inventario(3) as mi_uuid; -- Supongamos que retorna 'UUID-123'


-- 2. Registrar queries (Fase 2 de la respuesta anterior)
SELECT bck.fn_registrar_proceso('UUID-123', 'SELECT pg_sleep(10)');
SELECT bck.fn_registrar_proceso('UUID-123', 'SELECT pg_sleep(15)');
SELECT bck.fn_registrar_proceso('UUID-123', 'SELECT pg_sleep(5)');


 

-- 3. LANZAR TODO
SELECT bck.fn_orquestar_lanzamiento('UUID-PADRE');

-- 4. MONITOREAR (Usa tu vista)
SELECT * FROM bck.vw_status_progreso WHERE uuid_parent = 'UUID-PADRE';

-- 4. Ver progreso
SELECT * FROM bck.vw_status_progreso WHERE uuid_parent = '6a1b8b2e-e72c-4c87-a88d-29fe4f929b22'; 

select * from pg_stat_activity where backend_type  = 'pg_background'




update bck.background_process set status = 'REGISTRADO' , failed_attempts = 0, error_msg = null , execution_mode = 'PARALLEL';
SELECT bck.fn_launch_parallel_sync('5cf1f8cd-a7ca-4d01-a8d2-edd4eab51021');



