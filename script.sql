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




	
*/
 

---------------- ESTRUCTURA CORE ----------------
CREATE SCHEMA IF NOT EXISTS bck;


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
CREATE TABLE IF NOT EXISTS bck.background_process (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uuid_parent     uuid NOT NULL REFERENCES bck.background_inventory(uuid_parent) ON DELETE CASCADE,
    uuid_child     uuid DEFAULT gen_random_uuid() UNIQUE,
    pid             integer DEFAULT 0,
    status          text DEFAULT 'LISTO' 
                    CHECK (status IN ('LISTO', 'INICIALIZANDO', 'EJECUTANDO', 'COMPLETADO', 'FALLIDO')),
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
CREATE INDEX IF NOT EXISTS idx_bck_proc_lookup ON bck.background_process (uuid_parent, status, pid) WHERE status = 'LISTO';

CREATE INDEX IF NOT EXISTS idx_bck_proc_uuid ON bck.background_process(uuid_parent);
CREATE INDEX IF NOT EXISTS idx_bck_proc_status ON bck.background_process(status);

 
 



CREATE OR REPLACE VIEW bck.vw_status_progreso AS
WITH cte_actividad_real AS (
    -- Capturamos los workers de pg_background activos en el motor
    SELECT pid, state, query, backend_start
    FROM pg_stat_activity 
    WHERE backend_type = 'pg_background'
),
cte_metricas AS (
    SELECT 
        p.uuid_parent,
        count(*) as total_tareas,
        count(*) FILTER (WHERE p.status = 'COMPLETADO') as completados,
        count(*) FILTER (WHERE p.status = 'FALLIDO') as fallidos,
        -- Contamos procesos que están en nuestra tabla como EJECUTANDO 
        -- Y que además existen físicamente en pg_stat_activity
        count(a.pid) FILTER (WHERE p.status = 'EJECUTANDO') as workers_activos_motor,
        -- Procesos en cola que aún no inician
        count(*) FILTER (WHERE p.status = 'LISTO') as en_espera,
        -- Procesos asignados a un worker pero esperando señal
        count(*) FILTER (WHERE p.status = 'INICIALIZANDO') as inicializando
    FROM bck.background_process p
    LEFT JOIN cte_actividad_real a ON p.pid = a.pid
    GROUP BY p.uuid_parent
)
SELECT 
    uuid_parent,
    CASE 
        WHEN total_tareas = (completados + fallidos) AND total_tareas > 0 THEN 'FINALIZADO'
        WHEN workers_activos_motor > 0 THEN 'EJECUTANDO (RUNNING)'
        WHEN inicializando > 0 THEN 'INICIALIZANDO'
        WHEN en_espera = total_tareas THEN 'PENDIENTE (POR INICIAR)'
        ELSE 'EN PROCESO'
    END as "Estatus Global",
    total_tareas as "Total",
    completados as "Hechos",
    fallidos as "Errores",
    workers_activos_motor as "Activos Real",
    en_espera as "En Espera",
    CASE 
        WHEN total_tareas = 0 THEN '0%'
        ELSE round((completados::float / total_tareas::float) * 100)::text || '%' 
    END as "Avance",
    -- Barra de progreso visual para PSQL
    '[' || 
    rpad(
        repeat('█', (COALESCE(round((completados::float / NULLIF(total_tareas,0)::float) * 20),0))::int), 
        20, 
        ' '
    ) || 
    ']' as "Progreso"
FROM cte_metricas;



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
--- Quiero que el p_querys sea array y ese array se debe recorrer en orden y se debe insertar de manera automatica en la tabla bck.background_process esto en casos donde no quieres ejecutar 10 veces la misma funcion fn_registrar_proceso con la mimsa query o que sean diferentes querys pero no quieres insertarlo la n cantidad de veces la misma funcion

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
    v_i             integer; -- Contador para el bucle de repetición
BEGIN
    -- 1. Validación de parámetros iniciales
    IF p_uuid_parent IS NULL OR p_queries IS NULL OR array_length(p_queries, 1) = 0 THEN
        RAISE EXCEPTION 'Parámetros inválidos: El UUID y al menos una Query son obligatorios.';
    END IF;

    IF p_repeat <= 0 THEN
        RAISE EXCEPTION 'El parámetro p_repeat debe ser mayor a 0.';
    END IF;

    -- Cálculo de carga total
    v_num_queries   := array_length(p_queries, 1);
    v_total_inserts := v_num_queries * p_repeat;

    -- 2. Bloqueo de inventario para control de capacidad
    SELECT cnt_total_bck, cnt_used_bck
      INTO v_cnt_total, v_cnt_used
    FROM bck.background_inventory
    WHERE uuid_parent = p_uuid_parent
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Error: El UUID de inventario % no existe.', p_uuid_parent;
    END IF;

    -- 3. Validar si el lote total (incluyendo repeticiones) cabe
    IF (v_cnt_used + v_total_inserts) > v_cnt_total THEN
        RAISE EXCEPTION 'Capacidad insuficiente: Espacio disponible %, solicitado % (Queries: % x Repeticiones: %).', 
            (v_cnt_total - v_cnt_used), v_total_inserts, v_num_queries, p_repeat;
    END IF;

    -- 4. Recorrer el array y aplicar repeticiones
    FOREACH v_query_item IN ARRAY p_queries
    LOOP
        IF v_query_item IS NULL OR trim(v_query_item) = '' THEN
            RAISE EXCEPTION 'Error: Se encontró una query vacía o nula en el array.';
        END IF;

        -- Bucle de repetición
        FOR v_i IN 1..p_repeat LOOP
            INSERT INTO bck.background_process (
                uuid_parent, 
                query_exec, 
                status, 
                process_name,
                execution_mode,
                date_insert
            )
            VALUES (
                p_uuid_parent, 
                v_query_item, 
                'LISTO', 
                p_process_name,
                upper(p_mode),
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
|  1 | 8349d89c-6d3a-4eaf-aea9-53cf76109f4b |   0 | PARALLEL       | LISTO  | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:25:11.447487-07 |
|  2 | ac0fc0c6-b19f-45f8-8607-cecf47c37a83 |   0 | PARALLEL       | LISTO  | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:25:11.448472-07 |
|  3 | 25647917-0c01-4e95-b03a-53d87436f708 |   0 | PARALLEL       | LISTO  | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:25:11.448604-07 |
|  4 | b2e41db4-ddfa-4555-894c-632e6c78e241 |   0 | PARALLEL       | LISTO  | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:25:11.448701-07 |
+----+--------------------------------------+-----+----------------+--------+--------------------+----------+-----------------+--------------+-------------------------------+
(4 rows)





---------------------------------------------- EJECUTOR ----------------------------------------------


 
---------------- EXAMPLE USAGE ----------------
/**
Esta es la funcion que va utilizar cada proceso para ejecutar las instrucciones.
**/
	
-- En cada proceso al final agregarle para que valide si todavía hay procesos y en caso de que sea el.unico entonces que ponga que el proceso a finalizado con éxito

CREATE OR REPLACE FUNCTION bck.run_task(
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
        IF v_status NOT IN ('LISTO', 'INICIALIZANDO', 'EJECUTANDO') THEN
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
REVOKE ALL ON FUNCTION bck.run_task(uuid, text) FROM PUBLIC;

-- Dar permisos al rol que ejecutará los workers
GRANT EXECUTE ON FUNCTION bck.run_task(uuid, text) TO PUBLIC;

-- Seguridad adicional: Forzar search_path para evitar ataques de shadowing
ALTER FUNCTION bck.run_task(uuid, text) SET search_path TO bck, public, pg_temp;
 

 
---------------- EXAMPLE USAGE ----------------

SELECT bck.run_task('dab7cbd6-5f2a-492c-9b37-1de4abccd957');

select * from pg_stat_activity where  query ilike '%run_task%';
select pg_terminate_backend(2407201);
select SELECT pg_cancel_backend(16569);

 update bck.background_process set status = 'EJECUTANDO' where uuid_child = 'db6dfc08-55e1-4b71-8a64-78a4477e0d64';


select id,uuid_child,pid,execution_mode,status,query_exec, attempts,failed_attempts ,max_attempts , date_update  from bck.background_process where uuid_parent = '75916486-2a9f-41b1-a369-42b339d97d53';
+----+--------------------------------------+-----+----------------+------------+--------------------+----------+-----------------+--------------+-------------------------------+
| id |              uuid_child              | pid | execution_mode |   status   |     query_exec     | attempts | failed_attempts | max_attempts |          date_update          |
+----+--------------------------------------+-----+----------------+------------+--------------------+----------+-----------------+--------------+-------------------------------+
|  1 | dab7cbd6-5f2a-492c-9b37-1de4abccd957 |   0 | PARALLEL       | LISTO      | SELECT pg_sleep(1) |        0 |               0 |            3 | 2026-02-25 00:29:48.340886-07 |
|  2 | c4c3a8ad-902d-46e0-89d7-3fc4a1ebc1a1 |   0 | PARALLEL       | LISTO      | SELECT pg_sleep(2) |        0 |               0 |            3 | 2026-02-25 00:29:48.341405-07 |
|  4 | e253b5dc-8764-4eae-9b4d-808dff91198a |   0 | PARALLEL       | LISTO      | SELECT pg_sleep(4) |        0 |               0 |            3 | 2026-02-25 00:29:48.341599-07 |
|  3 | db6dfc08-55e1-4b71-8a64-78a4477e0d64 |   0 | PARALLEL       | COMPLETADO | SELECT pg_sleep(3) |        0 |               0 |            3 | 2026-02-25 00:43:42.64628-07  |
+----+--------------------------------------+-----+----------------+------------+--------------------+----------+-----------------+--------------+-------------------------------+
(4 rows)









----------- ## 5. Orquestador 

CREATE OR REPLACE FUNCTION bck.fn_orquestar_lanzamiento(
    p_uuid_parent uuid
)
RETURNS text
LANGUAGE plpgsql
AS $func$
DECLARE
    v_rec           record;
    v_launched_pid  integer;
    v_total         integer := 0;
    v_errors        integer := 0;
    v_all_ready     boolean := false;
    v_attempts      integer := 0;
BEGIN
    -- 1. Validación: ¿Existe el inventario?
    IF NOT EXISTS (SELECT 1 FROM bck.background_inventory WHERE uuid_parent = p_uuid_parent) THEN
        RAISE EXCEPTION 'El UUID de inventario % no existe.', p_uuid_parent;
    END IF;

    -- 2. LANZAMIENTO (Fase de Disparo)
    -- Recorremos solo los que están en LISTO para este padre
    FOR v_rec IN 
        SELECT uuid_child, execution_mode 
        FROM bck.background_process 
        WHERE uuid_parent = p_uuid_parent AND status = 'LISTO'
    LOOP
        BEGIN
            -- Disparamos el worker usando pg_background_launch
            -- Le pasamos su propio uuid_child para que sepa qué ejecutar
            v_launched_pid := pg_background_launch(
                format('SELECT bck.run_task(%L)', v_rec.uuid_child)
            );

            -- Registramos el PID inmediatamente y pasamos a INICIALIZANDO
            UPDATE bck.background_process 
            SET pid = v_launched_pid,
                status = 'INICIALIZANDO',
                attempts = attempts + 1,
                date_update = clock_timestamp()
            WHERE uuid_child = v_rec.uuid_child;

            v_total := v_total + 1;

        EXCEPTION WHEN OTHERS THEN
            v_errors := v_errors + 1;
            UPDATE bck.background_process 
            SET status = 'FALLIDO',
                error_msg = 'Error al lanzar pg_background: ' || SQLERRM
            WHERE uuid_child = v_rec.uuid_child;
        END;
    END LOOP;

    -- 3. SINCRONIZACIÓN (Fase de Validación)
    -- Esperamos a que el sistema operativo registre los PIDs en pg_stat_activity
    -- Esto asegura que los workers ya están en su bucle de polling
    WHILE NOT v_all_ready AND v_attempts < 50 LOOP
        SELECT NOT EXISTS (
            -- Buscamos si hay algún proceso que lanzamos que NO aparezca en stat_activity
            SELECT 1 
            FROM bck.background_process p
            LEFT JOIN pg_stat_activity a ON p.pid = a.pid
            WHERE p.uuid_parent = p_uuid_parent 
              AND p.status = 'INICIALIZANDO'
              AND a.pid IS NULL
        ) INTO v_all_ready;

        IF NOT v_all_ready THEN
            PERFORM pg_sleep(0.1); -- Espera activa corta
            v_attempts := v_attempts + 1;
        END IF;
    END LOOP;

    -- 4. DISPARO MASIVO (Luz Verde)
    -- Una vez confirmados los PIDs, liberamos a todos los workers al mismo tiempo
    UPDATE bck.background_process 
    SET status = 'EJECUTANDO',
        date_update = clock_timestamp()
    WHERE uuid_parent = p_uuid_parent 
      AND status = 'INICIALIZANDO';

    RETURN format('Orquestación completada. UUID: %s | Lanzados: %s | Fallidos: %s', 
                  p_uuid_parent, v_total, v_errors);

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Error crítico en orquestador: %', SQLERRM;
END;
$func$;


--- SELECT bck.fn_orquestar_lanzamiento('UUID-PADRE');
 




 


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
