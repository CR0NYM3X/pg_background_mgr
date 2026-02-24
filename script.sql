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
SELECT 
    uuid_parent,
    count(*) as total,
    count(*) FILTER (WHERE status = 'COMPLETADO') as hechos,
    count(*) FILTER (WHERE status = 'FALLIDO') as errores,
    round((count(*) FILTER (WHERE status = 'COMPLETADO')::float / count(*)::float) * 100) || '%' as porcentaje,
    rpad('', (round((count(*) FILTER (WHERE status = 'COMPLETADO')::float / count(*)::float) * 10)::int), '█') as barra
FROM bck.background_process
GROUP BY uuid_parent;




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
        count(*) FILTER (WHERE p.status = 'LISTO') as en_espera
    FROM bck.background_process p
    LEFT JOIN cte_actividad_real a ON p.pid = a.pid
    GROUP BY p.uuid_parent
)
SELECT 
    uuid_parent,
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











-- 3. Función 1: Inicialización de Inventario

-- Esta función valida el "cupo" y reserva el slot de procesos.
 
 
CREATE OR REPLACE FUNCTION bck.fn_crear_inventario(p_cantidad integer)
RETURNS uuid
LANGUAGE plpgsql
AS $func$
DECLARE
    v_uuid        uuid    := gen_random_uuid();
    v_max_allowed integer := 1000; -- Ajustado para alta carga de procesos
BEGIN
    -- 1. Validación de parámetros de entrada
    IF p_cantidad IS NULL OR p_cantidad <= 0 THEN
        RAISE EXCEPTION 'La cantidad de procesos debe ser mayor a 0.';
    ELSIF p_cantidad > v_max_allowed THEN
        RAISE EXCEPTION 'Cantidad de procesos (%) excede el límite corporativo permitido (%)', p_cantidad, v_max_allowed;
    END IF;

    -- 2. Registro en tabla de control interna
    INSERT INTO bck.background_inventory (
        uuid_parent, 
        cnt_total_bck, 
        cnt_used_bck,
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
        -- Reportamos el error directamente al cliente/app sin insertar en tablas externas
        RAISE EXCEPTION 'Error al crear inventario: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
END;
$func$;



select * from bck.fn_crear_inventario(4);

-- truncate table bck.background_inventory  RESTART IDENTITY ;
select * from bck.background_inventory where uuid_parent = '453d77bc-6e4b-42e0-911f-7a3016470b0b';

+----+--------------------------------------+---------------+--------------+-----------+-------------------------------+
| id |             uuid_parent              | cnt_total_bck | cnt_used_bck | force_cnt |          date_insert          |
+----+--------------------------------------+---------------+--------------+-----------+-------------------------------+
|  1 | 453d77bc-6e4b-42e0-911f-7a3016470b0b |             4 |            0 | f         | 2026-02-23 18:35:52.353918-07 |
+----+--------------------------------------+---------------+--------------+-----------+-------------------------------+
(1 row)






-- ## 4. Función 2: Registro de Tareas (Queries)
 

---------------- EXAMPLE USAGE ----------------
 

CREATE OR REPLACE FUNCTION bck.fn_registrar_proceso(
    p_uuid_parent uuid,
    p_query       text
)
RETURNS boolean
LANGUAGE plpgsql
AS $func$
DECLARE
    v_cnt_total integer;
    v_cnt_used  integer;
BEGIN
    -- 1. Validación de parámetros
    IF p_uuid_parent IS NULL OR p_query IS NULL OR trim(p_query) = '' THEN
        RAISE EXCEPTION 'Parámetros inválidos: El UUID y la Query son obligatorios.';
    END IF;

    -- 2. Bloqueo optimista y obtención de contadores
    -- El FOR UPDATE es crítico para evitar condiciones de carrera (race conditions)
    SELECT cnt_total_bck, cnt_used_bck
      INTO v_cnt_total, v_cnt_used
    FROM bck.background_inventory
    WHERE uuid_parent = p_uuid_parent
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Error: El UUID de inventario % no existe.', p_uuid_parent;
    END IF;

    -- 3. Validación de capacidad
    IF v_cnt_used >= v_cnt_total THEN
        RAISE EXCEPTION 'Inventario agotado: % posee %/% procesos utilizados.', 
            p_uuid_parent, v_cnt_used, v_cnt_total;
    END IF;

    -- 4. Registrar la tarea en la cola
    INSERT INTO bck.background_process (
        uuid_parent, 
        query_exec, 
        status, 
        date_insert
    )
    VALUES (
        p_uuid_parent, 
        p_query, 
        'LISTO', 
        clock_timestamp()
    );

    -- 5. Incrementar contador en el padre
    UPDATE bck.background_inventory 
    SET cnt_used_bck = cnt_used_bck + 1 
    WHERE uuid_parent = p_uuid_parent;

    RETURN true;

EXCEPTION 
    WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'Violación de integridad: El UUID padre no es válido.';
    WHEN OTHERS THEN
        -- Lanzamos el error directamente al contexto del llamador
        RAISE EXCEPTION 'Error en bck.fn_registrar_proceso: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
END;
$func$;





SELECT bck.fn_registrar_proceso( '453d77bc-6e4b-42e0-911f-7a3016470b0b', 'select pg_sleep(5);'); -- tiene que retornar la cantidad de registros

select * from bck.background_inventory where uuid_parent = '453d77bc-6e4b-42e0-911f-7a3016470b0b';

select id,uuid_child,pid,status,query_exec, attempts,failed_attempts ,max_attempts , date_update from bck.background_process;
+----+--------------------------------------+-----+--------+---------------------+----------+-----------------+--------------+-------------------------------+
| id |              uuid_child              | pid | status |     query_exec      | attempts | failed_attempts | max_attempts |          date_update          |
+----+--------------------------------------+-----+--------+---------------------+----------+-----------------+--------------+-------------------------------+
|  1 | fcf47a12-a702-4a8f-826e-4292b99f97e8 |   0 | LISTO  | select pg_sleep(5); |        0 |               0 |            3 | 2026-02-23 19:04:20.916348-07 |
|  2 | 67b48371-bc6e-4d5e-a3d8-244697d6830d |   0 | LISTO  | select pg_sleep(5); |        0 |               0 |            3 | 2026-02-23 19:04:21.524719-07 |
|  3 | c1dc6e9d-1a98-4a0f-9eaf-ae24c5c2375a |   0 | LISTO  | select pg_sleep(5); |        0 |               0 |            3 | 2026-02-23 19:04:21.972099-07 |
|  4 | 367e65cb-835c-4b23-b681-2223847e2749 |   0 | LISTO  | select pg_sleep(5); |        0 |               0 |            3 | 2026-02-23 19:04:22.483572-07 |
+----+--------------------------------------+-----+--------+---------------------+----------+-----------------+--------------+-------------------------------+
(4 rows)




 

---------------------------------------------- EJECUTOR ----------------------------------------------


 
---------------- EXAMPLE USAGE ----------------
-- SELECT bck.run_task('550e8400-e29b-41d4-a716-446655440000', pg_backend_pid());

CREATE OR REPLACE FUNCTION bck.run_task(
    p_child_uuid uuid
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY INVOKER
AS $func$
DECLARE
    v_process_id  bigint;
    v_sql_to_run  text;
    v_status      text;
    v_msg_error   text;
    v_context     text;
BEGIN
    -- 1. Validación de parámetros de entrada
    IF p_child_uuid IS NULL   THEN
        RAISE EXCEPTION 'Parámetros inválidos: UUID padre y PID son obligatorios.';
    END IF;

    -- 2. Localizar y Validar el registro asignado
    -- Se busca el registro que coincida con el UUID y el PID enviado
    SELECT id, status, query_exec 
      INTO v_process_id, v_status, v_sql_to_run
    FROM bck.background_process
    WHERE uuid_child = p_child_uuid ; -- Bloqueamos el registro para nosotros

    -- Si no existe un registro con ese PID asignado, retornamos false
    IF v_process_id IS NULL THEN
        RETURN false;
    END IF;

    -- 3. Bucle de Sincronización (Polling)
    -- Solo entramos si no está ya en 'EJECUTANDO'
    WHILE v_status != 'EJECUTANDO' LOOP
        -- Verificar si el estatus cambió a algo inválido (por ejemplo, CANCELADO)
        IF v_status NOT IN ('LISTO', 'INICIALIZANDO', 'EJECUTANDO') THEN
            RETURN false;
        END IF;

        PERFORM pg_sleep(0.1); -- Pausa de 100ms
        
        -- Refrescar estatus
        SELECT status INTO v_status 
        FROM bck.background_process 
        WHERE id = v_process_id;
    END LOOP;

    -- 4. Fase de Ejecución
    BEGIN
        -- Actualizar inicio real de ejecución
        UPDATE bck.background_process 
        SET start_time = clock_timestamp(),
            date_update = clock_timestamp()
        WHERE id = v_process_id;

        -- Ejecutar el comando SQL dinámico
        IF v_sql_to_run IS NOT NULL AND trim(v_sql_to_run) != '' THEN
            EXECUTE v_sql_to_run;
        ELSE
            RAISE EXCEPTION 'La instrucción SQL (query_exec) está vacía.';
        END IF;

        -- Registro de éxito (Completado)
        UPDATE bck.background_process 
        SET status = 'COMPLETADO',
            end_time = clock_timestamp()
        WHERE id = v_process_id;

    EXCEPTION 
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS 
                v_msg_error = MESSAGE_TEXT,
                v_context   = PG_EXCEPTION_CONTEXT;

            -- Registro de falla con detalle
            UPDATE bck.background_process 
            SET status = 'FALLIDO',
                end_time = clock_timestamp(),
                failed_attempts = failed_attempts + 1,
                error_msg = format('ERROR: uuid_child: %s - %s | CTX: %s',p_child_uuid, v_msg_error, v_context)
            WHERE id = v_process_id;
            
            RETURN false;
         
    END;

    RETURN true;
END;
$func$;


/* agregar la parte de cancelado

EXCEPTION 

	WHEN QUERY_CANCELED  THEN
		     -- Registro de falla con detalle
            UPDATE bck.background_process 
            SET status = 'FALLIDO',
                end_time = clock_timestamp(),
                failed_attempts = failed_attempts + 1,
                error_msg = format('ERROR: uuid_child: %s - %s | CTX: %s',p_child_uuid, v_msg_error, v_context)
            WHERE id = v_process_id; 

*/

-- Revocar público
REVOKE ALL ON FUNCTION bck.run_task(uuid, text) FROM PUBLIC;

-- Dar permisos al rol que ejecutará los workers
GRANT EXECUTE ON FUNCTION bck.run_task(uuid, text) TO PUBLIC;

-- Seguridad adicional: Forzar search_path para evitar ataques de shadowing
ALTER FUNCTION bck.run_task(uuid, text) SET search_path TO bck, public, pg_temp;
 

 
---------------- EXAMPLE USAGE ----------------

SELECT bck.run_task('fcf47a12-a702-4a8f-826e-4292b99f97e8');

 update bck.background_process set status = 'EJECUTANDO' where uuid_child = 'fcf47a12-a702-4a8f-826e-4292b99f97e8';

select id,pid,status,query_exec, attempts,failed_attempts ,max_attempts , date_update from bck.background_process where uuid_parent = '453d77bc-6e4b-42e0-911f-7a3016470b0b'   ;

 select pid,status,query_exec, attempts,failed_attempts ,max_attempts , start_time,end_time from bck.background_process where uuid_parent = '453d77bc-6e4b-42e0-911f-7a3016470b0b'   ;
+----+-----+------------+---------------------+----------+-----------------+--------------+-------------------------------+-------------------------------+
| id | pid |   status   |     query_exec      | attempts | failed_attempts | max_attempts |          start_time           |           end_time            |
+----+-----+------------+---------------------+----------+-----------------+--------------+-------------------------------+-------------------------------+
|  2 |   0 | LISTO      | select pg_sleep(5); |        0 |               0 |            3 | NULL                          | NULL                          |
|  3 |   0 | LISTO      | select pg_sleep(5); |        0 |               0 |            3 | NULL                          | NULL                          |
|  4 |   0 | LISTO      | select pg_sleep(5); |        0 |               0 |            3 | NULL                          | NULL                          |
|  1 |   0 | COMPLETADO | select pg_sleep(5); |        0 |               0 |            3 | 2026-02-23 19:09:12.415516-07 | 2026-02-23 19:09:17.420343-07 |
+----+-----+------------+---------------------+----------+-----------------+--------------+-------------------------------+-------------------------------+
(4 rows)









----------- ## 5. Orquestador 

-- El orquestador tendra la habilidad de lanzar los procesos de forma asincrona o sincrona dependiendo como lo solicitens por paramtro sync  o async  (default)


CREATE OR REPLACE FUNCTION bck.fn_lanzar_orquestador(
    p_uuid_parent uuid,
    p_force_cnt   boolean DEFAULT false -- p_force_cnt
)
RETURNS text
LANGUAGE plpgsql
AS $func$
DECLARE
    v_rec           record;
    v_cnt_inventary integer;
    v_cnt_actual    integer;
    v_launched      integer := 0;
    v_pid           integer;
BEGIN
    -- 1. Validaciones de control
    SELECT cnt_total_bck INTO v_cnt_inventary 
    FROM bck.background_inventory WHERE uuid_parent = p_uuid_parent;
    
    SELECT count(*) INTO v_cnt_actual 
    FROM bck.background_process WHERE uuid_parent = p_uuid_parent;

    IF p_force_cnt AND v_cnt_actual < v_cnt_inventary THEN
        RAISE EXCEPTION 'Error de Quórum: Se requieren % procesos, pero solo hay % registrados.', 
            v_cnt_inventary, v_cnt_actual;
    END IF;

    -- 2. Loop de lanzamiento
    FOR v_rec IN 
        SELECT id, query_exec 
        FROM bck.background_process 
        WHERE uuid_parent = p_uuid_parent AND status = 'LISTO'
    LOOP
        BEGIN
            -- Ejecución mediante pg_background (debe estar instalada la extensión)
            -- Nota: Se asume que la extensión está disponible en el search_path o esquema público
            v_pid := pg_background_launch(v_rec.query_exec);

            UPDATE bck.background_process 
            SET pid = v_pid,
                status = 'EJECUTANDO',
                start_time = clock_timestamp()
            WHERE id = v_rec.id;

            v_launched := v_launched + 1;
        EXCEPTION WHEN OTHERS THEN
            UPDATE bck.background_process 
            SET status = 'FALLIDO',
                error_msg = SQLERRM
            WHERE id = v_rec.id;
        END;
    END LOOP;

    RETURN format('Lanzamiento finalizado. Exitosos: %1. Fallidos/Omitidos: %2.', 
                  v_launched, (v_cnt_actual - v_launched));
END;
$func$;


 




 


------------------------------------------------------------------------------------------------------------------------

-- 1. Crear el inventario para 3 procesos
SELECT bck.fn_crear_inventario(3) as mi_uuid; -- Supongamos que retorna 'UUID-123'


-- 2. Registrar queries (Fase 2 de la respuesta anterior)
SELECT bck.fn_registrar_proceso('UUID-123', 'SELECT pg_sleep(10)');
SELECT bck.fn_registrar_proceso('UUID-123', 'SELECT pg_sleep(15)');
SELECT bck.fn_registrar_proceso('UUID-123', 'SELECT pg_sleep(5)');



-- 3. Lanzar (Fase 3)
SELECT bck.fn_lanzar_orquestador('UUID-123', true); -- sync o async

SELECT iniciar;

-- 4. Ver progreso
SELECT * FROM bck.vw_status_progreso WHERE uuid_parent = '6a1b8b2e-e72c-4c87-a88d-29fe4f929b22'; 

select * from pg_stat_activity where backend_type  = 'pg_background'
