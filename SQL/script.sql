
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

 


CREATE TABLE IF NOT EXISTS bck.background_process (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uuid_parent     uuid NOT NULL REFERENCES bck.background_inventory(uuid_parent) ON DELETE CASCADE,
    uuid_child     uuid DEFAULT gen_random_uuid() UNIQUE,
    pid             integer DEFAULT 0,
    status          text DEFAULT 'REGISTRADO' 
                    CHECK (status IN ('REGISTRADO', 'LANZADO', 'EJECUTANDO', 'COMPLETADO', 'FALLIDO')),
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




-- Revocar público
REVOKE ALL ON FUNCTION bck.fn_crear_inventario(integer) FROM PUBLIC;

-- Dar permisos al rol que ejecutará los workers
GRANT EXECUTE ON FUNCTION bck.fn_crear_inventario(integer) TO PUBLIC;

-- Seguridad adicional: Forzar search_path para evitar ataques de shadowing
ALTER FUNCTION bck.fn_crear_inventario(integer) SET search_path TO bck, public, pg_temp;
 


-- select * from bck.fn_crear_inventario(10);
 



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
$func$
SET client_min_messages = 'notice' 
SET log_statement  = 'none' 
SET log_min_messages = 'panic'
SET statement_timeout = 0		
SET lock_timeout = 0 ;




-- Revocar público
REVOKE ALL ON FUNCTION bck.fn_registrar_proceso(uuid,text[],text,text,integer) FROM PUBLIC;

-- Dar permisos al rol que ejecutará los workers
GRANT EXECUTE ON FUNCTION bck.fn_registrar_proceso(uuid,text[],text,text,integer) TO PUBLIC;

-- Seguridad adicional: Forzar search_path para evitar ataques de shadowing
ALTER FUNCTION bck.fn_registrar_proceso(uuid,text[],text,text,integer) SET search_path TO bck, public, pg_temp;
 



/***
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
***/


