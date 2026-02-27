 

INSERT INTO bck.background_config (config_key, config_value, description)
VALUES ('max_bck_random', '0', 'Límite de procesos simultáneos para modo RANDOM. 0 = Auto (max_worker_processes - 5).');



CREATE OR REPLACE FUNCTION bck.fn_launch_random_swarm(p_uuid_parent uuid)
RETURNS text LANGUAGE plpgsql AS $func$
DECLARE
    v_max_allowed integer;
    v_launched    integer := 0;
    v_rec         record;
    v_pid         integer;
BEGIN
    -- 1. Determinar el límite máximo (Config o Auto)
    SELECT config_value::integer INTO v_max_allowed 
    FROM bck.background_config WHERE config_key = 'max_bck_random';

    IF v_max_allowed IS NULL OR v_max_allowed = 0 THEN
        v_max_allowed := (current_setting('max_worker_processes')::integer - 5);
    END IF;

    RAISE NOTICE '[SWARM] Iniciando enjambre. Límite: % workers.', v_max_allowed;

    -- 2. Lanzar el primer escuadrón usando tu lógica de selección aleatoria
    FOR v_rec IN 
        SELECT uuid_child 
        FROM bck.background_process 
        WHERE uuid_parent = p_uuid_parent 
          AND status = 'REGISTRADO' 
          AND execution_mode = 'RANDOM'
        ORDER BY random() 
        LIMIT v_max_allowed
    LOOP
        -- Lanzamiento del worker hijo
        v_pid := pg_background_launch(format('SELECT bck.run_task_random(%L)', v_rec.uuid_child));
        
        -- IMPORTANTE: Pequeño respiro antes del detach para estabilidad del Postmaster
        PERFORM pg_sleep(0.1); 
        PERFORM pg_background_detach(v_pid);
        
        
        v_launched := v_launched + 1;
    END LOOP;

    RETURN format('ÉXITO: Enjambre iniciado con %s workers.', v_launched);
END;
$func$;

 

CREATE OR REPLACE FUNCTION bck.run_task_random(p_child_uuid uuid)
RETURNS boolean LANGUAGE plpgsql AS $func$
DECLARE
    v_rec         record;
    v_next_rec    record;
    v_next_pid    integer;
BEGIN
    -- 1. Localizar mi tarea asignada
    SELECT * INTO v_rec FROM bck.background_process WHERE uuid_child = p_child_uuid;
    IF NOT FOUND THEN RETURN false; END IF;

    -- 2. Ejecución con manejo de errores
    UPDATE bck.background_process SET status = 'EJECUTANDO', start_time = clock_timestamp() WHERE id = v_rec.id;
    
    BEGIN
        EXECUTE v_rec.query_exec;
        
        UPDATE bck.background_process 
        SET status = 'COMPLETADO', end_time = clock_timestamp() 
        WHERE id = v_rec.id;
    EXCEPTION WHEN OTHERS THEN
        UPDATE bck.background_process 
        SET status = 'FALLIDO', error_msg = SQLERRM, end_time = clock_timestamp() 
        WHERE id = v_rec.id;
        PERFORM pg_sleep(0.5); -- Freno de seguridad ante errores constantes
    END;

    -- 3. Liberar slot en inventario
    UPDATE bck.background_inventory SET cnt_used_bck = cnt_used_bck - 1 WHERE uuid_parent = v_rec.uuid_parent;

    -- 4. RELEVO: Buscar la SIGUIENTE tarea disponible para el siguiente worker
    SELECT uuid_child INTO v_next_rec
    FROM bck.background_process
    WHERE uuid_parent = v_rec.uuid_parent 
      AND status = 'REGISTRADO'
      AND execution_mode = 'RANDOM'
    ORDER BY id ASC -- Tomamos el siguiente en cola
    FOR UPDATE SKIP LOCKED LIMIT 1;

    -- 5. Si hay más trabajo, lanzar al sucesor antes de morir
    IF v_next_rec.uuid_child IS NOT NULL THEN
        v_next_pid := pg_background_launch(format('SELECT bck.run_task_random(%L)', v_next_rec.uuid_child));
        PERFORM pg_sleep(0.1); -- Sleep vital antes del detach
        PERFORM pg_background_detach(v_next_pid);
    END IF;

    RETURN true;
END;
$func$;


/***

 Select * from bck.vw_status_progreso ;
\watch 1



update bck.background_process set status = 'REGISTRADO' , failed_attempts = 0, attempts = 0 ,error_msg = null , execution_mode = 'RANDOM', pid = 0;
SELECT bck.fn_launch_random_swarm('5cf1f8cd-a7ca-4d01-a8d2-edd4eab51021');



****/



