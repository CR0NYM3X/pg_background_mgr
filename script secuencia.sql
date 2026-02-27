

CREATE OR REPLACE FUNCTION bck.fn_launch_sequential_chain(p_uuid_parent uuid)
RETURNS text LANGUAGE plpgsql AS $func$
DECLARE
    v_rec           record;
    v_pid           integer;
    v_launch_ok     boolean;
    v_fail_count    integer := 0;
    v_started_count integer;
    v_target_status text;
BEGIN
    -- 1. AUTODETECCIÓN: ¿Alguien ya empezó?
    -- Contamos procesos que ya salieron del estado 'REGISTRADO'
    SELECT count(*) INTO v_started_count 
    FROM bck.background_process 
    WHERE uuid_parent = p_uuid_parent AND status != 'REGISTRADO' and execution_mode = 'SEQUENTIAL';

    -- Si el conteo es 0, este es el inicio de la cadena
    IF v_started_count = 0 THEN
        v_target_status := 'EJECUTANDO';
        RAISE NOTICE '[CHAIN] Detectado como INICIO de cadena. Status: %', v_target_status;
    ELSE
        v_target_status := 'LANZADO';
        RAISE NOTICE '[CHAIN] Detectado como RELEVO de cadena. Status: %', v_target_status;
    END IF;

    -- 2. Buscar el SIGUIENTE proceso pendiente
    SELECT id, uuid_child, max_attempts 
    INTO v_rec
    FROM bck.background_process
    WHERE uuid_parent = p_uuid_parent AND status = 'REGISTRADO' and execution_mode = 'SEQUENTIAL'
    ORDER BY id ASC LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN 
        RAISE NOTICE '[CHAIN] No hay mas procesos. Fin de secuencia.';
        RETURN 'FIN_CADENA'; 
    END IF;

    -- 3. Bucle de Lanzamiento y Validación Física
    WHILE v_fail_count < v_rec.max_attempts LOOP
        BEGIN
            v_pid := pg_background_launch(format('SELECT bck.run_task(%L)', v_rec.uuid_child));
            
            v_launch_ok := false;
            FOR i IN 1..10 LOOP
                PERFORM pg_stat_clear_snapshot();
                IF EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = v_pid) THEN
                    v_launch_ok := true; EXIT;
                END IF;
                PERFORM pg_sleep(0.1);
            END LOOP;

            IF v_launch_ok THEN
                PERFORM pg_background_detach(v_pid);
                
                UPDATE bck.background_process 
                SET pid = v_pid, 
                    status = v_target_status, 
                    attempts = attempts + 1, 
                    date_update = clock_timestamp(),
                    start_time = CASE WHEN v_target_status = 'EJECUTANDO' THEN clock_timestamp() ELSE NULL END
                WHERE uuid_child = v_rec.uuid_child;
                
                RETURN 'OK';
            END IF;

            RAISE EXCEPTION 'Worker no visible en pg_stat_activity';

        EXCEPTION WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            UPDATE bck.background_process 
            SET failed_attempts = v_fail_count, error_msg = 'Fallo lanzamiento: ' || SQLERRM
            WHERE uuid_child = v_rec.uuid_child;
            PERFORM pg_sleep(0.5);
        END;
    END LOOP;

    UPDATE bck.background_process SET status = 'FALLIDO', error_msg = 'CADENA ROTA: Error fisico.' WHERE uuid_child = v_rec.uuid_child;
    RETURN 'ERROR_LAUNCH_FAILED';
END;
$func$;





CREATE OR REPLACE FUNCTION bck.run_task(p_child_uuid uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY INVOKER AS $func$
DECLARE
    v_rec           record;
    v_prev_status   text;
    v_chain_result  text;
BEGIN
    SELECT id, uuid_parent, query_exec, status INTO v_rec
    FROM bck.background_process WHERE uuid_child = p_child_uuid and execution_mode = 'SEQUENTIAL';

    -- 1. Si estamos en LANZADO, esperamos al hermano anterior
    IF v_rec.status = 'LANZADO' THEN
        LOOP
            SELECT status INTO v_prev_status 
            FROM bck.background_process 
            WHERE uuid_parent = v_rec.uuid_parent AND id < v_rec.id  and execution_mode = 'SEQUENTIAL'
            ORDER BY id DESC LIMIT 1;

            -- Si el anterior terminó con éxito, tomamos el control
            IF v_prev_status IS NULL OR v_prev_status = 'COMPLETADO' THEN
                UPDATE bck.background_process 
                SET status = 'EJECUTANDO', start_time = clock_timestamp() 
                WHERE id = v_rec.id;
                EXIT;
            END IF;

            -- Si el anterior falló, la cadena se rompe
            IF v_prev_status = 'FALLIDO' THEN
                UPDATE bck.background_process 
                SET status = 'FALLIDO', error_msg = 'CADENA ROTA: El proceso anterior fallo.' 
                WHERE id = v_rec.id;
                RETURN false;
            END IF;

            PERFORM pg_sleep(0.5);
        END LOOP;
    END IF;

    -- 2. Ejecución de la tarea
    BEGIN
        EXECUTE v_rec.query_exec;
        
        UPDATE bck.background_process SET status = 'COMPLETADO', end_time = clock_timestamp() WHERE id = v_rec.id;
        UPDATE bck.background_inventory SET cnt_used_bck = cnt_used_bck - 1 WHERE uuid_parent = v_rec.uuid_parent;

        -- 3. RELEVO: Disparar al siguiente
        v_chain_result := bck.fn_launch_sequential_chain(v_rec.uuid_parent);

    EXCEPTION WHEN OTHERS THEN
        UPDATE bck.background_process SET status = 'FALLIDO', error_msg = 'ERROR SQL: ' || SQLERRM, end_time = clock_timestamp() WHERE id = v_rec.id;
        UPDATE bck.background_inventory SET cnt_used_bck = cnt_used_bck - 1 WHERE uuid_parent = v_rec.uuid_parent;
        RETURN false;
    END;

    RETURN true;
END;
$func$;





