

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
            v_pid := pg_background_launch(format('SELECT bck.run_task_sequential(%L)', v_rec.uuid_child));
            
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
            PERFORM pg_sleep(0.1);
        END;
    END LOOP;

    UPDATE bck.background_process SET status = 'FALLIDO', error_msg = 'CADENA ROTA: Error fisico.' WHERE uuid_child = v_rec.uuid_child;
    RETURN 'ERROR_LAUNCH_FAILED';
END;
$func$;




CREATE OR REPLACE FUNCTION bck.run_task_sequential(p_child_uuid uuid)
RETURNS boolean LANGUAGE plpgsql SECURITY INVOKER AS $func$
DECLARE
    V_max_attempts   integer;
    v_fail_count     integer := 0;
    v_prev_status    text;
    v_chain_result   text;
    v_rec            record;
BEGIN
    -- 1. Obtener datos iniciales (incluyendo max_attempts)
    SELECT id, uuid_parent, query_exec, status, max_attempts 
    INTO v_rec
    FROM bck.background_process 
    WHERE uuid_child = p_child_uuid AND execution_mode = 'SEQUENTIAL';

    V_max_attempts := v_rec.max_attempts;

    -- 2. Lógica de Espera Secuencial
    IF v_rec.status = 'LANZADO' THEN
        LOOP
            SELECT status INTO v_prev_status 
            FROM bck.background_process 
            WHERE uuid_parent = v_rec.uuid_parent AND id < v_rec.id AND execution_mode = 'SEQUENTIAL'
            ORDER BY id DESC LIMIT 1;

            IF v_prev_status IS NULL OR v_prev_status = 'COMPLETADO' THEN
                UPDATE bck.background_process SET status = 'EJECUTANDO', start_time = clock_timestamp() WHERE id = v_rec.id;
                EXIT;
            END IF;

            IF v_prev_status = 'FALLIDO' THEN
                UPDATE bck.background_process SET status = 'FALLIDO', error_msg = 'CADENA ROTA: El proceso anterior falló.' WHERE id = v_rec.id;
                RETURN false;
            END IF;

            PERFORM pg_sleep(0.1);
        END LOOP;
    END IF;

    -- 3. BUCLE DE EJECUCIÓN (Reintentos de Query)
    WHILE v_fail_count < V_max_attempts LOOP
        BEGIN
            -- Intentar ejecutar la tarea
            EXECUTE v_rec.query_exec;
            
            -- Si llegamos aquí, fue ÉXITO
            UPDATE bck.background_process 
            SET status = 'COMPLETADO', end_time = clock_timestamp(), error_msg = NULL 
            WHERE id = v_rec.id;
            
            UPDATE bck.background_inventory SET cnt_used_bck = cnt_used_bck - 1 WHERE uuid_parent = v_rec.uuid_parent;

            -- RELEVO: Disparar al siguiente
            v_chain_result := bck.fn_launch_sequential_chain(v_rec.uuid_parent);
            
            RETURN true; -- Finaliza el worker con éxito

        EXCEPTION WHEN OTHERS THEN
            -- Manejo del fallo
            v_fail_count := v_fail_count + 1;
            
            -- Actualizar la tabla con el intento fallido
            UPDATE bck.background_process 
            SET failed_attempts = v_fail_count, 
                error_msg = 'Intento ' || v_fail_count || ' fallido: ' || SQLERRM,
                date_update = clock_timestamp()
            WHERE id = v_rec.id;

            -- Si aún quedan intentos, esperamos un poco y el WHILE hará la siguiente vuelta
            IF v_fail_count < V_max_attempts THEN
                PERFORM pg_sleep(0.1);
            ELSE
                -- Si ya no hay intentos, marcamos FALLIDO definitivo y cerramos inventario
                UPDATE bck.background_process SET status = 'FALLIDO', end_time = clock_timestamp() WHERE id = v_rec.id;
                UPDATE bck.background_inventory SET cnt_used_bck = cnt_used_bck - 1 WHERE uuid_parent = v_rec.uuid_parent;
                RETURN false; -- Rompe la cadena
            END IF;
        END;
    END LOOP;
    
    RETURN false;
END;
$func$;



/**


 CREATE TABLE clientes (
    cliente_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    fecha_registro TIMESTAMP DEFAULT now(),
    puntos_lealtad INTEGER DEFAULT 0,
    activo BOOLEAN DEFAULT true
);


INSERT INTO clientes (nombre, email, puntos_lealtad)
VALUES (
    'Usuario_' || substr(md5(random()::text), 1, 8), -- Nombre aleatorio corto
    'test_' || substr(md5(random()::text), 1, 5) || '@ejemplo.com', -- Email único
    floor(random() * 1000)::int -- Puntos entre 0 y 999
);

select * from clientes;

  select * from bck.fn_crear_inventario(1000);

SELECT bck.fn_registrar_proceso(
    p_uuid_parent       := 'e1ce73f0-0f57-4b8c-b31a-e6a8f23e07f7', 
    p_queries           := ARRAY['$$ INSERT INTO clientes (nombre, email, puntos_lealtad)
VALUES (
    'Usuario_' || substr(md5(random()::text), 1, 8), -- Nombre aleatorio corto
    'test_' || substr(md5(random()::text), 1, 5) || '@ejemplo.com', -- Email único
    floor(random() * 1000)::int -- Puntos entre 0 y 999
); $$], 
    p_process_name      := 'TEST_INDIVIDUAL', 
    p_mode              := 'PARALLEL', 
    p_repeat            := 1000
);



 update bck.background_process set status = 'REGISTRADO' , failed_attempts = 0, error_msg = null , execution_mode = 'SEQUENTIAL';
 
SELECT bck.fn_launch_sequential_chain('e1ce73f0-0f57-4b8c-b31a-e6a8f23e07f7');




**/

