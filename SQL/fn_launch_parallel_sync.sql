



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
    WHERE uuid_parent = p_uuid_parent AND status = 'REGISTRADO' AND execution_mode = 'PARALLEL';

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
$func$
SET client_min_messages = 'notice' 
SET log_statement  = 'none' 
SET log_min_messages = 'panic'
SET statement_timeout = 0		
SET lock_timeout = 0 ;





-- Revocar público
REVOKE ALL ON FUNCTION bck.fn_launch_parallel_sync(uuid) FROM PUBLIC;

-- Dar permisos al rol que ejecutará los workers
GRANT EXECUTE ON FUNCTION bck.fn_launch_parallel_sync(uuid) TO PUBLIC;

-- Seguridad adicional: Forzar search_path para evitar ataques de shadowing
ALTER FUNCTION bck.fn_launch_parallel_sync(uuid) SET search_path TO bck, public, pg_temp;
 




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
    WHERE uuid_child = p_child_uuid AND execution_mode = 'PARALLEL';

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
$func$
SET client_min_messages = 'notice' 
SET log_statement  = 'none' 
SET log_min_messages = 'panic'
SET statement_timeout = 0		
SET lock_timeout = 0 ;


-- Revocar público
REVOKE ALL ON FUNCTION bck.run_task_parallel(uuid) FROM PUBLIC;

-- Dar permisos al rol que ejecutará los workers
GRANT EXECUTE ON FUNCTION bck.run_task_parallel(uuid) TO PUBLIC;

-- Seguridad adicional: Forzar search_path para evitar ataques de shadowing
ALTER FUNCTION bck.run_task_parallel(uuid) SET search_path TO bck, public, pg_temp;
 



