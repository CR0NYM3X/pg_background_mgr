CREATE OR REPLACE FUNCTION bck.fn_dispatch_background_jobs(p_uuid_parent uuid)
RETURNS text 
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO bck, public, pg_temp
AS $func$
DECLARE
    -- Control Variables
    v_mode       text;
    v_count      integer;
    v_result     text;
    ex_message   text;
BEGIN
    -- 1. VALIDATION AND MODE DETECTION
    -- Identify the predominant execution mode for 'REGISTRADO' status
    SELECT execution_mode, count(*) 
    INTO v_mode, v_count
    FROM bck.background_process
    WHERE uuid_parent = p_uuid_parent
      AND status = 'REGISTRADO'
    GROUP BY execution_mode
    ORDER BY count(*) DESC
    LIMIT 1;

    -- Abort if no pending tasks are found
    IF v_count IS NULL OR v_count = 0 THEN
        RETURN 'ERROR: No "REGISTRADO" processes found for the provided UUID.';
    END IF;

    RAISE NOTICE '[MASTER] Dispatching batch in % mode (% tasks)', v_mode, v_count;

    -- 2. DISPATCHER LOGIC
    CASE v_mode
        WHEN 'PARALLEL' THEN
            v_result := bck.fn_launch_parallel_sync(p_uuid_parent);
        
        WHEN 'SEQUENTIAL' THEN
            v_result := bck.fn_launch_sequential_chain(p_uuid_parent);
            
        WHEN 'RANDOM' THEN
            v_result := bck.fn_launch_random_swarm(p_uuid_parent);
            
        ELSE
            RAISE EXCEPTION 'Execution mode "%" is invalid or not implemented.', v_mode;
    END CASE;

    -- 3. RETURN SUMMARY
    RETURN format('Batch %s processed via %s mode. Engine Detail: %s', p_uuid_parent, v_mode, v_result);

-- CRITICAL ERROR HANDLING
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS ex_message = MESSAGE_TEXT;
        RAISE EXCEPTION 'Critical failure in Master Dispatcher: %', ex_message;
END;
$func$;
