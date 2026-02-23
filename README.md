# .........EN PROCESO DE DESARROLLO DE FUNCIONES.........

# pg_background_mgr
pg_background_mgr —  Colección de funciones para lanzar, monitorear y controlar procesos en segundo plano dentro de PostgreSQL. El sistema administra múltiples tareas con control de concurrencia, cola dinámica y herramientas de seguimiento en tiempo real.  utiliza la extensión pg_background.

Beneficio: Te permitira ejecutar de forma paralela y no de forma escalonada , esto permite controlar cualquier error que se pueda presentar ya que hay escenarios donde uno puede mandar ejecutar 50 procesos y resulto que solo se ejecutaron 30 y faltaron 20 y esos hay 
que ejecutarlos de manera manual y aveces uno ni cuenta se da cuenta si no hasta que ve inconsistencia de los datos, entonces esta nueva forma fuerza a que se abran los 50 procesos y si no se abren entonces se cancela de lo contrario entonces ya que se detecta que estan los 50 proceos se les indica a los proceso que continuen , esto gracias a una tabla donde se registran los id de los procesos, esto permitira trabajar de manera sincrona.
esto sirve en los escenarios donde si un proceso falta puedes obtener resultados incorrectos,

Incluye:
- `pg_background_multilaunch`: Lanza multiples procesos y cola automática.
- `pg_background_monitor`: Consulta el estado de los procesos en ejecución o completados.
- `pg_background_stop`: Finaliza procesos en segundo plano de forma controlada.


 
### 🗃️ **log_background: Registro y trazabilidad de procesos**
Esta tabla captura información detallada de cada proceso lanzado por las funciones del sistema. Ayuda a auditar, monitorear y entender cómo se ejecutan las tareas en segundo plano dentro de PostgreSQL.

**Campos clave:**
- `id`: Identificador único del proceso.
- `pid`: ID del proceso del sistema operativo.
- `status`: Estado actual (ejecutando, terminado, detenido).
- `ip_client`: IP del cliente que solicitó el proceso.
- `user_name`: Usuario que lanzó la tarea.
- `uuid_parent` / `uuid_child`: Permiten rastrear jerarquía o procesos encadenados.
- `query`: Consulta ejecutada en el proceso.
- `result`: Salida o respuesta de la consulta.
- `start_time_exec` / `end_time_exec`: Marcas de tiempo que permiten calcular duración y rendimiento.



```sql


/**
CREATE EXTENSION "uuid-ossp";
CREATE EXTENSION pg_background;

**/

--- Te retorna una tabla con los pid de los procesos,status, lo que se ejecuto y fecha_inicio y fecha_fin cuendo se ejecuto y cuanto tiempo tiene , esta tabla se va actualizando, este va monitoreando los procesos y actualizando la tabla y una vez que terminen todos los proceos de ejecutarse se finaliza.  ESTE TAMBIEN VALIDA CUANDO UNO YA TERMINO DE EJECUTARSE PARA EJECUTAR PROCESOS EN COLA QUE NO SE PUDIERON EJECUTAR POR MOTIVO DE MAXIMO PROCESOS EN EJECUCION. SI NO DEFINES NINGUN PARAMETRO TE MOSTRARA LA TABLA 
-- STATUS : 'running' , 'sleep', 'stop', 'wait' , 
pg_background_monitor(uuid_father TEXT, p_show_verbose BOOLEAN true )


pg_background_stop(uuid_father_or_child TEXT)

--- Ejecutar multiples querys y activar protecion de lanzamiento, que valida que todos los procesos se hayan ejecutado
/*
- Lanzamiento síncrono: La ejecución espera que el proceso antes de continuar.
- Lanzamiento asíncrono:  La ejecución no espera: lanza el proceso y sigue con lo demás.
*/-- VALIDA LA CANTIDAD MAXIMA DE PROCESOS PERMITIDOS EN CASO DE QUE NO LOS METE EN COLA PARA CUANDO TERMINE UN PROCESO EJECUTO OTRO. 
-- primero va ejecutar el pg_monitor con la cantidad total de pg_backgruond a monitorear 



-- DROP FUNCTION pg_background_multilaunch(TEXT,TEXT,BOOLEAN);
CREATE OR REPLACE FUNCTION pg_background_multilaunch(
                          p_querys TEXT,
						  p_delimiter TEXT DEFAULT ',',
						  p_monitor BOOLEAN DEFAULT TRUE
						  --p_exec_type TEXT DEFAULT 'sync',
                        )
RETURNS VOID
/*TABLE
(
   pid INT,
   status TEXT,
   uuid_parent UUID,
   uuid_child UUID,
   

)*/
AS $$
DECLARE
        v_start_time timestamp;
        v_end_time timestamp;
		v_uuid_parent UUID := uuid_generate_v4() ;
		v_list_uuid_child uuid[]; 
		v_element_foreach TEXT;
		v_cnt_querys INT := array_length(string_to_array(p_querys,p_delimiter), 1);
		v_cnt_proc_allow INT := current_setting('max_worker_processes')::INT - 2;
		v_query_insert := 'INSERT INTO log_background(status,ip_client,user_name,uuid_parent,uuid_child,query,msg) ';
BEGIN
	v_start_time := clock_timestamp(); 	
	
	IF v_cnt_querys > v_cnt_proc_allow )   THEN
		RAISE EXCEPTION 'Estas intentando abrir [%] procesos y solo tienes permitido (% - 2), modifica el parametro max_worker_processes', v_cnt_querys, current_setting('max_worker_processes');
	END IF;

	SELECT array_agg(uuid_generate_v4() ) INTO v_list_uuid_child  FROM generate_series(1, v_cnt_querys);
	-- pg_background_monitor(v_uuid_parent);
	
	
	FOREACH v_element_foreach IN ARRAY string_to_array(p_querys,p_delimiter) LOOP		
		RAISE NOTICE '%' , v_element_foreach;	
		
		
		
		
	END LOOP;
	
	v_end_time := clock_timestamp(); 	
	
END;
$$ 
LANGUAGE plpgsql 
SECURITY DEFINER 
SET client_min_messages = 'notice' 
SET log_statement  = 'none' 
SET log_min_messages = 'panic'
SET statement_timeout = 0		
SET lock_timeout = 0 ;

 
 select * from pg_background_multilaunch('select version();,select version(),select version(),select version(),select version(),select version(),select version();  ');
 








select * from pg_background_launch($$  INSERT INTO empleados (nombre, puesto, salario) VALUES ('taizon Gómez', 'Desarrolladora', 85000.00); $$) where pg_sleep(4);



coalesce( host(inet_client_addr()) , '127.0.0.1'),session_user
INSERT INTO log_background(status,ip_client,user_name,uuid_parent,uuid_child,query,msg)

-- select * from log_background limit 10
-- DROP TABLE log_background;
 CREATE TABLE log_background
 (id             bigserial primary key, 
 pid    INT,
 status          character varying(100),      
 ip_client       character varying(15),
 user_name       character varying(255),
 uuid_parent     uuid,                        
 uuid_child      uuid,                        
 query           text,
 result          text,                        
 -- msg             text,                        
 start_time_exec TIMESTAMP default (clock_timestamp())::timestamp without time zone,
 end_time_exec   timestamp without time zone);
SELECT * FROM log_background;





```
