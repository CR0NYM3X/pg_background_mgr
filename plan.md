 

## El Concepto: "The Barrier Pattern" (Patrón de Barrera)


 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

pg_background_multilaunch: Lanza multiples procesos y cola automática.
pg_background_monitor: Consulta el estado de los procesos en ejecución o completados.
pg_background_stop: Finaliza procesos en segundo plano de forma controlada.

------- otra tabla que dira la cantidad de procesos a ejecutar y el uuid de proceso padre , este se usara para registrarlo en la tabla P donde  
Este solo permite ejecutar funciones no permite retornar nada!!!


1.- Tu usaras una funcion que le colocaras la cantidad de proceso que quieres y este te retornara un UUID el cual sera el que se usara para pasarselo a los procesos hijos  primero valida si el servidor permite registrar la cantidad de procesos en caso de que no retornara un error , esta informacion se registrara en una tabla:
 backgroup_inventary
  id
  uuid
  cnt_total_bck
  cnt_used


 2.- Se usara otra funcion el cual se indicara la query que quieres ejecutar y en que grupo de procesos quieres que se ejecute, cada vez que agregues una se registrara en una tabla y te dira cuantos procesos quedan libres esto lo validara en la tabla de backgroup_inventary con ayuda del uuid y tambien retornara true si se registra con exito , aparte validara si existe el uuid padre, en caso de que no haya mas procesos libres marcara un error y no te dejara agregarlos tabla background_process:

 id
 uuid_padre
 PID defaul 0
 status ('INICIALIZANDO', ejecutar, 'LISTO', 'EJECUTANDO', 'COMPLETADO').
 query_exec
 date_update
 date_insert

 3.- Fase de inciar procesos, ejecutaras una funcion que iniciara todos los procesos que deben iniciar y se lanzaran rapidamente pero todavia no se ejecutara, le puedes indicar con parametro force_cnt_proceess y true que a fuerzas tiene que estar la cantidad de procesos en la tabla backgroup_inventary o si no pues que se cancela la ejecucion , pero nunca se deben abrir menos de los que estan registrados en la tabla background_process por ejemplo tu inciaste el grupo con 50 pero solo registraste 10 , entonces no se pueden ejecutar nunca menos de 10 de lo contrario se cancelara esto solo si indicas false el force_cnt_proceess, en cada proceso se le indicara su UUID y cada uno agarrara un id no importa como lo agarre  . 


[NOTA] cada proceso se ejecutara una funcion la cual estara constantemente consultando su pid para ver si ya esta en la face de ejecutar 
[NOTA] - Tambien abra un tipo de cola el cual si superas el limite de procesos terminados los backgroud se reciclan esto permite ejecutar varios no en paralelo 
[NOTA] agregar intentos fallidos y exitosos a los backgroud y Agregarle barra de progreso en caso de que se ejecute por psql


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

* Tengo que crear una funcion X que ejecutara cada proceso, esa funcion entra en un bucle y estara validando su pid si ya esta listo para ejecutarse y ejecutara la query que se le indico.
* 

### 2. Flujo de Trabajo Propuesto

Para evitar la inconsistencia que mencionas, el flujo debe ser el siguiente:

1. **Orquestador:** Registra el Lote en la tabla con `Total_Esperado = 50`.
2. **Lanzamiento:** Lanza los 50 procesos usando `pg_background_launch`.
3. **Fase de Registro (Check-in):** Cada worker, al iniciar, hace un `UPDATE` en la tabla de control marcando su `PID` y estado como 'LISTO'.
4. **La Barrera de Validación:** * Cada worker ejecuta un loop de espera (polling) consultando: `SELECT COUNT(*) FROM tabla WHERE lote_id = X AND estado = 'LISTO'`.
* **Si el conteo llega a 50:** Todos los procesos proceden.
* **Si pasa un Timeout (ej. 10 segundos) y el conteo es < 50:** El proceso detecta que el "ejército" está incompleto.


5. **Aborto Seguro:** Si no están los 50, cada worker realiza un `ROLLBACK` y se cierra. El sistema queda limpio y tú recibes una alerta de que el lote no pudo inicializarse.

---

## Ventajas de este Enfoque en PostgreSQL

* **Sincronía Forzada:** Al usar una tabla intermedia, conviertes un proceso asíncrono por naturaleza en uno coordinado.
* **Visibilidad Total:** Ya no tendrás que "adivinar" si faltaron 20. Una simple consulta a la tabla de control te dirá exactamente qué PIDs no subieron.
* **Atomicidad Lógica:** Si falla la creación de un solo worker (por falta de slots en `max_worker_processes`), el resto no procesa datos basura.

### ¿Por qué no usar solo `max_parallel_workers`?

PostgreSQL gestiona el paralelismo interno para queries (`SELECT`, `JOIN`), pero para **tareas de escritura o lógica procedimental compleja**, las extensiones como `pg_background` o `pg_cron` son necesarias. Sin embargo, PostgreSQL no sabe que tus 50 workers están relacionados entre sí; para el motor, son entes independientes. Tu tabla de control les da la "conciencia de grupo".

---

## Consideración Crítica: `max_worker_processes`

Si vas a lanzar 50 procesos de golpe, debes asegurarte de que tu archivo `postgresql.conf` esté preparado. De lo contrario, `pg_background` fallará silenciosamente al intentar asignar un slot que no existe.

* **`max_worker_processes`**: Debe ser mayor a 50 + procesos del sistema.
* **`max_parallel_workers`**: Ajustar según los núcleos de tu CPU.
 
