
# 📊 Orquestador de Tareas en Segundo Plano (pg_background)

Un sistema robusto de orquestación de alto rendimiento para **PostgreSQL** que utiliza la extensión `pg_background` para gestionar tareas asíncronas en entornos de producción. Soporta múltiples estrategias de ejecución: **Paralelo**, **Secuencial (Cadenas)** y **Aleatorio (Enjambre/Swarm)**.

## 🚀 Características Principales

* **Ejecución Multi-Modo:**
* `PARALLEL`: Lanzamiento masivo síncrono con un "Kill Switch" global.
* `SEQUENTIAL`: Ejecución encadenada donde cada tarea dispara la siguiente (El orden importa).
* `RANDOM (Swarm)`: Workers auto-propagados para colas de alto volumen.
 

---

## 🏗️ Descripción de la Arquitectura

El sistema se divide en tres capas principales:

1. **Inventario y Registro:** Funciones para crear un lote (`fn_crear_inventario`) y registrar consultas (`fn_registrar_proceso`).
2. **Motores (Engines):** Orquestadores especializados para cada modo de ejecución.
3. **Despachador Maestro (Dispatcher):** El punto de entrada profesional `fn_dispatch_background_jobs` que auto-deriva el lote al motor correcto.

---

## 🛠️ Instalación y Configuración

### Prerrequisitos

* PostgreSQL 12 o superior.
* Extensión `pg_background` instalada y cargada.

```sql
CREATE EXTENSION pg_background;
CREATE SCHEMA bck;

```

### Componentes Core

Ejecuta los scripts SQL en el siguiente orden:

1. **Esquema y Tablas:** `background_inventory`, `background_process`, `background_config`.
2. **Vistas:** `vw_status_progreso`.
3. **Funciones Worker:** `run_task_parallel`, `run_task_sequential`, `run_task_random`.
4. **Motores:** `fn_launch_parallel_sync`, `fn_launch_sequential_chain`, `fn_launch_random_swarm`.
5. **Despachador Maestro:** `fn_dispatch_background_jobs`.

---

## 📖 Ejemplo de Uso

### 1. Inicializar un Lote

Crea un inventario para 100 slots:

```sql
SELECT bck.fn_crear_inventario(100) AS batch_uuid;
-- Resultado: 'e1ce73f0-0f57-4b8c-b31a-e6a8f23e07f7'

```

### 2. Registrar Procesos

Registra una tarea de inserción masiva en modo **RANDOM** con 50 repeticiones:

```sql
SELECT bck.fn_registrar_proceso(
    p_uuid_parent   := 'e1ce73f0-0f57-4b8c-b31a-e6a8f23e07f7', 
    p_queries       := ARRAY['INSERT INTO mi_tabla SELECT ...'], 
    p_process_name  := 'IMPORTACION_MASIVA', 
    p_mode          := 'RANDOM', 
    p_repeat        := 50
);

```

### 3. Lanzamiento vía Despachador Maestro

La forma profesional de iniciar la ejecución:

```sql
SELECT bck.fn_dispatch_background_jobs('e1ce73f0-0f57-4b8c-b31a-e6a8f23e07f7');

```

### 4. Monitorear Progreso

Consulta el estado en tiempo real:

```sql
SELECT * FROM bck.vw_status_progreso;
-- O vía psql: \watch 1

```

 
---



## 1. Modo PARALLEL (Sincronización Masiva)

**Concepto:** Se lanzan todas las tareas al mismo tiempo. Es ideal para procesos que deben terminar rápido y donde el servidor tiene capacidad para procesar todo el lote de golpe.

* **Mantenimiento de Índices:** Reconstruir varios índices de una tabla grande simultáneamente (`REINDEX`) o ejecutar `VACUUM ANALYZE` en múltiples tablas pequeñas al mismo tiempo.
* **Reportes Segmentados:** Si tienes una tabla de ventas y necesitas calcular el total por cada mes del año, puedes lanzar 12 procesos en paralelo (uno por mes) para consolidar los datos en una tabla de resultados en segundos.
* **Carga de Archivos Particionados:** Insertar datos desde diferentes fuentes externas hacia particiones específicas de una tabla maestra.
* **Notificaciones Masivas:** Disparar llamadas a procedimientos que envían correos o alertas de forma simultánea cuando el tiempo de respuesta es crítico.


## 2. Modo SEQUENTIAL (Cadena de Dependencias)

**Concepto:** Cada tarea depende de que la anterior haya terminado con éxito. Si una falla, la cadena se rompe por seguridad.

* **Flujos ETL Complejos:** 1. Primero: Limpiar la tabla temporal.
2. Segundo: Cargar datos desde el archivo.
3. Tercero: Validar integridad.
4. Cuarto: Mover a la tabla productiva.
* **Cierres Contables:** Procesos donde no puedes calcular el balance general si antes no se han cerrado los auxiliares y luego los diarios.
* **Actualizaciones de Esquema con Dependencias:** Alterar una tabla, luego crear una vista que depende de esa alteración y finalmente otorgar permisos sobre esa vista.
* **Procesamiento de Lotes con Estado:** Cuando la Tarea B necesita los datos transformados por la Tarea A para poder operar correctamente.


## 3. Modo RANDOM / SWARM (Enjambre de Relevos)

**Concepto:** Ideal para colas de trabajo inmensas (miles de registros) donde quieres mantener un paralelismo constante (ej. siempre 20 workers activos) sin saturar el procesador.

* **Procesamiento de Imágenes o Documentos:** Si tienes 50,000 registros que requieren una transformación pesada (ej. extraer texto con una función de IA o PL/Python), el modo Swarm irá procesando "n" registros a la vez de forma incansable.
* **Web Scraping / Consultas Externas:** Realizar peticiones API a servicios externos desde la base de datos sin bloquear la sesión del usuario y respetando un límite de concurrencia para no ser bloqueado por el proveedor.
* **Limpieza de Logs Históricos:** Borrar millones de registros antiguos en bloques de 10,000 para no inflar el log de transacciones (`WAL`) y no bloquear la tabla por demasiado tiempo.
* **Generación de Facturación Masiva:** Procesar miles de facturas individuales al final del mes; cada worker toma una factura, la genera y, al terminar, "le pasa la estafeta" al siguiente en la cola.


## Resumen de Selección

| Modo | ¿Cuándo usarlo? | Ventaja Principal |
| --- | --- | --- |
| **PARALLEL** | Pocas tareas (< 20) muy pesadas. | Velocidad bruta instantánea. |
| **SEQUENTIAL** | Pasos lógicos obligatorios (A -> B -> C). | Integridad y control de flujo. |
| **RANDOM** | Miles de tareas pequeñas o medianas. | Estabilidad del servidor (no se satura). |

 
