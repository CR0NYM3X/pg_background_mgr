
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



 
