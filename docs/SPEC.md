# baku-workflows — Especificación

Paquete Python expuesto como `from baku import workflows`. Runtime único (asyncio): HTTP, NATS y schedule. Los servicios producto (p. ej. `vidatec-workflows-service`) registran handlers con decoradores al importar módulos y arrancan con `workflows.run()`.

Dependencias: `baku-config`, `nats-py`, `aiohttp`, `PyJWT`, `croniter`.

Guías: [DEVELOP.md](DEVELOP.md) (integrar en otro proyecto), [AGENTS.md](../AGENTS.md) (IA).

---

## 1. Arranque

```python
from baku import config, workflows

import my_handlers  # decoradores registran flujos al importar

config.register_signal_handlers()

if __name__ == "__main__":
    workflows.run()
```

`workflows.run()`:

1. Lee configuración (`baku-config`).
2. Recolecta handlers registrados por decoradores.
3. Levanta en un solo event loop: servidor HTTP, suscriptores NATS (queue group), scheduler.
4. Por cada invocación: ejecuta handler async y **responde al origen** (cliente HTTP o ACK/reply NATS según trigger).

---

## 2. API pública

### 2.1 Decoradores HTTP

Verbos dedicados (no un decorador genérico con método):

- `workflows.get(path)`
- `workflows.post(path)`
- `workflows.patch(path)`
- `workflows.put(path)`
- `workflows.delete(path)`

**No** aceptan `scope` ni `id` en el decorador. Se derivan del path y del verbo HTTP.

#### Reglas de derivación

Path sin query, con barra inicial. Se ignora el prefijo `/api` si existe.

| Path ejemplo | Workflow `id` | Scope (POST) | Scope (GET) |
|--------------|---------------|--------------|-------------|
| `/api/workflow/commands` | `workflow.commands` | `workflow:commands:writable` | `workflow:commands:readonly` |

Reglas:

- **id:** segmentos tras `/api/` unidos por `.` (sin barra final).
- **scope:** `{id con puntos → dos puntos}:writable` para POST, PATCH, PUT, DELETE; `:readonly` para GET.
- Prefijo de scope siempre `workflow:` cuando el path empieza por `/api/workflow/`.

El handler recibe `req` (JSON body en `req.json`, JWT ya validado contra `auth`). Debe devolver `dict` serializable a JSON; el framework mapea a respuesta HTTP al **mismo cliente** que llamó.

Errores de negocio: convención compatible con servicios Baku existentes, p. ej. `raise Exception({"status": 400, "message": "..."})`.

### 2.2 Decorador NATS

```python
@workflows.nats("device.telemetry.ingest", match={"data.worker_name": "gps-ingest-worker"})
async def telemetry_ingest(payload: dict) -> dict:
    ...
```

- **Primer argumento (`workflow_id`):** identificador estable del workflow (string).
- **match:** filtra el mensaje entrante (envelope legacy o interno; ver §4).
- Subject de suscripción y queue group: variables de entorno (§5).
- Tras el handler, el framework publica resultado para correlación (p. ej. `workflows.jobs.result` + `job_id`) cuando el trigger lo exige.

### 2.3 Decorador schedule

```python
@workflows.schedule()
async def expire_critical_alerts(_ctx=None) -> dict:
    ...
```

- **id:** nombre de la función o clave en configuración cron.
- Expresión cron / timezone: `baku-config` (§5).

---

## 3. Request-reply (`workflows.request`)

Uso desde handlers o dominio del servicio producto. **Sin** `nats-py` en el código producto.

```python
result = await workflows.request("devices.command", payload)
result = workflows.request_sync("collections", {"action": "devices.search", "query": payload})
```

- **Primer argumento:** subject NATS (string).
- **Segundo argumento:** `payload` serializado a JSON (dict de negocio del producto).
- **Retorno:** cuerpo JSON de la respuesta NATS (`dict`, `list`, etc.).

El runtime solo hace **request-reply** al tópico indicado (timeout vía `DEVICES_COMMAND_TIMEOUT`). No interpreta `collections`, `devices` ni el contenido del payload; eso es contrato entre el servicio producto y quien escucha cada subject.

### Subject `collections`

Subject fijo: `"collections"`. El payload incluye `action` (`<colección>.<operación>`).

| Operación | `action` | Campos | Retorno |
|-----------|----------|--------|---------|
| search | `alerts.search` | `query` (filtros: `disabled`, `limit`, `order`, …) | `list[dict]` |
| create | `traffic.create` | `data` (documento) | `dict` |
| update | `alerts.update` | `query` (p. ej. `id` uuid o lista) + `data` | `{ "updated": N }` si varios ids |

```python
alerts = await workflows.request(
    "collections",
    {"action": "alerts.search", "query": {"disabled": False, "limit": 500}},
)
await workflows.request(
    "collections",
    {
        "action": "alerts.update",
        "query": {"id": ["<uuid>", "..."]},
        "data": {"disabled": True, "disabled_at": "...", "disabled_reason": "..."},
    },
)
```

Tipos en documentos y en query coinciden con el mantenedor (p. ej. `disabled` es `bool`).

### Subjects gateway y otros servicios

```python
result = await workflows.request("devices.command", {
    "device_id": "<imei>",
    "command": "CR",
    "parameters": {},
})
```

Compatibilidad con publicadores legacy (TCP dispatch, `devices.command`) mediante adaptadores en el runtime.

---

## 4. Triggers NATS — telemetría (compatibilidad Vigia)

**Entrada legacy** (publicador `vidatec-tcp-service`):

Subject default: `workflows.jobs.dispatch`

```json
{
  "pattern": "workflows.jobs.dispatch",
  "data": {
    "worker_name": "gps-ingest-worker",
    "job_id": "<uuid>",
    "payload": { }
  }
}
```

El handler recibe **`payload`** (contenido de negocio), no el envelope completo.

**Salida ACK gateway:** publish a `workflows.jobs.result`:

```json
{
  "data": {
    "job_id": "<uuid>",
    "success": true
  }
}
```

Con error: `"success": false`, `"error": "<código o mensaje>"`.

---

## 5. Variables de entorno

| Variable | Default | Uso |
|----------|---------|-----|
| `HTTP_PORT` | `3000` | REST |
| `JWT_SECRET` | — | Validación JWT (alineado con `auth`) |
| `NATS_HOST` | `localhost` | |
| `NATS_PORT` | `4222` | |
| `NATS_QUEUE_GROUP` | `workflows` | Consumidores |
| `DISPATCH_SUBJECT` | `workflows.jobs.dispatch` | Ingest |
| `RESULT_SUBJECT` | `workflows.jobs.result` | ACK TCP |
| `DEVICES_COMMAND_TIMEOUT` | `5` | Request-reply (segundos) |
| `WORKFLOW_TIMEZONE` | `UTC` | Schedule |
| `LOG_LEVEL` | `INFO` | |

Cron por workflow: definición en configuración (TBD formato v1).

### 5.1 Logging

- Framework and product services: **English** for `logging` (`info`, `warning`, `error`, `debug`, `exception`) and for machine-oriented API/workflow error `message` fields returned over HTTP or NATS.
- Level via `LOG_LEVEL` (`baku-config`). End-user UI copy (web/mobile) stays in the product locale; that is outside this package.

---

## 6. Referencia consumidor

Primer consumidor: **`vidatec-workflows-service`** (`src/main.py`, handlers en `src/*.py`).

Contratos REST/NATS de dominio Vigia: SPEC de ese repositorio.

---

## 7. Implementación

Mismo layout que **`baku-api`**: un solo módulo en el paquete `baku`.

```text
src/baku/workflows.py   # decoradores, run(), request / request_sync
```
