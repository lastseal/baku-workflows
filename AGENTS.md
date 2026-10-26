# Instrucciones para IA — baku-workflows

## Rol del paquete

`baku-workflows` es **solo runtime**. La lógica de negocio vive en el **servicio producto** (p. ej. `vidatec-workflows-service`). No añadir reglas de dominio de un producto en este repo salvo adaptadores genéricos del framework.

Fuente de verdad: `docs/SPEC.md` y `src/baku/workflows.py`.

## Al trabajar en un servicio que consume la librería

1. **No** modificar `baku-workflows` para un caso de un solo producto; extender el servicio producto o proponer cambio genérico en SPEC + `workflows.py`.
2. Patrón obligatorio:
   - Decoradores en módulos importados desde `main.py`.
   - `workflows.run()` como único punto de arranque.
   - Integración externa vía `await workflows.request(subject, dict)`.
3. **No** usar `nats-py`, servidor HTTP propio ni cron manual en el producto.
4. HTTP: paths bajo `/api/workflow/...`; scopes se derivan automáticamente (SPEC §2.1).
5. NATS ingest: `workflows.nats(workflow_id, match={...})`; handler `async def fn(payload: dict)`.
6. Schedule: `workflows.schedule(id="...")` + entrada en `WORKFLOW_SCHEDULES`.
7. Logging y `message` de errores API: **inglés** (SPEC §5.1).

## Al cambiar este repositorio

- Mantener API pública: `get`, `post`, `patch`, `put`, `delete`, `nats`, `schedule`, `run`, `request`, `request_sync`, `request_nest`, `request_nest_sync`.
- Cualquier cambio de contrato → actualizar `docs/SPEC.md` en el mismo cambio.
- Layout: un módulo principal `src/baku/workflows.py` (mismo criterio que `baku-api`).
- Tests: pytest en el paquete; el consumidor prueba sus handlers.

## Referencia rápida consumidor

```python
# main.py
from baku import config, workflows
import handlers  # noqa: F401
config.register_signal_handlers()
if __name__ == "__main__":
    workflows.run()
```

```python
# handlers.py
from baku import workflows

@workflows.post("/api/workflow/commands")
async def send_command(req):
    ...

@workflows.nats("device.telemetry.ingest", match={"data.worker_name": "gps-ingest-worker"})
async def ingest(payload: dict):
    ...

@workflows.schedule(id="alerts.expire_critical")
async def job():
    return await workflows.request_nest("collections", {"action": "...", "query": {}})
```

## Snippet para repos consumidores

Copiar en `.cursor/rules`, `AGENTS.md` o reglas del IDE del servicio producto:

```markdown
Workflows runtime: dependencia `baku-workflows`. Leer SPEC del servicio y [baku-workflows SPEC](https://github.com/lastseal/baku-workflows/blob/master/docs/SPEC.md) (request-reply, envelope NATS).
Nuevos endpoints: decoradores en `src/`, import en `main.py`. No editar `workflows.py` del paquete salvo necesidad genérica.
Guía humana: baku-workflows `docs/DEVELOP.md`.
```

## Archivos a leer antes de implementar

| Archivo | Uso |
|---------|-----|
| `docs/SPEC.md` | Contratos HTTP, NATS, env, collections |
| `docs/DEVELOP.md` | Montar un servicio nuevo |
| `src/baku/workflows.py` | Implementación (decoradores, run, request) |
| `vidatec-workflows-service/src/` | Ejemplos reales (ingest, schedule, commands) |
