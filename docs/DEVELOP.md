# Desarrollo — usar baku-workflows en otro proyecto

## Requisitos

- Python ≥ 3.9
- Dependencia transitiva: [baku-config](https://github.com/lastseal/baku-config)
- Infra típica: NATS, JWT alineado con `auth`, variables en `.env`

## Instalar la librería

En el servicio producto (no en este repo):

```text
git+https://github.com/lastseal/baku-config
git+https://github.com/lastseal/baku-workflows
```

Desarrollo local con path:

```bash
pip install -e /ruta/a/baku-workflows
```

## Estructura mínima del servicio

```text
mi-workflows-service/
  src/
    main.py              # workflows.run()
    handlers_*.py        # decoradores al importar
    domain.py            # lógica sin NATS/HTTP directo
  requirements.txt
  .env.example
  Dockerfile
```

## Arranque (`main.py`)

1. Importar `config` y `workflows` desde `baku`.
2. Importar módulos de handlers (side-effect: registran rutas).
3. `config.register_signal_handlers()`.
4. `workflows.run()` en `__main__`.

```python
from baku import config, workflows

import my_handlers  # noqa: F401

config.register_signal_handlers()

if __name__ == "__main__":
    workflows.run()
```

## Registrar workflows

| Tipo | Decorador | Dónde documentar contrato |
|------|-----------|---------------------------|
| REST | `workflows.get/post/...("/api/workflow/...")` | SPEC del servicio producto |
| NATS job | `workflows.nats("workflow.id", match={...})` | Subject dispatch + envelope (SPEC §4) |
| Cron | `workflows.schedule(id="job.key")` | `WORKFLOW_SCHEDULES` en env |

Handlers HTTP: `async def handler(req)` → `req.json`, `req.user`; devolver `dict`.

Errores HTTP: `raise Exception({"status": 400, "message": "..."})` (mensajes en inglés, ver SPEC §5.1).

Handlers NATS (legacy dispatch): reciben solo `payload` (no el envelope); el runtime publica ACK en `workflows.jobs.result` si hay `job_id`.

## Llamar a otros servicios

Usar **solo** `workflows.request` / `request_sync` (JSON crudo) o `request_nest` / `request_nest_sync` (handlers Nest) — no importar `nats-py` en el producto.

```python
rows = await workflows.request_nest("collections", {
    "action": "alerts.search",
    "query": {"disabled": False, "limit": 500},
})
```

Otros subjects (p. ej. `devices.command`): contrato con el listener; timeout `DEVICES_COMMAND_TIMEOUT`.

`request_sync` / `request_nest_sync` solo fuera de un event loop ya activo.

## Variables de entorno

Ver tabla en [SPEC.md §5](SPEC.md). Mínimo en producción: `JWT_SECRET`, `NATS_HOST`, `NATS_PORT`, `HTTP_PORT`.

Cron por job:

```env
WORKFLOW_SCHEDULES=alerts.expire_critical:*/5 * * * *
```

El `job_id` debe coincidir con `@workflows.schedule(id="...")`.

## Checklist nuevo servicio

- [ ] Handlers en módulos importados desde `main.py`
- [ ] Dominio en funciones sync/async; I/O bloqueante con `asyncio.to_thread` si hace falta
- [ ] Sin `nats-py` / servidor HTTP propio en el producto (solo vía `workflows`)
- [ ] `docs/SPEC.md` en el repo producto con paths REST y payloads NATS de dominio
- [ ] `.env.example` con variables del SPEC
- [ ] Docker: `CMD python src/main.py` (o equivalente)

## Probar local

1. NATS y dependencias arriba.
2. `PYTHONPATH=src python src/main.py`
3. REST con Bearer JWT que incluya scopes derivados del path (SPEC §2.1).
4. NATS: publicar envelope a `DISPATCH_SUBJECT` según SPEC §4.
