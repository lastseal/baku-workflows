# -*- coding: utf-8 -*-

from baku import config

from aiohttp import web
from croniter import croniter
from datetime import datetime
from zoneinfo import ZoneInfo

import asyncio
import json
import jwt
import logging
import nats
import uuid

_WRITABLE_METHODS = frozenset({"POST", "PATCH", "PUT", "DELETE"})

DISPATCH_SUBJECT = config.get("DISPATCH_SUBJECT", default="workflows.jobs.dispatch")
RESULT_SUBJECT = config.get("RESULT_SUBJECT", default="workflows.jobs.result")
NATS_HOST = config.get("NATS_HOST", default="localhost")
NATS_PORT = config.get("NATS_PORT", default="4222")
NATS_QUEUE_GROUP = config.get("NATS_QUEUE_GROUP", default="workflows")
NATS_REQUEST_TIMEOUT = float(config.get("DEVICES_COMMAND_TIMEOUT", default=5))

_nc = None
_schedule_last_minute = {}

_HTTP_ROUTES = []
_NATS_HANDLERS = []
_SCHEDULE_JOBS = []


class HttpRequest:
    def __init__(self, json_body, user):
        self.json = json_body
        self.user = user


def _workflow_id_from_path(path):
    path = path.split("?", 1)[0].rstrip("/") or "/"
    if path.startswith("/api/"):
        path = path[5:]
    elif path.startswith("/"):
        path = path[1:]
    return path.replace("/", ".")


def _scope_from_path(path, method):
    workflow_id = _workflow_id_from_path(path)
    suffix = "writable" if method.upper() in _WRITABLE_METHODS else "readonly"
    return f"{workflow_id.replace('.', ':')}:{suffix}"


def _jwt_secret():
    return config.get("JWT_SECRET") or config.get("SECRET_KEY")


def _nats_url():
    return f"nats://{NATS_HOST}:{NATS_PORT}"


def _timezone():
    name = config.get("WORKFLOW_TIMEZONE", default="UTC")
    try:
        return ZoneInfo(name)
    except Exception:
        logging.warning("unknown WORKFLOW_TIMEZONE %s, using UTC", name)
        return ZoneInfo("UTC")


def _parse_schedules():
    raw = config.get("WORKFLOW_SCHEDULES") or ""
    jobs = {}
    for line in raw.replace(",", "\n").splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        job_id, cron = line.split(":", 1)
        jobs[job_id.strip()] = cron.strip()
    return jobs


def _decode_reply(data):
    if not data:
        return None
    return json.loads(data.decode("utf-8"))


def _nest_rpc_envelope(pattern, data):
    return {
        "id": str(uuid.uuid4()),
        "pattern": pattern,
        "data": data,
    }


def _unwrap_nest_reply(parsed):
    if parsed is None:
        return None
    if not isinstance(parsed, dict):
        return parsed
    err = parsed.get("err")
    if err is not None:
        if isinstance(err, dict):
            message = err.get("message") or str(err)
        else:
            message = str(err)
        raise RuntimeError(message)
    if "response" in parsed:
        return parsed["response"]
    return parsed


async def request(subject, payload):
    if _nc is None:
        raise RuntimeError("NATS is not connected")
    msg = await _nc.request(
        subject,
        json.dumps(payload).encode("utf-8"),
        timeout=NATS_REQUEST_TIMEOUT,
    )
    return _decode_reply(msg.data)


def request_sync(subject, payload):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(request(subject, payload))
    raise RuntimeError("request_sync cannot be used inside a running event loop; use await request")


async def request_nest(pattern, payload):
    """Request-reply to a NestJS @MessagePattern handler (NATS transport envelope)."""
    if _nc is None:
        raise RuntimeError("NATS is not connected")
    envelope = _nest_rpc_envelope(pattern, payload)
    msg = await _nc.request(
        pattern,
        json.dumps(envelope).encode("utf-8"),
        timeout=NATS_REQUEST_TIMEOUT,
    )
    return _unwrap_nest_reply(_decode_reply(msg.data))


def request_nest_sync(pattern, payload):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(request_nest(pattern, payload))
    raise RuntimeError(
        "request_nest_sync cannot be used inside a running event loop; use await request_nest"
    )


def _register_http(method, path, handler):
    _HTTP_ROUTES.append({
        "path": path,
        "method": method.upper(),
        "scope": _scope_from_path(path, method),
        "handler": handler,
    })
    return handler


def get(path):
    def decorator(handler):
        return _register_http("GET", path, handler)
    return decorator


def post(path):
    def decorator(handler):
        return _register_http("POST", path, handler)
    return decorator


def patch(path):
    def decorator(handler):
        return _register_http("PATCH", path, handler)
    return decorator


def put(path):
    def decorator(handler):
        return _register_http("PUT", path, handler)
    return decorator


def delete(path):
    def decorator(handler):
        return _register_http("DELETE", path, handler)
    return decorator


def nats(workflow_id, match=None):
    def decorator(handler):
        _NATS_HANDLERS.append({
            "workflow_id": workflow_id,
            "match": match or {},
            "handler": handler,
        })
        return handler
    return decorator


def schedule(id=None):
    def decorator(handler):
        _SCHEDULE_JOBS.append({"job_id": id or handler.__name__, "handler": handler})
        return handler
    return decorator


def _match_envelope(envelope, rules):
    for dotted_key, expected in rules.items():
        node = envelope
        for part in dotted_key.split("."):
            if not isinstance(node, dict):
                node = None
                break
            node = node.get(part)
        if node != expected:
            return False
    return True


def _parse_envelope(data):
    payload = json.loads(data.decode())
    if isinstance(payload, dict):
        return payload
    return {"data": payload}


def _encode_result(job_id, success, error=None):
    body = {"job_id": job_id, "success": success}
    if error:
        body["error"] = error
    return json.dumps({"pattern": RESULT_SUBJECT, "data": body}).encode()


async def _publish_result(nc, job_id, success, error=None):
    await nc.publish(RESULT_SUBJECT, _encode_result(job_id, success, error))


async def _run_nats_job(nc, spec, data):
    job_id = data.get("job_id")
    inner = data.get("payload") or {}
    try:
        await spec["handler"](inner)
        if job_id:
            await _publish_result(nc, job_id, True)
    except Exception as ex:
        logging.exception("workflow %s failed", spec["workflow_id"])
        if job_id:
            await _publish_result(nc, job_id, False, error=str(ex))


def _authorize_http(request, scope):
    secret = _jwt_secret()
    if not secret:
        raise Exception({"status": 500, "message": "JWT_SECRET is not configured"})

    auth = request.headers.get("Authorization", "")
    parts = auth.split(" ", 1)
    if len(parts) != 2 or parts[0] != "Bearer":
        raise Exception({"status": 403, "message": "forbidden, error authorization"})

    try:
        user = jwt.decode(parts[1], secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise Exception({"status": 403, "message": "forbidden, invalid token"})

    if scope not in (user.get("scopes") or []):
        raise Exception({"status": 403, "message": "forbidden, error scope"})
    return user


def _http_error(ex):
    error = ex.args[0] if ex.args else None
    if isinstance(error, dict):
        status = int(error.get("status", 500))
        message = error.get("message", "Internal server error")
    elif isinstance(error, str):
        status = 500
        message = error
    else:
        status = 500
        message = "Internal server error"
    return web.json_response({"message": message, "status": status}, status=status)


def _make_http_view(route):
    async def view(request):
        try:
            user = _authorize_http(request, route["scope"])
            text = await request.text()
            body = json.loads(text) if text else {}
            req = HttpRequest(body, user)
            result = await route["handler"](req)
            if isinstance(result, (dict, list)):
                return web.json_response(result)
            return web.Response(text=str(result))
        except json.JSONDecodeError:
            return _http_error(Exception({"status": 400, "message": "invalid JSON body"}))
        except Exception as ex:
            logging.error("HTTP %s %s failed: %s", route["method"], route["path"], ex, exc_info=True)
            return _http_error(ex)

    return view


async def _on_dispatch(msg):
    try:
        envelope = _parse_envelope(msg.data)
        data = envelope.get("data") or {}
        for spec in _NATS_HANDLERS:
            if spec["match"] and not _match_envelope(envelope, spec["match"]):
                continue
            asyncio.create_task(_run_nats_job(_nc, spec, data))
            return
    except Exception:
        logging.exception("failed to process dispatch message")


async def _schedule_loop():
    schedules = _parse_schedules()
    tz = _timezone()
    while True:
        now = datetime.now(tz).replace(second=0, microsecond=0)
        minute_key = now.strftime("%Y%m%d%H%M")
        for job in _SCHEDULE_JOBS:
            cron = schedules.get(job["job_id"])
            if not cron:
                continue
            if not croniter.match(cron, now):
                continue
            if _schedule_last_minute.get(job["job_id"]) == minute_key:
                continue
            _schedule_last_minute[job["job_id"]] = minute_key
            logging.info("running scheduled job %s", job["job_id"])
            asyncio.create_task(job["handler"]())
        await asyncio.sleep(30)


async def _async_main():
    global _nc
    if _NATS_HANDLERS:
        _nc = await nats.connect(_nats_url())
        await _nc.subscribe(DISPATCH_SUBJECT, queue=NATS_QUEUE_GROUP, cb=_on_dispatch)
        logging.info(
            "NATS dispatch %s queue=%s handlers=%s",
            DISPATCH_SUBJECT,
            NATS_QUEUE_GROUP,
            len(_NATS_HANDLERS),
        )

    if _HTTP_ROUTES:
        app = web.Application()
        for route in _HTTP_ROUTES:
            app.router.add_route(route["method"], route["path"], _make_http_view(route))
            logging.info("HTTP %s %s scope=%s", route["method"], route["path"], route["scope"])
        port = int(config.get("HTTP_PORT", default="3000"))
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", port).start()
        logging.info("HTTP listening on 0.0.0.0:%s", port)

    if _SCHEDULE_JOBS:
        asyncio.create_task(_schedule_loop())

    while True:
        await asyncio.sleep(3600)


def run():
    if not _HTTP_ROUTES and not _NATS_HANDLERS and not _SCHEDULE_JOBS:
        raise RuntimeError("no workflows registered")
    asyncio.run(_async_main())
