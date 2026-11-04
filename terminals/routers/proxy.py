"""Catch-all reverse proxy into terminal instances.

Routing is based on the ``X-User-Id`` header — the caller (e.g. Open WebUI
backend) sets this header and the proxy resolves / provisions the correct
instance automatically.  The path structure mirrors open-terminal exactly
(``/execute``, ``/files/list``, …) so the two are interchangeable.

Named policy endpoints:
  /p/{policy_id}/*  — provisions using the named policy config
"""

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, urlencode

import httpx
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import InvalidHandshake
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response, WebSocket
from fastapi.responses import JSONResponse, StreamingResponse
from loguru import logger

from terminals.config import settings
from terminals.utils.context import (
    CONTEXT_HEADER,
    DEFAULT_CONTEXT_ID,
    normalize_context_id,
)
from terminals.utils.policy_specs import PolicyNotFoundError, resolve_policy_spec
from terminals.routers.auth import verify_api_key, verify_user_id

router = APIRouter()

# ---------------------------------------------------------------------------
# Proxy client
# ---------------------------------------------------------------------------

_proxy_clients: dict[tuple[str, int], httpx.AsyncClient] = {}

# Active WebSocket connection counter (for stats endpoint)
active_ws_connections: int = 0


async def _get_proxy_client(host: str, port: int) -> httpx.AsyncClient:
    client = _proxy_clients.pop((host, port), None)
    if client is None:
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=settings.proxy_connect_timeout_seconds,
                read=settings.proxy_read_timeout_seconds,
                write=settings.proxy_write_timeout_seconds,
                pool=settings.proxy_pool_timeout_seconds,
            ),
            limits=httpx.Limits(
                max_connections=32,
                max_keepalive_connections=4,
                keepalive_expiry=30.0,
            ),
        )
    _proxy_clients[(host, port)] = client
    if len(_proxy_clients) > 1200:
        await _proxy_clients.pop(next(iter(_proxy_clients))).aclose()
    return client


async def close_proxy_client() -> None:
    clients = list(_proxy_clients.values())
    _proxy_clients.clear()
    for client in clients:
        await client.aclose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_ip(request) -> str:
    """Extract client IP, respecting X-Forwarded-For."""
    headers = getattr(request, "headers", {})
    forwarded = headers.get("x-forwarded-for") if hasattr(headers, "get") else None
    if forwarded:
        return forwarded.split(",")[0].strip()
    client = getattr(request, "client", None)
    return client.host if client else ""


def _user_agent(request) -> str:
    headers = getattr(request, "headers", {})
    return headers.get("user-agent", "") if hasattr(headers, "get") else ""


def _request_id(request) -> Optional[str]:
    return getattr(getattr(request, "state", None), "request_id", None)


def _context_from_request(request: Request) -> str:
    try:
        return normalize_context_id(request.headers.get(CONTEXT_HEADER))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _context_from_websocket(ws: WebSocket, query_context_id: str) -> str:
    return normalize_context_id(ws.headers.get(CONTEXT_HEADER) or query_context_id)


# ---------------------------------------------------------------------------
# Resolve instance
# ---------------------------------------------------------------------------


@dataclass
class InstanceInfo:
    """Resolved terminal instance."""
    instance_id: str
    host: str
    port: int
    api_key: str


async def _resolve_instance(
    request,
    user_id: str,
    policy_id: str = "default",
    context_id: str = DEFAULT_CONTEXT_ID,
    spec: Optional[dict] = None,
) -> InstanceInfo:
    """Return a running instance, auto-provisioning if needed."""
    backend = request.app.state.backend
    info = await backend.ensure_terminal(
        user_id, policy_id=policy_id, context_id=context_id, spec=spec
    )
    if info is None:
        raise RuntimeError(f"Failed to provision terminal for user {user_id}")
    try:
        await backend.touch_activity(user_id, policy_id=policy_id, context_id=context_id)
    except Exception:
        logger.debug("touch_activity failed for user {}", user_id)
    return InstanceInfo(
        instance_id=info["instance_id"],
        host=info["host"],
        port=info["port"],
        api_key=info["api_key"],
    )


# ---------------------------------------------------------------------------
# Internal proxy helper
# ---------------------------------------------------------------------------


async def _proxy_request(
    request: Request, user_id: str, path: str,
    background_tasks: Optional[BackgroundTasks] = None,
    policy_id: str = "default",
    context_id: str = DEFAULT_CONTEXT_ID,
    spec: Optional[dict] = None,
) -> Response:
    """Forward an HTTP request to the user's terminal instance.

    Streams both the request body (upload) and response body (download)
    to avoid buffering large payloads in memory.
    """
    instance = await _resolve_instance(
        request, user_id, policy_id=policy_id, context_id=context_id, spec=spec,
    )

    target_url = f"http://{instance.host}:{instance.port}/{path}"
    if request.query_params:
        target_url += f"?{request.query_params}"

    headers = dict(request.headers)
    # Replace auth with the instance's own API key.
    headers["authorization"] = f"Bearer {instance.api_key}"
    # Remove hop-by-hop / routing headers.
    for h in ("host", "transfer-encoding", "connection", "x-user-id", CONTEXT_HEADER.lower()):
        headers.pop(h, None)
    # Runtime session/process ownership must match the verified WS identity.
    headers["x-user-id"] = user_id

    raw_content_length = request.headers.get("content-length")
    try:
        content_length = int(raw_content_length) if raw_content_length else None
    except ValueError:
        content_length = None
    stream_body = (
        (
            settings.replay_body_limit is not None
            and content_length is not None
            and content_length > settings.replay_body_limit
        )
        or request.headers.get("transfer-encoding") is not None
    )
    body = None if stream_body else await request.body()

    client = await _get_proxy_client(instance.host, instance.port)

    # Retry on connection errors — the container may still be starting.
    max_retries = 1 if stream_body else 5
    for attempt in range(max_retries):
        try:
            content = request.stream() if stream_body else body
            upstream_resp = await client.send(
                client.build_request(
                    method=request.method,
                    url=target_url,
                    headers=headers,
                    content=content,
                ),
                stream=True,
            )
            break
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            request.app.state.backend.invalidate_status(user_id, policy_id, context_id)
            if attempt < max_retries - 1:
                logger.debug("Proxy attempt {} to {} failed ({}), retrying...", attempt + 1, target_url, e)
                await asyncio.sleep(1)
                try:
                    instance = await _resolve_instance(
                        request,
                        user_id,
                        policy_id=policy_id,
                        context_id=context_id,
                        spec=spec,
                    )
                except Exception as resolve_err:
                    logger.error("Re-resolving terminal for user {} failed: {}", user_id, resolve_err)
                    return Response(
                        content='{"error": "Terminal instance not reachable"}',
                        status_code=502,
                        media_type="application/json",
                    )
                target_url = f"http://{instance.host}:{instance.port}/{path}"
                if request.query_params:
                    target_url += f"?{request.query_params}"
                headers["authorization"] = f"Bearer {instance.api_key}"
                client = await _get_proxy_client(instance.host, instance.port)
            else:
                logger.error("Proxy request to {} failed after {} retries: {}", target_url, max_retries, e)
                return Response(
                    content='{"error": "Terminal instance not reachable"}',
                    status_code=502,
                    media_type="application/json",
                )
        except httpx.ReadTimeout as e:
            logger.warning(
                "Proxy request to {} timed out waiting for upstream response: {}",
                target_url,
                e,
            )
            return Response(
                content='{"error": "Upstream response timed out"}',
                status_code=504,
                media_type="application/json",
            )
        except (httpx.RemoteProtocolError, httpx.ReadError) as e:
            request.app.state.backend.invalidate_status(
                user_id, policy_id, context_id
            )
            if attempt < max_retries - 1:
                logger.debug(
                    "Proxy attempt {} to {} hit a stale upstream connection ({}), retrying...",
                    attempt + 1,
                    target_url,
                    e,
                )
                try:
                    instance = await _resolve_instance(
                        request,
                        user_id,
                        policy_id=policy_id,
                        context_id=context_id,
                        spec=spec,
                    )
                except Exception as resolve_err:
                    logger.error("Re-resolving terminal for user {} failed: {}", user_id, resolve_err)
                    return Response(
                        content='{"error": "Upstream connection failed"}',
                        status_code=502,
                        media_type="application/json",
                    )
                target_url = f"http://{instance.host}:{instance.port}/{path}"
                if request.query_params:
                    target_url += f"?{request.query_params}"
                headers["authorization"] = f"Bearer {instance.api_key}"
                client = await _get_proxy_client(instance.host, instance.port)
            else:
                logger.error("Proxy request to {} failed after {} retries: {}", target_url, max_retries, e)
                return Response(
                    content='{"error": "Upstream connection failed"}',
                    status_code=502,
                    media_type="application/json",
                )

    # Strip hop-by-hop from response.
    response_headers = dict(upstream_resp.headers)
    for h in ("transfer-encoding", "connection", "content-encoding"):
        response_headers.pop(h, None)

    return StreamingResponse(
        content=upstream_resp.aiter_bytes(),
        status_code=upstream_resp.status_code,
        headers=response_headers,
        background=BackgroundTasks([upstream_resp.aclose]),
    )


# ---------------------------------------------------------------------------
# OpenAPI spec passthrough (tool discovery — no X-User-Id required)
# ---------------------------------------------------------------------------

_SPEC_CACHE_TTL = 300  # seconds
_spec_cache: dict[str, tuple[float, dict]] = {}  # key → (timestamp, spec)

_SYSTEM_USER_ID = "system"

# Max retries when waiting for a freshly provisioned container to become ready.
_SPEC_FETCH_RETRIES = 10
_SPEC_FETCH_DELAY = 1  # seconds between retries


async def _fetch_spec_with_retry(instance: InstanceInfo) -> dict:
    """Fetch the OpenAPI spec from an instance, retrying on connection errors.

    Newly provisioned containers may need a few seconds to start serving.
    This retries with a short delay to avoid returning 502 on first access.
    """
    client = await _get_proxy_client(instance.host, instance.port)
    url = f"http://{instance.host}:{instance.port}/openapi.json"
    headers = {"Authorization": f"Bearer {instance.api_key}"}

    last_err = None
    for attempt in range(_SPEC_FETCH_RETRIES):
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            if attempt < _SPEC_FETCH_RETRIES - 1:
                logger.debug("Spec fetch attempt {} failed ({}), retrying...", attempt + 1, e)
                await asyncio.sleep(_SPEC_FETCH_DELAY)
    raise last_err


def _strip_auth_from_spec(spec: dict) -> dict:
    """Remove security schemes — auth is handled transparently by the proxy.

    Returns a shallow copy to avoid mutating the cached dict.
    """
    spec = {**spec}
    spec.pop("security", None)
    components = spec.get("components")
    if components:
        spec["components"] = {**components}
        spec["components"].pop("securitySchemes", None)
    paths = spec.get("paths")
    if paths:
        new_paths = {}
        for path_key, path_methods in paths.items():
            new_methods = {}
            for method, op in path_methods.items():
                if isinstance(op, dict) and "security" in op:
                    op = {k: v for k, v in op.items() if k != "security"}
                new_methods[method] = op
            new_paths[path_key] = new_methods
        spec["paths"] = new_paths
    return spec


async def _get_cached_spec(
    request: Request,
    policy_id: str = "default",
    spec: Optional[dict] = None,
) -> dict:
    """Return cached OpenAPI spec, fetching from a system instance if stale."""
    now = time.monotonic()
    cached = _spec_cache.get(policy_id)
    if cached and (now - cached[0]) < _SPEC_CACHE_TTL:
        return cached[1]

    instance = await _resolve_instance(
        request, _SYSTEM_USER_ID, policy_id=policy_id, spec=spec,
    )
    try:
        raw = await _fetch_spec_with_retry(instance)
    except Exception:
        request.app.state.backend.invalidate_status(_SYSTEM_USER_ID, policy_id)
        raise
    stripped = _strip_auth_from_spec(raw)
    _spec_cache[policy_id] = (now, stripped)
    return stripped


@router.get(
    "/openapi.json",
    dependencies=[Depends(verify_api_key)],
)
async def get_openapi_spec(request: Request):
    """Return the open-terminal OpenAPI spec.

    The spec is fetched from a running instance and cached for 5 minutes.
    This endpoint does **not** require ``X-User-Id`` — it is used by
    Open WebUI for tool discovery before any user context exists.
    """
    try:
        spec = await _get_cached_spec(request)
    except Exception as e:
        logger.error("Failed to fetch OpenAPI spec from instance: {}", e)
        return JSONResponse(
            content={"error": "Failed to fetch OpenAPI spec from terminal instance"},
            status_code=502,
        )
    return JSONResponse(content=spec)


# ---------------------------------------------------------------------------
# Named policy proxy — /p/{policy_id}/{path}
# ---------------------------------------------------------------------------

_POLICY_CACHE_TTL = 60  # seconds
_policy_cache: dict[str, tuple[float, tuple[str, dict | None]]] = {}


def invalidate_policy_caches(policy_id: str) -> None:
    """Discard cached policy data and tool specs after a policy change."""
    _policy_cache.pop(policy_id, None)
    _spec_cache.pop(policy_id, None)


async def _resolve_policy_spec(policy_id: str) -> tuple[str, dict | None]:
    """Look up a policy by ID (cached for 60s). Returns (policy_id, merged spec)."""
    now = time.monotonic()
    cached = _policy_cache.get(policy_id)
    if cached and (now - cached[0]) < _POLICY_CACHE_TTL:
        return cached[1]

    try:
        result = await resolve_policy_spec(policy_id)
    except PolicyNotFoundError:
        raise HTTPException(status_code=404, detail=f"Policy '{policy_id}' not found")
    _policy_cache[policy_id] = (now, result)
    return result


@router.get(
    "/p/{policy_id}/openapi.json",
    dependencies=[Depends(verify_api_key)],
)
async def policy_openapi_spec(
    policy_id: str,
    request: Request,
):
    """Serve the OpenAPI spec via a named policy container (cached)."""
    _id, spec = await _resolve_policy_spec(policy_id)

    try:
        spec_data = await _get_cached_spec(request, policy_id=_id, spec=spec)
    except Exception as e:
        logger.error("Failed to fetch OpenAPI spec from policy instance: {}", e)
        return JSONResponse(
            content={"error": "Failed to fetch OpenAPI spec"},
            status_code=502,
        )

    return JSONResponse(content=spec_data)


@router.api_route(
    "/p/{policy_id}/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
)
async def policy_proxy(
    policy_id: str,
    path: str,
    request: Request,
    background_tasks: BackgroundTasks,
    x_user_id: str = Depends(verify_user_id),
):
    """Proxy a request through a named policy endpoint."""
    _id, spec = await _resolve_policy_spec(policy_id)
    context_id = _context_from_request(request)
    return await _proxy_request(
        request, x_user_id, path, background_tasks,
        policy_id=_id, context_id=context_id, spec=spec,
    )


# ---------------------------------------------------------------------------
# Header-based catch-all proxy — default endpoint (backward compat)
# ---------------------------------------------------------------------------


@router.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
)
async def proxy(
    path: str,
    request: Request,
    background_tasks: BackgroundTasks,
    x_user_id: str = Depends(verify_user_id),
):
    """Reverse-proxy any request into the user's Open Terminal instance.

    Uses settings.* defaults — no policy lookup.
    """
    context_id = _context_from_request(request)
    return await _proxy_request(
        request, x_user_id, path, background_tasks, context_id=context_id
    )


# ---------------------------------------------------------------------------
# WebSocket proxies for terminal sessions, streams and browser capabilities
# ---------------------------------------------------------------------------


def _ws_user_id(ws: WebSocket, verified_id: Optional[str]) -> str:
    """Bind every supplied routing identity to the authenticated user."""
    claimed = ws.query_params.getlist("user_id")
    if ws.headers.get("x-user-id") is not None:
        claimed.append(ws.headers["x-user-id"])
    effective = verified_id or next((value for value in claimed if value), "")
    if not effective:
        raise HTTPException(400, "Missing user_id")
    if any(value != effective for value in claimed):
        raise HTTPException(403, "User does not match authenticated identity")
    return effective


async def _validate_ws_auth(ws: WebSocket) -> Optional[str]:
    """Accept ordinary streams and consume only their first auth frame."""
    await ws.accept()
    try:
        raw = await asyncio.wait_for(ws.receive_text(), timeout=10.0)
        payload = json.loads(raw)
        if not isinstance(payload, dict) or payload.get("type") != "auth":
            raise ValueError("Expected auth message")
        token = payload.get("token", "")
        if not isinstance(token, str):
            raise ValueError("Invalid token")
        verified_id = await verify_api_key(authorization=f"Bearer {token}")
    except Exception:
        await ws.close(code=4001, reason="Invalid auth message or credentials")
        return None
    try:
        return _ws_user_id(ws, verified_id)
    except HTTPException as exc:
        await ws.close(code=4003 if exc.status_code == 403 else 4002, reason=exc.detail)
        return None


async def _validate_browser_ws_auth(ws: WebSocket) -> Optional[str]:
    """Browser tickets are checked by WebUI; relay credentials follow HTTP auth.

    Do not read a frame here: /ws starts with an application URL message, and
    /stream and /vnc may send output before the viewer sends anything.
    """
    try:
        verified_id = await verify_api_key(authorization=ws.headers.get("authorization"))
        if not ws.headers.get("x-user-id"):
            raise HTTPException(401, "Missing X-User-Id")
        user_id = await verify_user_id(
            verified_id=verified_id, x_user_id=ws.headers["x-user-id"]
        )
        effective = _ws_user_id(ws, verified_id or user_id)
    except HTTPException as exc:
        await ws.close(code=4003 if exc.status_code == 403 else 4001, reason=exc.detail)
        return None
    await ws.accept()
    return effective


def _close_code(code: Optional[int]) -> int:
    # Reserved codes cannot be sent in a WebSocket close frame.
    return code if code is not None and code not in (1005, 1006, 1015) else 1011


async def _relay_websocket(ws: WebSocket, upstream) -> None:
    async def client_to_upstream():
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                await upstream.close(code=_close_code(msg.get("code")))
                return
            if msg.get("bytes") is not None:
                await upstream.send(msg["bytes"])
            elif msg.get("text") is not None:
                await upstream.send(msg["text"])

    async def upstream_to_client():
        try:
            async for message in upstream:
                if isinstance(message, bytes):
                    await ws.send_bytes(message)
                else:
                    await ws.send_text(message)
        finally:
            await ws.close(code=_close_code(upstream.close_code))

    tasks = [
        asyncio.create_task(client_to_upstream()),
        asyncio.create_task(upstream_to_client()),
    ]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _ws_proxy_handler(
    ws: WebSocket,
    path: str,
    user_id: str,
    policy_id: str = "default",
    context_id: str = DEFAULT_CONTEXT_ID,
    spec: Optional[dict] = None,
    browser: bool = False,
):
    """Resolve a scoped worker and relay the explicit runtime WS path."""
    global active_ws_connections
    active_ws_connections += 1
    try:
        await _ws_proxy(ws, path, user_id, policy_id, context_id, spec, browser)
    finally:
        active_ws_connections -= 1
        try:
            await ws.close()
        except Exception:
            pass


async def _ws_proxy(
    ws: WebSocket,
    path: str,
    user_id: str,
    policy_id: str,
    context_id: str,
    spec: Optional[dict],
    browser: bool,
):
    try:
        instance = await _resolve_instance(
            ws, user_id, policy_id=policy_id, context_id=context_id, spec=spec
        )
    except Exception:
        await ws.close(code=4003, reason="Failed to resolve terminal instance")
        return

    # Routing hints have been consumed locally. Preserve runtime options, including
    # repeated query values, without allowing a query to select another worker.
    query = urlencode([
        (key, value) for key, value in ws.query_params.multi_items()
        if key not in ("user_id", "context_id")
    ])
    for attempt in range(5):
        headers = {
            "X-User-Id": user_id,
            "X-Session-Id": ws.headers.get("x-session-id", ""),
            CONTEXT_HEADER: context_id,
        }
        if browser:
            headers["Authorization"] = f"Bearer {instance.api_key}"
            if ws.headers.get("cookie"):
                headers["Cookie"] = ws.headers["cookie"]
        upstream_url = f"ws://{instance.host}:{instance.port}/{path}"
        if query:
            upstream_url += f"?{query}"
        try:
            upstream = await ws_connect(
                upstream_url,
                additional_headers=headers,
                compression="deflate" if settings.ws_compression else None,
                max_size=16 * 1024 * 1024,
            )
            break
        except (OSError, InvalidHandshake) as exc:
            ws.app.state.backend.invalidate_status(user_id, policy_id, context_id)
            if attempt == 4:
                await ws.close(code=4003, reason="Terminal instance not reachable")
                return
            # Avoid logging browser capability URLs or credentials.
            logger.debug("WS connect attempt {} failed ({})", attempt + 1, type(exc).__name__)
            await asyncio.sleep(1)
            try:
                instance = await _resolve_instance(
                    ws, user_id, policy_id=policy_id, context_id=context_id, spec=spec
                )
            except Exception:
                await ws.close(code=4003, reason="Terminal instance not reachable")
                return

    try:
        async with upstream:
            if not browser:
                await upstream.send(json.dumps({
                    "type": "auth",
                    "token": instance.api_key,
                    "chat_id": ws.headers.get("x-session-id", ""),
                }))
            await _relay_websocket(ws, upstream)
    except Exception as exc:
        logger.debug("WebSocket relay closed ({})", type(exc).__name__)
        try:
            await ws.close(code=1011)
        except Exception:
            pass


async def _dispatch_ws(ws: WebSocket, path: str, *, browser: bool = False):
    user_id = await (_validate_browser_ws_auth(ws) if browser else _validate_ws_auth(ws))
    if user_id is None:
        return
    policy_id = ws.path_params.get("policy_id", "default")
    spec = None
    try:
        # Match the HTTP default route: no policy lookup without /p/{policy_id}.
        if "policy_id" in ws.path_params:
            policy_id, spec = await _resolve_policy_spec(policy_id)
        context_id = _context_from_websocket(
            ws, ws.query_params.get("context_id", DEFAULT_CONTEXT_ID)
        )
    except (HTTPException, ValueError):
        await ws.close(code=4004, reason="Invalid policy or context")
        return
    await _ws_proxy_handler(ws, path, user_id, policy_id, context_id, spec, browser)


@router.websocket("/events")
@router.websocket("/p/{policy_id}/events")
async def ws_events_proxy(ws: WebSocket):
    await _dispatch_ws(ws, "events")


@router.websocket("/desktop/stream")
@router.websocket("/p/{policy_id}/desktop/stream")
async def ws_desktop_proxy(ws: WebSocket):
    await _dispatch_ws(ws, "desktop/stream")


@router.websocket("/desktop/audio")
@router.websocket("/p/{policy_id}/desktop/audio")
async def ws_audio_proxy(ws: WebSocket):
    await _dispatch_ws(ws, "desktop/audio")


@router.websocket("/execute")
@router.websocket("/p/{policy_id}/execute")
async def ws_processes_proxy(ws: WebSocket):
    await _dispatch_ws(ws, "execute")


@router.websocket("/execute/{process_id}/status")
@router.websocket("/p/{policy_id}/execute/{process_id}/status")
async def ws_process_output_proxy(ws: WebSocket, process_id: str):
    await _dispatch_ws(ws, f"execute/{quote(process_id, safe='')}/status")


@router.websocket("/api/terminals/{session_id}")
@router.websocket("/p/{policy_id}/api/terminals/{session_id}")
async def ws_terminal_proxy(ws: WebSocket, session_id: str):
    await _dispatch_ws(ws, f"api/terminals/{quote(session_id, safe='')}")


@router.websocket("/browser/view/{sid}/{capability}/{kind}")
@router.websocket("/p/{policy_id}/browser/view/{sid}/{capability}/{kind}")
async def ws_browser_proxy(ws: WebSocket, sid: str, capability: str, kind: str):
    if kind not in ("ws", "stream", "vnc"):
        await ws.close(code=4004, reason="Unknown browser stream")
        return
    await _dispatch_ws(
        ws, f"browser/view/{quote(sid, safe='')}/{quote(capability, safe='')}/{kind}",
        browser=True,
    )
