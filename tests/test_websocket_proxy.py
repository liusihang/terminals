"""Machine API ownership across normal HTTP creation and a real loopback WS relay."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.websockets import WebSocketDisconnect
from websockets.asyncio.server import serve

from machines.config import settings
from machines.routers import machines, proxy


class WebSocketProxyTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(settings, "api_key", "test-server-key"))
        self.enterContext(patch.object(proxy, "_resolve_template_spec", AsyncMock(return_value={"scope": "user"})))
        self.backend = SimpleNamespace(ensure_terminal=AsyncMock(), touch_activity=AsyncMock())
        app = FastAPI()
        app.state.backend = self.backend
        app.include_router(machines.router, prefix="/api/v1/machines")
        app.include_router(machines.router, prefix="/api/v1/terminals")
        app.include_router(proxy.router)
        self.client = self.enterContext(TestClient(app))
        self.client.portal.call(self.start_worker)
        self.addCleanup(self.client.portal.call, self.stop_worker)
        self.headers = {"X-User-Id": "alice", "X-Session-Id": "saved-chat", "X-Machine-Template-Id": "research"}
        self.auth = {"Authorization": "Bearer test-server-key", **self.headers}

    async def start_worker(self):
        self.owner = None

        def create_shell(request):
            if request.url.path == "/health":
                return httpx.Response(200, json={"status": "ok"})
            self.owner = tuple(request.headers.get(h) for h in ("x-user-id", "x-session-id", "authorization"))
            return httpx.Response(200, json={"id": "shell"})

        async def shell(ws):
            session = ws.request.headers
            auth = json.loads(await ws.recv())
            owner = (session.get("x-user-id"), session.get("x-session-id"), "Bearer " + auth["token"])
            if owner != self.owner:
                await ws.close(4004)
                return
            await ws.send("ready")
            await ws.send(await ws.recv())

        self.server = await serve(shell, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.backend.ensure_terminal.return_value = {"host": "127.0.0.1", "port": port, "api_key": "test-worker-key"}
        self.worker_client = httpx.AsyncClient(transport=httpx.MockTransport(create_shell))
        proxy._proxy_clients["127.0.0.1", port] = self.worker_client

    async def stop_worker(self):
        self.server.close()
        await self.server.wait_closed()
        await proxy.close_proxy_client()

    def test_session_reconnect_and_owner_boundaries(self):
        self.assertEqual(self.client.post("/api/terminals", headers=self.auth).status_code, 200)
        self.assertEqual(self.owner, ("alice", "saved-chat", "Bearer test-worker-key"))
        for stream in ("api/terminals/shell", "execute", "events", "desktop/stream", "agents/sessions/session/stream"):
            with self.subTest(stream=stream), self.client.websocket_connect("/" + stream, headers=self.headers) as ws:
                ws.send_json({"type": "auth", "token": "test-server-key"})
                self.assertEqual(ws.receive_text(), "ready")
                ws.send_bytes(b"")
                self.assertEqual(ws.receive_bytes(), b"")
        for other in ({"X-User-Id": "bob"}, {"X-Session-Id": "different-chat"}):
            with self.client.websocket_connect("/api/terminals/shell", headers={**self.headers, **other}) as ws:
                ws.send_json({"type": "auth", "token": "test-server-key"})
                with self.assertRaises(WebSocketDisconnect) as error:
                    ws.receive_text()
                self.assertEqual(error.exception.code, 4004)

    def test_health_distinguishes_orchestrator_and_scoped_runtime(self):
        self.assertEqual(self.client.get("/health").json(), {"status": True})
        self.backend.ensure_terminal.assert_not_awaited()
        self.assertEqual(self.client.get("/health", headers=self.headers).status_code, 401)
        self.backend.ensure_terminal.assert_not_awaited()
        for name in ("X-Machine-Template-Id", "X-Terminal-Template-Id"):
            headers = {k: v for k, v in self.auth.items() if k != "X-Machine-Template-Id"}
            headers[name] = "research"
            self.assertEqual(self.client.get("/health", headers=headers).json(), {"status": "ok"})
            self.backend.ensure_terminal.assert_awaited_with(template_id="research", context_id="user:alice", record_activity=True)

    def test_admin_and_proxy_auth(self):
        for prefix in ("machines", "terminals"):
            self.assertEqual(self.client.get(f"/api/v1/{prefix}/templates", headers=self.auth).status_code, 403)
            self.assertEqual(self.client.get(f"/api/v1/{prefix}/templates").status_code, 401)
        self.assertEqual(self.client.get("/files/list", headers=self.headers).status_code, 401)
        with self.client.websocket_connect("/execute", headers=self.headers) as ws:
            ws.send_json({"type": "auth", "token": "not-the-key"})
            with self.assertRaises(WebSocketDisconnect) as error:
                ws.receive_json()
            self.assertEqual(error.exception.code, 4001)
        self.backend.ensure_terminal.assert_not_awaited()

    def test_unconsumed_response_is_closed(self):
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b"file"

        async def check():
            async def receive():
                return {"type": "http.request", "body": b"", "more_body": False}

            req = Request({"type": "http", "method": "GET", "path": "/files/read", "query_string": b"",
                           "headers": [], "app": self.client.app}, receive)
            upstream = httpx.Response(200, stream=Stream())
            with patch.object(self.worker_client, "send", AsyncMock(return_value=upstream)):
                resp = await proxy._proxy_request(req, "alice", "files/read", template_id="research", context_id="user:alice")
            self.assertFalse(upstream.is_closed)
            await resp.background()
            self.assertTrue(upstream.is_closed)
        self.client.portal.call(check)
