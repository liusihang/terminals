"""Route-level compatibility tests; only WebUI/worker/backend boundaries are fake.

No production database, Docker daemon, GUI or browser is used. WebSockets use a
real loopback server, JWT validation uses a fake WebUI HTTP endpoint, and named
policies are resolved from a temporary SQLite database by the real resolver.
"""

import asyncio
import json
import unittest
from contextlib import ExitStack
from urllib.parse import parse_qsl, quote, urlsplit
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.websockets import WebSocketDisconnect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from terminals.config import settings
from terminals.models.base import Base
from terminals.models.policy import Policy
from terminals.routers import auth, lifecycle, policy, proxy
from terminals.utils import policy_specs


class FakeBackend:
    def __init__(self):
        self.instances = {}
        self.calls = []
        self.activity = []
        self.invalidated = []
        self.port = 0
        self.fail = False

    async def ensure_terminal(self, user_id, policy_id="default", context_id="default", spec=None):
        self.calls.append((user_id, policy_id, context_id, spec))
        if self.fail:
            raise RuntimeError("provision failed")
        key = (user_id, policy_id, context_id)
        if key not in self.instances:
            self.instances[key] = {
                "instance_id": f"instance-{len(self.instances)}",
                "host": "127.0.0.1", "port": self.port,
                "api_key": f"worker-secret-{len(self.instances)}",
            }
        return self.instances[key]

    async def touch_activity(self, user_id, **scope):
        self.activity.append((user_id, scope))

    def invalidate_status(self, *scope):
        self.invalidated.append(scope)


class FakeRuntime:
    def __init__(self, backend):
        self.backend = backend
        self.connections = []
        self.http_requests = []
        self.resources = {}
        self.enforce_visibility = False

    def http(self, request):
        self.http_requests.append(request)
        owner = (
            request.headers["authorization"], request.headers.get("x-user-id", ""),
            request.headers.get("x-session-id", ""),
        )
        if request.method == "POST":
            resource = "owned-shell" if request.url.path == "/api/terminals" else "owned-process"
            self.resources.setdefault(owner, []).append(resource)
            return httpx.Response(200, json={"id": resource})
        return httpx.Response(200, content=b"existing-http-result", headers={"content-type": "text/plain"})

    async def websocket(self, upstream):
        request = upstream.request
        record = {"path": request.path, "headers": dict(request.headers), "messages": [],
                  "done": asyncio.Event(), "close_code": None}
        self.connections.append(record)
        browser = urlsplit(request.path).path.startswith("/browser/view/")
        try:
            if browser:
                worker_key = request.headers.get("authorization", "").removeprefix("Bearer ")
            else:
                payload = json.loads(await upstream.recv())
                record["auth"] = payload
                worker_key = payload.get("token")
                if payload.get("type") != "auth":
                    await upstream.close(4001)
                    return
            scopes = [key for key, value in self.backend.instances.items() if value["api_key"] == worker_key]
            if len(scopes) != 1:
                await upstream.close(4001)
                return
            user_id, _, context_id = scopes[0]
            if request.headers.get("x-user-id") != user_id or request.headers.get("x-terminal-context-id") != context_id:
                await upstream.close(4003)
                return
            owner = (f"Bearer {worker_key}", user_id, request.headers.get("x-session-id", ""))
            resources = self.resources.get(owner, [])
            path = urlsplit(request.path).path
            if self.enforce_visibility:
                wanted = "owned-shell" if path.startswith("/api/terminals/") else "owned-process"
                if wanted not in resources:
                    await upstream.close(4004)
                    return
            if browser and path.endswith("/ws"):
                # The first frame must be the application's URL, never auth.
                record["messages"].append(await upstream.recv())
            await upstream.send(json.dumps({"type": "ready", "resources": resources}))
            async for message in upstream:
                record["messages"].append(message)
                if message in ("__close__", "__close_error__"):
                    await upstream.close(4004 if message == "__close__" else 1011, "runtime finished")
                    return
                await upstream.send(message)
        except ConnectionClosed:
            pass
        finally:
            record["close_code"] = upstream.close_code
            record["done"].set()


class WebSocketProxyTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for name, value in (("open_webui_url", "http://webui.invalid"), ("api_key", "shared-key"),
                            ("token_cache_ttl", 0), ("image", "default-image")):
            self.stack.enter_context(patch.object(settings, name, value))
        self.backend = FakeBackend()
        self.runtime = FakeRuntime(self.backend)
        self.webui_requests = []

        def webui(request):
            self.webui_requests.append(request)
            token = request.headers.get("authorization", "").removeprefix("Bearer ")
            if token in ("jwt-alice", "jwt-bob"):
                return httpx.Response(200, json={"id": token.removeprefix("jwt-")})
            return httpx.Response(401)

        self.auth_client = httpx.AsyncClient(transport=httpx.MockTransport(webui))
        self.stack.enter_context(patch.object(auth, "_owui_client", self.auth_client))
        proxy._policy_cache.clear()
        app = FastAPI()
        app.state.backend = self.backend
        # Same order as the service: administrative APIs precede the catch-all.
        app.include_router(lifecycle.router)
        app.include_router(policy.router)
        app.include_router(proxy.router)
        self.client = self.stack.enter_context(TestClient(app))
        self.client.portal.call(self._start_boundaries)
        self.stack.callback(self.client.portal.call, self._stop_boundaries)
        self.stack.enter_context(patch.object(policy_specs, "async_session", self.sessions))

    async def _start_boundaries(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with self.sessions() as session:
            session.add(Policy(id="research", data={"image": "research-image", "cpu_limit": "2"}))
            await session.commit()
        self.server = await serve(self.runtime.websocket, "127.0.0.1", 0, max_size=16 * 1024 * 1024)
        self.backend.port = self.server.sockets[0].getsockname()[1]
        self.worker_client = httpx.AsyncClient(transport=httpx.MockTransport(self.runtime.http))
        proxy._proxy_clients[("127.0.0.1", self.backend.port)] = self.worker_client

    async def _stop_boundaries(self):
        self.server.close()
        await self.server.wait_closed()
        await proxy.close_proxy_client()
        await self.auth_client.aclose()
        await self.engine.dispose()
        proxy._policy_cache.clear()

    def headers(self, user="alice", chat="saved-chat", context="chat:saved-chat", **extra):
        return {"X-User-Id": user, "X-Session-Id": chat, "X-Terminal-Context-Id": context, **extra}

    def finish(self, record):
        self.client.portal.call(asyncio.wait_for, record["done"].wait(), 2)
        self.assertEqual(proxy.active_ws_connections, 0)

    def assert_denied(self, path, *, token="jwt-alice", headers=None, code=4003, browser=False):
        before = len(self.backend.calls)
        with self.assertRaises(WebSocketDisconnect) as error:
            with self.client.websocket_connect(path, headers=headers or {}) as socket:
                if not browser:
                    socket.send_json({"type": "auth", "token": token})
                socket.receive_text()
        self.assertEqual(error.exception.code, code)
        self.assertEqual(len(self.backend.calls), before)
        self.assertEqual(proxy.active_ws_connections, 0)

    def test_all_streams_default_and_named_policy_with_jwt_and_shared_key(self):
        paths = ("events", "desktop/stream", "desktop/audio", "execute",
                 "execute/process-1/status", "api/terminals/shell-1")
        for shared in (False, True):
            with patch.object(settings, "open_webui_url", "" if shared else "http://webui.invalid"):
                for prefix in ("", "/p/research"):
                    for path in paths:
                        with self.subTest(shared=shared, prefix=prefix, path=path):
                            url = f"{prefix}/{path}?user_id=alice&offset=12&tail=9&option=a&option=b"
                            with self.client.websocket_connect(url, headers=self.headers()) as socket:
                                socket.send_json({"type": "auth", "token": "shared-key" if shared else "jwt-alice"})
                                self.assertEqual(socket.receive_json()["type"], "ready")
                                record = self.runtime.connections[-1]
                                self.assertEqual(urlsplit(record["path"]).path, f"/{path}")
                                self.assertEqual(parse_qsl(urlsplit(record["path"]).query),
                                                 [("offset", "12"), ("tail", "9"), ("option", "a"), ("option", "b")])
                                for message in ("text", "", b"\x00\xff", b""):
                                    if isinstance(message, bytes):
                                        socket.send_bytes(message)
                                        self.assertEqual(socket.receive_bytes(), message)
                                    else:
                                        socket.send_text(message)
                                        self.assertEqual(socket.receive_text(), message)
                                call = self.backend.calls[-1]
                                self.assertEqual(call[:3], ("alice", "research" if prefix else "default", "chat:saved-chat"))
                                self.assertEqual(call[3], {"image": "research-image", "cpu_limit": "2"} if prefix else None)
                                self.assertEqual(record["auth"]["chat_id"], "saved-chat")
                                self.assertTrue(record["auth"]["token"].startswith("worker-secret-"))
                                self.assertNotIn("authorization", record["headers"])
                                socket.close(1000)
                                socket.receive()
                            self.finish(record)
        self.assertTrue(self.webui_requests)
        self.assertTrue(all(request.url.path == "/api/v1/auths/" for request in self.webui_requests))

    def test_user_claims_cannot_override_jwt_or_conflict_with_shared_key(self):
        for shared in (False, True):
            with patch.object(settings, "open_webui_url", "" if shared else "http://webui.invalid"):
                for prefix in ("", "/p/research"):
                    token = "shared-key" if shared else "jwt-alice"
                    self.assert_denied(f"{prefix}/events?user_id=bob", token=token, headers=self.headers())
                    self.assert_denied(f"{prefix}/events?user_id=alice", token=token, headers=self.headers(user="bob"))
                    self.assert_denied(f"{prefix}/events?user_id=bob&user_id=alice", token=token, headers=self.headers())
        self.assert_denied("/events?user_id=bob", headers={}, code=4003)

    def test_invalid_missing_and_malformed_auth(self):
        for token in ("bad", "", 12, None, {}, "非ASCII"):
            self.assert_denied("/events?user_id=alice", token=token, code=4001)
        for payload in ([], None, {}, {"type": "input"}):
            with self.assertRaises(WebSocketDisconnect) as error:
                with self.client.websocket_connect("/events?user_id=alice") as socket:
                    socket.send_json(payload)
                    socket.receive_text()
            self.assertEqual(error.exception.code, 4001)
        with patch.object(settings, "open_webui_url", ""):
            self.assert_denied("/events", token="shared-key", code=4002)
            self.assert_denied("/events?user_id=alice", token="", code=4001)
        self.assertEqual(self.backend.calls, [])

    def test_verified_jwt_identity_without_routing_hint_and_context_selection(self):
        with self.client.websocket_connect("/events?context_id=automation:run-1") as socket:
            socket.send_json({"type": "auth", "token": "jwt-alice"})
            socket.receive_json()
            record = self.runtime.connections[-1]
            self.assertEqual(self.backend.calls[-1][:3], ("alice", "default", "automation:run-1"))
            socket.send_text("__close__")
            self.assertEqual(socket.receive()["code"], 4004)
        self.finish(record)
        with self.client.websocket_connect("/events?context_id=chat:query", headers=self.headers()) as socket:
            socket.send_json({"type": "auth", "token": "jwt-alice"})
            socket.receive_json()
            record = self.runtime.connections[-1]
            self.assertEqual(self.backend.calls[-1][2], "chat:saved-chat")
            socket.send_text("__close__")
            self.assertEqual(socket.receive()["code"], 4004)
        self.finish(record)

    def test_unknown_policy_invalid_context_and_unregistered_admin_ws(self):
        self.assert_denied("/p/missing/events?user_id=alice", code=4004)
        self.assert_denied("/events?user_id=alice&context_id=unsupported", code=4004)
        self.assert_denied("/events?user_id=alice&context_id=chat:%00bad", code=4004)
        self.assert_denied("/api/v1/terminals?user_id=alice", code=1000, browser=True)
        self.assert_denied("/p/research/api/v1/policies?user_id=alice", code=1000, browser=True)

    def test_browser_all_kinds_use_http_auth_without_consuming_app_frame(self):
        for shared in (False, True):
            with patch.object(settings, "open_webui_url", "" if shared else "http://webui.invalid"):
                for prefix in ("", "/p/research"):
                    for kind in ("ws", "stream", "vnc"):
                        with self.subTest(shared=shared, prefix=prefix, kind=kind):
                            headers = self.headers(Authorization=f"Bearer {'shared-key' if shared else 'jwt-alice'}", Cookie="relay=cookie")
                            path = f"{prefix}/browser/view/tab/cap%3F%23%26/{kind}?quality=80"
                            with self.client.websocket_connect(path, headers=headers) as socket:
                                first_message = json.dumps({"url": "wss://website.invalid/app", "protocols": ["test"]})
                                if kind == "ws":
                                    socket.send_text(first_message)
                                socket.receive_json()
                                record = self.runtime.connections[-1]
                                self.assertNotIn("auth", record)
                                self.assertEqual(record["messages"], [first_message] if kind == "ws" else [])
                                self.assertEqual(urlsplit(record["path"]).path, f"/browser/view/tab/cap%3F%23%26/{kind}")
                                self.assertEqual(record["headers"]["cookie"], "relay=cookie")
                                self.assertTrue(record["headers"]["authorization"].startswith("Bearer worker-secret-"))
                                socket.send_bytes(b"browser-input")
                                self.assertEqual(socket.receive_bytes(), b"browser-input")
                                socket.send_text("__close__")
                                self.assertEqual(socket.receive()["code"], 4004)
                            self.finish(record)

    def test_browser_credentials_and_identity_rejections(self):
        for prefix in ("", "/p/research"):
            path = f"{prefix}/browser/view/tab/cap/stream"
            self.assert_denied(path, browser=True, code=4001, headers=self.headers())
            self.assert_denied(path, browser=True, code=4001, headers=self.headers(Authorization="Bearer bad"))
            self.assert_denied(path, browser=True, code=4003, headers=self.headers(user="bob", Authorization="Bearer jwt-alice"))
            self.assert_denied(path+"?user_id=bob", browser=True, code=4003, headers=self.headers(Authorization="Bearer jwt-alice"))
            self.assert_denied(path, browser=True, code=4001, headers={"Authorization": "Bearer jwt-alice"})
            self.assert_denied(path.replace("stream", "unknown"), browser=True, code=4004)
        with patch.object(settings, "open_webui_url", ""):
            self.assert_denied("/browser/view/tab/cap/stream", browser=True, code=4001,
                               headers=self.headers(Authorization="Bearer bad"))

    def test_opaque_ids_and_query_do_not_inject_user_or_select_policy(self):
        for template in ("/api/terminals/{}", "/execute/{}/status", "/browser/view/{}/cap/stream"):
            opaque = "id?user_id=bob&context_id=chat:other#fragment"
            path = template.format(quote(opaque, safe="")) + "?user_id=alice&policy_id=research"
            browser = path.startswith("/browser/")
            with self.client.websocket_connect(path, headers=self.headers(Authorization="Bearer jwt-alice")) as socket:
                if not browser:
                    socket.send_json({"type": "auth", "token": "jwt-alice"})
                socket.receive_json()
                record = self.runtime.connections[-1]
                self.assertEqual(urlsplit(record["path"]).path, template.format(quote(opaque, safe="")))
                self.assertEqual(self.backend.calls[-1][:3], ("alice", "default", "chat:saved-chat"))
                self.assertEqual(parse_qsl(urlsplit(record["path"]).query), [("policy_id", "research")])
                socket.send_text("__close__")
                self.assertEqual(socket.receive()["code"], 4004)
            self.finish(record)

    def test_http_created_process_and_shell_share_ws_visibility_scope(self):
        self.runtime.enforce_visibility = True
        for prefix in ("", "/p/research"):
            for http_path, ws_path in (("/execute", "/execute/owned-process/status"),
                                       ("/api/terminals", "/api/terminals/owned-shell")):
                headers = self.headers(Authorization="Bearer jwt-alice")
                response = self.client.post(prefix+http_path, headers=headers, json={"command": "fake"})
                self.assertEqual(response.status_code, 200)
                request = self.runtime.http_requests[-1]
                self.assertEqual(request.headers["x-user-id"], "alice")
                self.assertEqual(request.headers["x-session-id"], "saved-chat")
                self.assertNotIn("x-terminal-context-id", request.headers)
                self.assertNotIn("jwt-alice", request.headers["authorization"])
                with self.client.websocket_connect(prefix+ws_path+"?user_id=alice", headers=self.headers()) as socket:
                    socket.send_json({"type": "auth", "token": "jwt-alice"})
                    self.assertIn(response.json()["id"], socket.receive_json()["resources"])
                    record = self.runtime.connections[-1]
                    socket.send_text("__close__")
                    socket.receive()
                self.finish(record)
                # Bob cannot select Alice's worker by either the query or header.
                self.assert_denied(prefix+ws_path+"?user_id=alice", token="jwt-bob", headers=self.headers())
                # Bob's legitimate worker has no access to Alice's objects.
                with self.assertRaises(WebSocketDisconnect) as error:
                    with self.client.websocket_connect(prefix+ws_path+"?user_id=bob", headers=self.headers(user="bob")) as socket:
                        socket.send_json({"type": "auth", "token": "jwt-bob"})
                        socket.receive_json()
                self.assertEqual(error.exception.code, 4004)
                self.finish(self.runtime.connections[-1])
                # A different chat in the same worker also cannot see it.
                with self.assertRaises(WebSocketDisconnect) as error:
                    with self.client.websocket_connect(prefix+ws_path+"?user_id=alice", headers=self.headers(chat="other")) as socket:
                        socket.send_json({"type": "auth", "token": "jwt-alice"})
                        socket.receive_json()
                self.assertEqual(error.exception.code, 4004)
                self.finish(self.runtime.connections[-1])

    def test_http_paths_queries_context_and_permissions_do_not_regress(self):
        for prefix in ("", "/p/research"):
            for path in ("/files/list", "/browser/view/tab/cap/runtime.js"):
                response = self.client.get(prefix+path+"?path=a%20b&option=1&option=2",
                                           headers=self.headers(Authorization="Bearer jwt-alice"))
                self.assertEqual(response.content, b"existing-http-result")
                request = self.runtime.http_requests[-1]
                self.assertEqual(request.url.path, path)
                self.assertEqual(request.url.params.multi_items(), [("path", "a b"), ("option", "1"), ("option", "2")])
                self.assertEqual(self.backend.calls[-1][:3], ("alice", "research" if prefix else "default", "chat:saved-chat"))
        before = len(self.backend.calls)
        self.assertEqual(self.client.get("/files/list", headers=self.headers(user="bob", Authorization="Bearer jwt-alice")).status_code, 403)
        self.assertEqual(self.client.get("/files/list", headers=self.headers()).status_code, 401)
        for path in ("/api/v1/terminals", "/api/v1/policies"):
            self.assertEqual(self.client.get(path, headers=self.headers(Authorization="Bearer jwt-alice")).status_code, 403)
            self.assertEqual(self.client.get(path, headers={"Authorization": "Bearer jwt-alice"}).status_code, 401)
        self.assertEqual(self.client.post("/api/v1/terminals/refresh", headers=self.headers(Authorization="Bearer jwt-alice"), json={"reset": True}).status_code, 403)
        self.assertEqual(len(self.backend.calls), before)

    def test_large_desktop_binary_and_client_close_code(self):
        with self.client.websocket_connect("/desktop/stream?user_id=alice", headers=self.headers()) as socket:
            socket.send_json({"type": "auth", "token": "jwt-alice"})
            socket.receive_json()
            record = self.runtime.connections[-1]
            frame = b"\x00" * (2 * 1024 * 1024)
            socket.send_bytes(frame)
            self.assertEqual(socket.receive_bytes(), frame)
            socket.close(4000)
            socket.receive()
        self.finish(record)
        self.assertEqual(record["close_code"], 4000)

    def test_encoded_path_separators_are_rejected_without_selecting_worker(self):
        for value in ("bad%2F..%2Fevents", "bad%252F..%252Fevents"):
            for template in ("/api/terminals/{}", "/execute/{}/status", "/browser/view/{}/cap/stream"):
                self.assert_denied(template.format(value)+"?user_id=alice", browser=True, code=1000)

    def test_distinct_contexts_select_distinct_worker_keys(self):
        keys = []
        for context in ("chat:first", "chat:second", "automation:job"):
            with self.client.websocket_connect("/events?user_id=alice", headers=self.headers(context=context)) as socket:
                socket.send_json({"type": "auth", "token": "jwt-alice"})
                socket.receive_json()
                record = self.runtime.connections[-1]
                keys.append(record["auth"]["token"])
                socket.send_text("__close__")
                socket.receive()
            self.finish(record)
        self.assertEqual(len(set(keys)), 3)

    def test_upstream_error_close_terminates_both_relay_tasks(self):
        with self.client.websocket_connect("/events?user_id=alice", headers=self.headers()) as socket:
            socket.send_json({"type": "auth", "token": "jwt-alice"})
            socket.receive_json()
            record = self.runtime.connections[-1]
            socket.send_text("__close_error__")
            self.assertEqual(socket.receive()["code"], 1011)
        self.finish(record)

    def test_connect_retry_resolves_same_scope_and_replaces_worker_credentials(self):
        real_connect = proxy.ws_connect
        attempted = []

        async def connect(url, **kwargs):
            attempted.append((url, kwargs))
            if len(attempted) == 1:
                # Simulate a replaced worker before the retry, keeping scope fixed.
                self.backend.instances[("alice", "research", "chat:saved-chat")]["api_key"] = "worker-replacement"
                raise ConnectionRefusedError("worker starting")
            return await real_connect(url, **kwargs)

        with patch.object(proxy, "ws_connect", side_effect=connect), patch.object(proxy.asyncio, "sleep", new=AsyncMock()):
            headers = self.headers(Authorization="Bearer jwt-alice")
            with self.client.websocket_connect("/p/research/browser/view/tab/cap/stream?quality=80", headers=headers) as socket:
                socket.receive_json()
                record = self.runtime.connections[-1]
                socket.send_text("__close__")
                socket.receive()
            self.finish(record)
        self.assertEqual(len(attempted), 2)
        self.assertEqual(attempted[0][1]["additional_headers"]["Authorization"], "Bearer worker-secret-0")
        self.assertEqual(attempted[1][1]["additional_headers"]["Authorization"], "Bearer worker-replacement")
        self.assertEqual(self.backend.invalidated, [("alice", "research", "chat:saved-chat")])
        self.assertEqual([call[:3] for call in self.backend.calls], [("alice", "research", "chat:saved-chat")] * 2)

    def test_resolution_failure_cleans_connection_counter(self):
        self.backend.fail = True
        with self.assertRaises(WebSocketDisconnect) as error:
            with self.client.websocket_connect("/events?user_id=alice") as socket:
                socket.send_json({"type": "auth", "token": "jwt-alice"})
                socket.receive_text()
        self.assertEqual(error.exception.code, 4003)
        self.assertEqual(proxy.active_ws_connections, 0)


if __name__ == "__main__":
    unittest.main()
