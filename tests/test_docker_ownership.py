import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiodocker

from terminals.backends.docker import DockerBackend
from terminals.config import settings


class DockerOwnershipTests(unittest.IsolatedAsyncioTestCase):
    def test_namespace_preserves_live_names_and_context_limit(self):
        with patch.object(settings, "instance_namespace", "live20261018"):
            self.assertEqual(
                DockerBackend._container_name("clash-proxy", "system"),
                "terminals-live20261018-bbc5e661e106-clash-proxy",
            )
            contextual = DockerBackend._container_name("policy-" * 10, "user", "chat:123")
            self.assertLessEqual(len(contextual), 63)
            self.assertNotEqual(contextual, DockerBackend._container_name("policy-" * 10, "user"))
        with patch.object(settings, "instance_namespace", ""):
            self.assertEqual(DockerBackend._container_name("default", "system"), "terminals-bbc5e661e106")

    async def test_reconcile_only_owned_instances(self):
        owned = SimpleNamespace(show=AsyncMock(return_value={
            "Name": "/owned", "Config": {"Env": ["OPEN_TERMINAL_API_KEY=test"], "Labels": {
                "openwebui.com/instance-namespace": "live20261018",
                "openwebui.com/user-id": "user", "openwebui.com/policy": "clash-proxy",
            }},
        }))
        foreign = SimpleNamespace(show=AsyncMock(return_value={
            "Name": "/foreign", "Config": {"Labels": {
                "openwebui.com/instance-namespace": "other",
                "openwebui.com/user-id": "user", "openwebui.com/policy": "clash-proxy",
            }},
        }))
        containers = SimpleNamespace(list=AsyncMock(return_value=[foreign, owned]))
        backend = DockerBackend()
        backend._get_docker = AsyncMock(return_value=SimpleNamespace(containers=containers))
        backend._extract_instance_info = AsyncMock(return_value={"host": "owned", "port": 8000})
        backend._adopted_spec = AsyncMock(return_value={})
        backend._seed_adopted_activity = AsyncMock()
        with patch.object(settings, "instance_namespace", "live20261018"):
            await backend.reconcile()
        self.assertEqual(containers.list.call_args.kwargs["filters"]["label"], [
            "app.kubernetes.io/managed-by=terminals", "openwebui.com/instance-namespace=live20261018",
        ])
        self.assertEqual(list(backend._instances), [("user", "clash-proxy", "default")])
        backend._extract_instance_info.assert_awaited_once_with(owned, "owned", "test")

    async def test_conflict_does_not_adopt_or_delete_foreign_container(self):
        foreign = SimpleNamespace(show=AsyncMock(return_value={
            "State": {"Running": True}, "Config": {"Labels": {"openwebui.com/instance-namespace": "other"}},
        }), delete=AsyncMock())
        containers = SimpleNamespace(
            create=AsyncMock(side_effect=aiodocker.exceptions.DockerError(409, {"message": "Conflict"})),
            get=AsyncMock(return_value=foreign),
        )
        backend = DockerBackend()
        backend._get_docker = AsyncMock(return_value=SimpleNamespace(containers=containers))
        with patch.object(settings, "instance_namespace", "live20261018"):
            with self.assertRaisesRegex(RuntimeError, "another orchestrator"):
                await backend.provision("user")
        foreign.delete.assert_not_awaited()

    async def test_custom_network_keeps_runtime_ports_internal(self):
        for network, published in [("terminal-pool", False), ("", True)]:
            with self.subTest(network=network):
                containers = SimpleNamespace(create=AsyncMock(return_value=SimpleNamespace(start=AsyncMock())))
                backend = DockerBackend()
                backend._get_docker = AsyncMock(return_value=SimpleNamespace(containers=containers))
                backend._extract_instance_info = AsyncMock(return_value={"host": "runtime", "port": 8000})
                backend._wait_until_ready = AsyncMock()
                with patch.object(settings, "instance_namespace", "live20261018"), patch.object(settings, "docker_network", network):
                    await backend.provision("user", policy_id="clash-proxy")
                config = containers.create.call_args.args[0]
                self.assertEqual(config["HostConfig"]["PublishAllPorts"], published)
                self.assertEqual(config["Labels"]["openwebui.com/instance-namespace"], "live20261018")
                self.assertEqual(config["HostConfig"].get("NetworkMode"), network or None)


if __name__ == "__main__":
    unittest.main()
