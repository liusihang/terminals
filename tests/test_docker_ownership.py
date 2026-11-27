import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiodocker.exceptions import DockerError
from fastapi import HTTPException
from machines.backends.docker import DockerBackend, ProvisioningError
from machines.config import settings
from machines.utils.instances import stop_instance


class DockerOwnershipTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.template, self.context = "research", "user:alice"
        self.backend = DockerBackend()
        self.labels = {
            "app.kubernetes.io/managed-by": "terminals", "openwebui.com/template": self.template,
            "openwebui.com/context-id": self.context, "openwebui.com/instance-namespace": "staging",
        }
        self.enterContext(patch.object(settings, "instance_namespace", "staging"))
        self.enterContext(patch.object(settings, "docker_network", "isolated-pool"))
        self.enterContext(patch("machines.backends.docker.ensure_docker_image", AsyncMock()))
        self.containers = SimpleNamespace(run=AsyncMock(), get=AsyncMock(), list=AsyncMock())
        self.backend._get_docker = AsyncMock(return_value=SimpleNamespace(containers=self.containers))
        self.backend._extract_instance_info = AsyncMock(return_value={"host": "worker", "port": 8000})
        self.backend._wait_until_ready = AsyncMock()

    async def test_provision_and_reconcile_only_our_namespace(self):
        await self.backend.provision(self.template, self.context)
        config = self.containers.run.call_args.args[0]
        self.assertFalse(config["HostConfig"]["PublishAllPorts"])
        self.assertEqual(config["HostConfig"]["NetworkMode"], "isolated-pool")
        self.assertEqual(config["Labels"]["openwebui.com/instance-namespace"], "staging")
        name = self.backend._container_name(self.template, self.context)
        def worker(namespace):
            return SimpleNamespace(show=AsyncMock(return_value={"Name": "/"+name, "Config": {
                "Env": ["OPEN_TERMINAL_API_KEY=test"], "Labels": {**self.labels, "openwebui.com/instance-namespace": namespace}
            }}))
        ours, other = worker("staging"), worker("production")
        self.containers.list.return_value = [other, ours]
        with patch("machines.backends.docker.resolve_template_spec", AsyncMock(return_value={"scope": "user"})):
            await self.backend.reconcile()
        self.assertIn("openwebui.com/instance-namespace=staging", self.containers.list.call_args.kwargs["filters"]["label"])
        self.backend._extract_instance_info.assert_awaited_with(ours, name, "test")

    async def test_name_conflict_and_stop_check_ownership(self):
        foreign = SimpleNamespace(show=AsyncMock(return_value={
            "State": {"Running": True}, "Config": {"Labels": {**self.labels, "openwebui.com/instance-namespace": "other"}}}),
            delete=AsyncMock())
        self.containers.get.return_value = foreign
        self.containers.run.side_effect = DockerError(409, {"message": "Conflict"})
        with self.assertRaises(ProvisioningError):
            await self.backend.provision(self.template, self.context)
        with self.assertRaises(HTTPException) as error:
            await stop_instance(self.backend, self.template, self.context)
        self.assertEqual(error.exception.status_code, 409)
        foreign.delete.assert_not_awaited()
