from types import SimpleNamespace

import pytest

from terminals.backends import docker as docker_module
from terminals.backends.docker import DockerBackend


SAMPLE_INFO = {
    "Id": "container-id",
    "NetworkSettings": {
        "Networks": {
            "bridge": {
                "IPAddress": "172.17.0.23",
            }
        },
        "Ports": {
            "8000/tcp": [
                {
                    "HostIp": "0.0.0.0",
                    "HostPort": "49152",
                }
            ]
        },
    },
}

INFO_WITHOUT_BRIDGE_IP = {
    "Id": "container-id",
    "NetworkSettings": {
        "Networks": {
            "bridge": {
                "IPAddress": "",
            }
        },
        "Ports": {
            "8000/tcp": [
                {
                    "HostIp": "0.0.0.0",
                    "HostPort": "49152",
                }
            ]
        },
    },
}


def patch_settings(monkeypatch, **overrides):
    settings = SimpleNamespace(
        network="",
        docker_connect_mode="host",
        docker_host="127.0.0.1",
    )
    for key, value in overrides.items():
        setattr(settings, key, value)
    monkeypatch.setattr(docker_module, "settings", settings)
    return settings


def test_resolve_connection_target_uses_container_name_on_custom_network(
    monkeypatch,
):
    backend = DockerBackend()
    patch_settings(monkeypatch, network="terminals-net", docker_connect_mode="host")

    host, port = backend._resolve_connection_target(SAMPLE_INFO, "terminals-default-user")

    assert (host, port) == ("terminals-default-user", 8000)


def test_resolve_connection_target_uses_bridge_ip_in_bridge_mode(monkeypatch):
    backend = DockerBackend()
    patch_settings(monkeypatch, network="", docker_connect_mode="bridge")

    host, port = backend._resolve_connection_target(SAMPLE_INFO, "terminals-default-user")

    assert (host, port) == ("172.17.0.23", 8000)


def test_resolve_connection_target_uses_host_port_in_host_mode(monkeypatch):
    backend = DockerBackend()
    patch_settings(
        monkeypatch,
        network="",
        docker_connect_mode="host",
        docker_host="host.docker.internal",
    )

    host, port = backend._resolve_connection_target(SAMPLE_INFO, "terminals-default-user")

    assert (host, port) == ("host.docker.internal", 49152)


def test_bridge_mode_requires_a_bridge_ip(monkeypatch):
    backend = DockerBackend()
    patch_settings(monkeypatch, network="", docker_connect_mode="bridge")

    with pytest.raises(RuntimeError, match="bridge IP"):
        backend._resolve_connection_target(INFO_WITHOUT_BRIDGE_IP, "terminals-default-user")
