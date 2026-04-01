from __future__ import annotations

import socket

import pytest
import requests

from tests.worker.mock_master_for_worker_server import MockMasterForWorkerServer
from tests.worker.worker_mocker import WorkerMocker


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def worker_mocker() -> WorkerMocker:
    return WorkerMocker()


@pytest.fixture
def worker_master_api_base_url(worker_mocker: WorkerMocker) -> str:
    server = MockMasterForWorkerServer("127.0.0.1", _free_port(), worker_mocker)
    server.start()
    base_url = f"http://{server.host}:{server.port}"
    response = requests.get(f"{base_url}/health", timeout=2)
    assert response.status_code == 200
    try:
        yield base_url
    finally:
        server.stop()
