from __future__ import annotations

import socket

import pytest
import requests

from tests.master.db_mocker import DbMocker
from tests.master.mock_master_server import MockMasterServer


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def db_mocker() -> DbMocker:
    return DbMocker()


@pytest.fixture
def master_api_base_url(db_mocker: DbMocker) -> str:
    server = MockMasterServer("127.0.0.1", _free_port(), db_mocker)
    server.start()
    base_url = f"http://{server.host}:{server.port}"
    response = requests.get(f"{base_url}/health", timeout=2)
    assert response.status_code == 200
    try:
        yield base_url
    finally:
        server.stop()
