from __future__ import annotations

import pytest
import requests


@pytest.mark.integration
def test_master_agent_routes_return_db_error_when_db_unavailable(master_api_base_url: str) -> None:
    upsert_payload = {
        "os": "linux",
        "version": "1.0.0",
        "resources": {"cpu_cores": 4, "ram_mb": 2048, "slots": 1},
    }
    response = requests.put(
        f"{master_api_base_url}/api/v1/agents/agent-1",
        json=upsert_payload,
        timeout=3,
    )
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "DB_ERROR"

    response = requests.get(f"{master_api_base_url}/api/v1/agents/agent-1", timeout=3)
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "DB_ERROR"


@pytest.mark.integration
def test_master_task_routes_return_db_error_when_db_unavailable(master_api_base_url: str) -> None:
    create_payload = {
        "command": "/bin/echo",
        "args": ["hello"],
        "env": {"K": "V"},
    }
    response = requests.post(
        f"{master_api_base_url}/api/v1/tasks",
        json=create_payload,
        timeout=3,
    )
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "DB_ERROR"

    response = requests.get(f"{master_api_base_url}/api/v1/tasks/1", timeout=3)
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "DB_ERROR"

    update_payload = {"state": "succeeded", "exit_code": 0}
    response = requests.post(
        f"{master_api_base_url}/api/v1/tasks/1/status",
        json=update_payload,
        timeout=3,
    )
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "DB_ERROR"

    response = requests.get(f"{master_api_base_url}/api/v1/tasks", timeout=3)
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "DB_ERROR"


@pytest.mark.integration
def test_master_unknown_route_returns_not_found(master_api_base_url: str) -> None:
    response = requests.get(f"{master_api_base_url}/api/v1/does-not-exist", timeout=3)
    assert response.status_code == 404
