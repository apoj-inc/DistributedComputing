from __future__ import annotations

import pytest
import requests

from tests.master.db_mocker import DbMocker


@pytest.mark.integration
def test_master_agent_api_uses_db_mocker(master_api_base_url: str, db_mocker: DbMocker) -> None:
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
    assert response.status_code == 200

    response = requests.get(f"{master_api_base_url}/api/v1/agents/agent-1", timeout=3)
    assert response.status_code == 200
    assert response.json()["agent"]["agent_id"] == "agent-1"

    tx_names = [tx["tx"] for tx in db_mocker.transactions()]
    assert "upsert_agent" in tx_names
    assert "get_agent" in tx_names


@pytest.mark.integration
def test_master_task_api_intercepts_db_transactions(
    master_api_base_url: str, db_mocker: DbMocker
) -> None:
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
    assert response.status_code == 201
    task_id = response.json()["task_id"]

    response = requests.get(f"{master_api_base_url}/api/v1/tasks/{task_id}", timeout=3)
    assert response.status_code == 200
    assert response.json()["task"]["command"] == "/bin/echo"

    update_payload = {"state": "succeeded", "exit_code": 0}
    response = requests.post(
        f"{master_api_base_url}/api/v1/tasks/{task_id}/status",
        json=update_payload,
        timeout=3,
    )
    assert response.status_code == 200

    response = requests.get(f"{master_api_base_url}/api/v1/tasks", timeout=3)
    assert response.status_code == 200
    tasks = response.json()["tasks"]
    assert any(task["task_id"] == task_id for task in tasks)

    tx_names = [tx["tx"] for tx in db_mocker.transactions()]
    assert "create_task" in tx_names
    assert "get_task" in tx_names
    assert "update_task_status" in tx_names
    assert "list_tasks" in tx_names
