from __future__ import annotations

import pytest
import requests

from tests.worker.worker_mocker import WorkerMocker


@pytest.mark.integration
def test_worker_register_heartbeat_poll_flow(
    worker_master_api_base_url: str, worker_mocker: WorkerMocker
) -> None:
    register_payload = {
        "os": "linux",
        "version": "1.0.0",
        "resources": {"cpu_cores": 4, "ram_mb": 2048, "slots": 2},
    }
    response = requests.put(
        f"{worker_master_api_base_url}/api/v1/agents/worker-1",
        json=register_payload,
        timeout=3,
    )
    assert response.status_code == 200
    assert response.json()["heartbeat_interval_sec"] == 5

    worker_mocker.enqueue_task_for_agent(
        "worker-1",
        {
            "task_id": 101,
            "command": "/bin/echo",
            "args": ["hello"],
            "env": {"X": "1"},
            "timeout_sec": 30,
            "constraints": {"os": "linux"},
        },
    )

    heartbeat_payload = {"status": "idle"}
    response = requests.post(
        f"{worker_master_api_base_url}/api/v1/agents/worker-1/heartbeat",
        json=heartbeat_payload,
        timeout=3,
    )
    assert response.status_code == 200

    response = requests.post(
        f"{worker_master_api_base_url}/api/v1/agents/worker-1/tasks:poll",
        json={"free_slots": 1},
        timeout=3,
    )
    assert response.status_code == 200
    tasks = response.json()["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["task_id"] == 101

    tx_names = [tx["tx"] for tx in worker_mocker.transactions()]
    assert "register_agent" in tx_names
    assert "send_heartbeat" in tx_names
    assert "poll_tasks" in tx_names


@pytest.mark.integration
def test_worker_task_status_and_log_upload_are_intercepted(
    worker_master_api_base_url: str, worker_mocker: WorkerMocker
) -> None:
    requests.put(
        f"{worker_master_api_base_url}/api/v1/agents/worker-2",
        json={
            "os": "linux",
            "version": "1.0.0",
            "resources": {"cpu_cores": 2, "ram_mb": 1024, "slots": 1},
        },
        timeout=3,
    )
    worker_mocker.enqueue_task_for_agent(
        "worker-2",
        {"task_id": 202, "command": "/bin/echo", "args": [], "env": {}, "constraints": {}},
    )

    response = requests.post(
        f"{worker_master_api_base_url}/api/v1/agents/worker-2/tasks:poll",
        json={"free_slots": 1},
        timeout=3,
    )
    assert response.status_code == 200

    response = requests.post(
        f"{worker_master_api_base_url}/api/v1/tasks/202/status",
        json={"state": "succeeded", "exit_code": 0},
        timeout=3,
    )
    assert response.status_code == 200

    response = requests.post(
        f"{worker_master_api_base_url}/api/v1/tasks/202/logs:upload",
        json={"stream": "stdout", "data": "ok"},
        timeout=3,
    )
    assert response.status_code == 200
    assert response.json()["size_bytes"] == 2

    response = requests.get(f"{worker_master_api_base_url}/api/v1/tasks/202", timeout=3)
    assert response.status_code == 200
    assert response.json()["task"]["state"] == "succeeded"

    tx_names = [tx["tx"] for tx in worker_mocker.transactions()]
    assert "update_task_status" in tx_names
    assert "upload_log" in tx_names
    assert "get_task_state" in tx_names
