from __future__ import annotations

import json

import pytest
import requests


def _response_dump(response: requests.Response) -> str:
    try:
        payload = response.json()
        payload_text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    except ValueError:
        payload_text = "<non-json>"
    return (
        f"request={response.request.method} {response.url}\n"
        f"status={response.status_code}\n"
        f"headers={dict(response.headers)}\n"
        f"text={response.text}\n"
        f"json={payload_text}"
    )


@pytest.mark.integration
def test_master_agent_api_with_postgres_container(master_api_base_url: str) -> None:
    upsert_payload = {
        'os': 'linux',
        'version': '1.0.0',
        'resources': {'cpu_cores': 4, 'ram_mb': 2048, 'slots': 1},
    }
    response = requests.put(
        f'{master_api_base_url}/api/v1/agents/agent-1',
        json=upsert_payload,
        timeout=3,
    )
    assert response.status_code == 200, _response_dump(response)

    response = requests.get(f'{master_api_base_url}/api/v1/agents/agent-1', timeout=3)
    assert response.status_code == 200, _response_dump(response)
    agent = response.json()['agent']
    assert agent['agent_id'] == 'agent-1'
    assert agent['status'] == 'idle'


@pytest.mark.integration
def test_master_task_api_with_postgres_container(master_api_base_url: str) -> None:
    create_payload = {
        'command': '/bin/echo',
        'args': ['hello'],
        'env': {'K': 'V'},
    }
    response = requests.post(
        f'{master_api_base_url}/api/v1/tasks',
        json=create_payload,
        timeout=3,
    )
    assert response.status_code == 201, _response_dump(response)
    task_id = int(response.json()['task_id'])

    response = requests.get(f'{master_api_base_url}/api/v1/tasks/{task_id}', timeout=3)
    assert response.status_code == 200, _response_dump(response)
    assert response.json()['task']['command'] == '/bin/echo'

    update_payload = {'state': 'running', 'exit_code': 0}
    response = requests.post(
        f'{master_api_base_url}/api/v1/tasks/{task_id}/status',
        json=update_payload,
        timeout=3,
    )
    assert response.status_code == 200, _response_dump(response)

    update_payload = {'state': 'succeeded', 'exit_code': 0}
    response = requests.post(
        f'{master_api_base_url}/api/v1/tasks/{task_id}/status',
        json=update_payload,
        timeout=3,
    )
    assert response.status_code == 200, _response_dump(response)

    response = requests.get(f'{master_api_base_url}/api/v1/tasks', timeout=3)
    assert response.status_code == 200, _response_dump(response)
    tasks = response.json()['tasks']
    assert any(int(task['task_id']) == task_id for task in tasks), (
        f"created task_id={task_id} not found in tasks list:\n"
        f"{json.dumps(tasks, ensure_ascii=False, indent=2)}"
    )


@pytest.mark.integration
def test_master_unknown_route_returns_not_found(master_api_base_url: str) -> None:
    response = requests.get(f'{master_api_base_url}/api/v1/does-not-exist', timeout=3)
    assert response.status_code == 404, _response_dump(response)
