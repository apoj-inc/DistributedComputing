import json
import os
import time
import pytest
import requests
from pathlib import Path

from test_common_utils import DbHelper, MasterProcess, wait_for_master_ready, make_id

# ----------------------------------------------------------------------
# Fixtures (similar to previous)
# ----------------------------------------------------------------------
@pytest.fixture(scope="session")
def master_config():
    master_env_path = os.environ.get("DC_MASTER_ENV_FILE", "master_env.json")
    with open(master_env_path) as f:
        data = json.load(f)

    config = {
        "db_host": data["DB_HOST"],
        "db_port": data["DB_PORT"],
        "db_user": data["DB_USER"],
        "db_password": data["DB_PASSWORD"],
        "db_name": data["DB_NAME"],
        "db_sslmode": "true" if data.get("DB_SSLMODE") else "false",
        "master_host": data.get("MASTER_HOST", "127.0.0.1"),
        "master_port": int(data.get("MASTER_PORT", 8080)),
        "master_bin": os.environ["DC_MASTER_BIN"],
        "log_dir": data.get("LOG_DIR", "test_logs"),
        "heartbeat_sec": data.get("HEARTBEAT_SEC", 1),
        "offline_sec": data.get("OFFLINE_SEC", 1),
        "work_dir": Path.cwd(),
    }
    return config

@pytest.fixture(scope="session")
def db_helper(master_config):
    helper = DbHelper(
        master_config["db_host"],
        master_config["db_port"],
        master_config["db_user"],
        master_config["db_password"],
        master_config["db_name"],
        master_config["db_sslmode"]
    )
    yield helper
    helper.conn.close()

@pytest.fixture(scope="session")
def master_process(master_config):
    env = {
        "DB_HOST": master_config["db_host"],
        "DB_PORT": str(master_config["db_port"]),
        "DB_USER": master_config["db_user"],
        "DB_PASSWORD": master_config["db_password"],
        "DB_NAME": master_config["db_name"],
        "DB_SSLMODE": master_config["db_sslmode"],
        "MASTER_HOST": master_config["master_host"],
        "MASTER_PORT": str(master_config["master_port"]),
        "LOG_DIR": master_config["log_dir"],
        "HEARTBEAT_SEC": str(master_config["heartbeat_sec"]),
        "OFFLINE_SEC": str(master_config["offline_sec"]),
    }
    master = MasterProcess(master_config["master_bin"], env, work_dir=master_config["work_dir"])
    master.start()
    if not wait_for_master_ready(master_config["master_host"], master_config["master_port"], timeout=10):
        master.stop()
        pytest.fail("Master did not become ready")
    yield master
    master.stop()

@pytest.fixture(autouse=True)
def clear_db(db_helper):
    db_helper.clear_all()

@pytest.fixture
def client(master_config):
    return requests.Session()

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def expect_error(res, expected_status, expected_code):
    assert res.status_code == expected_status
    data = res.json()
    assert "error" in data
    assert data["error"]["code"] == expected_code

def expect_status(res, expected_status):
    assert res.status_code == expected_status

def submit_task(client, base_url, payload):
    resp = client.post(f"{base_url}/api/v1/tasks", json=payload)
    expect_status(resp, 201)
    task_id = str(resp.json()["task_id"])
    return task_id

# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------
def test_register_agent_and_heartbeat(master_config, client, db_helper):
    base_url = f"http://{master_config['master_host']}:{master_config['master_port']}"
    agent_id = make_id("agent")
    payload = {
        "os": "linux",
        "version": "1.0",
        "resources": {"cpu_cores": 4, "ram_mb": 2048, "slots": 2}
    }
    resp = client.put(f"{base_url}/api/v1/agents/{agent_id}", json=payload)
    expect_status(resp, 200)
    data = resp.json()
    assert data["status"] == "ok"
    assert data["heartbeat_interval_sec"] == master_config["heartbeat_sec"]
    assert db_helper.get_agent_status(agent_id) == "idle"

    heartbeat = {"status": "busy"}
    resp = client.post(f"{base_url}/api/v1/agents/{agent_id}/heartbeat", json=heartbeat)
    expect_status(resp, 200)
    assert db_helper.get_agent_status(agent_id) == "busy"

def test_register_agent_validation(master_config, client, db_helper):
    base_url = f"http://{master_config['master_host']}:{master_config['master_port']}"
    agent_id = make_id("agent_bad")
    payload = {"os": "linux"}  # missing version and resources
    resp = client.put(f"{base_url}/api/v1/agents/{agent_id}", json=payload)
    expect_error(resp, 400, "BAD_REQUEST")
    assert not db_helper.has_agent(agent_id)

def test_submit_task_and_get(master_config, client, db_helper):
    base_url = f"http://{master_config['master_host']}:{master_config['master_port']}"
    payload = {
        "command": "echo",
        "args": ["hello"],
        "env": {"KEY": "VALUE"},
        "timeout_sec": 5,
        "constraints": {"os": "linux", "cpu_cores": 1}
    }
    resp = client.post(f"{base_url}/api/v1/tasks", json=payload)
    expect_status(resp, 201)
    task_id = str(resp.json()["task_id"])

    assert db_helper.get_task_state(task_id) == "queued"
    assert db_helper.get_constraint_os(task_id) == "linux"
    assert db_helper.get_constraint_cpu(task_id) == 1

    resp = client.get(f"{base_url}/api/v1/tasks/{task_id}")
    expect_status(resp, 200)
    data = resp.json()["task"]
    assert data["task_id"] == task_id
    assert data["state"] == "queued"

def test_poll_assigns_and_completes_tasks(master_config, client, db_helper):
    base_url = f"http://{master_config['master_host']}:{master_config['master_port']}"
    # Register agent
    agent_id = make_id("agent_poll")
    agent_payload = {
        "os": "linux",
        "version": "1.0",
        "resources": {"cpu_cores": 4, "ram_mb": 2048, "slots": 2}
    }
    client.put(f"{base_url}/api/v1/agents/{agent_id}", json=agent_payload)

    # Submit two tasks
    task_payload = {"command": "echo", "constraints": {"os": "linux"}}
    task1 = submit_task(client, base_url, task_payload)
    task2 = submit_task(client, base_url, task_payload)

    # Poll with free_slots=1
    poll_resp = client.post(f"{base_url}/api/v1/agents/{agent_id}/tasks:poll", json={"free_slots": 1})
    expect_status(poll_resp, 200)
    tasks = poll_resp.json()["tasks"]
    assert len(tasks) == 1
    assigned_task = str(tasks[0]["task_id"])

    # Task should be running
    assert db_helper.get_task_state(assigned_task) == "running"
    assert db_helper.get_task_assigned_agent(assigned_task) == agent_id
    assert db_helper.task_started(assigned_task) is True
    assert db_helper.count_assignments(assigned_task, only_open=True) == 1

    # Mark as succeeded
    status_payload = {"state": "succeeded", "exit_code": 0}
    status_resp = client.post(f"{base_url}/api/v1/tasks/{assigned_task}/status", json=status_payload)
    expect_status(status_resp, 200)
    assert db_helper.get_task_state(assigned_task) == "succeeded"
    assert db_helper.get_task_exit_code(assigned_task) == 0

    # Poll again, get second task
    poll_resp2 = client.post(f"{base_url}/api/v1/agents/{agent_id}/tasks:poll", json={"free_slots": 1})
    expect_status(poll_resp2, 200)
    tasks2 = poll_resp2.json()["tasks"]
    assert len(tasks2) == 1
    assert str(tasks2[0]["task_id"]) != assigned_task

def test_cancel_task(master_config, client, db_helper):
    base_url = f"http://{master_config['master_host']}:{master_config['master_port']}"
    task_id = submit_task(client, base_url, {"command": "echo"})
    resp = client.post(f"{base_url}/api/v1/tasks/{task_id}/cancel")
    expect_status(resp, 200)
    assert db_helper.get_task_state(task_id) == "canceled"
    assert "canceled_by_user" in db_helper.get_task_error_message(task_id)

def test_logs_endpoint(tmp_path, master_config, client):
    base_url = f"http://{master_config['master_host']}:{master_config['master_port']}"
    task_id = submit_task(client, base_url, {"command": "echo"})

    # Create log files manually
    log_dir = Path(master_config["log_dir"]) / task_id
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_log = log_dir / "stdout.log"
    stdout_log.write_text("hello stdout\n")
    stderr_log = log_dir / "stderr.log"
    stderr_log.write_text("hello stderr\n")

    # Get stdout
    resp = client.get(f"{base_url}/api/v1/tasks/{task_id}/logs")
    expect_status(resp, 200)
    assert resp.text == "hello stdout\n"

    # Get stderr
    resp = client.get(f"{base_url}/api/v1/tasks/{task_id}/logs?stream=stderr")
    expect_status(resp, 200)
    assert resp.text == "hello stderr\n"

    # Tail
    resp = client.get(f"{base_url}/api/v1/tasks/{task_id}/logs:tail?stream=stdout&from=6")
    expect_status(resp, 200)
    assert resp.text == "stdout\n"
    assert "X-Log-Size" in resp.headers
