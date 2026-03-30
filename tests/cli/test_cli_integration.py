import json
import os
import pytest
import requests
import subprocess
import time
from pathlib import Path

import psycopg2

from test_common_utils import DbHelper, MasterProcess, wait_for_master_ready, extract_numeric_token

# ----------------------------------------------------------------------
# Fixtures (reused from previous integration tests)
# ----------------------------------------------------------------------
@pytest.fixture(scope="session")
def master_config():
    """Load configuration from master_env.json and env vars."""
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
    if not wait_for_master_ready(master_config["master_host"], master_config["master_port"], timeout=5):
        master.stop()
        pytest.fail("Master did not become ready")
    yield master
    master.stop()

@pytest.fixture(autouse=True)
def clear_db(db_helper):
    db_helper.clear_all()

# ----------------------------------------------------------------------
# CLI helper
# ----------------------------------------------------------------------
def run_cli_command(args, env=None):
    """Run the CLI binary with given arguments and return (stdout, stderr, exitcode)."""
    cli_bin = os.environ.get("DC_CLI_BIN")
    if not cli_bin:
        pytest.skip("DC_CLI_BIN not set")
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    proc = subprocess.run([cli_bin] + args, capture_output=True, text=True, env=full_env)
    return proc.stdout, proc.stderr, proc.returncode

# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------
def test_tasks_submit_and_list(master_config, db_helper):
    """Test submitting a task and then listing it."""
    base_url = f"http://{master_config['master_host']}:{master_config['master_port']}"
    env = {"MASTER_URL": base_url}  # CLI uses MASTER_URL env var

    # Submit task
    stdout, stderr, rc = run_cli_command(["tasks", "submit", "--cmd", "/bin/true"], env=env)
    assert rc == 0
    task_id = extract_numeric_token(stdout)
    assert task_id is not None

    # Wait for task to be queued
    state = db_helper.get_task_state(task_id)
    assert state == "queued"

    # Get task by ID
    stdout, stderr, rc = run_cli_command(["tasks", "get", task_id], env=env)
    assert rc == 0
    assert task_id in stdout

    # List tasks
    stdout, stderr, rc = run_cli_command(["tasks", "list"], env=env)
    assert rc == 0
    assert task_id in stdout

def test_agents_list(master_config):
    """Test registering an agent and listing it via CLI."""
    base_url = f"http://{master_config['master_host']}:{master_config['master_port']}"
    env = {"MASTER_URL": base_url}

    # Register an agent via API
    agent_id = f"agent_test_{os.getpid()}_{int(time.time())}"
    agent_payload = {
        "os": "linux",
        "version": "1.0",
        "resources": {"cpu_cores": 2, "ram_mb": 512, "slots": 1}
    }
    resp = requests.put(f"{base_url}/api/v1/agents/{agent_id}", json=agent_payload)
    assert resp.status_code == 200

    # List agents via CLI
    stdout, stderr, rc = run_cli_command(["agents", "list"], env=env)
    assert rc == 0
    assert agent_id in stdout
