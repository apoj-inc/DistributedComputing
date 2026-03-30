import os
import sys
import json
import time
import signal
import subprocess
import tempfile
import shutil
import pytest
import requests
import psycopg2
from pathlib import Path
from typing import Dict, Optional, Any
from contextlib import contextmanager

# ----------------------------------------------------------------------
# Configuration helpers
# ----------------------------------------------------------------------
def load_json(path: str) -> Dict[str, Any]:
    """Load JSON file and return dict."""
    with open(path, 'r') as f:
        return json.load(f)

def get_required_env(var: str) -> str:
    """Get required environment variable or raise."""
    val = os.environ.get(var)
    if not val:
        raise RuntimeError(f"Missing required env: {var}")
    return val

class TestConfig:
    """Configuration loaded from master_env.json and environment variables."""

    def __init__(self, master_env_path: Optional[str] = None):
        # Master config from JSON
        if master_env_path is None:
            master_env_path = os.environ.get("DC_MASTER_ENV_FILE", "master_env.json")
        self.master_env = load_json(master_env_path)

        # Required environment variables for binaries
        self.master_bin = get_required_env("DC_MASTER_BIN")
        self.worker_bin = get_required_env("DC_WORKER_BIN")

        # Work directory (used as cwd for master and worker)
        self.work_dir = Path(os.environ.get("DC_WORK_DIR", Path.cwd()))

        # Temporary directory for logs (master and worker logs will be placed here)
        self.log_dir = Path(tempfile.mkdtemp(prefix="distcomp_test_"))

        # Expose master config fields as attributes
        self.db_host = self.master_env.get("DB_HOST")
        self.db_port = str(self.master_env.get("DB_PORT"))
        self.db_user = self.master_env.get("DB_USER")
        self.db_password = self.master_env.get("DB_PASSWORD")
        self.db_name = self.master_env.get("DB_NAME")
        self.db_sslmode = "true" if self.master_env.get("DB_SSLMODE") else "false"
        self.master_host = self.master_env.get("MASTER_HOST", "127.0.0.1")
        self.master_port = int(self.master_env.get("MASTER_PORT", 8080))
        self.master_log_dir = Path(self.master_env.get("LOG_DIR", str(self.log_dir / "master")))
        self.heartbeat_sec = int(self.master_env.get("HEARTBEAT_SEC", 1))
        self.offline_sec = int(self.master_env.get("OFFLINE_SEC", 1))

        # Ensure master log directory exists
        self.master_log_dir.mkdir(parents=True, exist_ok=True)

    def connection_string(self) -> str:
        """PostgreSQL connection string."""
        parts = []
        if self.db_host:
            parts.append(f"host={self.db_host}")
        if self.db_port:
            parts.append(f"port={self.db_port}")
        if self.db_user:
            parts.append(f"user={self.db_user}")
        if self.db_password:
            parts.append(f"password={self.db_password}")
        if self.db_name:
            parts.append(f"dbname={self.db_name}")
        if self.db_sslmode:
            parts.append(f"sslmode={self.db_sslmode}")
        return " ".join(parts)

    def master_environment(self) -> Dict[str, str]:
        """Environment variables to pass to the master process."""
        env = {
            "DB_HOST": self.db_host,
            "DB_PORT": self.db_port,
            "DB_USER": self.db_user,
            "DB_PASSWORD": self.db_password,
            "DB_NAME": self.db_name,
            "DB_SSLMODE": self.db_sslmode,
            "MASTER_HOST": self.master_host,
            "MASTER_PORT": str(self.master_port),
            "LOG_DIR": str(self.master_log_dir),
            "HEARTBEAT_SEC": str(self.heartbeat_sec),
            "OFFLINE_SEC": str(self.offline_sec),
        }
        return {k: v for k, v in env.items() if v is not None}

# ----------------------------------------------------------------------
# Database helper
# ----------------------------------------------------------------------
class DbHelper:
    def __init__(self, config: TestConfig):
        self.conn = psycopg2.connect(config.connection_string())
        self.conn.autocommit = False

    def clear_all(self):
        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE task_assignments, tasks, agents RESTART IDENTITY CASCADE")
        self.conn.commit()

    def has_agent(self, agent_id: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM agents WHERE agent_id = %s", (agent_id,))
            return cur.fetchone() is not None

    def get_task_state(self, task_id: str) -> str:
        with self.conn.cursor() as cur:
            cur.execute("SELECT state::text FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            if row:
                return row[0].lower()
            return ""

    def get_task_assigned_agent(self, task_id: str) -> Optional[str]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT assigned_agent FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            if row and row[0] is not None:
                return row[0]
            return None

# ----------------------------------------------------------------------
# Process management
# ----------------------------------------------------------------------
class MasterProcess:
    def __init__(self, config: TestConfig):
        self.config = config
        self.process = None

    def start(self):
        """Start master process."""
        env = os.environ.copy()
        env.update(self.config.master_environment())
        self.process = subprocess.Popen(
            [self.config.master_bin],
            cwd=str(self.config.work_dir),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid  # to kill process group
        )

    def stop(self):
        """Terminate master process."""
        if self.process and self.process.poll() is None:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait()
        self.process = None

def wait_for_ready(config: TestConfig, timeout: float = 10) -> bool:
    """Wait for master HTTP API to be ready."""
    url = f"http://{config.master_host}:{config.master_port}/api/v1/agents?limit=1"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=1)
            if r.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.1)
    return False

def wait_for_condition(predicate, timeout: float = 10, interval: float = 0.2) -> bool:
    """Wait for predicate to become True."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()

# ----------------------------------------------------------------------
# Worker runner
# ----------------------------------------------------------------------
def run_worker_once(config: TestConfig, env_vars: Dict[str, str]) -> int:
    """Run worker with --once, return exit code."""
    env = os.environ.copy()
    env.update(env_vars)
    try:
        result = subprocess.run(
            [config.worker_bin, "--once"],
            cwd=str(config.work_dir),
            env=env,
            capture_output=True,
            text=True
        )
        return result.returncode
    except Exception as e:
        raise RuntimeError(f"Failed to run worker: {e}")

# ----------------------------------------------------------------------
# Test fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="session")
def config() -> TestConfig:
    """Session-scoped configuration."""
    return TestConfig()

@pytest.fixture(scope="session")
def db_helper(config: TestConfig) -> DbHelper:
    """Session-scoped database helper."""
    helper = DbHelper(config)
    yield helper
    helper.conn.close()

@pytest.fixture(scope="session")
def master_process(config: TestConfig) -> MasterProcess:
    """Session-scoped master process."""
    master = MasterProcess(config)
    master.start()

    if not wait_for_ready(config, timeout=10):
        master.stop()
        raise RuntimeError(f"Master did not become ready on port {config.master_port}")

    yield master
    master.stop()

@pytest.fixture(scope="function")
def worker_env_base() -> Dict[str, str]:
    """Load worker base environment from worker_env.json."""
    path = os.environ.get("DC_WORKER_ENV_FILE", "worker_env.json")
    data = load_json(path)
    # Convert all values to strings (environment variables are strings)
    return {k: str(v) for k, v in data.items()}

# ----------------------------------------------------------------------
# Test functions
# ----------------------------------------------------------------------
def test_worker_runs_task_and_reports_success(
    config: TestConfig,
    db_helper: DbHelper,
    master_process: MasterProcess,
    worker_env_base: Dict[str, str]
):
    # Clear database before test
    db_helper.clear_all()

    # Create a unique test subdirectory under config.log_dir for worker logs
    test_run_id = f"test_{os.getpid()}_{int(time.time()*1000)}"
    worker_log_dir = config.log_dir / "worker" / test_run_id
    worker_log_dir.mkdir(parents=True, exist_ok=True)

    # Create a task via HTTP API
    url = f"http://{config.master_host}:{config.master_port}/api/v1/tasks"
    task_payload = {
        "command": "/bin/sh",
        "args": ["-c", "echo worker ok"],
    }
    resp = requests.post(url, json=task_payload)
    assert resp.status_code == 201
    task_id = str(resp.json()["task_id"])

    # Generate a unique agent ID
    agent_id = f"agent_{test_run_id}"

    # Prepare worker environment: merge base with dynamic values
    worker_env = worker_env_base.copy()
    worker_env.update({
        "MASTER_URL": f"http://{config.master_host}:{config.master_port}",
        "AGENT_ID": agent_id,
        "WORKER_LOG_DIR": str(worker_log_dir),
        "WORKER_LOG_FILE": str(worker_log_dir / "worker.log"),
    })

    # Run worker once
    rc = run_worker_once(config, worker_env)
    assert rc == 0, f"Worker exited with code {rc}"

    # Wait for task to become succeeded
    def task_succeeded():
        return db_helper.get_task_state(task_id) == "succeeded"

    assert wait_for_condition(task_succeeded, timeout=10), "Task did not succeed"

    # Verify agent is registered and task assigned to it
    assert db_helper.has_agent(agent_id), "Agent not found in database"
    assigned_agent = db_helper.get_task_assigned_agent(task_id)
    assert assigned_agent == agent_id, f"Task assigned to wrong agent: {assigned_agent}"

# ----------------------------------------------------------------------
# Cleanup after all tests
# ----------------------------------------------------------------------
def pytest_sessionfinish(session, exitstatus):
    """Remove temporary log directory after tests."""
    # Get the config object from the session's fixture cache if possible
    # We'll use a simpler approach: store the log_dir in a module variable
    # But here we just rely on the config fixture being session-scoped.
    # Actually, we can access the config via the session's fixture cache.
    # For simplicity, we can register a cleanup in the config fixture.
    pass  # The config's log_dir is already removed by the fixture's teardown? Not yet.
    # We'll add an explicit cleanup in the config fixture's finalizer.

# Modify config fixture to remove log_dir after session
@pytest.fixture(scope="session")
def config(request):
    cfg = TestConfig()
    def cleanup():
        shutil.rmtree(cfg.log_dir, ignore_errors=True)
    request.addfinalizer(cleanup)
    return cfg
