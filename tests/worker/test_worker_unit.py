import os
import tempfile
import time
import pytest
from unittest.mock import Mock, patch, call
from pathlib import Path

from worker.worker_config import (
    WorkerConfig,
    load_worker_config_from_env,
    validate_worker_config,
)
from worker.agent_client import AgentClient
from worker.task_executor import TaskExecutor, TaskResult
from worker.worker_app import WorkerApp, create_worker_app_from_env
from worker.task_dispatch import TaskDispatch

# ----------------------------------------------------------------------
# WorkerConfig tests
# ----------------------------------------------------------------------
def test_worker_config_defaults():
    with patch.dict(os.environ, {}, clear=True):
        cfg = load_worker_config_from_env()
        assert cfg.master_url == "http://localhost:8080"
        assert cfg.agent_id == ""
        assert cfg.version == "dev"
        assert cfg.slots == 1
        assert cfg.http_timeout_ms == 5000
        assert cfg.log_dir == "logs/worker"
        assert cfg.cancel_check_interval_sec == 1
        assert cfg.cpu_cores > 0
        assert cfg.os != ""

def test_worker_config_from_env():
    env = {
        "MASTER_URL": "http://master:9000",
        "AGENT_ID": "agent-42",
        "AGENT_OS": "darwin",
        "AGENT_VERSION": "1.2.3",
        "CPU_CORES": "8",
        "RAM_MB": "4096",
        "SLOTS": "3",
        "WORKER_HTTP_TIMEOUT_MS": "2500",
        "WORKER_LOG_DIR": "/tmp/worker_logs",
        "CANCEL_CHECK_SEC": "5",
    }
    with patch.dict(os.environ, env):
        cfg = load_worker_config_from_env()
        assert cfg.master_url == "http://master:9000"
        assert cfg.agent_id == "agent-42"
        assert cfg.os == "darwin"
        assert cfg.version == "1.2.3"
        assert cfg.cpu_cores == 8
        assert cfg.ram_mb == 4096
        assert cfg.slots == 3
        assert cfg.http_timeout_ms == 2500
        assert cfg.log_dir == "/tmp/worker_logs"
        assert cfg.cancel_check_interval_sec == 5

def test_validate_worker_config():
    cfg = WorkerConfig()
    ok, error = validate_worker_config(cfg)
    assert not ok
    assert "MASTER_URL is required" in error

    cfg.master_url = "http://localhost"
    ok, error = validate_worker_config(cfg)
    assert not ok
    assert "AGENT_ID is required" in error

    cfg.agent_id = "agent"
    cfg.cpu_cores = 0
    ok, error = validate_worker_config(cfg)
    assert not ok
    assert "CPU_CORES must be positive" in error

    cfg.cpu_cores = 2
    cfg.slots = 0
    ok, error = validate_worker_config(cfg)
    assert not ok
    assert "SLOTS must be positive" in error

    cfg.slots = 1
    cfg.cancel_check_interval_sec = 0
    ok, error = validate_worker_config(cfg)
    assert not ok
    assert "CANCEL_CHECK_SEC must be positive" in error

    cfg.cancel_check_interval_sec = 1
    ok, error = validate_worker_config(cfg)
    assert ok
    assert error == ""

def test_create_worker_app_from_env_fails():
    with patch.dict(os.environ, {"MASTER_URL": "http://localhost"}):
        app, error = create_worker_app_from_env()
        assert app is None
        assert "AGENT_ID is required" in error

# ----------------------------------------------------------------------
# TaskExecutor tests (using real subprocess)
# ----------------------------------------------------------------------
def test_task_executor_runs_successfully(tmp_path):
    executor = TaskExecutor(log_root=str(tmp_path))
    task = TaskDispatch(
        task_id="task-ok",
        command="/bin/sh",
        args=["-c", "echo hello"],
        env={},
        constraints={}
    )
    result = executor.run(task, cancel_check=lambda: False)
    assert result.exit_code == 0
    assert not result.failed_to_start
    assert not result.timed_out
    assert not result.canceled
    stdout_path = tmp_path / "task-ok" / "stdout.log"
    assert stdout_path.exists()
    assert stdout_path.read_text().strip() == "hello"

def test_task_executor_times_out(tmp_path):
    executor = TaskExecutor(log_root=str(tmp_path))
    task = TaskDispatch(
        task_id="task-timeout",
        command="/bin/sh",
        args=["-c", "sleep 2"],
        env={},
        constraints={},
        timeout_sec=1
    )
    result = executor.run(task, cancel_check=lambda: False)
    assert result.timed_out
    assert result.exit_code != 0

def test_task_executor_canceled(tmp_path):
    executor = TaskExecutor(log_root=str(tmp_path))
    task = TaskDispatch(
        task_id="task-cancel",
        command="/bin/sh",
        args=["-c", "sleep 5"],
        env={},
        constraints={}
    )
    # Cancel after 0.2 seconds
    import threading
    cancel_event = threading.Event()
    def cancel_check():
        return cancel_event.is_set()
    def set_cancel():
        time.sleep(0.2)
        cancel_event.set()
    t = threading.Thread(target=set_cancel)
    t.start()
    result = executor.run(task, cancel_check)
    t.join()
    assert result.canceled
    assert result.exit_code != 0

# ----------------------------------------------------------------------
# WorkerApp tests with mock AgentClient
# ----------------------------------------------------------------------
def test_worker_app_run_once_no_tasks(tmp_path):
    cfg = WorkerConfig()
    cfg.master_url = "http://localhost"
    cfg.agent_id = "agent-1"
    cfg.os = "linux"
    cfg.version = "1.0"
    cfg.cpu_cores = 2
    cfg.ram_mb = 256
    cfg.slots = 1
    cfg.log_dir = str(tmp_path)
    cfg.cancel_check_interval_sec = 1
    cfg.upload_logs = False

    mock_client = Mock(spec=AgentClient)
    mock_client.register_agent.return_value = (True, None, 2)
    mock_client.send_heartbeat.return_value = (True, None)
    mock_client.poll_tasks.return_value = (True, [], None)

    app = WorkerApp(cfg, mock_client)
    rc = app.run(once=True)
    assert rc == 0

    mock_client.register_agent.assert_called_once()
    mock_client.send_heartbeat.assert_called_once_with("agent-1", "idle")
    mock_client.poll_tasks.assert_called_once_with("agent-1", 1)

def test_worker_app_run_once_with_task_success(tmp_path):
    cfg = WorkerConfig()
    cfg.master_url = "http://localhost"
    cfg.agent_id = "agent-1"
    cfg.os = "linux"
    cfg.version = "1.0"
    cfg.cpu_cores = 2
    cfg.ram_mb = 256
    cfg.slots = 1
    cfg.log_dir = str(tmp_path)
    cfg.cancel_check_interval_sec = 1
    cfg.upload_logs = True
    cfg.max_upload_bytes = 1024 * 1024

    mock_client = Mock(spec=AgentClient)
    mock_client.register_agent.return_value = (True, None, 2)
    mock_client.send_heartbeat.return_value = (True, None)
    # Poll returns one task
    task = TaskDispatch(
        task_id="task-1",
        command="/bin/sh",
        args=["-c", "echo out; echo err >&2"],
        env={},
        constraints={}
    )
    mock_client.poll_tasks.return_value = (True, [task], None)
    mock_client.get_task_state.return_value = (True, "running", None)
    mock_client.upload_task_log.side_effect = lambda tid, stream, data, _: (True, None)
    mock_client.update_task_status.side_effect = lambda tid, state, exit_code, started_at, finished_at, error_msg, _: (True, None)

    app = WorkerApp(cfg, mock_client)
    rc = app.run(once=True)
    assert rc == 0

    # Verify task executed
    task_dir = tmp_path / "task-1"
    assert task_dir.exists()
    stdout_log = task_dir / "stdout.log"
    stderr_log = task_dir / "stderr.log"
    assert stdout_log.exists()
    assert "out" in stdout_log.read_text()
    assert stderr_log.exists()
    assert "err" in stderr_log.read_text()

    # Verify status updates
    expected_calls = [
        call.update_task_status("task-1", "running", None, call.ANY, None, None),
        call.update_task_status("task-1", "succeeded", 0, call.ANY, call.ANY, None)
    ]
    mock_client.update_task_status.assert_has_calls(expected_calls, any_order=False)
    # Verify logs uploaded
    mock_client.upload_task_log.assert_any_call("task-1", "stdout", call.ANY)
    mock_client.upload_task_log.assert_any_call("task-1", "stderr", call.ANY)

def test_worker_app_handles_os_constraint_mismatch(tmp_path):
    cfg = WorkerConfig()
    cfg.master_url = "http://localhost"
    cfg.agent_id = "agent-1"
    cfg.os = "linux"
    cfg.version = "1.0"
    cfg.cpu_cores = 2
    cfg.ram_mb = 256
    cfg.slots = 1
    cfg.log_dir = str(tmp_path)
    cfg.cancel_check_interval_sec = 1
    cfg.upload_logs = False

    mock_client = Mock(spec=AgentClient)
    mock_client.register_agent.return_value = (True, None, 2)
    mock_client.send_heartbeat.return_value = (True, None)
    # Task with os constraint "windows"
    task = TaskDispatch(
        task_id="task-1",
        command="/bin/echo",
        args=["hello"],
        env={},
        constraints={"os": "windows"}
    )
    mock_client.poll_tasks.return_value = (True, [task], None)

    app = WorkerApp(cfg, mock_client)
    rc = app.run(once=True)
    assert rc == 0

    # Task should be marked as failed with appropriate error
    mock_client.update_task_status.assert_called_once()
    args, _ = mock_client.update_task_status.call_args
    assert args[0] == "task-1"
    assert args[1] == "failed"
    assert args[2] == 1
    assert args[4] is not None  # finished_at
    assert "OS constraint mismatch" in args[5]
