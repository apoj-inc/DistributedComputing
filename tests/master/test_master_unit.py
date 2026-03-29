import json
import tempfile
from pathlib import Path
import pytest

from master.api_mappers import (
    parse_agent_input,
    parse_agent_heartbeat,
    parse_task_create,
    parse_task_status_update,
    parse_task_id,
    is_valid_task_state_transition,
    task_record_to_json,
    task_dispatch_to_json
)
from master.log_store import LogStore
from master.status import (
    agent_status_from_api,
    agent_status_to_db,
    agent_status_to_api,
    task_state_from_api,
    task_state_to_db,
    task_state_to_api,
    AgentStatus,
    TaskState
)

# ----------------------------------------------------------------------
# Status tests
# ----------------------------------------------------------------------
def test_agent_status_from_api():
    assert agent_status_from_api("idle") == AgentStatus.IDLE
    assert agent_status_from_api("BUSY") == AgentStatus.BUSY
    assert agent_status_from_api("Offline") == AgentStatus.OFFLINE
    assert agent_status_from_api("unknown") is None

def test_agent_status_to_db():
    assert agent_status_to_db(AgentStatus.IDLE) == "Idle"
    assert agent_status_to_db(AgentStatus.BUSY) == "Busy"
    assert agent_status_to_db(AgentStatus.OFFLINE) == "Offline"

def test_agent_status_to_api():
    assert agent_status_to_api(AgentStatus.IDLE) == "idle"
    assert agent_status_to_api(AgentStatus.BUSY) == "busy"
    assert agent_status_to_api(AgentStatus.OFFLINE) == "offline"

def test_task_state_from_api():
    assert task_state_from_api("queued") == TaskState.QUEUED
    assert task_state_from_api("Running") == TaskState.RUNNING
    assert task_state_from_api("SUCCEEDED") == TaskState.SUCCEEDED
    assert task_state_from_api("failed") == TaskState.FAILED
    assert task_state_from_api("Canceled") == TaskState.CANCELED
    assert task_state_from_api("") is None

def test_task_state_to_db():
    assert task_state_to_db(TaskState.QUEUED) == "Queued"
    assert task_state_to_db(TaskState.RUNNING) == "Running"
    assert task_state_to_db(TaskState.SUCCEEDED) == "Succeeded"
    assert task_state_to_db(TaskState.FAILED) == "Failed"
    assert task_state_to_db(TaskState.CANCELED) == "Canceled"

def test_task_state_to_api():
    assert task_state_to_api(TaskState.QUEUED) == "queued"
    assert task_state_to_api(TaskState.RUNNING) == "running"
    assert task_state_to_api(TaskState.SUCCEEDED) == "succeeded"
    assert task_state_to_api(TaskState.FAILED) == "failed"
    assert task_state_to_api(TaskState.CANCELED) == "canceled"

# ----------------------------------------------------------------------
# API mappers tests
# ----------------------------------------------------------------------
def test_parse_agent_input():
    payload = {
        "os": "linux",
        "version": "1.0",
        "resources": {"cpu_cores": 4, "ram_mb": 2048, "slots": 2}
    }
    out, error = parse_agent_input("agent-1", payload)
    assert out is not None
    assert out.agent_id == "agent-1"
    assert out.os == "linux"
    assert out.version == "1.0"
    assert out.cpu_cores == 4
    assert out.ram_mb == 2048
    assert out.slots == 2

def test_parse_agent_input_missing_fields():
    payload = {}
    out, error = parse_agent_input("agent-1", payload)
    assert out is None
    assert "Missing required fields" in error

def test_parse_agent_heartbeat():
    payload = {"status": "busy"}
    out, error = parse_agent_heartbeat("agent-1", payload)
    assert out is not None
    assert out.agent_id == "agent-1"
    assert out.status == AgentStatus.BUSY

def test_parse_task_create():
    payload = {
        "command": "run",
        "args": ["one", "two"],
        "env": {"KEY": "VALUE"},
        "timeout_sec": 30,
        "constraints": {"os": "linux"}
    }
    out, error = parse_task_create(payload)
    assert out is not None
    assert out.command == "run"
    assert out.args == ["one", "two"]
    assert out.env == {"KEY": "VALUE"}
    assert out.timeout_sec == 30
    assert out.constraints == {"os": "linux"}

def test_parse_task_create_filters_non_string_args():
    payload = {
        "command": "run",
        "args": ["keep", 2, True, "also-keep"]
    }
    out, error = parse_task_create(payload)
    assert out.args == ["keep", "also-keep"]

def test_parse_task_status_update():
    payload = {
        "state": "failed",
        "exit_code": 127,
        "started_at": "2024-01-01T00:00:00Z",
        "finished_at": "2024-01-01T00:01:00Z",
        "error_message": "boom"
    }
    out, error = parse_task_status_update(payload)
    assert out is not None
    assert out.state == TaskState.FAILED
    assert out.exit_code == 127
    assert out.started_at == "2024-01-01T00:00:00Z"
    assert out.finished_at == "2024-01-01T00:01:00Z"
    assert out.error_message == "boom"

def test_parse_task_id():
    assert parse_task_id("1") == 1
    assert parse_task_id("12345") == 12345
    assert parse_task_id("") is None
    assert parse_task_id("0") is None
    assert parse_task_id("-1") is None
    assert parse_task_id("bad.id") is None

def test_is_valid_task_state_transition():
    assert is_valid_task_state_transition(TaskState.QUEUED, TaskState.RUNNING)
    assert is_valid_task_state_transition(TaskState.QUEUED, TaskState.CANCELED)
    assert not is_valid_task_state_transition(TaskState.QUEUED, TaskState.FAILED)
    assert is_valid_task_state_transition(TaskState.RUNNING, TaskState.SUCCEEDED)
    assert not is_valid_task_state_transition(TaskState.RUNNING, TaskState.QUEUED)

def test_task_record_to_json():
    record = type('', (), {
        "task_id": 1,
        "state": TaskState.RUNNING,
        "command": "run",
        "args": [1],
        "env": {},
        "timeout_sec": 10,
        "assigned_agent": "agent-1",
        "created_at": "2024-01-01T00:00:00Z",
        "started_at": "2024-01-01T00:00:01Z",
        "finished_at": None,
        "exit_code": None,
        "error_message": "err",
        "constraints": {}
    })()
    data = task_record_to_json(record)
    assert data["task_id"] == 1
    assert data["state"] == "running"
    assert data["timeout_sec"] == 10
    assert data["assigned_agent"] == "agent-1"
    assert data["started_at"] == "2024-01-01T00:00:01Z"
    assert data["error_message"] == "err"

def test_task_dispatch_to_json():
    dispatch = type('', (), {
        "task_id": 1,
        "command": "run",
        "args": [],
        "env": {},
        "constraints": None
    })()
    data = task_dispatch_to_json(dispatch)
    assert data["task_id"] == 1
    assert "constraints" not in data

# ----------------------------------------------------------------------
# LogStore tests
# ----------------------------------------------------------------------
def test_log_store_read_all(tmp_path):
    log_dir = tmp_path / "logs"
    store = LogStore(str(log_dir))
    # Write a file
    task_dir = log_dir / "task-1"
    task_dir.mkdir(parents=True)
    (task_dir / "stdout.log").write_text("hello")
    result = store.read_all("task-1", "stdout")
    assert result.exists
    assert result.size_bytes == 5
    assert result.data == "hello"

def test_log_store_read_all_missing():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = LogStore(tmpdir)
        result = store.read_all("missing", "stdout")
        assert not result.exists
        assert result.size_bytes == 0

def test_log_store_read_from_offset(tmp_path):
    log_dir = tmp_path / "logs"
    store = LogStore(str(log_dir))
    task_dir = log_dir / "task-1"
    task_dir.mkdir(parents=True)
    (task_dir / "stdout.log").write_text("abcdef")
    result = store.read_from_offset("task-1", "stdout", 2)
    assert result.data == "cdef"

def test_log_store_rejects_path_traversal(tmp_path):
    store = LogStore(str(tmp_path))
    result = store.read_all("..", "stdout")
    assert not result.exists

def test_log_store_write_all(tmp_path):
    store = LogStore(str(tmp_path))
    ok = store.write_all("task-1", "stdout", "hello")
    assert ok
    assert (tmp_path / "task-1" / "stdout.log").exists()
    # meta.json should exist
    assert (tmp_path / "task-1" / "meta.json").exists()
