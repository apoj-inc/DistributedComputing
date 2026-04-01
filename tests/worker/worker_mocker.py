from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class WorkerMockState:
    agents: dict[str, dict[str, Any]] = field(default_factory=dict)
    task_states: dict[int, str] = field(default_factory=dict)
    queued_tasks_by_agent: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    logs: dict[int, dict[str, str]] = field(default_factory=dict)
    transactions: list[dict[str, Any]] = field(default_factory=list)


class WorkerMocker:
    """In-memory worker-facing mock that records API transaction calls."""

    def __init__(self) -> None:
        self._state = WorkerMockState()
        self._lock = Lock()

    def _record(self, tx: str, payload: dict[str, Any]) -> None:
        self._state.transactions.append(
            {
                "tx": tx,
                "payload": payload,
                "at": _utc_now(),
            }
        )

    def register_agent(self, agent_id: str, body: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._record("register_agent", {"agent_id": agent_id, "body": body})
            self._state.agents[agent_id] = {
                "agent_id": agent_id,
                "os": body.get("os", ""),
                "version": body.get("version", ""),
                "resources": body.get("resources", {}),
                "status": "idle",
                "last_heartbeat": _utc_now(),
            }
            self._state.queued_tasks_by_agent.setdefault(agent_id, [])
            return {"status": "ok", "heartbeat_interval_sec": 5}

    def send_heartbeat(self, agent_id: str, body: dict[str, Any]) -> bool:
        with self._lock:
            self._record("send_heartbeat", {"agent_id": agent_id, "body": body})
            if agent_id not in self._state.agents:
                return False
            self._state.agents[agent_id]["status"] = body.get("status", "idle")
            self._state.agents[agent_id]["last_heartbeat"] = _utc_now()
            return True

    def enqueue_task_for_agent(self, agent_id: str, task: dict[str, Any]) -> None:
        with self._lock:
            self._record("enqueue_task_for_agent", {"agent_id": agent_id, "task": task})
            self._state.queued_tasks_by_agent.setdefault(agent_id, []).append(task)
            if "task_id" in task:
                self._state.task_states[int(task["task_id"])] = "queued"

    def poll_tasks(self, agent_id: str, free_slots: int) -> tuple[bool, list[dict[str, Any]]]:
        with self._lock:
            self._record("poll_tasks", {"agent_id": agent_id, "free_slots": free_slots})
            if agent_id not in self._state.agents:
                return False, []
            queue = self._state.queued_tasks_by_agent.setdefault(agent_id, [])
            take = max(0, min(free_slots, len(queue)))
            tasks = queue[:take]
            del queue[:take]
            for task in tasks:
                if "task_id" in task:
                    self._state.task_states[int(task["task_id"])] = "running"
            return True, tasks

    def update_task_status(self, task_id: int, body: dict[str, Any]) -> bool:
        with self._lock:
            self._record("update_task_status", {"task_id": task_id, "body": body})
            if task_id not in self._state.task_states:
                return False
            self._state.task_states[task_id] = body.get("state", self._state.task_states[task_id])
            return True

    def upload_log(self, task_id: int, body: dict[str, Any]) -> bool:
        with self._lock:
            self._record("upload_log", {"task_id": task_id, "body": body})
            if task_id not in self._state.task_states:
                return False
            stream = body.get("stream", "stdout")
            data = body.get("data", "")
            self._state.logs.setdefault(task_id, {})[stream] = str(data)
            return True

    def get_task_state(self, task_id: int) -> str | None:
        with self._lock:
            self._record("get_task_state", {"task_id": task_id})
            return self._state.task_states.get(task_id)

    def transactions(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(tx) for tx in self._state.transactions]
