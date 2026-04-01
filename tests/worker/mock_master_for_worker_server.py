from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from tests.worker.worker_mocker import WorkerMocker


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    raw = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


class _Handler(BaseHTTPRequestHandler):
    worker: WorkerMocker

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        body = self.rfile.read(length).decode("utf-8")
        if not body:
            return {}
        return json.loads(body)

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def do_PUT(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path.startswith("/api/v1/agents/"):
            agent_id = path.rsplit("/", 1)[-1]
            body = self._read_json()
            payload = self.worker.register_agent(agent_id, body)
            _json_response(self, 200, payload)
            return
        _json_response(self, 404, {"error": {"code": "NOT_FOUND"}})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        body = self._read_json()

        if path.startswith("/api/v1/agents/") and path.endswith("/heartbeat"):
            agent_id = path.split("/")[4]
            ok = self.worker.send_heartbeat(agent_id, body)
            if not ok:
                _json_response(self, 404, {"error": {"code": "AGENT_NOT_FOUND"}})
                return
            _json_response(self, 200, {"status": "ok"})
            return

        if path.startswith("/api/v1/agents/") and path.endswith("/tasks:poll"):
            agent_id = path.split("/")[4]
            free_slots = int(body.get("free_slots", 0))
            ok, tasks = self.worker.poll_tasks(agent_id, free_slots)
            if not ok:
                _json_response(self, 404, {"error": {"code": "AGENT_NOT_FOUND"}})
                return
            _json_response(self, 200, {"tasks": tasks})
            return

        if path.startswith("/api/v1/tasks/") and path.endswith("/status"):
            try:
                task_id = int(path.split("/")[4])
            except (IndexError, ValueError):
                _json_response(self, 400, {"error": {"code": "BAD_REQUEST"}})
                return
            ok = self.worker.update_task_status(task_id, body)
            if not ok:
                _json_response(self, 404, {"error": {"code": "TASK_NOT_FOUND"}})
                return
            _json_response(self, 200, {"status": "ok"})
            return

        if path.startswith("/api/v1/tasks/") and path.endswith("/logs:upload"):
            try:
                task_id = int(path.split("/")[4])
            except (IndexError, ValueError):
                _json_response(self, 400, {"error": {"code": "BAD_REQUEST"}})
                return
            ok = self.worker.upload_log(task_id, body)
            if not ok:
                _json_response(self, 404, {"error": {"code": "TASK_NOT_FOUND"}})
                return
            _json_response(self, 200, {"status": "ok", "size_bytes": len(body.get("data", ""))})
            return

        _json_response(self, 404, {"error": {"code": "NOT_FOUND"}})

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/health":
            _json_response(self, 200, {"status": "ok"})
            return
        if path.startswith("/api/v1/tasks/"):
            try:
                task_id = int(path.rsplit("/", 1)[-1])
            except ValueError:
                _json_response(self, 400, {"error": {"code": "BAD_REQUEST"}})
                return
            state = self.worker.get_task_state(task_id)
            if state is None:
                _json_response(self, 404, {"error": {"code": "TASK_NOT_FOUND"}})
                return
            _json_response(self, 200, {"task": {"task_id": task_id, "state": state}})
            return
        _json_response(self, 404, {"error": {"code": "NOT_FOUND"}})


class MockMasterForWorkerServer:
    def __init__(self, host: str, port: int, worker_mocker: WorkerMocker) -> None:
        handler_cls = type("WorkerHandler", (_Handler,), {})
        handler_cls.worker = worker_mocker
        self._httpd = ThreadingHTTPServer((host, port), handler_cls)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def host(self) -> str:
        return self._httpd.server_address[0]

    @property
    def port(self) -> int:
        return int(self._httpd.server_address[1])

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)
