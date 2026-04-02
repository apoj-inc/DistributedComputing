from __future__ import annotations

import os
import pathlib
import socket

import pytest

from tests.utils.process import ManagedProcess, combined_output, start_process, stop_process, wait_for_http_ready


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
def master_api_base_url(dc_master_bin: pathlib.Path, tmp_path: pathlib.Path) -> str:
    port = _free_port()
    log_dir = tmp_path / "master-logs"
    env = os.environ.copy()
    env.update(
        {
            "DB_BACKEND": "postgres",
            "DB_HOST": "127.0.0.1",
            "DB_PORT": "1",
            "DB_USER": "postgres",
            "DB_PASSWORD": "secret",
            "DB_NAME": "dc_test",
            "MASTER_HOST": "127.0.0.1",
            "MASTER_PORT": str(port),
            "LOG_DIR": str(log_dir),
            "MASTER_LOG_FILE": str(log_dir / "master.log"),
            "MASTER_SKIP_DB_MIGRATION": "1",
        }
    )
    process: ManagedProcess = start_process([str(dc_master_bin)], env=env)
    base_url = f"http://127.0.0.1:{port}"
    status = wait_for_http_ready(
        f"{base_url}/api/v1/tasks",
        process,
        timeout_sec=10,
        acceptable_statuses={200, 400, 404, 500},
    )
    if status == -1:
        returncode, stdout, stderr = stop_process(process)
        pytest.fail(
            "dc_master did not become ready.\n"
            f"returncode={returncode}\n"
            f"output:\n{combined_output(stdout, stderr)}"
        )
    try:
        yield base_url
    finally:
        stop_process(process)
