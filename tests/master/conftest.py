from __future__ import annotations

import os
import pathlib
import socket
import sys
from datetime import datetime, timezone

import pytest
from testcontainers.postgres import PostgresContainer

from tests.utils.process import (
    ManagedProcess,
    combined_output,
    start_process,
    stop_process,
    wait_for_http_ready,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return int(sock.getsockname()[1])


def _venv_python(repo_root: pathlib.Path) -> str:
    candidate = repo_root / ".venv" / "bin" / "python3"
    if not candidate.is_file():
        pytest.fail(
            f"Expected tests to use .venv Python, but it was not found: {candidate}",
            pytrace=False,
        )
    return str(candidate)


def _slug(text: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in text)
    return cleaned or "test"


def _read_postgres_logs(container: PostgresContainer) -> str:
    # testcontainers API may return bytes or (stdout, stderr)
    try:
        raw = container.get_logs()
        if isinstance(raw, tuple):
            chunks: list[str] = []
            for part in raw:
                if isinstance(part, bytes):
                    chunks.append(part.decode("utf-8", errors="replace"))
                else:
                    chunks.append(str(part))
            return "\n".join(chunks).strip()
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="replace")
        return str(raw)
    except Exception:
        pass

    try:
        docker_obj = getattr(container, "_container", None)
        if docker_obj is not None:
            raw2 = docker_obj.logs(stdout=True, stderr=True, timestamps=True)
            if isinstance(raw2, bytes):
                return raw2.decode("utf-8", errors="replace")
            return str(raw2)
    except Exception as exc:  # pragma: no cover
        return f"<failed to fetch postgres logs: {exc}>"

    return "<postgres logs unavailable>"


def _dump_postgres_logs(
    container: PostgresContainer, nodeid: str, repo_root: pathlib.Path, phase: str
) -> None:
    logs_dir = repo_root / "logs" / "postgres"
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_file = logs_dir / f"{_slug(nodeid)}-{_slug(phase)}.log"
    out_file.write_text(_read_postgres_logs(container), encoding="utf-8")


@pytest.fixture
def postgres_container(request: pytest.FixtureRequest) -> PostgresContainer:
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    container = PostgresContainer(
        'postgres:16-alpine',
        username='postgres',
        password='secret',
        dbname='dc_test',
    )
    try:
        container.start()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f'Postgres container is unavailable: {exc}')
    _dump_postgres_logs(container, request.node.nodeid, repo_root, "after-init")
    try:
        yield container
    finally:
        _dump_postgres_logs(container, request.node.nodeid, repo_root, "teardown")
        container.stop()


@pytest.fixture
def master_api_base_url( 
    dc_master_bin: pathlib.Path,
    postgres_container: PostgresContainer
) -> str:
    port = _free_port()
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    migration_python = _venv_python(repo_root)
    log_dir = repo_root / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    db_host = postgres_container.get_container_host_ip()
    db_port = postgres_container.get_exposed_port(5432)
    env = os.environ.copy()
    env.update(
        {
            'DB_BACKEND': 'postgres',
            'DB_HOST': db_host,
            'DB_PORT': str(db_port),
            'DB_USER': 'postgres',
            'DB_PASSWORD': 'secret',
            'DB_NAME': 'dc_test',
            'MASTER_HOST': '127.0.0.1',
            'MASTER_PORT': str(port),
            'LOG_DIR': str(log_dir),
            'MASTER_LOG_FILE': str(log_dir / 'master.log'),
            'INIT_DB_PYTHON': migration_python,
            'INIT_DB_SCRIPT': str(repo_root / 'scripts' / 'init_pg.py'),
            'PG_MIGRATIONS_DIR': str(repo_root / 'migrations_broker_pg'),
            'MASTER_SKIP_DB_MIGRATION': 'false',
        }
    )
    process: ManagedProcess = start_process([str(dc_master_bin)], env=env, cwd=repo_root)
    base_url = f'http://127.0.0.1:{port}'
    status = wait_for_http_ready(
        f'{base_url}/api/v1/tasks',
        process,
        timeout_sec=30,
        acceptable_statuses={200},
    )
    if status == -1:
        returncode, stdout, stderr = stop_process(process)
        pytest.fail(
            'dc_master did not become ready.\n'
            f'returncode={returncode}\n'
            f'output:\n{combined_output(stdout, stderr)}'
        )
    try:
        yield base_url
    finally:
        stop_process(process)
