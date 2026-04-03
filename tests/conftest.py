from __future__ import annotations

import os
import pathlib
import subprocess
from typing import Callable

import pytest


def _repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parent.parent


def _build_dir() -> pathlib.Path:
    override = os.getenv('DC_BUILD_DIR')
    if override:
        return pathlib.Path(override).expanduser().resolve()
    return _repo_root() / 'build' / 'x86_64-linux'


def _resolve_binary_path(env_var: str, relative_path: str) -> pathlib.Path:
    override = os.getenv(env_var)
    if override:
        return pathlib.Path(override).expanduser().resolve()
    return (_build_dir() / relative_path).resolve()


def _require_binary(path: pathlib.Path, env_var: str) -> pathlib.Path:
    if path.is_file():
        return path
    pytest.skip(
        f'Binary not found at {path}. Build binaries first or set {env_var} to an executable path.'
    )


@pytest.fixture(scope='session')
def dc_master_bin() -> pathlib.Path:
    path = _resolve_binary_path('DC_MASTER_BIN', 'src/master/dc_master')
    return _require_binary(path, 'DC_MASTER_BIN')


@pytest.fixture(scope='session')
def dc_worker_bin() -> pathlib.Path:
    path = _resolve_binary_path('DC_WORKER_BIN', 'src/worker/dc_worker')
    return _require_binary(path, 'DC_WORKER_BIN')


@pytest.fixture(scope='session')
def dc_cli_bin() -> pathlib.Path:
    path = _resolve_binary_path('DC_CLI_BIN', 'src/cli/dc_cli')
    return _require_binary(path, 'DC_CLI_BIN')


@pytest.fixture(scope='session')
def run_binary() -> Callable[..., subprocess.CompletedProcess[str]]:
    def _run_binary(
        binary: pathlib.Path,
        *args: str,
        timeout: int = 15,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        process_env = os.environ.copy()
        if env:
            process_env.update(env)
        return subprocess.run(
            [str(binary), *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
            env=process_env,
        )

    return _run_binary
