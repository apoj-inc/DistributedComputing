from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.utils.process import combined_output


@pytest.mark.integration
def test_master_rejects_unknown_db_backend(dc_master_bin, run_binary) -> None:
    env = os.environ.copy()
    env["DB_BACKEND"] = "invalid-backend"
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode != 0
    assert "Invalid DB_BACKEND" in output


@pytest.mark.integration
def test_master_mongo_backend_requires_connection_env(dc_master_bin, run_binary) -> None:
    env = os.environ.copy()
    env["DB_BACKEND"] = "mongo"
    env.pop("MONGO_URI", None)
    env.pop("MONGO_DB", None)
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode != 0
    assert "Missing MONGO_URI or MONGO_DB" in output


@pytest.mark.integration
def test_master_runs_mongo_migration_script_on_mongo_backend(
    dc_master_bin, run_binary, tmp_path: Path
) -> None:
    mongo_script = tmp_path / "mongo_init_fail.py"
    mongo_script.write_text(
        "import sys\n"
        "print('mongo migration script invoked')\n"
        "sys.exit(73)\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["DB_BACKEND"] = "mongo"
    env["MONGO_URI"] = "mongodb://127.0.0.1:27017"
    env["MONGO_DB"] = "dc_test"
    env["INIT_MONGO_SCRIPT"] = str(mongo_script)
    env["INIT_DB_SCRIPT"] = str(tmp_path / "postgres_should_not_run.py")
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode == 73
    assert "mongo migration script invoked" in output
    assert "Mongo migrations failed with code 73" in output


@pytest.mark.integration
def test_master_postgres_backend_requires_credentials(dc_master_bin, run_binary) -> None:
    env = os.environ.copy()
    env["DB_BACKEND"] = "postgres"
    env.pop("DB_USER", None)
    env.pop("DB_NAME", None)
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode != 0
    assert "Missing DB_USER or DB_NAME" in output


@pytest.mark.integration
def test_master_runs_postgres_migration_script_on_postgres_backend(
    dc_master_bin, run_binary, tmp_path: Path
) -> None:
    pg_script = tmp_path / "pg_migrate_fail.py"
    pg_script.write_text(
        "import sys\n"
        "print('postgres migration script invoked')\n"
        "sys.exit(74)\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["DB_BACKEND"] = "postgres"
    env["DB_HOST"] = "127.0.0.1"
    env["DB_PORT"] = "5432"
    env["DB_USER"] = "postgres"
    env["DB_PASSWORD"] = "secret"
    env["DB_NAME"] = "dc_test"
    env["INIT_DB_SCRIPT"] = str(pg_script)
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode == 74
    assert "postgres migration script invoked" in output
    assert "PostgreSQL migrations failed with code 74" in output
