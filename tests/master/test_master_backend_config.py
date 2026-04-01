from __future__ import annotations

import os

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
def test_master_postgres_backend_requires_credentials(dc_master_bin, run_binary) -> None:
    env = os.environ.copy()
    env["DB_BACKEND"] = "postgres"
    env.pop("DB_USER", None)
    env.pop("DB_NAME", None)
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode != 0
    assert "Missing DB_USER or DB_NAME" in output
