from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.utils.process import combined_output


@pytest.mark.integration
def test_master_rejects_unknown_db_backend(dc_master_bin, run_binary) -> None:
    env = os.environ.copy()
    env['DB_BACKEND'] = 'invalid-backend'
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode != 0
    assert 'Invalid DB_BACKEND' in output


@pytest.mark.integration
def test_master_mongo_backend_requires_connection_env(dc_master_bin, run_binary) -> None:
    env = os.environ.copy()
    env['DB_BACKEND'] = 'mongo'
    env.pop('DB_HOST', None)
    env.pop('DB_PORT', None)
    env.pop('DB_USER', None)
    env.pop('DB_PASSWORD', None)
    env.pop('DB_NAME', None)
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode != 0
    assert 'Missing one of  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME environment variables.' in output


@pytest.mark.integration
def test_master_runs_mongo_migration_script_on_mongo_backend(
    dc_master_bin, run_binary, tmp_path: Path
) -> None:
    mongo_script = tmp_path / 'mongo_init_fail.py'
    mongo_script.write_text(
        'import sys\n'
        'print(\'mongo migration script invoked\')\n'
        'sys.exit(73)\n',
        encoding='utf-8',
    )

    env = os.environ.copy()
    env['DB_BACKEND'] = 'mongo'
    env['DB_HOST'] = '127.0.0.1'
    env['DB_PORT'] = '27017'
    env['DB_USER'] = 'local'
    env['DB_PASSWORD'] = 'local'
    env['DB_NAME'] = 'dc_test'
    env['INIT_DB_SCRIPT'] = str(mongo_script)
    
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode == 73
    assert 'mongo migration script invoked' in output, output
    assert 'Migrations failed with code 73' in output, output


@pytest.mark.integration
def test_master_postgres_backend_requires_credentials(dc_master_bin, run_binary) -> None:
    env = os.environ.copy()
    env['DB_BACKEND'] = 'postgres'
    env.pop('DB_USER', None)
    env.pop('DB_NAME', None)
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode != 0
    assert 'Missing DB_USER or DB_NAME' in output


@pytest.mark.integration
def test_master_runs_postgres_migration_script_on_postgres_backend(
    dc_master_bin, run_binary, tmp_path: Path
) -> None:
    pg_script = tmp_path / 'pg_migrate_fail.py'
    pg_script.write_text(
        'import sys\n'
        'print(\'postgres migration script invoked\')\n'
        'sys.exit(74)\n',
        encoding='utf-8',
    )

    env = os.environ.copy()
    env['DB_BACKEND'] = 'postgres'
    env['DB_HOST'] = '127.0.0.1'
    env['DB_PORT'] = '5432'
    env['DB_USER'] = 'postgres'
    env['DB_PASSWORD'] = 'secret'
    env['DB_NAME'] = 'dc_test'
    env['INIT_DB_SCRIPT'] = str(pg_script)
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode == 74
    assert 'postgres migration script invoked' in output, output
    assert 'Migrations failed with code 74' in output, output


@pytest.mark.integration
def test_master_skips_migrations_when_flag_enabled(
    dc_master_bin, run_binary, tmp_path: Path
) -> None:
    pg_script = tmp_path / 'pg_should_not_run.py'
    pg_script.write_text(
        'import sys\n'
        'print(\'postgres migration script invoked\')\n'
        'sys.exit(74)\n',
        encoding='utf-8',
    )

    env = os.environ.copy()
    env['DB_BACKEND'] = 'postgres'
    env['DB_HOST'] = '127.0.0.1'
    env['DB_PORT'] = '5432'
    env['DB_USER'] = 'postgres'
    env['DB_PASSWORD'] = 'secret'
    env['DB_NAME'] = 'dc_test'
    env['INIT_DB_SCRIPT'] = str(pg_script)
    env['MASTER_SKIP_DB_MIGRATION'] = '1'
    env['MASTER_HOST'] = '256.256.256.256'
    result = run_binary(dc_master_bin, env=env)
    output = combined_output(result.stdout, result.stderr)

    assert result.returncode != 74
    assert 'postgres migration script invoked' not in output
    assert 'DB migration step skipped' in output
