#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from urllib.parse import quote, urlencode


def parse_env_file(path):
    data = {}
    if not path:
        return data
    if not os.path.exists(path):
        raise FileNotFoundError(f'Config file not found: {path}')
    with open(path, 'r', encoding='utf-8') as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            if line.startswith('export '):
                line = line[len('export ') :]
            if '=' not in line:
                continue
            key, val = line.split('=', 1)
            data[key.strip()] = val.strip().strip(').strip(')
    return data


def pick_value(cli_value, env, config, key, default=None):
    if cli_value is not None:
        return cli_value
    if key in env:
        return env[key]
    if key in config:
        return config[key]
    return default


def build_database_url(host, port, user, password, dbname, sslmode):
    user_enc = quote(user, safe='')
    password_enc = quote(password or '', safe='')
    auth = user_enc if not password else f'{user_enc}:{password_enc}'
    query = {}
    if sslmode:
        query['sslmode'] = sslmode
    query_part = f'?{urlencode(query)}' if query else ''
    return f'postgresql://{auth}@{host}:{port}/{quote(dbname, safe='')}{query_part}'


def main():
    parser = argparse.ArgumentParser(description='Run PostgreSQL migrations (yoyo).')
    parser.add_argument('--config', help='Path to config file with DB_* variables')
    parser.add_argument('--host')
    parser.add_argument('--port', type=int)
    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--dbname')
    parser.add_argument('--sslmode')
    parser.add_argument('--migrations')
    args = parser.parse_args()

    env = os.environ
    config_path = args.config or env.get('DB_CONFIG')
    config = parse_env_file(config_path) if config_path else {}

    host = pick_value(args.host, env, config, 'DB_HOST', 'localhost')
    port = int(pick_value(args.port, env, config, 'DB_PORT', 5432))
    user = pick_value(args.user, env, config, 'DB_USER')
    password = pick_value(args.password, env, config, 'DB_PASSWORD', '')
    dbname = pick_value(args.dbname, env, config, 'DB_NAME')
    sslmode = pick_value(args.sslmode, env, config, 'DB_SSLMODE')
    migrations = pick_value(
        args.migrations,
        env,
        config,
        'MIGRATIONS_DIR',
        'migrations_broker_pg',
    )

    if not user or not dbname:
        print('ERROR: missing required params: DB_USER and/or DB_NAME', file=sys.stderr)
        return 2

    database_url = build_database_url(host, port, user, password, dbname, sslmode)
    command = [
        sys.executable,
        '-m',
        'yoyo',
        'apply',
        '--batch',
        '--database',
        database_url,
        migrations,
    ]
    print(f'Running PostgreSQL migrations: {' '.join(command)}')
    result = subprocess.run(command, check=False)
    return int(result.returncode)


if __name__ == '__main__':
    sys.exit(main())
