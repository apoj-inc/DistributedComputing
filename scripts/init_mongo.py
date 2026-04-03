#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
import sys


def parse_env_file(path):
    data = {}
    if not path:
        return data
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :]
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            data[key.strip()] = val.strip().strip("'").strip('"')
    return data


def pick_value(cli_value, env, config, key, default=None):
    if cli_value is not None:
        return cli_value
    if key in env:
        return env[key]
    if key in config:
        return config[key]
    return default


def build_command(binary, url, database, migrations, metastore):
    return [
        binary,
        "--url",
        url,
        "--database",
        database,
        "--migrations",
        migrations,
        "--metastore",
        metastore,
    ]


def run_command(command):
    print(f"Running Mongo migrations: {' '.join(command)}")
    result = subprocess.run(command, check=False)
    return int(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Run MongoDB migrations.")
    parser.add_argument("--config", help="Path to config file with MONGO_* variables")
    parser.add_argument("--url")
    parser.add_argument("--database")
    parser.add_argument("--migrations")
    parser.add_argument("--metastore")
    parser.add_argument("--binary")
    args = parser.parse_args()

    env = os.environ
    config_path = args.config or env.get("DB_CONFIG")
    config = parse_env_file(config_path) if config_path else {}

    mongo_url = pick_value(args.url, env, config, "MONGO_URI")
    mongo_db = pick_value(args.database, env, config, "MONGO_DB")
    migrations = pick_value(
        args.migrations,
        env,
        config,
        "MONGO_MIGRATIONS_DIR",
        "migrations_broker_mongo",
    )
    metastore = pick_value(
        args.metastore,
        env,
        config,
        "MONGO_MIGRATIONS_METASTORE",
        "database_migrations",
    )
    explicit_binary = pick_value(args.binary, env, config, "MONGO_MIGRATIONS_BIN")

    if not mongo_url or not mongo_db:
        print(
            "ERROR: missing required params: MONGO_URI and/or MONGO_DB",
            file=sys.stderr,
        )
        return 2

    if explicit_binary:
        binaries = [explicit_binary]
    else:
        binaries = ["mongodb-migrate", "mongodb-migrations"]

    for binary in binaries:
        resolved = shutil.which(binary)
        if not resolved:
            continue
        return run_command(build_command(resolved, mongo_url, mongo_db, migrations, metastore))

    print(
        "ERROR: mongodb-migrations executable not found. "
        "Install dependencies and ensure 'mongodb-migrate' is in PATH.",
        file=sys.stderr,
    )
    return 127


if __name__ == "__main__":
    sys.exit(main())
