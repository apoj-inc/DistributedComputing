# Distributed Computing System

C++ distributed computing system with Master, Worker, and CLI components.

## Structure
- doc/ - requirements and design docs
- src/master/ - control service
- src/worker/ - worker agent
- src/cli/ - CLI client
- src/common/ - shared libraries
- api/ - OpenAPI/Proto definitions
- configs/ - example configs and policies
- scripts/ - deployment and tooling scripts
- tests/ - unit/integration/e2e tests

## Dependencies
- CMake 3.20+ and a C++20 compiler
- PostgreSQL server
- Python 3 with `yoyo-migrations` and `mongodb-migrations` for DB migration scripts
- Build prerequisites for FetchContent MongoDB C++ driver on Linux:
  `pkg-config`, `libssl-dev`, `libsasl2-dev`, `zlib1g-dev`

## Header-only dependencies (third_party)
Place the header-only libraries into `third_party/`:
- cpp-httplib: `third_party/httplib.h`
- nlohmann/json: `third_party/nlohmann/json.hpp`

## Configuration
Database (used by Master startup migration scripts):
- `DB_BACKEND` (default: `postgres`; allowed: `postgres`, `mongo`)
- `DB_HOST` (default: `localhost`)
- `DB_PORT` (default: `5432`)
- `DB_USER` (required)
- `DB_PASSWORD` (optional)
- `DB_NAME` (required)
- `DB_SSLMODE` (optional)
- `DB_CONFIG` (optional path to `.env` file; e.g. `configs/db.env`)
- `MONGO_URI` (required when `DB_BACKEND=mongo`; e.g. `mongodb://127.0.0.1:27017`)
- `MONGO_DB` (required when `DB_BACKEND=mongo`)

Master service:
- `MASTER_HOST` (default: `0.0.0.0`)
- `MASTER_PORT` (default: `8080`)
- `LOG_DIR` (default: `logs`)
- `MASTER_LOG_FILE` (default: `logs/master.log`, rotated 10MB x 5)
- `MASTER_LOG_LEVEL` (default: `trace`)
- `HEARTBEAT_SEC` (default: `30`)
- `OFFLINE_SEC` (default: `120`)
- `MAX_LOG_UPLOAD_BYTES` (default: `10485760`, limit на размер загружаемого лога от агента)
- `INIT_DB_PYTHON` (optional python executable override for Postgres migrations runner)
- `INIT_DB_SCRIPT` (optional path to Postgres migrations script, default: `scripts/init_pg.py`)
- `PG_MIGRATIONS_DIR` (optional Postgres migrations directory, default: `migrations_broker_pg`)
- `INIT_MONGO_PYTHON` (optional python executable override for Mongo migrations)
- `INIT_MONGO_SCRIPT` (optional path to Mongo migration script, default: `scripts/init_mongo.py`)
- `MONGO_MIGRATIONS_DIR` (optional migrations directory, default: `migrations_broker_mongo`)
- `MONGO_MIGRATIONS_METASTORE` (optional metastore collection, default: `database_migrations`)
- `MASTER_SKIP_DB_MIGRATION` (optional bool; when true, skips DB migration scripts before startup)
  Alias: `SKIP_DB_MIGRATION`
- `MONGO_MIGRATIONS_BIN` (optional override of `mongodb-migrate` executable path)

Worker service:
- `UPLOAD_LOGS` (default: `true`)
- `MAX_UPLOAD_BYTES` (default: `10485760`, локальный лимит перед отправкой)
- `WORKER_LOG_DIR` (default: `logs/worker`)

## Build
```
cmake -S . -B build
cmake --build build
```

Mongo backend note:
- Master pulls `mongo-cxx-driver` via CMake `FetchContent` (`r4.2.0`).
- If you switch between WSL and Windows build contexts, use a fresh build directory to avoid CMake cache/source path mismatch.

### One command: native + arm + win_worker
Build matrix into a dedicated folder (`build/full`) with one command:
```
./scripts/build_full_matrix.sh
```

Artifacts:
- native binaries (+ `win_worker` cross-build): `build/full/native`
- ARM binaries (`master + worker + cli`): `build/full/arm`

Requirements for matrix build:
- MinGW-w64 cross compiler (`x86_64-w64-mingw32-g++-posix` or `x86_64-w64-mingw32-g++`) for `win_worker`
- ARM64 cross compiler (`aarch64-linux-gnu-g++`) for ARM build
- ARM-target `libpqxx`/`libpq` must be available for ARM `master`

Notes:
- Matrix build is for binaries only (`DC_BUILD_TESTS=OFF`).
- You can override ARM compiler prefix: `DC_ARM_CROSS_PREFIX=<prefix> ./scripts/build_full_matrix.sh`

## Tests
All repository tests are run with `pytest`.
Current suite covers binary smoke/integration checks and does not require PostgreSQL.

Install Python dependencies:
```
python -m pip install -r requirements.txt
```

Build binaries first (Linux preset expected by default test paths):
```
cmake --preset x86_64-linux -S . -B build/x86_64-linux -G Ninja
cmake --build build/x86_64-linux --config Release
```

Run all tests:
```
python -m pytest
```

Run only smoke tests:
```
python -m pytest -m smoke
```

Optional binary path overrides:
- `DC_BUILD_DIR` (default: `build/x86_64-linux`)
- `DC_MASTER_BIN`
- `DC_WORKER_BIN`
- `DC_CLI_BIN`

## Run Master
```
export DB_CONFIG=configs/db.env
./build/src/master/dc_master
```

When `DB_BACKEND=postgres`, the Master runs script from `INIT_DB_SCRIPT` (default: `scripts/init_pg.py`)
which applies `yoyo` migrations from `migrations_broker_pg`.
When `DB_BACKEND=mongo`, the Master runs script from `INIT_MONGO_SCRIPT` (default: `scripts/init_mongo.py`)
which executes `mongodb-migrations` over `migrations_broker_mongo`.
Set `MASTER_SKIP_DB_MIGRATION=1` (or `SKIP_DB_MIGRATION=1`) to skip these migration scripts.

## Example env for Master
Create a `.env` file (or reuse `configs/master.env`) and export it before запуском:
```env
DB_HOST=127.0.0.1
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=secret
DB_NAME=distributed
DB_SSLMODE=disable
DB_BACKEND=postgres

MASTER_HOST=0.0.0.0
MASTER_PORT=8080
LOG_DIR=logs
MASTER_LOG_FILE=logs/master.log
MASTER_LOG_LEVEL=trace
HEARTBEAT_SEC=30
OFFLINE_SEC=120
```

Run with:
```
set -a
source configs/master.env
set +a
./build/src/master/dc_master
```
