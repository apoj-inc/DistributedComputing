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
- Python 3 with `psycopg2-binary` for `scripts/init_db.py`

## Header-only dependencies (third_party)
Place the header-only libraries into `third_party/`:
- cpp-httplib: `third_party/httplib.h`
- nlohmann/json: `third_party/nlohmann/json.hpp`

## Configuration
Database (used by Master and `scripts/init_db.py`):
- `DB_HOST` (default: `localhost`)
- `DB_PORT` (default: `5432`)
- `DB_USER` (required)
- `DB_PASSWORD` (optional)
- `DB_NAME` (required)
- `DB_SSLMODE` (optional)
- `DB_CONFIG` (optional path to `.env` file; e.g. `configs/db.env`)

Master service:
- `MASTER_HOST` (default: `0.0.0.0`)
- `MASTER_PORT` (default: `8080`)
- `LOG_DIR` (default: `logs`)
- `MASTER_LOG_FILE` (default: `logs/master.log`, rotated 10MB x 5)
- `MASTER_LOG_LEVEL` (default: `trace`)
- `HEARTBEAT_SEC` (default: `30`)
- `OFFLINE_SEC` (default: `120`)
- `MAX_LOG_UPLOAD_BYTES` (default: `10485760`, limit на размер загружаемого лога от агента)
- `INIT_DB_PYTHON` (optional python executable override for init_db)
- `INIT_DB_SCRIPT` (optional path to DB init script, default: `scripts/init_db.py`)

Worker service:
- `UPLOAD_LOGS` (default: `true`)
- `MAX_UPLOAD_BYTES` (default: `10485760`, локальный лимит перед отправкой)
- `WORKER_LOG_DIR` (default: `logs/worker`)

## Build
```
cmake -S . -B build
cmake --build build
```

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

On startup the Master runs script from `INIT_DB_SCRIPT` (default: `scripts/init_db.py`).
If schema differences are detected, the script prints
diffs and Master exits with code `4`.

## Example env for Master
Create a `.env` file (or reuse `configs/master.env`) and export it before запуском:
```env
DB_HOST=127.0.0.1
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=secret
DB_NAME=distributed
DB_SSLMODE=disable

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
