#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${1:-${ROOT_DIR}/build/full}"
NATIVE_DIR="${OUT_DIR}/native"
ARM_DIR="${OUT_DIR}/arm"
BUILD_TYPE="${BUILD_TYPE:-Release}"
JOBS="${JOBS:-$(nproc)}"
ARM_CROSS_PREFIX="${DC_ARM_CROSS_PREFIX:-aarch64-linux-gnu}"
ARM_CXX="${ARM_CROSS_PREFIX}-g++"

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: missing required command '$1'" >&2
        exit 1
    fi
}

require_cmd cmake

if ! command -v "${ARM_CXX}" >/dev/null 2>&1; then
    echo "error: missing '${ARM_CXX}' for ARM cross-build" >&2
    echo "hint: install ARM64 cross compiler or set DC_ARM_CROSS_PREFIX" >&2
    exit 1
fi

echo "==> Configure native + win_worker: ${NATIVE_DIR}"
cmake -S "${ROOT_DIR}" -B "${NATIVE_DIR}" \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DDC_BUILD_TESTS=OFF \
    -DDC_BUILD_WIN_WORKER=ON

echo "==> Build native + win_worker"
cmake --build "${NATIVE_DIR}" --parallel "${JOBS}"

echo "==> Configure arm (master + worker + cli): ${ARM_DIR}"
cmake -S "${ROOT_DIR}" -B "${ARM_DIR}" \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DCMAKE_TOOLCHAIN_FILE="${ROOT_DIR}/toolchains/arm64-linux-gnu.cmake" \
    -DDC_ARM_CROSS_PREFIX="${ARM_CROSS_PREFIX}" \
    -DDC_ONLY_WORKER=OFF \
    -DDC_BUILD_MASTER=ON \
    -DDC_BUILD_CLI=ON \
    -DDC_BUILD_TESTS=OFF \
    -DDC_BUILD_WIN_WORKER=OFF

echo "==> Build arm (master + worker + cli)"
cmake --build "${ARM_DIR}" --parallel "${JOBS}"

echo "Done. Artifacts are in: ${OUT_DIR}"
