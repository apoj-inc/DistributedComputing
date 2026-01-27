#!/usr/bin/env bash
set -euo pipefail

# Smoke script that drives the dc_cli to submit multiple echo tasks and prints their results.
# Usage:
#   BASE_URL=http://127.0.0.1:8080 ./scripts/cli_echo_smoke.sh
#   CLI_BIN=./build/debug/src/cli/dc_cli ECHO_SCRIPT=./scripts/test_echo.sh ./scripts/cli_echo_smoke.sh

BASE_URL=${BASE_URL:-http://127.0.0.1:8080}
CLI_BIN=${CLI_BIN:-./build/debug/src/cli/dc_cli}
ECHO_SCRIPT=${ECHO_SCRIPT:-"$(pwd)/scripts/test_echo.py"}

if [[ ! -x "$CLI_BIN" ]]; then
  echo "Missing cli binary: $CLI_BIN (build with cmake --preset debug && cmake --build --preset debug)" >&2
  exit 1
fi

if [[ ! -x "$ECHO_SCRIPT" ]]; then
  echo "Missing echo script: $ECHO_SCRIPT" >&2
  exit 1
fi

timestamp=$(date +%s)
tasks=(
  "echo-${timestamp}-1|hello|world"
  "echo-${timestamp}-2|foo|bar|baz"
  "echo-${timestamp}-3|quick|brown|fox"
)

declare -a task_ids
for entry in "${tasks[@]}"; do
  IFS='|' read -r -a parts <<<"$entry"
  task_id=${parts[0]}
  args=("${parts[@]:1}")
  task_ids+=("$task_id")

  submit_args=()
  for arg in "${args[@]}"; do
    submit_args+=(--arg "$arg")
  done

  echo "Submitting task ${task_id}..."
  "$CLI_BIN" --base-url "$BASE_URL" tasks submit --id "$task_id" --cmd "$ECHO_SCRIPT" "${submit_args[@]}"
done

declare -a task_ids
for entry in "${tasks[@]}"; do
  IFS='|' read -r -a parts <<<"$entry"
  task_id=${parts[0]}2
  args=("${parts[@]:1}")
  task_ids+=("$task_id")

  submit_args=()
  for arg in "${args[@]}"; do
    submit_args+=(--arg "$arg")
  done

  echo "Submitting task ${task_id}..."
  "$CLI_BIN" --base-url "$BASE_URL" tasks submit --id "$task_id" --cmd "$ECHO_SCRIPT" "${submit_args[@]}"
done

declare -a task_ids
for entry in "${tasks[@]}"; do
  IFS='|' read -r -a parts <<<"$entry"
  task_id=${parts[0]}3
  args=("${parts[@]:1}")
  task_ids+=("$task_id")

  submit_args=()
  for arg in "${args[@]}"; do
    submit_args+=(--arg "$arg")
  done

  echo "Submitting task ${task_id}..."
  "$CLI_BIN" --base-url "$BASE_URL" tasks submit --id "$task_id" --cmd "$ECHO_SCRIPT" "${submit_args[@]}"
done

echo "Done."
