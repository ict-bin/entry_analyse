#!/bin/bash

set -euo pipefail

PID_FILE="${SECFLOW_MAIN_PID_FILE:-/tmp/secflow-main.pid}"
STARTED_AT_FILE="${SECFLOW_MAIN_STARTED_AT_FILE:-/tmp/secflow-main.started_at}"
SERVICE_NAME="${SECFLOW_PROBE_SERVICE_NAME:-secflow-app}"

PYTHON_BIN="$(command -v python3 || command -v python)"
rm -f "${PID_FILE}" "${STARTED_AT_FILE}"

echo "[${SERVICE_NAME}] starting independent probe process"
"${PYTHON_BIN}" -m app.probe_process &
probe_pid=$!

echo "[${SERVICE_NAME}] starting main process: $*"
"$@" &
main_pid=$!

printf '%s\n' "${main_pid}" > "${PID_FILE}"
date +%s > "${STARTED_AT_FILE}"
echo "[${SERVICE_NAME}] main pid=${main_pid} probe pid=${probe_pid}"

terminate_children() {
    kill -TERM "${main_pid}" 2>/dev/null || true
    kill -TERM "${probe_pid}" 2>/dev/null || true
}

cleanup() {
    local exit_code="$1"
    echo "[${SERVICE_NAME}] stopping with exit_code=${exit_code}"
    rm -f "${PID_FILE}" "${STARTED_AT_FILE}"
    kill -TERM "${probe_pid}" 2>/dev/null || true
    wait "${probe_pid}" 2>/dev/null || true
    exit "${exit_code}"
}

trap 'terminate_children' TERM INT

set +e
wait "${main_pid}"
main_status=$?
set -e

cleanup "${main_status}"
