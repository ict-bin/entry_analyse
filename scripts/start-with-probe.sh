#!/bin/bash

set -euo pipefail

PID_FILE="${SECFLOW_MAIN_PID_FILE:-/tmp/secflow-main.pid}"
STARTED_AT_FILE="${SECFLOW_MAIN_STARTED_AT_FILE:-/tmp/secflow-main.started_at}"

PYTHON_BIN="$(command -v python3 || command -v python)"
rm -f "${PID_FILE}" "${STARTED_AT_FILE}"

"${PYTHON_BIN}" -m app.probe_process &
probe_pid=$!

"$@" &
main_pid=$!

printf '%s\n' "${main_pid}" > "${PID_FILE}"
date +%s > "${STARTED_AT_FILE}"

terminate_children() {
    kill -TERM "${main_pid}" 2>/dev/null || true
    kill -TERM "${probe_pid}" 2>/dev/null || true
}

cleanup() {
    local exit_code="$1"
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
