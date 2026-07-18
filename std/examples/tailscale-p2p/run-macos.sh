#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly BINARY="${REPO_ROOT}/ndnd"
readonly CONFIG="${SCRIPT_DIR}/fw.yml"
readonly PORT=6363
readonly PREFIX="${NDN_PING_PREFIX:-/p2p/mac}"
readonly AGENT_A_TS_IP="${AGENT_A_TS_IP:-100.81.98.57}"
readonly MODE="${1:-serve}"
readonly LOG_ROOT="${NDN_LOG_DIR:-${SCRIPT_DIR}/logs}"

RUNTIME_DIR=""
FORWARDER_LOG=""
PINGSERVER_LOG=""
CLIENT_LOG=""
CHECKPOINT_FILE=""
FORWARDER_PID=""
PINGSERVER_PID=""
TAIL_PID=""
TAILSCALE_STATUS=""
MAC_TS_IP="${MAC_TS_IP:-}"

usage() {
  cat <<'EOF'
Usage: run-macos.sh [serve|local-test]

  serve       Build and run the Mac forwarder and ping server (default).
  local-test  Build, start both services, and ping /p2p/mac locally.

Environment overrides:
  MAC_TS_IP          Mac Tailscale IPv4 address (detected by default).
  AGENT_A_TS_IP      Agent A Tailscale IPv4 address (default: 100.81.98.57).
  NDN_PING_PREFIX    Served NDN prefix (default: /p2p/mac).
  NDN_LOG_DIR        Checkpoint root (default: tailscale-p2p/logs).
  SKIP_BUILD=1       Reuse the existing ./ndnd binary.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

init_checkpoint() {
  local base_dir
  local git_commit
  local go_version
  local started_at

  started_at="$(date '+%Y-%m-%dT%H:%M:%S%z')"
  base_dir="${LOG_ROOT}/checkpoint-$(date '+%Y%m%d-%H%M%S')-${MODE}"
  RUNTIME_DIR="${base_dir}"
  if [[ -e "${RUNTIME_DIR}" ]]; then
    RUNTIME_DIR="${base_dir}-$$"
  fi

  mkdir -p "${RUNTIME_DIR}" || die "Unable to create checkpoint directory: ${RUNTIME_DIR}"
  FORWARDER_LOG="${RUNTIME_DIR}/forwarder.log"
  PINGSERVER_LOG="${RUNTIME_DIR}/pingserver.log"
  CLIENT_LOG="${RUNTIME_DIR}/client.log"
  CHECKPOINT_FILE="${RUNTIME_DIR}/checkpoint.txt"

  git_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || printf 'unknown')"
  go_version="$(go version 2>/dev/null || printf 'unavailable')"

  {
    printf 'started_at: %s\n' "${started_at}"
    printf 'mode: %s\n' "${MODE}"
    printf 'prefix: %s\n' "${PREFIX}"
    printf 'agent_a_tailscale_ip: %s\n' "${AGENT_A_TS_IP}"
    printf 'git_commit: %s\n' "${git_commit}"
    printf 'go_version: %s\n' "${go_version}"
    printf 'forwarder_log: %s\n' "${FORWARDER_LOG}"
    printf 'pingserver_log: %s\n' "${PINGSERVER_LOG}"
    printf 'client_log: %s\n' "${CLIENT_LOG}"
  } >"${CHECKPOINT_FILE}"

  echo "Checkpoint directory: ${RUNTIME_DIR}"
}

finish_checkpoint() {
  local exit_code="$1"
  local result

  case "${exit_code}" in
    0) result="completed" ;;
    129|130|143) result="stopped" ;;
    *) result="failed" ;;
  esac

  {
    printf 'finished_at: %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')"
    printf 'result: %s\n' "${result}"
    printf 'exit_code: %s\n' "${exit_code}"
    printf 'mac_tailscale_ip: %s\n' "${MAC_TS_IP:-not-detected}"
  } >>"${CHECKPOINT_FILE}"
}

is_ipv4() {
  [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

stop_child() {
  local pid="$1"
  local label="$2"

  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "Stopping ${label} (pid ${pid})..."
    kill -TERM "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM HUP

  stop_child "${TAIL_PID}" "log follower"
  stop_child "${PINGSERVER_PID}" "ping server"
  stop_child "${FORWARDER_PID}" "forwarder"
  finish_checkpoint "${exit_code}"

  case "${exit_code}" in
    0|129|130|143) ;;
    *)
      if [[ -s "${FORWARDER_LOG}" ]]; then
        echo "Last forwarder log lines:" >&2
        tail -n 30 "${FORWARDER_LOG}" >&2 || true
      fi
      ;;
  esac

  echo "Checkpoint logs: ${RUNTIME_DIR}"
  exit "${exit_code}"
}

detect_tailscale_ip() {
  local candidate=""

  if [[ -n "${MAC_TS_IP}" ]]; then
    is_ipv4 "${MAC_TS_IP}" || die "MAC_TS_IP is not an IPv4 address: ${MAC_TS_IP}"
    return
  fi

  TAILSCALE_STATUS="$(tailscale status 2>&1 || true)"
  candidate="$(tailscale ip -4 2>/dev/null | head -n 1 || true)"
  if ! is_ipv4 "${candidate}"; then
    candidate="$(printf '%s\n' "${TAILSCALE_STATUS}" | awk '$1 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ { print $1; exit }')"
  fi
  MAC_TS_IP="${candidate}"

  is_ipv4 "${MAC_TS_IP}" || \
    die "Unable to detect the Mac Tailscale IPv4 address; confirm Tailscale is running or set MAC_TS_IP explicitly"
}

build_binary() {
  local machine
  local goarch

  if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    [[ -x "${BINARY}" ]] || die "SKIP_BUILD=1 but ${BINARY} is not executable"
    echo "Reusing ${BINARY}"
    return
  fi

  machine="$(uname -m)"
  case "${machine}" in
    arm64) goarch="arm64" ;;
    x86_64) goarch="amd64" ;;
    *) die "Unsupported Mac architecture: ${machine}" ;;
  esac

  echo "Building ndnd with $(go version)..."
  (
    cd "${REPO_ROOT}"
    CGO_ENABLED=0 GOOS=darwin GOARCH="${goarch}" \
      go build -o "${BINARY}" ./cmd/ndnd
  )
}

check_port_available() {
  local listeners

  listeners="$(lsof -nP -iTCP:${PORT} -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${listeners}" ]]; then
    echo "${listeners}" >&2
    die "TCP port ${PORT} is already in use"
  fi
}

wait_for_forwarder() {
  local attempt

  for ((attempt = 1; attempt <= 50; attempt++)); do
    if ! kill -0 "${FORWARDER_PID}" 2>/dev/null; then
      die "The forwarder exited during startup"
    fi
    if nc -z 127.0.0.1 "${PORT}" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done

  die "Timed out waiting for the forwarder on TCP ${PORT}"
}

start_forwarder() {
  export NDN_CLIENT_TRANSPORT="tcp://127.0.0.1:${PORT}"

  echo "Starting the Mac forwarder..."
  "${BINARY}" fw run "${CONFIG}" >"${FORWARDER_LOG}" 2>&1 &
  FORWARDER_PID=$!
  wait_for_forwarder

  "${BINARY}" fw status
}

print_agent_a_commands() {
  cat <<EOF

Mac side is ready:
  Tailscale IPv4 : ${MAC_TS_IP}
  NDN prefix     : ${PREFIX}
  TCP listener   : ${MAC_TS_IP}:${PORT}
  Checkpoint     : ${RUNTIME_DIR}
  Forwarder log  : ${FORWARDER_LOG}

On Agent A, start its forwarder from the same repository revision:

  MAC_TS_HOST=${MAC_TS_IP} NDN_PING_PREFIX=${PREFIX} \
    std/examples/tailscale-p2p/run-linux.sh

Agent A uses userspace Tailscale, so the Linux runner creates a local
tailscale-nc bridge instead of dialing ${MAC_TS_IP}:${PORT} directly.

Press Ctrl-C on this Mac when the remote test is complete.
EOF
}

wait_for_pingserver_route() {
  local attempt
  local routes

  for ((attempt = 1; attempt <= 30; attempt++)); do
    routes="$("${BINARY}" fw route-list 2>/dev/null || true)"
    if printf '%s\n' "${routes}" | grep -Fq "prefix=${PREFIX} "; then
      return
    fi
    if ! kill -0 "${PINGSERVER_PID}" 2>/dev/null; then
      die "The ping server exited during startup"
    fi
    sleep 0.1
  done

  die "Timed out waiting for the ping server route ${PREFIX}"
}

start_pingserver() {
  echo "Starting the ping server for ${PREFIX}..."
  "${BINARY}" pingserver "${PREFIX}" >"${PINGSERVER_LOG}" 2>&1 &
  PINGSERVER_PID=$!
  wait_for_pingserver_route
}

run_local_test() {
  echo "Running five local NDN pings..."
  "${BINARY}" ping "${PREFIX}" -c 5 -t 2000 | tee "${CLIENT_LOG}"

  echo
  echo "Ping server output:"
  cat "${PINGSERVER_LOG}"
}

follow_pingserver() {
  echo
  echo "Ping server output:"
  cat "${PINGSERVER_LOG}"
  tail -n 0 -F "${PINGSERVER_LOG}" &
  TAIL_PID=$!
  wait "${PINGSERVER_PID}"
}

case "${MODE}" in
  serve|local-test) ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

init_checkpoint
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

[[ "$(uname -s)" == "Darwin" ]] || die "This runner is for macOS"
require_command go
require_command lsof
require_command nc
require_command awk
require_command grep
require_command tee

if [[ "${MODE}" == "serve" ]]; then
  require_command tailscale
  detect_tailscale_ip
fi

check_port_available
build_binary
start_forwarder

if [[ "${MODE}" == "serve" ]]; then
  echo
  echo "Checking Agent A at ${AGENT_A_TS_IP} (a timeout is only a warning)..."
  AGENT_PING_OUTPUT="$(tailscale ping --c=1 --timeout=3s "${AGENT_A_TS_IP}" 2>&1 || true)"
  printf '%s\n' "${AGENT_PING_OUTPUT}"
  if printf '%s\n' "${AGENT_PING_OUTPUT}" | grep -Fq "pong from"; then
    echo "Agent A is reachable over Tailscale."
  else
    echo "WARNING: Agent A did not answer yet; keep this Mac service running and bring Agent A online." >&2
  fi
fi

start_pingserver

if [[ "${MODE}" == "local-test" ]]; then
  run_local_test
else
  print_agent_a_commands
  follow_pingserver
fi
