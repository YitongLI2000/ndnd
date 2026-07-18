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
readonly RUNTIME_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ndnd-p2p-macos.XXXXXX")"
readonly FORWARDER_LOG="${RUNTIME_DIR}/forwarder.log"
readonly PINGSERVER_LOG="${RUNTIME_DIR}/pingserver.log"

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

  case "${exit_code}" in
    0|129|130|143) ;;
    *)
      if [[ -s "${FORWARDER_LOG}" ]]; then
        echo "Last forwarder log lines:" >&2
        tail -n 30 "${FORWARDER_LOG}" >&2 || true
      fi
      ;;
  esac

  echo "Runtime logs: ${RUNTIME_DIR}"
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
  Forwarder log  : ${FORWARDER_LOG}

On Agent A, start its forwarder from the same repository revision:

  export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:${PORT}
  ./ndnd fw run std/examples/tailscale-p2p/fw.yml

In a second Agent A terminal, create the route and run the test:

  export NDN_CLIENT_TRANSPORT=tcp://127.0.0.1:${PORT}
  tailscale ping ${MAC_TS_IP}
  nc -vz ${MAC_TS_IP} ${PORT}
  ./ndnd fw route-add prefix=${PREFIX} persistency=permanent face=tcp4://${MAC_TS_IP}:${PORT}
  ./ndnd fw face-list
  ./ndnd fw route-list
  ./ndnd ping ${PREFIX} -c 5 -t 2000

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
  "${BINARY}" ping "${PREFIX}" -c 5 -t 2000

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
