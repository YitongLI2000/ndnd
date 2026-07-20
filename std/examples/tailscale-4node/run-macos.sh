#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly BINARY="${NDND_BINARY:-${REPO_ROOT}/ndnd}"
readonly CONFIG="${NDND_CONFIG:-${SCRIPT_DIR}/fw.yml}"
readonly NDN_PORT="${NDN_PORT:-6363}"
readonly EXPERIMENT_PREFIX="${EXPERIMENT_PREFIX:-/ndn/4node}"
readonly NODE_PREFIX="${NODE_PREFIX:-${EXPERIMENT_PREFIX}/mac}"
readonly MODE="${1:-serve}"
readonly LOG_ROOT="${NDN_LOG_DIR:-${SCRIPT_DIR}/logs}"

RUNTIME_DIR=""
CHECKPOINT_FILE=""
FORWARDER_LOG=""
PINGSERVER_LOG=""
CLIENT_LOG=""
FORWARDER_PID=""
PINGSERVER_PID=""
TAIL_PID=""
TAILSCALE_STATUS=""
MAC_TS_IP="${MAC_TS_IP:-}"

usage() {
  cat <<'EOF'
Usage: run-macos.sh [serve|local-test]

  serve       Start the Mac as the four-node NDN hub and serve
              /ndn/4node/mac (default).
  local-test  Start the Mac forwarder and ping server, then require five
              local Data replies without involving Tailscale peers.

Environment overrides:
  MAC_TS_IP          Mac Tailscale IPv4 (detected by default).
  EXPERIMENT_PREFIX  Experiment root (default: /ndn/4node).
  NODE_PREFIX        Mac prefix (default: /ndn/4node/mac).
  NDND_BINARY        ndnd path (default: repository root/ndnd).
  SKIP_BUILD=1       Reuse NDND_BINARY.
  GO_BIN             Go executable used for a build.
  NDN_LOG_DIR        Checkpoint root (default: tailscale-4node/logs).
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

show_log_tail() {
  local label="$1"
  local path="$2"

  if [[ -s "${path}" ]]; then
    echo "Last ${label} log lines:" >&2
    tail -n 40 "${path}" >&2 || true
  fi
}

init_checkpoint() {
  local base_dir
  local git_commit
  local go_version

  base_dir="${LOG_ROOT}/checkpoint-$(date '+%Y%m%d-%H%M%S')-mac-${MODE}"
  RUNTIME_DIR="${base_dir}"
  if [[ -e "${RUNTIME_DIR}" ]]; then
    RUNTIME_DIR="${base_dir}-$$"
  fi
  mkdir -p "${RUNTIME_DIR}"

  CHECKPOINT_FILE="${RUNTIME_DIR}/checkpoint.txt"
  FORWARDER_LOG="${RUNTIME_DIR}/forwarder.log"
  PINGSERVER_LOG="${RUNTIME_DIR}/pingserver.log"
  CLIENT_LOG="${RUNTIME_DIR}/client.log"
  git_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || printf 'unknown')"
  go_version="$(go version 2>/dev/null || printf 'unavailable')"

  {
    printf 'started_at: %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')"
    printf 'node_id: mac\n'
    printf 'node_prefix: %s\n' "${NODE_PREFIX}"
    printf 'role: hub\n'
    printf 'mode: %s\n' "${MODE}"
    printf 'git_commit: %s\n' "${git_commit}"
    printf 'go_version: %s\n' "${go_version}"
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

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM HUP

  stop_child "${TAIL_PID}" "log follower"
  stop_child "${PINGSERVER_PID}" "ping server"
  stop_child "${FORWARDER_PID}" "Mac hub forwarder"
  finish_checkpoint "${exit_code}"

  case "${exit_code}" in
    0|129|130|143) ;;
    *)
      show_log_tail "forwarder" "${FORWARDER_LOG}"
      show_log_tail "ping server" "${PINGSERVER_LOG}"
      show_log_tail "client" "${CLIENT_LOG}"
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
    candidate="$(printf '%s\n' "${TAILSCALE_STATUS}" | \
      awk '$1 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ { print $1; exit }')"
  fi
  MAC_TS_IP="${candidate}"
  is_ipv4 "${MAC_TS_IP}" || \
    die "Unable to detect the Mac Tailscale IPv4; start Tailscale or set MAC_TS_IP"
}

choose_go_binary() {
  if [[ -n "${GO_BIN:-}" ]]; then
    printf '%s\n' "${GO_BIN}"
  elif [[ -x /usr/local/go/bin/go ]]; then
    printf '%s\n' /usr/local/go/bin/go
  else
    command -v go
  fi
}

build_binary() {
  local go_bin
  local go_root

  if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    [[ -x "${BINARY}" ]] || \
      die "SKIP_BUILD=1 but ${BINARY} is not executable"
    echo "Reusing ${BINARY}"
    return
  fi

  go_bin="$(choose_go_binary)" || die "Go is required to build ndnd"
  go_root="$(env -u GOROOT "${go_bin}" env GOROOT)" || \
    die "Unable to determine GOROOT for ${go_bin}"
  [[ -d "${go_root}" ]] || die "Go reported an invalid GOROOT: ${go_root}"

  echo "Building native Mac ndnd with $(env -u GOROOT "${go_bin}" version)..."
  (
    cd "${REPO_ROOT}"
    GOROOT="${go_root}" CGO_ENABLED=0 \
      "${go_bin}" build -o "${BINARY}" ./cmd/ndnd
  )
}

check_port_available() {
  local listeners

  listeners="$(lsof -nP -iTCP:"${NDN_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -n "${listeners}" ]]; then
    printf '%s\n' "${listeners}" >&2
    die "TCP port ${NDN_PORT} is already in use"
  fi
}

wait_for_forwarder() {
  local attempt
  local status_output

  for ((attempt = 1; attempt <= 50; attempt++)); do
    kill -0 "${FORWARDER_PID}" 2>/dev/null || die "The Mac forwarder exited"
    if status_output="$("${BINARY}" fw status 2>/dev/null)"; then
      printf '%s\n' "${status_output}"
      return
    fi
    sleep 0.1
  done
  die "Timed out waiting for the Mac forwarder on TCP ${NDN_PORT}"
}

start_forwarder() {
  export NDN_CLIENT_TRANSPORT="tcp://127.0.0.1:${NDN_PORT}"
  echo "Starting the Mac hub forwarder..."
  "${BINARY}" fw run "${CONFIG}" >"${FORWARDER_LOG}" 2>&1 &
  FORWARDER_PID=$!
  wait_for_forwarder
}

wait_for_pingserver_route() {
  local attempt
  local routes

  for ((attempt = 1; attempt <= 30; attempt++)); do
    routes="$("${BINARY}" fw route-list 2>/dev/null || true)"
    if grep -Fq "prefix=${NODE_PREFIX} " <<<"${routes}"; then
      return
    fi
    kill -0 "${PINGSERVER_PID}" 2>/dev/null || die "The ping server exited"
    sleep 0.1
  done
  die "Timed out waiting for local route ${NODE_PREFIX}"
}

start_pingserver() {
  echo "Starting the Mac ping server for ${NODE_PREFIX}..."
  "${BINARY}" pingserver "${NODE_PREFIX}" >"${PINGSERVER_LOG}" 2>&1 &
  PINGSERVER_PID=$!
  wait_for_pingserver_route
}

run_local_test() {
  local output
  local received
  local status=0

  output="$("${BINARY}" ping "${NODE_PREFIX}" -c 5 -t 2000 2>&1)" || status=$?
  printf '%s\n' "${output}" | tee "${CLIENT_LOG}"
  received="$(grep -Fc "content from ${NODE_PREFIX}:" <<<"${output}" || true)"
  if ((status != 0)) || [[ "${received}" != "5" ]]; then
    die "Mac local test received ${received}/5 Data packets"
  fi
  echo "Mac hub local test passed."
}

print_ready() {
  cat <<EOF

Mac NDN hub is ready:
  Tailscale IPv4 : ${MAC_TS_IP}
  TCP listener   : ${MAC_TS_IP}:${NDN_PORT}
  Local prefix   : ${NODE_PREFIX}
  Checkpoint     : ${RUNTIME_DIR}

Start Agent A, Agent B, and the Windows coordinator as edges. After each edge
completes its warm-up, identify its long-lived incoming FaceId:

  ./ndnd fw face-list

Bind the three edge prefixes to their actual FaceIds:

  ./ndnd fw route-add prefix=${EXPERIMENT_PREFIX}/agent-a face=<AGENT_A_FACE_ID>
  ./ndnd fw route-add prefix=${EXPERIMENT_PREFIX}/coordinator face=<WINDOWS_FACE_ID>
  ./ndnd fw route-add prefix=${EXPERIMENT_PREFIX}/agent-b face=<AGENT_B_FACE_ID>
  ./ndnd fw route-list

Do not add a parent ${EXPERIMENT_PREFIX} route on the Mac hub.
Press Ctrl-C only after the four-node matrix is complete.
EOF
}

follow_pingserver() {
  echo
  echo "Mac ping server output:"
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

[[ "$(uname -s)" == "Darwin" ]] || die "This hub runner is for macOS"
[[ -f "${CONFIG}" ]] || die "Forwarder config not found: ${CONFIG}"
require_command env
require_command grep
require_command lsof
require_command tail
require_command tee

if [[ "${MODE}" == "serve" ]]; then
  require_command tailscale
  require_command awk
  detect_tailscale_ip
fi

init_checkpoint
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

check_port_available
build_binary
start_forwarder
start_pingserver

if [[ "${MODE}" == "local-test" ]]; then
  run_local_test
else
  print_ready
  follow_pingserver
fi
