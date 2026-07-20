#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly BINARY="${NDND_BINARY:-${REPO_ROOT}/ndnd}"
readonly CONFIG="${NDND_CONFIG:-${SCRIPT_DIR}/fw.yml}"
readonly BRIDGE_SCRIPT="${SCRIPT_DIR}/tailscale-nc-bridge.py"

readonly NODE_ID="${NODE_ID:-}"
readonly NODE_TS_IP="${NODE_TS_IP:-}"
readonly TRANSPORT_MODE="${TRANSPORT_MODE:-direct}"
readonly EXPERIMENT_PREFIX="${EXPERIMENT_PREFIX:-/ndn/4node}"
readonly NODE_PREFIX="${NODE_PREFIX:-${EXPERIMENT_PREFIX}/${NODE_ID}}"
readonly HUB_TS_HOST="${HUB_TS_HOST:-castermacbook}"
readonly HUB_TS_IP="${HUB_TS_IP:-100.77.224.3}"
readonly NDN_PORT="${NDN_PORT:-6363}"
readonly BRIDGE_HOST="${BRIDGE_HOST:-127.0.0.1}"
readonly BRIDGE_PORT="${BRIDGE_PORT:-16363}"
readonly MODE="${1:-serve}"
readonly LOG_ROOT="${NDN_LOG_DIR:-${SCRIPT_DIR}/logs}"

readonly TAILSCALE_BIN="${TAILSCALE_BIN:-tailscale}"
readonly TAILSCALE_SOCKET="${TAILSCALE_SOCKET:-/srv/yitong/ioa/.ioa_runtime/agent_a/tailscale/run/tailscaled.sock}"
readonly TAILSCALE_PID_FILE="${TAILSCALE_PID_FILE:-/srv/yitong/ioa/.ioa_runtime/agent_a/tailscale/run/tailscaled.pid}"
readonly TAILSCALE_START_SCRIPT="${TAILSCALE_START_SCRIPT:-/srv/yitong/ioa/scripts/agent_a_tailscale_nc/start_tailscaled.sh}"
readonly TAILSCALE_STOP_SCRIPT="${TAILSCALE_STOP_SCRIPT:-/srv/yitong/ioa/scripts/agent_a_tailscale_nc/stop_tailscaled.sh}"

RUNTIME_DIR=""
CHECKPOINT_FILE=""
FORWARDER_LOG=""
PINGSERVER_LOG=""
BRIDGE_LOG=""
CLIENT_LOG=""
WARMUP_LOG=""
FORWARDER_PID=""
PINGSERVER_PID=""
BRIDGE_PID=""
TAIL_PID=""
TAILSCALE_STARTED=0
FACE_URI=""

usage() {
  cat <<'EOF'
Usage: run-edge.sh [serve|local-test]

This is the shared implementation behind run-agent-a.sh and run-agent-b.sh.
Invoke one of those node wrappers instead of calling this file directly.

  serve       Start the edge forwarder, local ping server, and parent route
              to the Mac hub (default).
  local-test  Start only the local forwarder and ping server, then verify five
              local Data responses without requiring Tailscale.

Common overrides:
  HUB_TS_HOST          Mac Tailscale name (default: castermacbook).
  HUB_TS_IP            Mac Tailscale IPv4 (default: 100.77.224.3).
  EXPERIMENT_PREFIX    Parent prefix (default: /ndn/4node).
  NODE_PREFIX          This node's served prefix.
  NDND_BINARY          ndnd binary path (default: repository root/ndnd).
  SKIP_BUILD=1         Reuse NDND_BINARY.
  NDN_LOG_DIR          Checkpoint root (default: tailscale-4node/logs).
  KEEP_TAILSCALE=1     Keep isolated Tailscale if this runner started it.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

pid_is_running() {
  local pid="${1:-}"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

stop_child() {
  local pid="$1"
  local label="$2"

  if pid_is_running "${pid}"; then
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

  base_dir="${LOG_ROOT}/checkpoint-$(date '+%Y%m%d-%H%M%S')-${NODE_ID}-${MODE}"
  RUNTIME_DIR="${base_dir}"
  if [[ -e "${RUNTIME_DIR}" ]]; then
    RUNTIME_DIR="${base_dir}-$$"
  fi
  mkdir -p "${RUNTIME_DIR}"

  CHECKPOINT_FILE="${RUNTIME_DIR}/checkpoint.txt"
  FORWARDER_LOG="${RUNTIME_DIR}/forwarder.log"
  PINGSERVER_LOG="${RUNTIME_DIR}/pingserver.log"
  BRIDGE_LOG="${RUNTIME_DIR}/bridge.log"
  CLIENT_LOG="${RUNTIME_DIR}/client.log"
  WARMUP_LOG="${RUNTIME_DIR}/hub-warmup.log"

  git_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || printf 'unknown')"
  go_version="$(go version 2>/dev/null || printf 'unavailable')"
  {
    printf 'started_at: %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')"
    printf 'node_id: %s\n' "${NODE_ID}"
    printf 'node_tailscale_ip: %s\n' "${NODE_TS_IP}"
    printf 'node_prefix: %s\n' "${NODE_PREFIX}"
    printf 'mode: %s\n' "${MODE}"
    printf 'transport_mode: %s\n' "${TRANSPORT_MODE}"
    printf 'hub_host: %s\n' "${HUB_TS_HOST}"
    printf 'hub_ip: %s\n' "${HUB_TS_IP}"
    printf 'git_commit: %s\n' "${git_commit}"
    printf 'go_version: %s\n' "${go_version}"
  } >"${CHECKPOINT_FILE}"

  echo "Checkpoint directory: ${RUNTIME_DIR}"
}

finish_checkpoint() {
  local exit_code="$1"
  local result

  [[ -n "${CHECKPOINT_FILE}" && -f "${CHECKPOINT_FILE}" ]] || return
  case "${exit_code}" in
    0) result="completed" ;;
    129|130|143) result="stopped" ;;
    *) result="failed" ;;
  esac
  {
    printf 'finished_at: %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')"
    printf 'result: %s\n' "${result}"
    printf 'exit_code: %s\n' "${exit_code}"
    printf 'face_uri: %s\n' "${FACE_URI:-not-created}"
  } >>"${CHECKPOINT_FILE}"
}

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM HUP

  stop_child "${TAIL_PID}" "log follower"
  stop_child "${PINGSERVER_PID}" "ping server"
  stop_child "${FORWARDER_PID}" "forwarder"
  stop_child "${BRIDGE_PID}" "Tailscale TCP bridge"

  if [[ "${TAILSCALE_STARTED}" == "1" ]]; then
    if [[ "${KEEP_TAILSCALE:-0}" == "1" ]]; then
      echo "Keeping isolated Agent A Tailscale (KEEP_TAILSCALE=1)."
    elif [[ -x "${TAILSCALE_STOP_SCRIPT}" ]]; then
      echo "Stopping isolated Agent A Tailscale..."
      "${TAILSCALE_STOP_SCRIPT}" || \
        echo "WARNING: failed to stop isolated Tailscale cleanly" >&2
    fi
  fi

  finish_checkpoint "${exit_code}"
  case "${exit_code}" in
    0|129|130|143) ;;
    *)
      show_log_tail "forwarder" "${FORWARDER_LOG}"
      show_log_tail "ping server" "${PINGSERVER_LOG}"
      show_log_tail "bridge" "${BRIDGE_LOG}"
      show_log_tail "client" "${CLIENT_LOG}"
      show_log_tail "hub warm-up" "${WARMUP_LOG}"
      ;;
  esac
  echo "Checkpoint logs: ${RUNTIME_DIR}"
  exit "${exit_code}"
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
  local go_cache
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

  go_cache="${GOCACHE:-${TMPDIR:-/tmp}/ndnd-go-build}"
  mkdir -p "${go_cache}"
  echo "Building native ndnd with $(env -u GOROOT "${go_bin}" version)..."
  (
    cd "${REPO_ROOT}"
    GOROOT="${go_root}" GOCACHE="${go_cache}" CGO_ENABLED=0 \
      "${go_bin}" build -o "${BINARY}" ./cmd/ndnd
  )
}

check_port_available() {
  local label="$1"
  local listeners=""
  local port="$2"

  case "$(uname -s)" in
    Linux)
      listeners="$(ss -H -ltn "sport = :${port}" 2>/dev/null || true)"
      ;;
    Darwin)
      listeners="$(lsof -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
      ;;
  esac
  if [[ -n "${listeners}" ]]; then
    printf '%s\n' "${listeners}" >&2
    die "${label} TCP port ${port} is already in use"
  fi
}

wait_for_forwarder() {
  local attempt
  local status_output

  for ((attempt = 1; attempt <= 50; attempt++)); do
    pid_is_running "${FORWARDER_PID}" || die "The forwarder exited during startup"
    if status_output="$("${BINARY}" fw status 2>/dev/null)"; then
      printf '%s\n' "${status_output}"
      return
    fi
    sleep 0.1
  done
  die "Timed out waiting for the local forwarder on TCP ${NDN_PORT}"
}

start_forwarder() {
  export NDN_CLIENT_TRANSPORT="tcp://127.0.0.1:${NDN_PORT}"
  echo "Starting ${NODE_ID} NDNd forwarder..."
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
    pid_is_running "${PINGSERVER_PID}" || die "The ping server exited during startup"
    sleep 0.1
  done
  die "Timed out waiting for local route ${NODE_PREFIX}"
}

start_pingserver() {
  echo "Starting ping server for ${NODE_PREFIX}..."
  "${BINARY}" pingserver "${NODE_PREFIX}" >"${PINGSERVER_LOG}" 2>&1 &
  PINGSERVER_PID=$!
  wait_for_pingserver_route
}

run_local_test() {
  local output
  local received
  local status=0

  echo "Sending five local NDN pings to ${NODE_PREFIX}..."
  output="$("${BINARY}" ping "${NODE_PREFIX}" -c 5 -t 2000 2>&1)" || status=$?
  printf '%s\n' "${output}" | tee "${CLIENT_LOG}"
  received="$(grep -Fc "content from ${NODE_PREFIX}:" <<<"${output}" || true)"
  if ((status != 0)) || [[ "${received}" != "5" ]]; then
    die "Local test received ${received}/5 Data packets"
  fi
  echo "Local ${NODE_ID} test passed."
}

tailscale_is_running() {
  local status_json

  [[ -S "${TAILSCALE_SOCKET}" ]] || return 1
  status_json="$("${TAILSCALE_BIN}" --socket="${TAILSCALE_SOCKET}" \
    status --json 2>/dev/null)" || return 1
  grep -Eq '"BackendState"[[:space:]]*:[[:space:]]*"Running"' \
    <<<"${status_json}"
}

private_tailscale_pid_is_running() {
  local pid=""

  [[ -s "${TAILSCALE_PID_FILE}" ]] || return 1
  read -r pid <"${TAILSCALE_PID_FILE}" || return 1
  pid_is_running "${pid}"
}

start_private_tailscale() {
  if tailscale_is_running; then
    echo "Reusing the running isolated Agent A Tailscale daemon."
    return
  fi

  [[ -x "${TAILSCALE_START_SCRIPT}" ]] || \
    die "Tailscale start script is not executable: ${TAILSCALE_START_SCRIPT}"
  if ! private_tailscale_pid_is_running; then
    TAILSCALE_STARTED=1
  fi

  echo "Starting only the isolated Agent A userspace Tailscale daemon..."
  "${TAILSCALE_START_SCRIPT}"
  tailscale_is_running || die "Isolated Tailscale did not become ready"
}

check_userspace_hub() {
  local output

  echo "Checking the Mac hub through the isolated Tailscale socket..."
  if ! output="$("${TAILSCALE_BIN}" --socket="${TAILSCALE_SOCKET}" \
      ping --c=1 --timeout=5s "${HUB_TS_HOST}" 2>&1)"; then
    printf '%s\n' "${output}" >&2
    die "Mac hub is not reachable: ${HUB_TS_HOST}"
  fi
  printf '%s\n' "${output}"

  python3 "${BRIDGE_SCRIPT}" \
    --probe \
    --target-host "${HUB_TS_HOST}" \
    --target-port "${NDN_PORT}" \
    --tailscale-socket "${TAILSCALE_SOCKET}" \
    --tailscale-bin "${TAILSCALE_BIN}"
}

check_direct_hub() {
  local output

  echo "Checking the Mac hub over native Tailscale..."
  if ! output="$("${TAILSCALE_BIN}" ping --c=1 --timeout=5s \
      "${HUB_TS_HOST}" 2>&1)"; then
    printf '%s\n' "${output}" >&2
    die "Mac hub is not reachable: ${HUB_TS_HOST}"
  fi
  printf '%s\n' "${output}"
  nc -z -w 3 "${HUB_TS_IP}" "${NDN_PORT}" || \
    die "Mac hub NDNd is not listening at ${HUB_TS_IP}:${NDN_PORT}"
}

wait_for_bridge() {
  local attempt

  for ((attempt = 1; attempt <= 50; attempt++)); do
    pid_is_running "${BRIDGE_PID}" || die "The Tailscale bridge exited during startup"
    if grep -Fq '[bridge] listening on' "${BRIDGE_LOG}" 2>/dev/null; then
      return
    fi
    sleep 0.1
  done
  die "Timed out waiting for the Tailscale bridge"
}

start_bridge() {
  echo "Starting ${BRIDGE_HOST}:${BRIDGE_PORT} -> ${HUB_TS_HOST}:${NDN_PORT}..."
  python3 -u "${BRIDGE_SCRIPT}" \
    --listen-host "${BRIDGE_HOST}" \
    --listen-port "${BRIDGE_PORT}" \
    --target-host "${HUB_TS_HOST}" \
    --target-port "${NDN_PORT}" \
    --tailscale-socket "${TAILSCALE_SOCKET}" \
    --tailscale-bin "${TAILSCALE_BIN}" \
    >"${BRIDGE_LOG}" 2>&1 &
  BRIDGE_PID=$!
  wait_for_bridge
  FACE_URI="tcp4://${BRIDGE_HOST}:${BRIDGE_PORT}"
}

prepare_transport() {
  case "${TRANSPORT_MODE}" in
    direct)
      check_direct_hub
      FACE_URI="tcp4://${HUB_TS_IP}:${NDN_PORT}"
      ;;
    tailscale-nc)
      start_private_tailscale
      check_userspace_hub
      start_bridge
      ;;
    *) die "Unknown TRANSPORT_MODE: ${TRANSPORT_MODE}" ;;
  esac
}

add_parent_route() {
  local routes

  echo "Adding ${EXPERIMENT_PREFIX} through ${FACE_URI}..."
  "${BINARY}" fw route-add \
    "prefix=${EXPERIMENT_PREFIX}" \
    persistency=permanent \
    "face=${FACE_URI}"

  routes="$("${BINARY}" fw route-list)"
  printf '%s\n' "${routes}"
  grep -Fq "prefix=${EXPERIMENT_PREFIX} " <<<"${routes}" || \
    die "Parent route was not installed: ${EXPERIMENT_PREFIX}"
}

warm_hub_face() {
  local hub_prefix="${EXPERIMENT_PREFIX}/mac"
  local output
  local received
  local status=0

  echo "Warming the Mac hub face with two NDN pings..."
  output="$("${BINARY}" ping "${hub_prefix}" \
    -c 2 -t 3000 2>&1)" || status=$?
  printf '%s\n' "${output}" | tee "${WARMUP_LOG}"
  received="$(grep -Fc "content from ${hub_prefix}:" \
    <<<"${output}" || true)"
  if ((status != 0)) || [[ "${received}" != "2" ]]; then
    die "Mac hub warm-up received ${received}/2 Data packets"
  fi
}

print_ready() {
  cat <<EOF

${NODE_ID} is ready:
  Node prefix       : ${NODE_PREFIX}
  Node Tailscale IP: ${NODE_TS_IP}
  Mac hub          : ${HUB_TS_IP}:${NDN_PORT}
  Parent face      : ${FACE_URI}
  Checkpoint       : ${RUNTIME_DIR}

On the Mac hub, find the long-lived incoming face whose remote address starts
with ${NODE_TS_IP}, then bind this node prefix to that FaceId:

  ./ndnd fw face-list
  ./ndnd fw route-add prefix=${NODE_PREFIX} face=<FACE_ID>

Keep this process running until the four-node matrix is complete.
EOF
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

[[ -n "${NODE_ID}" ]] || die "NODE_ID is required; invoke a node wrapper"
[[ "${NODE_ID}" =~ ^[a-z0-9-]+$ ]] || die "Invalid NODE_ID: ${NODE_ID}"
[[ -f "${CONFIG}" ]] || die "Forwarder config not found: ${CONFIG}"
[[ -f "${BRIDGE_SCRIPT}" ]] || die "Bridge helper not found: ${BRIDGE_SCRIPT}"

init_checkpoint
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

require_command env
require_command grep
require_command tail
require_command tee
case "$(uname -s)" in
  Linux) require_command ss ;;
  Darwin) require_command lsof ;;
  *) die "This edge runner supports Linux and macOS only" ;;
esac

if [[ "${MODE}" == "serve" ]]; then
  require_command "${TAILSCALE_BIN}"
  if [[ "${TRANSPORT_MODE}" == "direct" ]]; then
    require_command nc
  else
    require_command python3
    check_port_available "bridge" "${BRIDGE_PORT}"
  fi
  prepare_transport
fi

check_port_available "local NDNd" "${NDN_PORT}"
build_binary
start_forwarder
start_pingserver

if [[ "${MODE}" == "local-test" ]]; then
  run_local_test
else
  add_parent_route
  warm_hub_face
  echo
  echo "Edge face table:"
  "${BINARY}" fw face-list
  print_ready
  follow_pingserver
fi
