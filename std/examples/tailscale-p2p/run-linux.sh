#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly BINARY="${NDND_BINARY:-${REPO_ROOT}/ndnd}"
readonly CONFIG="${NDND_CONFIG:-${SCRIPT_DIR}/fw.yml}"
readonly BRIDGE_SCRIPT="${SCRIPT_DIR}/tailscale-nc-bridge.py"
readonly PREFIX="${NDN_PING_PREFIX:-/p2p/mac}"
readonly MAC_TS_HOST="${MAC_TS_HOST:-castermacbook}"
readonly MAC_NDN_PORT="${MAC_NDN_PORT:-6363}"
readonly LOCAL_NDN_PORT="${LOCAL_NDN_PORT:-6363}"
readonly BRIDGE_HOST="${BRIDGE_HOST:-127.0.0.1}"
readonly BRIDGE_PORT="${BRIDGE_PORT:-16363}"
readonly PING_COUNT="${PING_COUNT:-5}"
readonly PING_TIMEOUT_MS="${PING_TIMEOUT_MS:-2000}"

readonly TAILSCALE_BIN="${TAILSCALE_BIN:-/usr/bin/tailscale}"
readonly TAILSCALE_SOCKET="${TAILSCALE_SOCKET:-/srv/yitong/ioa/.ioa_runtime/agent_a/tailscale/run/tailscaled.sock}"
readonly TAILSCALE_PID_FILE="${TAILSCALE_PID_FILE:-/srv/yitong/ioa/.ioa_runtime/agent_a/tailscale/run/tailscaled.pid}"
readonly TAILSCALE_START_SCRIPT="${TAILSCALE_START_SCRIPT:-/srv/yitong/ioa/scripts/agent_a_tailscale_nc/start_tailscaled.sh}"
readonly TAILSCALE_STOP_SCRIPT="${TAILSCALE_STOP_SCRIPT:-/srv/yitong/ioa/scripts/agent_a_tailscale_nc/stop_tailscaled.sh}"

readonly MODE="${1:-test}"

FORWARDER_PID=""
BRIDGE_PID=""
TAILSCALE_STARTED=0

usage() {
  cat <<'EOF'
Usage: run-linux.sh [test]

  test  Start the isolated Tailscale path and local NDNd forwarder, send five
        NDN pings to the Mac, then clean up (default).

Environment overrides:
  MAC_TS_HOST       Mac Tailscale name or 100.x address (default: castermacbook).
  MAC_NDN_PORT      Mac NDNd TCP port (default: 6363).
  NDN_PING_PREFIX   Remote ping prefix (default: /p2p/mac).
  BRIDGE_PORT       Local tailscale-nc bridge port (default: 16363).
  NDND_BINARY       ndnd binary path (default: repository root/ndnd).
  SKIP_BUILD=1      Reuse NDND_BINARY instead of building it.
  GO_BIN            Go executable used for a build.
  KEEP_TAILSCALE=1  Keep isolated Tailscale running if this script started it.

Advanced overrides:
  TAILSCALE_BIN, TAILSCALE_SOCKET, TAILSCALE_PID_FILE,
  TAILSCALE_START_SCRIPT, TAILSCALE_STOP_SCRIPT, BRIDGE_HOST,
  PING_COUNT, PING_TIMEOUT_MS. Override NDND_CONFIG and LOCAL_NDN_PORT
  together when using a forwarder configuration with a different TCP port.
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

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM HUP

  stop_child "${FORWARDER_PID}" "Linux forwarder"
  stop_child "${BRIDGE_PID}" "Tailscale TCP bridge"

  if [[ "${TAILSCALE_STARTED}" == "1" ]]; then
    if [[ "${KEEP_TAILSCALE:-0}" == "1" ]]; then
      echo "Keeping the isolated Tailscale daemon running (KEEP_TAILSCALE=1)."
    elif [[ -x "${TAILSCALE_STOP_SCRIPT}" ]]; then
      echo "Stopping the isolated Agent A Tailscale daemon..."
      "${TAILSCALE_STOP_SCRIPT}" || \
        echo "WARNING: failed to stop isolated Tailscale cleanly" >&2
    fi
  fi

  case "${exit_code}" in
    0|129|130|143) ;;
    *)
      show_log_tail "forwarder" "${FORWARDER_LOG}"
      show_log_tail "bridge" "${BRIDGE_LOG}"
      show_log_tail "ping" "${PING_LOG}"
      ;;
  esac

  echo "Runtime logs: ${RUNTIME_DIR}"
  exit "${exit_code}"
}

tailscale_is_running() {
  local status_json

  [[ -S "${TAILSCALE_SOCKET}" ]] || return 1
  status_json="$("${TAILSCALE_BIN}" --socket="${TAILSCALE_SOCKET}" status --json 2>/dev/null)" || return 1
  grep -Eq '"BackendState"[[:space:]]*:[[:space:]]*"Running"' \
    <<<"${status_json}"
}

private_tailscale_pid_is_running() {
  local pid=""

  [[ -s "${TAILSCALE_PID_FILE}" ]] || return 1
  read -r pid <"${TAILSCALE_PID_FILE}" || return 1
  pid_is_running "${pid}"
}

start_tailscale() {
  if tailscale_is_running; then
    echo "Reusing the already-running isolated Agent A Tailscale daemon."
    return
  fi

  [[ -x "${TAILSCALE_START_SCRIPT}" ]] || \
    die "Tailscale start script is not executable: ${TAILSCALE_START_SCRIPT}"

  if ! private_tailscale_pid_is_running; then
    TAILSCALE_STARTED=1
  fi

  echo "Starting only the isolated Agent A userspace Tailscale daemon..."
  "${TAILSCALE_START_SCRIPT}"
  tailscale_is_running || \
    die "Isolated Tailscale did not reach BackendState=Running"
}

check_mac_reachable() {
  local ping_output

  echo "Checking ${MAC_TS_HOST} through the isolated Tailscale socket..."
  if ! ping_output="$("${TAILSCALE_BIN}" --socket="${TAILSCALE_SOCKET}" \
      ping --c=1 --timeout=5s "${MAC_TS_HOST}" 2>&1)"; then
    printf '%s\n' "${ping_output}" >&2
    die "Mac Tailscale peer is not reachable: ${MAC_TS_HOST}"
  fi
  printf '%s\n' "${ping_output}"
}

check_mac_ndnd_listener() {
  local probe_output

  echo "Checking the Mac NDNd listener at ${MAC_TS_HOST}:${MAC_NDN_PORT}..."
  if ! probe_output="$(python3 "${BRIDGE_SCRIPT}" \
      --probe \
      --target-host "${MAC_TS_HOST}" \
      --target-port "${MAC_NDN_PORT}" \
      --tailscale-socket "${TAILSCALE_SOCKET}" \
      --tailscale-bin "${TAILSCALE_BIN}" 2>&1)"; then
    printf '%s\n' "${probe_output}" >&2
    die "Mac is online, but its NDNd TCP listener is unavailable; keep run-macos.sh serve active"
  fi
  printf '%s\n' "${probe_output}"
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
  if [[ "${go_bin}" == */* ]]; then
    [[ -x "${go_bin}" ]] || die "Go executable is not usable: ${go_bin}"
  else
    require_command "${go_bin}"
  fi

  go_root="$(env -u GOROOT "${go_bin}" env GOROOT)" || \
    die "Unable to determine GOROOT for ${go_bin}"
  [[ -d "${go_root}" ]] || die "Go reported an invalid GOROOT: ${go_root}"

  go_cache="${GOCACHE:-${TMPDIR:-/tmp}/ndnd-go-build}"
  mkdir -p "${go_cache}"
  echo "Building Linux ndnd with $(env -u GOROOT "${go_bin}" version)..."
  (
    cd "${REPO_ROOT}"
    GOROOT="${go_root}" GOCACHE="${go_cache}" CGO_ENABLED=0 \
      "${go_bin}" build -o "${BINARY}" ./cmd/ndnd
  )
}

check_port_available() {
  local port="$1"
  local label="$2"
  local listeners

  listeners="$(ss -H -ltn "sport = :${port}" 2>/dev/null || true)"
  if [[ -n "${listeners}" ]]; then
    printf '%s\n' "${listeners}" >&2
    die "${label} TCP port ${port} is already in use"
  fi
}

wait_for_bridge() {
  local attempt

  for ((attempt = 1; attempt <= 50; attempt++)); do
    if ! pid_is_running "${BRIDGE_PID}"; then
      die "The Tailscale TCP bridge exited during startup"
    fi
    if grep -Fq '[bridge] listening on' "${BRIDGE_LOG}" 2>/dev/null; then
      return
    fi
    sleep 0.1
  done

  die "Timed out waiting for the Tailscale TCP bridge"
}

start_bridge() {
  echo "Starting ${BRIDGE_HOST}:${BRIDGE_PORT} -> ${MAC_TS_HOST}:${MAC_NDN_PORT} via tailscale nc..."
  python3 -u "${BRIDGE_SCRIPT}" \
    --listen-host "${BRIDGE_HOST}" \
    --listen-port "${BRIDGE_PORT}" \
    --target-host "${MAC_TS_HOST}" \
    --target-port "${MAC_NDN_PORT}" \
    --tailscale-socket "${TAILSCALE_SOCKET}" \
    --tailscale-bin "${TAILSCALE_BIN}" \
    >"${BRIDGE_LOG}" 2>&1 &
  BRIDGE_PID=$!
  wait_for_bridge
  cat "${BRIDGE_LOG}"
}

wait_for_forwarder() {
  local attempt
  local status_output

  for ((attempt = 1; attempt <= 50; attempt++)); do
    if ! pid_is_running "${FORWARDER_PID}"; then
      die "The Linux forwarder exited during startup"
    fi
    if status_output="$("${BINARY}" fw status 2>/dev/null)"; then
      printf '%s\n' "${status_output}"
      return
    fi
    sleep 0.1
  done

  die "Timed out waiting for the Linux forwarder on TCP ${LOCAL_NDN_PORT}"
}

start_forwarder() {
  export NDN_CLIENT_TRANSPORT="tcp://127.0.0.1:${LOCAL_NDN_PORT}"

  echo "Starting the Linux NDNd forwarder..."
  "${BINARY}" fw run "${CONFIG}" >"${FORWARDER_LOG}" 2>&1 &
  FORWARDER_PID=$!
  wait_for_forwarder
}

run_ping_test() {
  local faces
  local ping_output
  local ping_status=0
  local received_count
  local routes

  echo "Adding ${PREFIX} through the local userspace-Tailscale bridge..."
  "${BINARY}" fw route-add \
    "prefix=${PREFIX}" \
    persistency=permanent \
    "face=tcp4://${BRIDGE_HOST}:${BRIDGE_PORT}"

  echo
  echo "Linux face list:"
  faces="$("${BINARY}" fw face-list)"
  printf '%s\n' "${faces}"

  echo
  echo "Linux route list:"
  routes="$("${BINARY}" fw route-list)"
  printf '%s\n' "${routes}"
  grep -Fq "prefix=${PREFIX}" <<<"${routes}" || \
    die "The expected route was not installed: ${PREFIX}"

  echo
  echo "Sending ${PING_COUNT} NDN pings to ${PREFIX}..."
  ping_output="$("${BINARY}" ping "${PREFIX}" \
    -c "${PING_COUNT}" -t "${PING_TIMEOUT_MS}" 2>&1)" || ping_status=$?
  printf '%s\n' "${ping_output}"
  printf '%s\n' "${ping_output}" >"${PING_LOG}"

  if ((ping_status != 0)); then
    die "ndnd ping exited with status ${ping_status}"
  fi

  received_count="$(grep -Fc "content from ${PREFIX}:" \
    <<<"${ping_output}" || true)"
  if [[ "${received_count}" != "${PING_COUNT}" ]]; then
    die "NDN ping received ${received_count}/${PING_COUNT} Data packets"
  fi
  echo "P2P NDN ping test passed."
}

case "${MODE}" in
  test) ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

readonly RUNTIME_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ndnd-p2p-linux.XXXXXX")"
readonly FORWARDER_LOG="${RUNTIME_DIR}/forwarder.log"
readonly BRIDGE_LOG="${RUNTIME_DIR}/bridge.log"
readonly PING_LOG="${RUNTIME_DIR}/ping.log"

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

[[ "$(uname -s)" == "Linux" ]] || die "This runner is for Linux"
[[ -f "${CONFIG}" ]] || die "Forwarder config not found: ${CONFIG}"
[[ -f "${BRIDGE_SCRIPT}" ]] || die "Bridge helper not found: ${BRIDGE_SCRIPT}"
require_command python3
require_command env
require_command grep
require_command ss
require_command "${TAILSCALE_BIN}"

check_port_available "${LOCAL_NDN_PORT}" "Local NDNd"
check_port_available "${BRIDGE_PORT}" "Bridge"
start_tailscale
check_mac_reachable
check_mac_ndnd_listener
build_binary
start_bridge
start_forwarder
run_ping_test
