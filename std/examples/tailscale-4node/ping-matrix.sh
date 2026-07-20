#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
readonly BINARY="${NDND_BINARY:-${REPO_ROOT}/ndnd}"
readonly EXPERIMENT_PREFIX="${EXPERIMENT_PREFIX:-/ndn/4node}"
readonly LOCAL_NODE_ID="${1:-${NODE_ID:-}}"
readonly PING_COUNT="${PING_COUNT:-5}"
readonly PING_TIMEOUT_MS="${PING_TIMEOUT_MS:-3000}"
readonly LOG_ROOT="${NDN_LOG_DIR:-${SCRIPT_DIR}/logs}"
readonly RUN_LOG="${LOG_ROOT}/matrix-$(date '+%Y%m%d-%H%M%S')-${LOCAL_NODE_ID:-unknown}.log"

usage() {
  cat <<'EOF'
Usage: ping-matrix.sh NODE_ID

Run five NDN pings from this Unix node to each of the other three nodes.
NODE_ID is one of: agent-a, mac, coordinator, agent-b.

Environment overrides:
  NDND_BINARY, EXPERIMENT_PREFIX, PING_COUNT, PING_TIMEOUT_MS, NDN_LOG_DIR.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

case "${LOCAL_NODE_ID}" in
  agent-a|mac|coordinator|agent-b) ;;
  -h|--help|help|"")
    usage
    [[ -n "${LOCAL_NODE_ID}" ]] && exit 0 || exit 2
    ;;
  *)
    usage >&2
    die "Unknown NODE_ID: ${LOCAL_NODE_ID}"
    ;;
esac

[[ -x "${BINARY}" ]] || die "ndnd binary is not executable: ${BINARY}"
mkdir -p "${LOG_ROOT}"
export NDN_CLIENT_TRANSPORT="tcp://127.0.0.1:6363"

failures=0
for target in agent-a mac coordinator agent-b; do
  if [[ "${target}" == "${LOCAL_NODE_ID}" ]]; then
    continue
  fi

  prefix="${EXPERIMENT_PREFIX}/${target}"
  status=0
  echo "=== ${LOCAL_NODE_ID} -> ${target} (${prefix}) ===" | tee -a "${RUN_LOG}"
  output="$("${BINARY}" ping "${prefix}" \
    -c "${PING_COUNT}" -t "${PING_TIMEOUT_MS}" 2>&1)" || status=$?
  printf '%s\n' "${output}" | tee -a "${RUN_LOG}"

  received="$(grep -Fc "content from ${prefix}:" <<<"${output}" || true)"
  if ((status != 0)) || [[ "${received}" != "${PING_COUNT}" ]]; then
    echo "FAILED: ${target} returned ${received}/${PING_COUNT} Data packets" | \
      tee -a "${RUN_LOG}" >&2
    failures=$((failures + 1))
  else
    echo "PASSED: ${target} returned ${received}/${PING_COUNT} Data packets" | \
      tee -a "${RUN_LOG}"
  fi
  echo | tee -a "${RUN_LOG}"
done

echo "Matrix log: ${RUN_LOG}"
if ((failures > 0)); then
  die "${failures} remote node tests failed"
fi
echo "All three remote node tests passed."
