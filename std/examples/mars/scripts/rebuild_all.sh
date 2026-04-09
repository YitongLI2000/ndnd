#!/usr/bin/env bash

set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NDND_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
NDND_INSTALL_PATH="/usr/local/bin/ndnd"

fail() {
  local step="$1"
  local code="${2:-1}"
  echo "[ERROR] ${step} failed (exit=${code})"
  exit "${code}"
}

run_step() {
  local step="$1"
  shift
  echo "[INFO] ${step}"
  "$@"
  local code=$?
  if [[ ${code} -ne 0 ]]; then
    fail "${step}" "${code}"
  fi
}

echo "[INFO] Rebuild script starting"
echo "[INFO] NDND_ROOT=${NDND_ROOT}"
echo "[INFO] EXAMPLE_ROOT=${EXAMPLE_ROOT}"
echo "[INFO] GOCACHE=${GOCACHE:-<go-default>}"

run_step "Check go toolchain" go version

# Build core ndnd (includes fw/dv code paths).
run_step "Build core ndnd binary" bash -lc "cd '${NDND_ROOT}' && make ndnd"

# Install core ndnd globally so runtime scripts that call `ndnd` use the rebuilt binary.
if [[ -w "$(dirname "${NDND_INSTALL_PATH}")" ]]; then
  run_step "Install core ndnd binary globally" install -m 755 "${NDND_ROOT}/ndnd" "${NDND_INSTALL_PATH}"
else
  run_step "Install core ndnd binary globally (sudo)" sudo install -m 755 "${NDND_ROOT}/ndnd" "${NDND_INSTALL_PATH}"
fi

# Build applications in this example module.
run_step "Build producer app" bash -lc "cd '${EXAMPLE_ROOT}' && go build -o apps/producer/producer ./apps/producer"
run_step "Build consumer app" bash -lc "cd '${EXAMPLE_ROOT}' && go build -o apps/consumer/consumer ./apps/consumer"

echo "[INFO] Rebuild succeeded"
echo "[INFO] Artifacts:"
echo "  - ${NDND_ROOT}/ndnd"
echo "  - ${NDND_INSTALL_PATH}"
echo "  - ${EXAMPLE_ROOT}/apps/producer/producer"
echo "  - ${EXAMPLE_ROOT}/apps/consumer/consumer"
