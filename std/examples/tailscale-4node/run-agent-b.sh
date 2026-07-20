#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export NODE_ID="${NODE_ID:-agent-b}"
export NODE_TS_IP="${NODE_TS_IP:-100.85.242.9}"
export TRANSPORT_MODE="${TRANSPORT_MODE:-direct}"

exec "${SCRIPT_DIR}/run-edge.sh" "$@"
