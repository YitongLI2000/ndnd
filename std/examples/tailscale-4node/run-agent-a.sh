#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export NODE_ID="${NODE_ID:-agent-a}"
export NODE_TS_IP="${NODE_TS_IP:-100.81.98.57}"
export TRANSPORT_MODE="tailscale-nc"

exec "${SCRIPT_DIR}/run-edge.sh" "$@"
