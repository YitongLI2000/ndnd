#!/usr/bin/env bash

set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NDND_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

exec "${NDND_ROOT}/rebuild.sh" "$@"
