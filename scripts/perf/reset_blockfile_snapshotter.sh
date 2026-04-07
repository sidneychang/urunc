#!/usr/bin/env bash
# Legacy entry: use reset_snapshotter_storage.sh (--full | --blockfile-data-only).
set -euo pipefail
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/reset_snapshotter_storage.sh" "$@"
