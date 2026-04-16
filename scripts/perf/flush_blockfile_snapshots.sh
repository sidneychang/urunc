#!/usr/bin/env bash
# Legacy entry: same as flush_namespace_snapshots.sh with historical defaults
# (NS=default, SNAPSHOTTER=blockfile unless already set in the environment).
set -euo pipefail
export NS="${NS:-default}"
export SNAPSHOTTER="${SNAPSHOTTER:-blockfile}"
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/flush_namespace_snapshots.sh"
