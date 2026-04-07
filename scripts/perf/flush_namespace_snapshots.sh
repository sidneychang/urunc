#!/usr/bin/env bash
# Remove all snapshots for a snapshotter in one containerd namespace (devmapper | blockfile | …).
# Same algorithm for any snapshotter: leaf keys first until empty.
#
# Use when nerdctl reports: target snapshot "sha256:...": already exists
# or: ctr rm ... "cannot remove snapshot with child: failed precondition"
#
# Removal order: repeatedly delete *leaf* snapshots (keys that never appear
# as PARENT in another row), until none remain.
#
# Usage:
#   sudo ./flush_namespace_snapshots.sh
#   NS=my-ns SNAPSHOTTER=blockfile sudo ./flush_namespace_snapshots.sh
#   SNAPSHOTTER=devmapper sudo ./flush_namespace_snapshots.sh
#
# Env:
#   NS            containerd namespace (default: urunc-bench, 与 reset_urunc_bench_state 一致)
#   SNAPSHOTTER   devmapper | blockfile | … (default: devmapper)
#
# 兼容旧名: flush_blockfile_snapshots.sh（默认 NS=default、SNAPSHOTTER=blockfile）

set -euo pipefail

_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_bench_common.sh
source "$_LIB_DIR/lib_bench_common.sh"

NS="${NS:-urunc-bench}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"

echo "== flush snapshotter=$SNAPSHOTTER namespace=$NS =="

while read -r id; do
  [ -z "$id" ] && continue
  bench_sudo nerdctl -n "$NS" rm -f "$id" >/dev/null 2>&1 || true
done < <(bench_sudo nerdctl -n "$NS" ps -aq 2>/dev/null || true)

# Repeatedly remove leaf snapshots (not listed as any row's PARENT).
round=0
while true; do
  round=$((round + 1))
  n="$(sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null | awk 'NR>1' | wc -l | tr -d '[:space:]')"
  [ "${n:-0}" -eq 0 ] && break
  leaves="$(
    sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null | awk '
      NR == 1 { next }
      {
        key[$1] = 1
        if ($2 != "") has_child[$2] = 1
      }
      END {
        for (k in key)
          if (!(k in has_child))
            print k
      }
    '
  )"
  if [ -z "$leaves" ]; then
    echo "no leaves found but $n snapshots remain (cycle or unexpected); stopping" >&2
    sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null || true
    exit 1
  fi
  removed_any=0
  while read -r k; do
    [ -z "$k" ] && continue
    if sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" rm "$k" >/dev/null 2>&1; then
      removed_any=1
    fi
  done <<<"$leaves"
  [ "$removed_any" -eq 0 ] && break
  [ "$round" -gt 500 ] && echo "too many rounds; abort" >&2 && exit 1
done

remaining="$(sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null | awk 'NR>1' | wc -l)"
echo "remaining_snapshots=$remaining"
echo "Next: nerdctl -n $NS pull --snapshotter $SNAPSHOTTER <image>"
