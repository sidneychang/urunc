#!/usr/bin/env bash
# Destructive storage reset for containerd snapshotters.
#
# --full
#   Stops containerd, moves aside ALL of /var/lib/containerd, recreates an empty tree
#   and a fresh blockfile scratch tree. Clears devmapper, blockfile, and every other
#   snapshotter on disk together (same granularity for both backends).
#
# --blockfile-data-only
#   Blockfile only: replaces blockfile root_path on disk; containerd Bolt metadata may
#   still reference old snapshots — use --full if pull still fails.
#
# Devmapper-only on-disk reset (thin pool / root_path) is not scripted here: after
# --full, recreate the pool if needed with script/dm_create.sh per docs/installation.md.
#
# Use when nerdctl pull --snapshotter blockfile fails with:
#   unable to prepare extraction snapshot: target snapshot "sha256:...": already exists
# and flush_namespace_snapshots.sh cannot remove snapshots (Bolt vs disk mismatch).
#
# Usage:
#   sudo ./reset_snapshotter_storage.sh --full
#   RESET_SNAPSHOTTER_CONFIRM=yes sudo ./reset_snapshotter_storage.sh --full
#   sudo ./reset_snapshotter_storage.sh --blockfile-data-only
#
# Env:
#   CONTAINERD_CONFIG=/etc/containerd/config.toml
#   SCRATCH_MB=500
#   RESET_SNAPSHOTTER_CONFIRM=yes | RESET_BLOCKFILE_CONFIRM=yes   (skip typing YES)

set -euo pipefail

CONFIG="${CONTAINERD_CONFIG:-/etc/containerd/config.toml}"
SCRATCH_MB="${SCRATCH_MB:-500}"
DEFAULT_ROOT="/var/lib/containerd/io.containerd.snapshotter.v1.blockfile"

usage() {
  sed -n '1,35p' "$0" | sed -n '/^# /s/^# //p'
  echo "Options: --full | --blockfile-data-only | -h | --help"
  exit "${1:-0}"
}

MODE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --full) MODE=full ;;
    --blockfile-data-only) MODE=blockfile_only ;;
    -h|--help) usage 0 ;;
    *) echo "unknown arg: $1" >&2; usage 1 ;;
  esac
  shift
done

[ -n "$MODE" ] || { echo "specify --full or --blockfile-data-only" >&2; usage 1; }

if [ "$(id -u)" -ne 0 ]; then
  echo "run as root (sudo)" >&2
  exit 1
fi

toml_get_in_blockfile() {
  local key="$1"
  [ -f "$CONFIG" ] || return 1
  awk -v key="$key" '
    /^\[plugins/ {
      if ($0 ~ /blockfile/) inbf = 1
      else if (inbf) exit 0
      next
    }
    inbf && $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      sub("^[[:space:]]*" key "[[:space:]]*=[[:space:]]*", "")
      gsub(/#.*/, "")
      gsub(/^[[:space:]]+|[[:space:]]+$/, "")
      gsub(/^["\047]|["\047]$/, "")
      print
      exit 0
    }
  ' "$CONFIG"
}

FS_TYPE="$(toml_get_in_blockfile fs_type || true)"
FS_TYPE="${FS_TYPE:-ext4}"
ROOT_PATH="$(toml_get_in_blockfile root_path || true)"
ROOT_PATH="${ROOT_PATH:-$DEFAULT_ROOT}"
SCRATCH_FILE="$(toml_get_in_blockfile scratch_file || true)"
SCRATCH_FILE="${SCRATCH_FILE:-$ROOT_PATH/scratch}"

echo "== reset_snapshotter_storage =="
echo "config=$CONFIG"
echo "mode=$MODE"
echo "root_path=$ROOT_PATH"
echo "scratch_file=$SCRATCH_FILE"
echo "fs_type=$FS_TYPE scratch_mb=$SCRATCH_MB"

confirm() {
  if [ "${RESET_SNAPSHOTTER_CONFIRM:-}" = "yes" ] || [ "${RESET_BLOCKFILE_CONFIRM:-}" = "yes" ]; then
    return 0
  fi
  read -r -p "Type YES to continue: " ans
  [ "$ans" = "YES" ]
}

recreate_scratch() {
  mkdir -p "$ROOT_PATH"
  mkdir -p "$(dirname "$SCRATCH_FILE")"
  rm -f "$SCRATCH_FILE"
  dd if=/dev/zero of="$SCRATCH_FILE" bs=1M count="$SCRATCH_MB" status=none
  case "$FS_TYPE" in
    ext4) mkfs.ext4 -F "$SCRATCH_FILE" >/dev/null ;;
    *)
      echo "unsupported fs_type=$FS_TYPE (only ext4 automated); format $SCRATCH_FILE yourself" >&2
      exit 1
      ;;
  esac
  chown -R root:root "$ROOT_PATH"
}

if [ "$MODE" = "full" ]; then
  echo "This moves aside ALL of /var/lib/containerd (images, containers, all snapshotters)."
  confirm || { echo "aborted" >&2; exit 1; }
  systemctl stop containerd
  ts="$(date +%s)"
  if [ -d /var/lib/containerd ]; then
    mv /var/lib/containerd "/var/lib/containerd.bak.${ts}"
    echo "previous data -> /var/lib/containerd.bak.${ts}"
  fi
  mkdir -p /var/lib/containerd
  recreate_scratch
  systemctl start containerd
  echo "done. Re-pull images for devmapper and/or blockfile as needed."
  echo "  e.g. nerdctl -n default pull --snapshotter devmapper <image>"
  echo "       nerdctl -n default pull --snapshotter blockfile <image>"
  echo "If you use devmapper thin pool files under the old tree, recreate with script/dm_create.sh"
  exit 0
fi

# blockfile-data-only
echo "WARNING: containerd Bolt metadata may still list old snapshots; use --full if pull still fails."
confirm || { echo "aborted" >&2; exit 1; }
systemctl stop containerd
ts="$(date +%s)"
if [ -d "$ROOT_PATH" ]; then
  mv "$ROOT_PATH" "${ROOT_PATH}.bak.${ts}"
  echo "previous blockfile root -> ${ROOT_PATH}.bak.${ts}"
fi
recreate_scratch
systemctl start containerd
echo "done."
