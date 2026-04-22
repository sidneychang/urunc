#!/usr/bin/env bash
set -euo pipefail

# Recover devmapper busy snapshots that keep containerd cleanup stuck.
# Default behavior is conservative: target only containerd-pool-snap-* devices.
#
# Usage:
#   recover_devmapper_busy_snapshot.sh
#   recover_devmapper_busy_snapshot.sh --device containerd-pool-snap-9742
#   recover_devmapper_busy_snapshot.sh --ns default --ns n200bench
#   DRY_RUN=1 recover_devmapper_busy_snapshot.sh
#
# Env:
#   DRY_RUN=1                 print actions only
#   NS_LIST="default n200bench"   namespaces to cleanup
#   DEVICE_REGEX="^containerd-pool-snap-[0-9]+$"
#   RETRIES=3
#   RETRY_SLEEP_SEC=2

DRY_RUN="${DRY_RUN:-0}"
RETRIES="${RETRIES:-3}"
RETRY_SLEEP_SEC="${RETRY_SLEEP_SEC:-2}"
DEVICE_REGEX="${DEVICE_REGEX:-^containerd-pool-snap-[0-9]+$}"
NS_LIST="${NS_LIST:-default n200bench}"
TARGET_DEVICE="${TARGET_DEVICE:-}"

log() { printf '[recover] %s\n' "$*"; }

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '[dry-run] %s\n' "$*"
    return 0
  fi
  eval "$@"
}

usage() {
  cat <<'EOF'
Usage:
  recover_devmapper_busy_snapshot.sh [--device <dm_name>] [--ns <namespace>]...

Options:
  --device <dm_name>   Recover one specific dm device name (e.g. containerd-pool-snap-9742)
  --ns <namespace>     Add namespace to cleanup list (default: "default n200bench")
  -h, --help           Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --device)
      [[ $# -ge 2 ]] || { echo "--device requires value" >&2; exit 1; }
      TARGET_DEVICE="$2"
      shift 2
      ;;
    --ns)
      [[ $# -ge 2 ]] || { echo "--ns requires value" >&2; exit 1; }
      NS_LIST="${NS_LIST} $2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! command -v dmsetup >/dev/null 2>&1; then
  echo "dmsetup not found" >&2
  exit 1
fi

list_target_devices() {
  if [[ -n "$TARGET_DEVICE" ]]; then
    printf '%s\n' "$TARGET_DEVICE"
    return 0
  fi
  sudo dmsetup ls --target thin 2>/dev/null | awk -v re="$DEVICE_REGEX" '$1 ~ re {print $1}'
}

kill_runtime_processes() {
  log "killing urunc shims/qemu processes"
  run "sudo pkill -9 -f 'containerd-shim-urunc-v2 -namespace' >/dev/null 2>&1 || true"
  run "sudo pkill -9 -f 'qemu-system' >/dev/null 2>&1 || true"
}

cleanup_namespaces() {
  local ns
  for ns in $NS_LIST; do
    log "cleanup namespace: $ns"
    run "sudo nerdctl -n '$ns' rm -f \$(sudo nerdctl -n '$ns' ps -a -q 2>/dev/null || true) >/dev/null 2>&1 || true"
    run "ids=\$(sudo ctr -n '$ns' c ls -q 2>/dev/null || true); \
      for id in \$ids; do \
        sudo ctr -n '$ns' tasks kill -s SIGKILL \"\$id\" >/dev/null 2>&1 || true; \
        sudo ctr -n '$ns' tasks delete -f \"\$id\" >/dev/null 2>&1 || true; \
        sudo ctr -n '$ns' c delete \"\$id\" >/dev/null 2>&1 || true; \
      done"
  done
}

remove_device_with_retry() {
  local dev="$1" i
  log "removing dm device: $dev"
  for ((i=1; i<=RETRIES; i++)); do
    if run "sudo dmsetup remove --retry --force '$dev' >/dev/null 2>&1"; then
      log "removed: $dev"
      return 0
    fi
    log "remove failed (attempt $i/$RETRIES): $dev"
    [[ "$i" -lt "$RETRIES" ]] && sleep "$RETRY_SLEEP_SEC"
  done
  log "remove still failed: $dev"
  return 1
}

verify_state() {
  log "containerd active: $(systemctl is-active containerd 2>/dev/null || true)"
  log "remaining target devices:"
  sudo dmsetup ls 2>/dev/null | awk -v re="$DEVICE_REGEX" '$1 ~ re {print "  "$1}' || true
  local ns
  for ns in $NS_LIST; do
    local c t
    c="$(sudo nerdctl -n "$ns" ps -a -q 2>/dev/null | wc -l | tr -d ' ')"
    t="$(sudo ctr -n "$ns" tasks ls 2>/dev/null | awk 'NR>1 {n++} END {print n+0}')"
    log "ns=$ns containers=$c tasks=$t"
  done
}

main() {
  log "starting recovery (dry_run=$DRY_RUN)"
  kill_runtime_processes
  cleanup_namespaces

  local dev failed=0
  while read -r dev; do
    [[ -z "$dev" ]] && continue
    if ! remove_device_with_retry "$dev"; then
      failed=1
    fi
  done < <(list_target_devices)

  # Restart containerd to refresh snapshotter state after force cleanup.
  log "restarting containerd"
  run "sudo systemctl restart containerd"
  sleep 2

  verify_state

  if [[ "$failed" -ne 0 ]]; then
    log "recovery finished with unresolved devices"
    exit 1
  fi
  log "recovery finished"
}

main "$@"

