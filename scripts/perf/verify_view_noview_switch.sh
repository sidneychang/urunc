#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RESET_SCRIPT="${SCRIPT_DIR}/reset_urunc_bench_state.sh"

NS="${NS:-default}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
RUNTIME="${RUNTIME:-io.containerd.urunc.v2}"
IMAGE="${IMAGE:-harbor.nbfc.io/nubificus/urunc/nginx-qemu-linux-raw:latest}"
RUNS="${RUNS:-10}"
NAME_PREFIX="${NAME_PREFIX:-storage-check}"
THIN_POOL="${THIN_POOL:-}"

usage() {
  cat <<'EOF'
Usage:
  verify_view_noview_switch.sh sample-one <view|noview>
  verify_view_noview_switch.sh ab
  verify_view_noview_switch.sh switch-view
  verify_view_noview_switch.sh switch-noview

Environment:
  NS=default
  SNAPSHOTTER=devmapper
  RUNTIME=io.containerd.urunc.v2
  IMAGE=harbor.nbfc.io/nubificus/urunc/nginx-qemu-linux-raw:latest
  RUNS=10
  NAME_PREFIX=storage-check
  THIN_POOL=<dmsetup thin-pool name, optional>

What the script measures:
  1. Host-side /run tmpfs usage and /proc/meminfo Shmem
  2. Thin-pool status from dmsetup/lvs when THIN_POOL is provided
  3. Bundle/rootfs/monRootfs view from inside the urunc container mount namespace
  4. monRootfs storage: du with -x vs without (see below)

Storage proof (why du -x matters):
  Default du follows bind mounts and counts the unikernel/initrd bytes even when
  those files live on the snapshot view (different fs). That can hide the benefit.
  du --one-file-system (-x) only counts bytes stored on the same fs as monRootfs
  (usually tmpfs under /run). noview: large copies live on tmpfs → -x is large.
  view: payloads are bind-mounted from the view → -x stays small; du without -x
  is still large (shows the files are visible, not that tmpfs held a full copy).

Mode summary:
  sample-one <view|noview>
    Switch binaries, clean state, start one container, and dump the storage-
    relevant view from both host and the container mount namespace.

  ab
    Run no-view first and view second. For each mode it:
    - switches binaries
    - resets state
    - samples host metrics before
    - starts RUNS containers
    - samples host metrics after
    - samples one live container from inside its mount namespace
    - cleans up
EOF
}

log() {
  printf '[verify] %s\n' "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_tools() {
  require_cmd sudo
  require_cmd nerdctl
  require_cmd jq
  require_cmd nsenter
  require_cmd awk
  require_cmd grep
  require_cmd df
  require_cmd du
  require_cmd findmnt
}

switch_view() {
  log "switching binaries to VIEW via make install"
  (
    cd "$REPO_ROOT"
    sudo make install
  )
}

switch_noview() {
  log "switching binaries to NO-VIEW via /usr/local/bin/bak"
  sudo cp /usr/local/bin/bak/* /usr/local/bin/
}

reset_env() {
  log "resetting namespace state: NS=$NS SNAPSHOTTER=$SNAPSHOTTER"
  NS="$NS" SNAPSHOTTER="$SNAPSHOTTER" "$RESET_SCRIPT"
}

ensure_image() {
  if ! sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" image inspect "$IMAGE" >/dev/null 2>&1; then
    log "image missing locally, pulling $IMAGE"
    sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" pull "$IMAGE" >/dev/null
  fi
}

full_id_for() {
  local cid="$1"
  sudo nerdctl -n "$NS" inspect -f '{{.ID}}' "$cid"
}

bundle_path_for() {
  local cid="$1"
  printf '/run/containerd/io.containerd.runtime.v2.task/%s/%s' "$NS" "$cid"
}

run_used_bytes() {
  df -B1 /run | awk 'NR==2 {print $3}'
}

# Bytes on the same filesystem as monRootfs only (excludes bind-mounted subtrees
# that live on e.g. devmapper snapshot view). Proves tmpfs/run footprint.
monrootfs_du_x_bytes() {
  local mon="$1"
  [[ -d "$mon" ]] || {
    echo 0
    return 0
  }
  sudo du -sx --block-size=1 "$mon" 2>/dev/null | awk '{print $1}'
}

# Bytes if du descends into bind mounts (not a tmpfs-only metric).
monrootfs_du_all_bytes() {
  local mon="$1"
  [[ -d "$mon" ]] || {
    echo 0
    return 0
  }
  sudo du -s --block-size=1 "$mon" 2>/dev/null | awk '{print $1}'
}

# Sum monRootfs du -x across RUNS containers named ${name_prefix_mode}-<i>.
sum_monrootfs_du_x_for_run() {
  local name_prefix_mode="$1"
  local total=0
  local i short full bundle mon b

  for i in $(seq 1 "$RUNS"); do
    short="$(sudo nerdctl -n "$NS" ps -q --filter "name=${name_prefix_mode}-${i}" | head -1)"
    [[ -z "$short" ]] && continue
    full="$(full_id_for "$short")"
    bundle="$(bundle_path_for "$full")"
    mon="${bundle}/monRootfs"
    b="$(monrootfs_du_x_bytes "$mon")"
    total=$((total + ${b:-0}))
  done
  echo "$total"
}

meminfo_pair() {
  awk '/MemAvailable:|Shmem:/' /proc/meminfo
}

thinpool_sample() {
  if [[ -z "$THIN_POOL" ]]; then
    log "thin-pool sampling skipped: THIN_POOL is not set"
    return 0
  fi

  log "thin-pool sample: dmsetup status $THIN_POOL"
  sudo dmsetup status "$THIN_POOL" || true

  log "thin-pool sample: lvs"
  sudo lvs -o lv_name,data_percent,metadata_percent --noheadings 2>/dev/null || true
}

host_sample() {
  local tag="$1"

  log "$tag host df /run"
  df -B1 /run

  log "$tag host meminfo"
  meminfo_pair

  thinpool_sample
}

namespace_sample() {
  local cid="$1"
  local pid bundle cfg view proc_root_bundle proc_root_mon proc_root_rootfs

  pid="$(sudo nerdctl -n "$NS" inspect -f '{{.State.Pid}}' "$cid")"
  bundle="$(bundle_path_for "$cid")"
  cfg="${bundle}/config.json"
  view="$(sudo jq -r '.annotations["com.urunc.snapshot.view.mount_path"] // empty' "$cfg")"
  proc_root_bundle="/proc/${pid}/root${bundle}"
  proc_root_mon="/proc/${pid}/root${bundle}/monRootfs"
  proc_root_rootfs="/proc/${pid}/root${bundle}/rootfs"

  log "namespace sample: cid=$cid pid=$pid"
  log "namespace sample: task mnt ns $(readlink "/proc/${pid}/ns/mnt")"
  log "namespace sample: bundle=$bundle"
  if [[ -n "$view" ]]; then
    log "namespace sample: snapshot view mount path=$view"
  else
    log "namespace sample: snapshot view mount path is empty"
  fi

  log "namespace sample: mountinfo lines related to bundle"
  sudo /bin/grep "$bundle" "/proc/${pid}/mountinfo" || true

  log "namespace sample: mountinfo lines related to monRootfs"
  sudo /bin/grep "${bundle}/monRootfs" "/proc/${pid}/mountinfo" || true

  if [[ -n "$view" ]]; then
    log "namespace sample: bind-mounted paths from snapshot view into monRootfs"
    sudo awk -v mon="${bundle}/monRootfs" -v view="$view" '
      index($0, mon) == 0 { next }
      {
        split($0, parts, " - ")
        if (length(parts) != 2) next
        n = split(parts[1], right, " ")
        if (n < 2) next
        src = right[2]
        m = split(parts[0], left, " ")
        if (m < 5) next
        dst = left[5]
        if (index(src, view) == 1 && index(dst, mon) == 1) {
          print "src=" src " dst=" dst
        }
      }
    ' "/proc/${pid}/mountinfo" || true
  fi

  log "namespace sample: proc-root path probes"
  sudo ls -ld "$proc_root_bundle" "$proc_root_mon" "$proc_root_rootfs" 2>/dev/null || true

  log "namespace sample: statfs via /proc/<pid>/root (same mount namespace view)"
  sudo stat -f -c '%n type=%T bsize=%S blocks=%b bavail=%a' \
    "$proc_root_bundle" "$proc_root_mon" "$proc_root_rootfs" 2>/dev/null || true

  log "namespace sample: du via /proc/<pid>/root (supporting signal only)"
  sudo du -sb "$proc_root_bundle" "$proc_root_mon" "$proc_root_rootfs" 2>/dev/null || true

  log "namespace sample: monRootfs du on host bundle (tmpfs-only vs follow-binds)"
  log "namespace sample:   monRootfs du_x_bytes=$(monrootfs_du_x_bytes "${bundle}/monRootfs") du_all_bytes=$(monrootfs_du_all_bytes "${bundle}/monRootfs") path=${bundle}/monRootfs"
}

cleanup_named_containers() {
  local prefix="$1"
  local ids

  ids="$(sudo nerdctl -n "$NS" ps -a --filter "name=${prefix}" -q || true)"
  if [[ -n "$ids" ]]; then
    while read -r cid; do
      [[ -z "$cid" ]] && continue
      sudo nerdctl -n "$NS" rm -f "$cid" >/dev/null 2>&1 || true
    done <<<"$ids"
  fi
}

start_one() {
  local name="$1"

  sudo nerdctl -n "$NS" run -d \
    --snapshotter "$SNAPSHOTTER" \
    --runtime "$RUNTIME" \
    --name "$name" \
    "$IMAGE" >/dev/null

  sudo nerdctl -n "$NS" ps -q --filter "name=${name}" | head -1
}

sample_one_mode() {
  local mode="$1"
  local name short_cid cid

  case "$mode" in
    noview) switch_noview ;;
    view) switch_view ;;
    *)
      echo "unsupported mode: $mode" >&2
      exit 1
      ;;
  esac

  reset_env
  ensure_image

  host_sample "before-$mode"

  name="${NAME_PREFIX}-${mode}-one"
  short_cid="$(start_one "$name")"
  cid="$(full_id_for "$short_cid")"

  host_sample "after-$mode"
  namespace_sample "$cid"

  cleanup_named_containers "$name"
  reset_env
}

measure_mode() {
  local mode="$1"
  local before_run after_run delta_run
  local name_prefix_mode sample_cid_short sample_cid

  case "$mode" in
    noview) switch_noview ;;
    view) switch_view ;;
    *)
      echo "unsupported mode: $mode" >&2
      exit 1
      ;;
  esac

  reset_env
  ensure_image

  host_sample "before-$mode"
  before_run="$(run_used_bytes)"
  name_prefix_mode="${NAME_PREFIX}-${mode}"

  log "starting $RUNS containers in mode=$mode"
  for i in $(seq 1 "$RUNS"); do
    start_one "${name_prefix_mode}-${i}" >/dev/null
  done

  after_run="$(run_used_bytes)"
  delta_run="$((after_run - before_run))"

  host_sample "after-$mode"
  sum_x="$(sum_monrootfs_du_x_for_run "$name_prefix_mode")"
  log "summary mode=$mode run_used_delta_bytes=$delta_run sum_monRootfs_du_x_bytes_${RUNS}_containers=$sum_x"

  sample_cid_short="$(sudo nerdctl -n "$NS" ps -q --filter "name=${name_prefix_mode}-1" | head -1)"
  if [[ -n "$sample_cid_short" ]]; then
    sample_cid="$(full_id_for "$sample_cid_short")"
    namespace_sample "$sample_cid"
  else
    log "namespace sample skipped: no running container found for $mode"
  fi

  cleanup_named_containers "$name_prefix_mode-"
  reset_env
}

ab_mode() {
  measure_mode noview
  measure_mode view
}

main() {
  require_tools

  case "${1:-}" in
    sample-one)
      [[ $# -eq 2 ]] || {
        usage
        exit 1
      }
      sample_one_mode "$2"
      ;;
    ab)
      [[ $# -eq 1 ]] || {
        usage
        exit 1
      }
      ab_mode
      ;;
    switch-view)
      switch_view
      ;;
    switch-noview)
      switch_noview
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      echo "unknown mode: $1" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
