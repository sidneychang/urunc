#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-urunc-bench}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
RUNTIME_REGEX="${RUNTIME_REGEX:-io.containerd.urunc.v2}"
PRUNE_IMAGES="${PRUNE_IMAGES:-0}"
PRUNE_IMAGES_REMOVE_IMAGES="${PRUNE_IMAGES_REMOVE_IMAGES:-1}"
STRICT_RESET="${STRICT_RESET:-0}"

SUDO="${SUDO:-sudo}"
# If already running as root, don't call sudo (avoids env changes/overhead).
if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=""
fi

cleanup_shared_snapshot_views() {
  # Some workloads create a shared snapshot view under /run/urunc/shared-views.
  # If the lease/mount survives across reset rounds, devmapper cleanup may fail
  # with "device or resource busy", and subsequent nerdctl runs can hang/fail.
  if [[ "${SKIP_SHARED_VIEW_CLEANUP:-0}" == "1" ]]; then
    return 0
  fi

  if [[ -d "/run/urunc/shared-views" ]]; then
    # 1) Delete containerd leases that correspond to shared view.
    # ctr output columns: ID, CREATED AT, LABELS
    local lease_ids
    lease_ids="$(${SUDO} ctr -n "$NS" leases ls 2>/dev/null | awk '/urunc-shared-view/ {print $1}' | sort -u)"
    if [[ -n "$lease_ids" ]]; then
      echo "[reset] cleanup shared-view leases: $lease_ids"
      while read -r id; do
        [[ -z "$id" ]] && continue
        ${SUDO} ctr -n "$NS" leases delete "$id" >/dev/null 2>&1 || true
      done <<<"$lease_ids"
    fi

    # 2) Unmount all mounts under /run/urunc/shared-views.
    local mps
    mps="$(awk '$2 ~ /^\/run\/urunc\/shared-views\// {print $2}' /proc/mounts 2>/dev/null | sort -u)"
    if [[ -n "$mps" ]]; then
      echo "[reset] cleanup shared-view mounts:"
      while read -r mp; do
        [[ -z "$mp" ]] && continue
        # Try normal umount first; then lazy umount to break lingering references.
        ${SUDO} umount "$mp" >/dev/null 2>&1 || ${SUDO} umount -l "$mp" >/dev/null 2>&1 || true
      done <<<"$mps"
    fi
  fi
}

list_runtime_containers() {
  ${SUDO} ctr -n "$NS" c ls 2>/dev/null | awk -v re="$RUNTIME_REGEX" 'NR>1 && $3 ~ re {print $1}'
}

list_tasks() {
  ${SUDO} ctr -n "$NS" tasks ls 2>/dev/null | awk 'NR>1 {print $1}'
}

list_shims() {
  ps -eo pid=,args= | awk -v ns="$NS" '$0 ~ ("containerd-shim-urunc-v2 -namespace " ns) {print $1}'
}

descendants_of() {
  local roots="$*"
  local frontier="$roots"
  local seen=""

  while [ -n "$frontier" ]; do
    local children
    children="$(
      ps -eo pid=,ppid= | awk -v roots="$frontier" '
        BEGIN {
          split(roots, a, " ")
          for (i in a) {
            if (a[i] != "") {
              wanted[a[i]] = 1
            }
          }
        }
        {
          if ($2 in wanted) {
            print $1
          }
        }
      '
    )"

    frontier=""
    for pid in $children; do
      case " $seen " in
        *" $pid "*) ;;
        *)
          seen="$seen $pid"
          frontier="$frontier $pid"
          ;;
      esac
    done
  done

  echo "$seen" | xargs -n1 2>/dev/null || true
}

remove_container() {
  local id="$1"
  ${SUDO} ctr -n "$NS" tasks kill -s SIGKILL "$id" >/dev/null 2>&1 || true
  ${SUDO} ctr -n "$NS" tasks delete -f "$id" >/dev/null 2>&1 || true
  ${SUDO} nerdctl -n "$NS" rm -f "$id" >/dev/null 2>&1 || true
  ${SUDO} ctr -n "$NS" c delete "$id" >/dev/null 2>&1 || true
}

remove_snapshots() {
  local keys kind

  keys="$(
    ${SUDO} ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null | awk '
      NR>1 && $3 != "Committed" {print $1}
    '
  )"
  for kind in $keys; do
    ${SUDO} ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" rm "$kind" >/dev/null 2>&1 || true
  done

  if [ "$PRUNE_IMAGES" = "1" ]; then
    local image_ids committed

    if [[ "${PRUNE_IMAGES_REMOVE_IMAGES}" == "1" ]]; then
      image_ids="$(${SUDO} nerdctl -n "$NS" images -q 2>/dev/null | sort -u)"
      if [ -n "$image_ids" ]; then
        ${SUDO} nerdctl -n "$NS" rmi -f $image_ids >/dev/null 2>&1 || true
      fi
    fi

    committed="$(
      ${SUDO} ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null | awk '
        NR>1 && $3 == "Committed" {print $1}
      ' | tac
    )"
    for kind in $committed; do
      ${SUDO} ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" rm "$kind" >/dev/null 2>&1 || true
    done
  fi
}

echo "== reset namespace: $NS =="

cleanup_shared_snapshot_views

container_ids="$(list_runtime_containers)"
task_ids="$(list_tasks)"
shim_ids="$(list_shims)"

if [ -n "$container_ids" ]; then
  echo "[reset] removing runtime containers"
  for id in $container_ids; do
    echo "[reset] remove container $id"
    remove_container "$id"
  done
fi

if [ -n "$task_ids" ]; then
  echo "[reset] removing remaining tasks"
  for id in $task_ids; do
    echo "[reset] remove task $id"
    ${SUDO} ctr -n "$NS" tasks kill -s SIGKILL "$id" >/dev/null 2>&1 || true
    ${SUDO} ctr -n "$NS" tasks delete -f "$id" >/dev/null 2>&1 || true
  done
fi

if [ -n "$shim_ids" ]; then
  echo "[reset] killing shims"
  descendants="$(descendants_of $shim_ids)"
  if [ -n "$descendants" ]; then
    echo "[reset] kill shim descendants $descendants"
    ${SUDO} kill -9 $descendants >/dev/null 2>&1 || true
  fi
  echo "[reset] kill shims $shim_ids"
  ${SUDO} kill -9 $shim_ids >/dev/null 2>&1 || true
fi

echo "[reset] removing snapshots"
remove_snapshots

remaining_containers="$(${SUDO} ctr -n "$NS" c ls 2>/dev/null | awk 'NR>1 {print $1}')"
remaining_tasks="$(${SUDO} ctr -n "$NS" tasks ls 2>/dev/null | awk 'NR>1 {print $1}')"
remaining_shims="$(list_shims)"
remaining_snapshots="$(${SUDO} ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null | awk 'NR>1 {print $1}' | wc -l)"

echo "remaining_containers=${remaining_containers:-0}"
echo "remaining_tasks=${remaining_tasks:-0}"
echo "remaining_shims=${remaining_shims:-0}"
remaining_containers_count="$(echo "${remaining_containers:-}" | awk 'NF{c++} END{print c+0}')"
remaining_tasks_count="$(echo "${remaining_tasks:-}" | awk 'NF{c++} END{print c+0}')"
remaining_shims_count="$(echo "${remaining_shims:-}" | awk 'NF{c++} END{print c+0}')"
echo "remaining_containers_count=$remaining_containers_count"
echo "remaining_tasks_count=$remaining_tasks_count"
echo "remaining_shims_count=$remaining_shims_count"
echo "remaining_snapshots=$remaining_snapshots"
echo "global_dmsetup_devices=$(${SUDO} dmsetup ls | wc -l)"

if [[ "${STRICT_RESET}" == "1" ]]; then
  if [[ "$remaining_containers_count" -ne 0 || "$remaining_tasks_count" -ne 0 || "$remaining_shims_count" -ne 0 ]]; then
    echo "[reset] strict mode: namespace is not clean (containers=$remaining_containers_count tasks=$remaining_tasks_count shims=$remaining_shims_count)" >&2
    exit 2
  fi
fi
