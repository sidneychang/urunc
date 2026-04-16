#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-urunc-bench}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
RUNTIME_REGEX="${RUNTIME_REGEX:-io.containerd.urunc.v2}"
PRUNE_IMAGES="${PRUNE_IMAGES:-0}"

list_runtime_containers() {
  sudo ctr -n "$NS" c ls 2>/dev/null | awk -v re="$RUNTIME_REGEX" 'NR>1 && $3 ~ re {print $1}'
}

list_tasks() {
  sudo ctr -n "$NS" tasks ls 2>/dev/null | awk 'NR>1 {print $1}'
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
  sudo ctr -n "$NS" tasks kill -s SIGKILL "$id" >/dev/null 2>&1 || true
  sudo ctr -n "$NS" tasks delete -f "$id" >/dev/null 2>&1 || true
  sudo nerdctl -n "$NS" rm -f "$id" >/dev/null 2>&1 || true
  sudo ctr -n "$NS" c delete "$id" >/dev/null 2>&1 || true
}

remove_snapshots() {
  local keys kind

  keys="$(
    sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null | awk '
      NR>1 && $3 != "Committed" {print $1}
    '
  )"
  for kind in $keys; do
    sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" rm "$kind" >/dev/null 2>&1 || true
  done

  if [ "$PRUNE_IMAGES" = "1" ]; then
    local image_ids committed

    image_ids="$(sudo nerdctl -n "$NS" images -q 2>/dev/null | sort -u)"
    if [ -n "$image_ids" ]; then
      sudo nerdctl -n "$NS" rmi -f $image_ids >/dev/null 2>&1 || true
    fi

    committed="$(
      sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null | awk '
        NR>1 && $3 == "Committed" {print $1}
      ' | tac
    )"
    for kind in $committed; do
      sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" rm "$kind" >/dev/null 2>&1 || true
    done
  fi
}

echo "== reset namespace: $NS =="

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
    sudo ctr -n "$NS" tasks kill -s SIGKILL "$id" >/dev/null 2>&1 || true
    sudo ctr -n "$NS" tasks delete -f "$id" >/dev/null 2>&1 || true
  done
fi

if [ -n "$shim_ids" ]; then
  echo "[reset] killing shims"
  descendants="$(descendants_of $shim_ids)"
  if [ -n "$descendants" ]; then
    echo "[reset] kill shim descendants $descendants"
    sudo kill -9 $descendants >/dev/null 2>&1 || true
  fi
  echo "[reset] kill shims $shim_ids"
  sudo kill -9 $shim_ids >/dev/null 2>&1 || true
fi

echo "[reset] removing snapshots"
remove_snapshots

remaining_containers="$(sudo ctr -n "$NS" c ls 2>/dev/null | awk 'NR>1 {print $1}')"
remaining_tasks="$(sudo ctr -n "$NS" tasks ls 2>/dev/null | awk 'NR>1 {print $1}')"
remaining_shims="$(list_shims)"
remaining_snapshots="$(sudo ctr -n "$NS" snapshots --snapshotter "$SNAPSHOTTER" ls 2>/dev/null | awk 'NR>1 {print $1}' | wc -l)"

echo "remaining_containers=${remaining_containers:-0}"
echo "remaining_tasks=${remaining_tasks:-0}"
echo "remaining_shims=${remaining_shims:-0}"
echo "remaining_snapshots=$remaining_snapshots"
echo "global_dmsetup_devices=$(sudo dmsetup ls | wc -l)"
