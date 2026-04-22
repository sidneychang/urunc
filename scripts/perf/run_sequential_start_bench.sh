#!/usr/bin/env bash
# 顺序 nerdctl run：大规模 N 次、无并发；资源采样 v2（粗系统 + 细 RSS/du）。
# 环境: NS, IMAGE, SNAPSHOTTER, RUNTIME, SEQUENTIAL_TOTAL, SAMPLE_EVERY, RESOURCE_SAMPLE, RESULT_TSV,
# RESOURCE_FORMAT, RESOURCE_GRANULARITY, RESOURCE_SAMPLE_LIGHT, URUNC_BENCH_LABEL, SKIP_RESET, FORCE_PULL
set -euo pipefail

NS="${NS:-default}"
IMAGE="${IMAGE:-harbor.nbfc.io/nubificus/urunc/busybox-qemu-linux-raw:latest}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
FORCE_PULL="${FORCE_PULL:-0}"
RUNTIME="${RUNTIME:-io.containerd.urunc.v2}"
RESET_SCRIPT="${RESET_SCRIPT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/reset_urunc_bench_state.sh}"
# shellcheck source=lib_bench_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_bench_common.sh"

# Avoid sudo when running as root to preserve environment variables.
BENCH_SUDO=""
if [[ "$(id -u)" -ne 0 ]]; then
  BENCH_SUDO="sudo"
fi

NAME_PREFIX="${NAME_PREFIX:-urunc-seqbench}"
SCRIPT_TAG="seqbench-$$"
NERDCTL_RUN_TIMEOUT_SEC="${NERDCTL_RUN_TIMEOUT_SEC:-180}"
NERDCTL_PULL_TIMEOUT_SEC="${NERDCTL_PULL_TIMEOUT_SEC:-180}"
BENCH_DEBUG="${BENCH_DEBUG:-1}"
BENCH_DEBUG_LOG="${BENCH_DEBUG_LOG:-/tmp/urunc-seqbench-debug.log}"

log() { printf '%s\n' "$*"; }

debug_log() {
  [[ "${BENCH_DEBUG:-0}" == "1" ]] || return 0
  local ts
  ts="$(date -Iseconds)"
  printf '[%s] %s\n' "$ts" "$*" | tee -a "$BENCH_DEBUG_LOG" >/dev/null
}

debug_dump_runtime_state() {
  [[ "${BENCH_DEBUG:-0}" == "1" ]] || return 0
  {
    echo "----- runtime state -----"
    echo "ns=$NS snapshotter=$SNAPSHOTTER runtime=$RUNTIME"
    echo "running_containers=$(bench_sudo nerdctl -n "$NS" ps -q 2>/dev/null | wc -l | tr -d ' ')"
    echo "all_containers=$(bench_sudo nerdctl -n "$NS" ps -a -q 2>/dev/null | wc -l | tr -d ' ')"
    echo "tasks:"
    bench_sudo ctr -n "$NS" tasks ls 2>/dev/null | sed 's/^/  /' || true
    echo "recent_shims:"
    ps -eo pid=,args= 2>/dev/null | awk '$0 ~ /containerd-shim-urunc-v2/ {print "  "$0}' | tail -n 5 || true
    echo "-------------------------"
  } >>"$BENCH_DEBUG_LOG"
}

run_nerdctl_detached_with_timeout() {
  # Echo container ID on success; returns non-zero on failure/timeout.
  local name="$1"
  local timeout_sec="${2:-0}"
  local rc=0 out=""
  _bench_fill_sudo_env_cmd
  local cmd=()
  if [[ -n "${BENCH_SUDO:-}" ]]; then
    cmd+=("${BENCH_SUDO}")
  fi
  cmd+=(
    "${_BENCH_SUDO_ENV[@]}" nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" run -d
    --name "$name"
    --runtime "$RUNTIME"
    "$IMAGE"
  )
  if [[ "$timeout_sec" =~ ^[0-9]+$ ]] && [[ "$timeout_sec" -gt 0 ]] && command -v timeout >/dev/null 2>&1; then
    out="$(timeout "${timeout_sec}" "${cmd[@]}" 2>/dev/null | tail -1)" || rc=$?
  else
    out="$("${cmd[@]}" 2>/dev/null | tail -1)" || rc=$?
  fi
  if [[ $rc -ne 0 || -z "$out" ]]; then
    return 1
  fi
  printf '%s\n' "$out"
}

ensure_image() {
  debug_log "ensure_image: start force_pull=${FORCE_PULL} image=${IMAGE}"
  local need_pull=0
  if [[ "${FORCE_PULL}" == "1" ]]; then
    need_pull=1
  elif ! bench_sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" image inspect "$IMAGE" >/dev/null 2>&1; then
    need_pull=1
  fi
  if [[ "${need_pull}" == "1" ]]; then
    debug_log "ensure_image: pulling image"
    local rc=0
    if [[ "$NERDCTL_PULL_TIMEOUT_SEC" =~ ^[0-9]+$ ]] && [[ "$NERDCTL_PULL_TIMEOUT_SEC" -gt 0 ]] && command -v timeout >/dev/null 2>&1; then
      _bench_fill_sudo_env_cmd
      if [[ -n "${BENCH_SUDO:-}" ]]; then
        timeout "$NERDCTL_PULL_TIMEOUT_SEC" "${BENCH_SUDO}" "${_BENCH_SUDO_ENV[@]}" nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" pull "$IMAGE" || rc=$?
      else
        timeout "$NERDCTL_PULL_TIMEOUT_SEC" "${_BENCH_SUDO_ENV[@]}" nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" pull "$IMAGE" || rc=$?
      fi
    else
      bench_sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" pull "$IMAGE" || rc=$?
    fi
    if [[ $rc -ne 0 ]]; then
      log "[error] image pull failed/timeout rc=$rc ns=$NS image=$IMAGE"
      debug_log "ensure_image: pull failed rc=$rc timeout_sec=${NERDCTL_PULL_TIMEOUT_SEC}"
      debug_dump_runtime_state
      exit "$rc"
    fi
    debug_log "ensure_image: pull done"
  else
    debug_log "ensure_image: image already present"
  fi
}

run_reset() {
  if [[ "${SKIP_RESET:-0}" == "1" ]]; then
    debug_log "run_reset: skipped"
    return 0
  fi
  debug_log "run_reset: invoking reset script"
  NS="$NS" SNAPSHOTTER="$SNAPSHOTTER" "$RESET_SCRIPT"
  debug_log "run_reset: completed"
}

cleanup_ids_file() {
  local f=$1
  [[ ! -f "$f" ]] || [[ ! -s "$f" ]] && return 0
  sort -u "$f" -o "$f.uniq"
  local id
  while read -r id; do
    [[ -z "$id" ]] && continue
    bench_sudo nerdctl -n "$NS" rm -f "$id" >/dev/null 2>&1 || true
  done <"$f.uniq"
  rm -f "$f.uniq"
}

print_help() {
  log "用法: $0 [选项] <正整数 N>"
  log "  顺序启动 N 个容器；每 SAMPLE_EVERY 个（默认 1）打一条资源样本。"
  log "  选项: --blockfile|-B  --devmapper|-D  --snapshotter|-s <name>  --sample-every K  -h"
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --blockfile|-B) SNAPSHOTTER=blockfile; shift ;;
      --devmapper|-D) SNAPSHOTTER=devmapper; shift ;;
      --snapshotter|-s)
        [[ $# -ge 2 ]] || { log "错误: -s 需要参数"; exit 1; }
        SNAPSHOTTER=$2
        shift 2
        ;;
      --sample-every)
        [[ $# -ge 2 ]] || { log "错误: --sample-every 需要数字"; exit 1; }
        SAMPLE_EVERY=$2
        shift 2
        ;;
      -h|--help) print_help; exit 0 ;;
      -*)
        log "错误: 未知选项: $1"
        exit 1
        ;;
      *) break ;;
    esac
  done

  local total="${1:-}"
  if [[ -z "$total" ]] || ! [[ "$total" =~ ^[1-9][0-9]*$ ]]; then
    log "错误: 需要一个正整数 N"
    print_help
    exit 1
  fi

  local SAMPLE_EVERY="${SAMPLE_EVERY:-1}"
  if ! [[ "$SAMPLE_EVERY" =~ ^[1-9][0-9]*$ ]]; then
    log "错误: SAMPLE_EVERY 必须为正整数"
    exit 1
  fi

  local SEQUENTIAL_TOTAL="$total"

  log "== 顺序启动基准 =="
  log "NS=$NS IMAGE=$IMAGE SNAPSHOTTER=$SNAPSHOTTER RUNTIME=$RUNTIME"
  log "SEQUENTIAL_TOTAL=$SEQUENTIAL_TOTAL SAMPLE_EVERY=$SAMPLE_EVERY NAME_PREFIX=$NAME_PREFIX"
  debug_log "bench_start: total=${SEQUENTIAL_TOTAL} sample_every=${SAMPLE_EVERY} timeout_sec=${NERDCTL_RUN_TIMEOUT_SEC}"

  bench_apply_devmapper_base_image_size

  unset BENCH_CONTAINER_START_SEC BENCH_CONTAINER_START_MEAN_SEC BENCH_BATCH_ELAPSED_SEC 2>/dev/null || true
  if [[ "${RESOURCE_SAMPLE:-0}" == "1" ]]; then
    if [[ -n "${RESULT_TSV:-}" ]]; then
      if [[ ! -f "$RESULT_TSV" ]] || [[ ! -s "$RESULT_TSV" ]]; then
        if [[ "${RESOURCE_FORMAT:-v2}" == "legacy" ]]; then
          bench_resource_print_header >>"$RESULT_TSV"
        else
          bench_resource_print_header_v2 >>"$RESULT_TSV"
        fi
      fi
    fi
    local line
    if [[ "${RESOURCE_FORMAT:-v2}" == "legacy" ]]; then
      line="$(bench_resource_tsv_line "before_sequential_${SEQUENTIAL_TOTAL}" "$NS" "$SNAPSHOTTER")"
    else
      line="$(bench_resource_sample_line_v2 "before_sequential_${SEQUENTIAL_TOTAL}" "$NS" "$SNAPSHOTTER")"
    fi
    log "[resource] $line"
    [[ -n "${RESULT_TSV:-}" ]] && echo "$line" >>"$RESULT_TSV"
  fi

  run_reset
  ensure_image
  debug_log "main_loop: start creating containers"

  local ALL_CIDS
  ALL_CIDS="$(mktemp "${TMPDIR:-/tmp}/${SCRIPT_TAG}-cids.XXXXXX")"
  trap 'cleanup_ids_file "$ALL_CIDS"; rm -f "$ALL_CIDS" 2>/dev/null || true' EXIT

  local seq_t0 seq_t1 t0 t1 elapsed
  seq_t0=$(date +%s.%N)
  local i name cid rc=0
  for ((i = 1; i <= SEQUENTIAL_TOTAL; i++)); do
    name="${NAME_PREFIX}-${SCRIPT_TAG}-${i}"
    t0=$(date +%s.%N)
    rc=0
    cid="$(run_nerdctl_detached_with_timeout "$name" "$NERDCTL_RUN_TIMEOUT_SEC")" || rc=$?
    t1=$(date +%s.%N)
    elapsed=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.6f", b - a }')
    if [[ $rc -ne 0 || -z "$cid" ]]; then
      log "[error] run $i failed rc=$rc cid=${cid:-empty}"
      debug_log "run_failed: idx=${i} name=${name} rc=${rc} elapsed=${elapsed}"
      debug_dump_runtime_state
      exit 1
    fi
    log "[start] container $i/${SEQUENTIAL_TOTAL} nerdctl_run_sec=${elapsed}"
    debug_log "run_ok: idx=${i} cid=${cid} elapsed=${elapsed}"
    printf '%s\n' "$cid" >>"$ALL_CIDS"

    if [[ "${RESOURCE_SAMPLE:-0}" == "1" ]]; then
      if (( i % SAMPLE_EVERY == 0 || i == SEQUENTIAL_TOTAL )); then
        local line2
        export BENCH_CONTAINER_START_SEC="$elapsed"
        unset BENCH_CONTAINER_START_MEAN_SEC BENCH_BATCH_ELAPSED_SEC
        if [[ "${RESOURCE_FORMAT:-v2}" == "legacy" ]]; then
          line2="$(bench_resource_tsv_line "after_start_${i}_of_${SEQUENTIAL_TOTAL}" "$NS" "$SNAPSHOTTER")"
        else
          line2="$(bench_resource_sample_line_v2 "after_start_${i}_of_${SEQUENTIAL_TOTAL}" "$NS" "$SNAPSHOTTER")"
        fi
        unset BENCH_CONTAINER_START_SEC
        log "[resource] $line2"
        [[ -n "${RESULT_TSV:-}" ]] && echo "$line2" >>"$RESULT_TSV"
      fi
    fi
  done
  seq_t1=$(date +%s.%N)
  BENCH_BATCH_ELAPSED_SEC=$(awk -v a="$seq_t0" -v b="$seq_t1" 'BEGIN { printf "%.6f", b - a }')
  export BENCH_BATCH_ELAPSED_SEC
  debug_log "main_loop: all containers started batch_elapsed=${BENCH_BATCH_ELAPSED_SEC}"

  # Optional: wait and sample stable points while containers are still running.
  if [[ "${RESOURCE_SAMPLE:-0}" == "1" ]] && [[ "${RESOURCE_FORMAT:-v2}" != "legacy" ]]; then
    bench_resource_v2_emit_start_settle "after_start_${SEQUENTIAL_TOTAL}_of_${SEQUENTIAL_TOTAL}" "$NS" "$SNAPSHOTTER"
  fi

  log "======== 删除 $SEQUENTIAL_TOTAL 个容器 ========"
  cleanup_ids_file "$ALL_CIDS"
  debug_log "cleanup: removed created containers"
  rm -f "$ALL_CIDS"
  trap - EXIT

  if [[ "${RESOURCE_SAMPLE:-0}" == "1" ]]; then
    local line3
    unset BENCH_CONTAINER_START_SEC BENCH_CONTAINER_START_MEAN_SEC
    if [[ "${RESOURCE_FORMAT:-v2}" == "legacy" ]]; then
      line3="$(bench_resource_tsv_line "after_cleanup_sequential_${SEQUENTIAL_TOTAL}" "$NS" "$SNAPSHOTTER")"
    else
      line3="$(bench_resource_sample_line_v2 "after_cleanup_sequential_${SEQUENTIAL_TOTAL}" "$NS" "$SNAPSHOTTER")"
    fi
    unset BENCH_BATCH_ELAPSED_SEC
    log "[resource] $line3"
    [[ -n "${RESULT_TSV:-}" ]] && echo "$line3" >>"$RESULT_TSV"
    bench_resource_v2_emit_cleanup_settle "after_cleanup_sequential_${SEQUENTIAL_TOTAL}" "$NS" "$SNAPSHOTTER"
  fi
}

main "$@"
