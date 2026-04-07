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

NAME_PREFIX="${NAME_PREFIX:-urunc-seqbench}"
SCRIPT_TAG="seqbench-$$"

log() { printf '%s\n' "$*"; }

ensure_image() {
  local need_pull=0
  if [[ "${FORCE_PULL}" == "1" ]]; then
    need_pull=1
  elif ! bench_sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" image inspect "$IMAGE" >/dev/null 2>&1; then
    need_pull=1
  fi
  if [[ "${need_pull}" == "1" ]]; then
    bench_sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" pull "$IMAGE" || exit $?
  fi
}

run_reset() {
  if [[ "${SKIP_RESET:-0}" == "1" ]]; then
    return 0
  fi
  NS="$NS" SNAPSHOTTER="$SNAPSHOTTER" "$RESET_SCRIPT"
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
    cid="$(bench_sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" run -d \
      --name "$name" \
      --runtime "$RUNTIME" \
      "$IMAGE" 2>/dev/null | tail -1)" || rc=$?
    t1=$(date +%s.%N)
    elapsed=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.6f", b - a }')
    if [[ $rc -ne 0 || -z "$cid" ]]; then
      log "[error] run $i failed rc=$rc cid=${cid:-empty}"
      exit 1
    fi
    log "[start] container $i/${SEQUENTIAL_TOTAL} nerdctl_run_sec=${elapsed}"
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

  log "======== 删除 $SEQUENTIAL_TOTAL 个容器 ========"
  cleanup_ids_file "$ALL_CIDS"
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
