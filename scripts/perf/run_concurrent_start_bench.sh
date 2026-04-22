#!/usr/bin/env bash
# 并发 nerdctl run 耗时。并发度: 1|10|32|64；--blockfile / --devmapper / -s <name>。
# 简入口: ./urunc_bench.sh concurrent <devmapper|blockfile> <并发度> [--pull] [--tsv 文件]
# 环境变量: NS, IMAGE, RUNTIME, FORCE_PULL, SKIP_RESET；资源 RESOURCE_SAMPLE + RESULT_TSV；
# RESOURCE_FORMAT=legacy|v2（默认 v2）；RESOURCE_GRANULARITY=both|coarse|fine；RESOURCE_SAMPLE_LIGHT=1 跳过 du；
# devmapper 改层大小等见 lib_bench_common.sh（一般不用）。
set -euo pipefail

NS="${NS:-default}"
IMAGE="${IMAGE:-harbor.nbfc.io/nubificus/urunc/busybox-qemu-linux-raw:latest}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
# 设为 1 时始终 nerdctl pull（保证当前 snapshotter 下已解包，换 devmapper/blockfile 时可用）
FORCE_PULL="${FORCE_PULL:-0}"
RUNTIME="${RUNTIME:-io.containerd.urunc.v2}"
RESET_SCRIPT="${RESET_SCRIPT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/reset_urunc_bench_state.sh}"
# shellcheck source=lib_bench_common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_bench_common.sh"
# 容器名前缀，便于识别与清理
NAME_PREFIX="${NAME_PREFIX:-urunc-concbench}"

SCRIPT_TAG="concbench-$$"

log() { printf '%s\n' "$*"; }

ensure_image() {
  local need_pull=0
  if [[ "${FORCE_PULL}" == "1" ]]; then
    need_pull=1
    log "[pull] FORCE_PULL=1，对 snapshotter=$SNAPSHOTTER 执行 pull"
  elif ! bench_sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" image inspect "$IMAGE" >/dev/null 2>&1; then
    need_pull=1
    log "[pull] snapshotter=$SNAPSHOTTER 下无可用镜像记录，正在拉取: $IMAGE"
  fi
  if [[ "${need_pull}" == "1" ]]; then
    # pull 失败时 set -e 会直接退出，后面「并发数」等日志不会出现；nerdctl 的 FATA 多在 stderr。
    bench_sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" pull "$IMAGE" || {
      rc=$?
      log "[pull] nerdctl pull 失败（退出码 $rc）。请向上翻看 nerdctl 的 stderr；常见：DNS/网络、代理、镜像不可达。" >&2
      exit "$rc"
    }
    log "[pull] 完成"
  else
    log "[pull] snapshotter=$SNAPSHOTTER 下 inspect 通过，跳过 pull（与 devmapper/blockfile 另一 snapshotter 的解包层无关；换后端请 FORCE_PULL=1 或手动 pull）"
  fi
}

run_reset() {
  if [[ "${SKIP_RESET:-0}" == "1" ]]; then
    log "[reset] SKIP_RESET=1，跳过 reset_urunc_bench_state"
    return 0
  fi
  log "[reset] 清理 namespace 运行时状态..."
  NS="$NS" SNAPSHOTTER="$SNAPSHOTTER" "$RESET_SCRIPT"
}

stats_from_times_file() {
  local f=$1
  if [[ ! -s "$f" ]]; then
    log "  (无有效耗时样本)"
    return 0
  fi
  awk '
    NF {
      v[++n] = $1 + 0
      sum += v[n]
      if (n == 1 || v[n] < min) min = v[n]
      if (n == 1 || v[n] > max) max = v[n]
    }
    END {
      if (n < 1) exit 0
      mean = sum / n
      for (i = 1; i <= n; i++) { ss += (v[i] - mean) ^ 2 }
      sd = (n > 1) ? sqrt(ss / (n - 1)) : 0
      printf "  样本数: %d\n", n
      printf "  各次耗时(s):"
      for (i = 1; i <= n; i++) printf " %.4f", v[i]
      printf "\n"
      printf "  平均(s): %.4f\n", mean
      printf "  最小(s): %.4f  最大(s): %.4f\n", min, max
      printf "  标准差(s, 样本): %.4f\n", sd
    }
  ' "$f"
}

run_batch() {
  local n=$1
  local work
  work="$(mktemp -d "${TMPDIR:-/tmp}/${SCRIPT_TAG}-batch${n}-XXXXXX")"
  log ""
  log "======== 并发数: $n ========"

  local t_batch0 t_batch1
  t_batch0=$(date +%s.%N)
  local i pids=()
  for ((i = 1; i <= n; i++)); do
    (
      set +e
      local t0 t1 elapsed cid name rc
      name="${NAME_PREFIX}-${SCRIPT_TAG}-${n}-${i}"
      t0=$(date +%s.%N)
      cid=$(bench_sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" run -d \
        --name "$name" \
        --runtime "$RUNTIME" \
        "$IMAGE" 2>/dev/null | tail -1)
      rc=$?
      t1=$(date +%s.%N)
      elapsed=$(awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.6f", b - a }')
      if [[ $rc -eq 0 && -n "$cid" ]]; then
        printf '%s\n' "$elapsed" >>"$work/times.ok"
        printf '%s\n' "$cid" >>"$work/cids"
      else
        printf '%s nerdctl_rc=%s\n' "$elapsed" "$rc" >>"$work/failures.log"
      fi
    ) &
    pids+=($!)
  done

  local pid
  for pid in "${pids[@]}"; do
    wait "$pid" || true
  done
  t_batch1=$(date +%s.%N)
  BENCH_BATCH_ELAPSED_SEC=$(awk -v a="$t_batch0" -v b="$t_batch1" 'BEGIN { printf "%.6f", b - a }')
  export BENCH_BATCH_ELAPSED_SEC
  if [[ -f "$work/times.ok" ]]; then
    BENCH_CONTAINER_START_MEAN_SEC=$(awk '{s+=$1;n++} END { if (n > 0) printf "%.6f", s/n; else print "" }' "$work/times.ok")
    export BENCH_CONTAINER_START_MEAN_SEC
  else
    BENCH_CONTAINER_START_MEAN_SEC=""
    export BENCH_CONTAINER_START_MEAN_SEC
  fi

  if [[ -f "$work/times.ok" ]]; then
    stats_from_times_file "$work/times.ok"
  fi
  if [[ -f "$work/failures.log" ]]; then
    log "  失败或空容器 ID 的记录 (elapsed, rc):"
    sed 's/^/    /' "$work/failures.log" || true
  fi

  # 供最后统一删除
  if [[ -f "$work/cids" ]]; then
    cat "$work/cids" >>"$ALL_CIDS_FILE"
  fi
  rm -rf "$work"
}

cleanup_all() {
  local f=$1
  if [[ ! -f "$f" ]] || [[ ! -s "$f" ]]; then
    log "[cleanup] 无容器 ID 列表，跳过删除"
    return 0
  fi
  log "[cleanup] 停止并删除本脚本创建的容器..."
  # 去重
  sort -u "$f" -o "$f.uniq"
  local id
  while read -r id; do
    [[ -z "$id" ]] && continue
    bench_sudo nerdctl -n "$NS" rm -f "$id" >/dev/null 2>&1 || true
  done <"$f.uniq"
  rm -f "$f.uniq"
  log "[cleanup] 完成"
}

ALL_CIDS_FILE=""
print_help() {
  log "用法: $0 [选项] <1|10|32|64>"
  log ""
  log "选项:"
  log "  --blockfile, -B     使用 blockfile snapshotter（覆盖 SNAPSHOTTER）"
  log "  --devmapper, -D     使用 devmapper snapshotter（默认）"
  log "  --snapshotter, -s <name>  指定任意 snapshotter（如 overlayfs）"
  log "  -h, --help          显示本帮助"
  log ""
  log "示例:"
  log "  $0 1"
  log "  $0 --blockfile 64"
  log "  SNAPSHOTTER=blockfile $0 10"
  log ""
  log "简入口: urunc_bench.sh concurrent <devmapper|blockfile> <1|10|32|64> [--pull] [--tsv 文件]"
}

usage() {
  print_help
  exit 1
}

usage_help() {
  print_help
  exit 0
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --blockfile|-B)
        SNAPSHOTTER=blockfile
        shift
        ;;
      --devmapper|-D)
        SNAPSHOTTER=devmapper
        shift
        ;;
      --snapshotter|-s)
        if [[ $# -lt 2 ]]; then
          log "错误: -s/--snapshotter 需要参数（如 blockfile、devmapper）"
          usage
        fi
        SNAPSHOTTER=$2
        shift 2
        ;;
      -h|--help)
        usage_help
        ;;
      -*)
        log "错误: 未知选项: $1"
        usage
        ;;
      *)
        break
        ;;
    esac
  done

  local concurrency="${1:-}"
  case "$concurrency" in
    1|10|32|64) ;;
    *)
      log "错误: 并发数必须是 1、10、32 或 64 之一。"
      usage
      ;;
  esac

  log "== 并发启动基准 =="
  log "NS=$NS (nerdctl 默认命名空间为 default) IMAGE=$IMAGE SNAPSHOTTER=$SNAPSHOTTER RUNTIME=$RUNTIME"
  log "本次并发数: $concurrency  NAME_PREFIX=$NAME_PREFIX"
  if [[ -n "${DEVMAPPER_BASE_IMAGE_SIZE:-}" ]]; then
    log "DEVMAPPER_BASE_IMAGE_SIZE=${DEVMAPPER_BASE_IMAGE_SIZE} APPLY_DEVMAPPER_BASE_IMAGE_SIZE=${APPLY_DEVMAPPER_BASE_IMAGE_SIZE:-0}"
  fi

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
    if [[ "${RESOURCE_FORMAT:-v2}" == "legacy" ]]; then
      line="$(bench_resource_tsv_line "before_batch_concurrent_${concurrency}" "$NS" "$SNAPSHOTTER")"
    else
      line="$(bench_resource_sample_line_v2 "before_batch_concurrent_${concurrency}" "$NS" "$SNAPSHOTTER")"
    fi
    log "[resource] $line"
    [[ -n "${RESULT_TSV:-}" ]] && echo "$line" >>"$RESULT_TSV"
  fi

  run_reset
  ensure_image

  ALL_CIDS_FILE="$(mktemp "${TMPDIR:-/tmp}/${SCRIPT_TAG}-all-cids.XXXXXX")"
  trap 'cleanup_all "$ALL_CIDS_FILE"; rm -f "$ALL_CIDS_FILE" 2>/dev/null || true' EXIT

  run_batch "$concurrency"

  log ""
  log "======== 本轮结束，正在删除容器 ========"
  cleanup_all "$ALL_CIDS_FILE"
  if [[ "${RESOURCE_SAMPLE:-0}" == "1" ]]; then
    unset BENCH_CONTAINER_START_SEC
    if [[ "${RESOURCE_FORMAT:-v2}" == "legacy" ]]; then
      line="$(bench_resource_tsv_line "after_cleanup_concurrent_${concurrency}" "$NS" "$SNAPSHOTTER")"
    else
      line="$(bench_resource_sample_line_v2 "after_cleanup_concurrent_${concurrency}" "$NS" "$SNAPSHOTTER")"
    fi
    unset BENCH_CONTAINER_START_MEAN_SEC BENCH_BATCH_ELAPSED_SEC
    log "[resource] $line"
    [[ -n "${RESULT_TSV:-}" ]] && echo "$line" >>"$RESULT_TSV"
    bench_resource_v2_emit_cleanup_settle "after_cleanup_concurrent_${concurrency}" "$NS" "$SNAPSHOTTER"
  fi
  rm -f "$ALL_CIDS_FILE"
  ALL_CIDS_FILE=""
  trap - EXIT
}

main "$@"
