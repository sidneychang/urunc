#!/usr/bin/env bash
# 统一入口：常用场景只需本脚本 + 少量参数；高级选项仍可用环境变量（见 lib_bench_common.sh）。
set -euo pipefail

_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_bench_common.sh
source "$_DIR/lib_bench_common.sh"

usage() {
  cat <<'EOF'
用法:
  urunc_bench.sh concurrent <devmapper|blockfile> <1|10|32|64> [--pull] [--tsv 文件]
  urunc_bench.sh sequential <devmapper|blockfile> <N> [--sample-every K] [--pull] [--tsv 文件]
  urunc_bench.sh reset     <devmapper|blockfile> [--ns <namespace>]
  urunc_bench.sh flush     <devmapper|blockfile> [--ns <namespace>]
  urunc_bench.sh help

说明:
  concurrent  默认 NS=default（可用 URUNC_BENCH_NS=… 覆盖）
  sequential  顺序启动 N 个容器（无并发）；资源 TSV 默认 v2（粗系统 meminfo + 细 RSS/du）
  --pull       强制 nerdctl pull（换 snapshotter 后建议至少用一次）
  --tsv 文件   打开资源采样并写入该 TSV（等价 RESOURCE_SAMPLE=1 RESULT_TSV=…）
  --sample-every K  仅 sequential：每 K 个容器记一条资源（默认 1）
  reset        调用 reset_urunc_bench_state.sh（清容器/task/shim/快照）；默认 NS=urunc-bench，
               concurrent/sequential 所用 NS=default 时请: --ns default 或 URUNC_BENCH_NS=default
  flush        调用 flush_namespace_snapshots.sh（按叶节点顺序删光该 snapshotter 快照）；NS 规则同 reset

资源采样环境变量（见 lib_bench_common.sh）:
  RESOURCE_FORMAT=legacy|v2（默认 v2）
  RESOURCE_GRANULARITY=both|coarse|fine
  RESOURCE_SAMPLE_LIGHT=1  跳过 du
  URUNC_BENCH_LABEL=…     实验标签写入 TSV
  DEVMAPPER_THIN_LV=vg/lv  devmapper thin pool lvs 百分比（可选）

devmapper 专用（仍通过环境变量，不常用不必记）:
  DEVMAPPER_BASE_IMAGE_SIZE=10GB APPLY_DEVMAPPER_BASE_IMAGE_SIZE=1
  DEVMAPPER_PRUNE_IMAGES_BEFORE_RESIZE=1   # 改 size 时彻底重建层

示例:
  ./urunc_bench.sh concurrent devmapper 10
  ./urunc_bench.sh sequential blockfile 50 --sample-every 10 --tsv /tmp/s.tsv
  ./urunc_bench.sh concurrent blockfile 32 --pull --tsv /tmp/c.tsv
  ./urunc_bench.sh reset blockfile --ns default
  ./urunc_bench.sh flush devmapper
EOF
}

_resolve_snap() {
  case "$1" in
    devmapper|dm|D) echo devmapper ;;
    blockfile|bf|B) echo blockfile ;;
    *)
      echo "错误: 第一个参数应为 devmapper 或 blockfile，收到: $1" >&2
      exit 1
      ;;
  esac
}

# Optional: --ns <name>. Sets BENCH_NS_OVERRIDE; unknown tokens go to _REST_ARGS.
_parse_ns_optional() {
  BENCH_NS_OVERRIDE=""
  _REST_ARGS=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --ns)
        [[ $# -ge 2 ]] || { echo "错误: --ns 需要 namespace 名" >&2; exit 1; }
        BENCH_NS_OVERRIDE=$2
        shift 2
        ;;
      *)
        _REST_ARGS+=("$1")
        shift
        ;;
    esac
  done
}

_parse_sequential_flags() {
  PULL=0
  TSV=""
  SAMPLE_EVERY=1
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --pull)
        PULL=1
        shift
        ;;
      --tsv)
        [[ $# -ge 2 ]] || { echo "错误: --tsv 需要文件路径" >&2; exit 1; }
        TSV=$2
        shift 2
        ;;
      --sample-every)
        [[ $# -ge 2 ]] || { echo "错误: --sample-every 需要正整数" >&2; exit 1; }
        SAMPLE_EVERY=$2
        shift 2
        ;;
      *)
        echo "错误: 未知参数: $1" >&2
        exit 1
        ;;
    esac
  done
}

_parse_tail_flags() {
  PULL=0
  TSV=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --pull)
        PULL=1
        shift
        ;;
      --tsv)
        [[ $# -ge 2 ]] || { echo "错误: --tsv 需要文件路径" >&2; exit 1; }
        TSV=$2
        shift 2
        ;;
      *)
        echo "错误: 未知参数: $1" >&2
        exit 1
        ;;
    esac
  done
}

cmd="${1:-help}"
shift || true

case "$cmd" in
  help|-h|--help)
    usage
    exit 0
    ;;

  reset)
    [[ $# -ge 1 ]] || { usage; exit 1; }
    SNAP="$(_resolve_snap "$1")"
    shift
    _parse_ns_optional "$@"
    [[ ${#_REST_ARGS[@]} -eq 0 ]] || { echo "错误: 未知参数: ${_REST_ARGS[*]}" >&2; exit 1; }
    export SNAPSHOTTER="$SNAP"
    if [[ -n "${BENCH_NS_OVERRIDE}" ]]; then
      export NS="$BENCH_NS_OVERRIDE"
    elif [[ -n "${URUNC_BENCH_NS:-}" ]]; then
      export NS="$URUNC_BENCH_NS"
    else
      export NS="${NS:-urunc-bench}"
    fi
    exec bash "$_DIR/reset_urunc_bench_state.sh"
    ;;

  flush)
    [[ $# -ge 1 ]] || { usage; exit 1; }
    SNAP="$(_resolve_snap "$1")"
    shift
    _parse_ns_optional "$@"
    [[ ${#_REST_ARGS[@]} -eq 0 ]] || { echo "错误: 未知参数: ${_REST_ARGS[*]}" >&2; exit 1; }
    export SNAPSHOTTER="$SNAP"
    if [[ -n "${BENCH_NS_OVERRIDE}" ]]; then
      export NS="$BENCH_NS_OVERRIDE"
    elif [[ -n "${URUNC_BENCH_NS:-}" ]]; then
      export NS="$URUNC_BENCH_NS"
    else
      export NS="${NS:-urunc-bench}"
    fi
    exec bash "$_DIR/flush_namespace_snapshots.sh"
    ;;

  concurrent)
    [[ $# -ge 2 ]] || { usage; exit 1; }
    SNAP="$(_resolve_snap "$1")"
    CONC="$2"
    shift 2
    _parse_tail_flags "$@"
    export NS="${URUNC_BENCH_NS:-default}"
    export SNAPSHOTTER="$SNAP"
    export FORCE_PULL="$PULL"
    if [[ -n "$TSV" ]]; then
      export RESOURCE_SAMPLE=1
      export RESULT_TSV="$TSV"
    fi
    if [[ "$SNAP" == "blockfile" ]]; then
      exec bash "$_DIR/run_concurrent_start_bench.sh" --blockfile "$CONC"
    else
      exec bash "$_DIR/run_concurrent_start_bench.sh" --devmapper "$CONC"
    fi
    ;;

  sequential)
    [[ $# -ge 2 ]] || { usage; exit 1; }
    SNAP="$(_resolve_snap "$1")"
    SEQ_N="$2"
    shift 2
    _parse_sequential_flags "$@"
    export NS="${URUNC_BENCH_NS:-default}"
    export SNAPSHOTTER="$SNAP"
    export FORCE_PULL="$PULL"
    export SAMPLE_EVERY
    if [[ -n "$TSV" ]]; then
      export RESOURCE_SAMPLE=1
      export RESULT_TSV="$TSV"
    fi
    if [[ "$SNAP" == "blockfile" ]]; then
      exec bash "$_DIR/run_sequential_start_bench.sh" --blockfile --sample-every "$SAMPLE_EVERY" "$SEQ_N"
    else
      exec bash "$_DIR/run_sequential_start_bench.sh" --devmapper --sample-every "$SAMPLE_EVERY" "$SEQ_N"
    fi
    ;;

  *)
    echo "错误: 未知子命令: $cmd" >&2
    usage
    exit 1
    ;;
esac
