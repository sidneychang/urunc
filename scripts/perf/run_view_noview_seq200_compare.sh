#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NS="${NS:-default}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
RUNTIME="${RUNTIME:-io.containerd.urunc.v2}"
IMAGE="${IMAGE:-harbor.nbfc.io/nubificus/urunc/busybox-qemu-linux-raw:latest}"

N="${N:-200}"
SAMPLE_EVERY="${SAMPLE_EVERY:-10}"

# settle=60s 用于稳定点对比；脚本默认会生成 after_start_N_of_N_settle_60s
RESOURCE_START_SAMPLE_DELAYS="${RESOURCE_START_SAMPLE_DELAYS:-5,20,60}"
RESOURCE_CLEANUP_SAMPLE_DELAYS="${RESOURCE_CLEANUP_SAMPLE_DELAYS:-5,20,60}"

# reset 时的额外清理策略：
# - 默认只清 committed snapshot（避免 device busy），不删 images（避免网络/重拉）。
RESET_PRUNE_IMAGES="${RESET_PRUNE_IMAGES:-1}"
PRUNE_IMAGES_REMOVE_IMAGES="${PRUNE_IMAGES_REMOVE_IMAGES:-0}"
RESET_NS_SLEEP_SEC="${RESET_NS_SLEEP_SEC:-5}"
# - shared snapshot view 残留：reset_urunc_bench_state.sh 内默认会处理（可用 SKIP_SHARED_VIEW_CLEANUP=1 跳过）

OUT_DIR="${OUT_DIR:-/tmp/urunc-view-noview-seq${N}-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "${OUT_DIR}"

VIEW_TSV="${OUT_DIR}/view_seq${N}.tsv"
NOVIEW_TSV="${OUT_DIR}/noview_seq${N}.tsv"
SUMMARY_JSON="${OUT_DIR}/summary.json"

URUNC_BENCH_SH="${URUNC_BENCH_SH:-${SCRIPT_DIR}/urunc_bench.sh}"
ANALYZE_PY="${ANALYZE_PY:-${SCRIPT_DIR}/analyze_seq200_view_noview_tsv.py}"
RESET_SCRIPT="${RESET_SCRIPT:-${SCRIPT_DIR}/reset_urunc_bench_state.sh}"

_CLEANUP_DONE=0
cleanup_on_exit() {
  local rc=$?
  [[ "${_CLEANUP_DONE}" == "1" ]] && exit "$rc"
  _CLEANUP_DONE=1
  echo "[cleanup] interrupted/exit rc=$rc; best-effort reset NS=$NS SNAPSHOTTER=$SNAPSHOTTER" >&2
  NS="$NS" SNAPSHOTTER="$SNAPSHOTTER" "$RESET_SCRIPT" >/dev/null 2>&1 || true
  exit "$rc"
}
trap cleanup_on_exit INT TERM EXIT

strict_reset_ns() {
  local attempt max_attempts sleep_s
  max_attempts="${RESET_MAX_ATTEMPTS:-3}"
  sleep_s="${RESET_RETRY_SLEEP_SEC:-3}"
  echo "[reset] strict reset start: NS=${NS} SNAPSHOTTER=${SNAPSHOTTER} attempts=${max_attempts}"

  for attempt in $(seq 1 "$max_attempts"); do
    echo "[reset] strict reset attempt ${attempt}/${max_attempts}"
    # Use strict mode so we can detect "not clean" and retry instead of hanging later.
    PRUNE_IMAGES="${RESET_PRUNE_IMAGES}" PRUNE_IMAGES_REMOVE_IMAGES="${PRUNE_IMAGES_REMOVE_IMAGES}" \
      STRICT_RESET=1 NS="${NS}" SNAPSHOTTER="${SNAPSHOTTER}" "${RESET_SCRIPT}" && {
        echo "[reset] strict reset OK"
        sleep "${RESET_NS_SLEEP_SEC}"
        return 0
      }
    echo "[reset] strict reset not clean yet; sleep ${sleep_s}s then retry" >&2
    sleep "${sleep_s}" || true
  done

  echo "[reset] strict reset failed: namespace still not clean after ${max_attempts} attempts" >&2
  # One last non-strict reset to dump diagnostics to stdout.
  PRUNE_IMAGES="${RESET_PRUNE_IMAGES}" PRUNE_IMAGES_REMOVE_IMAGES="${PRUNE_IMAGES_REMOVE_IMAGES}" \
    STRICT_RESET=0 NS="${NS}" SNAPSHOTTER="${SNAPSHOTTER}" "${RESET_SCRIPT}" || true
  return 1
}

switch_view() {
  echo "[switch] view -> make install"
  if [[ "$(id -u)" -eq 0 ]]; then
    (cd "${REPO_ROOT}" && make install)
  else
    (cd "${REPO_ROOT}" && sudo make install)
  fi
}

switch_noview() {
  echo "[switch] no-view -> cp /usr/local/bin/bak/* /usr/local/bin/"
  if [[ "$(id -u)" -eq 0 ]]; then
    cp /usr/local/bin/bak/* /usr/local/bin/
  else
    sudo cp /usr/local/bin/bak/* /usr/local/bin/
  fi
}

reset_ns() {
  # Important: urunc_bench.sh sequential writes "before_*" lines before its internal reset,
  # so we reset here (strictly) to keep baseline uncontaminated and avoid later hangs.
  strict_reset_ns
}

run_one() {
  local mode="$1"  # view | noview
  local label="$2" # must include "_view" or "_noview" for later parsing
  local tsv="$3"

  echo "=============================="
  echo "[run] mode=${mode} N=${N} sample-every=${SAMPLE_EVERY} tsv=${tsv}"
  echo "=============================="

  case "${mode}" in
    view) switch_view ;;
    noview) switch_noview ;;
    *) echo "unknown mode: ${mode}" >&2; exit 1 ;;
  esac

  reset_ns
  sleep 2

  export URUNC_BENCH_NS="${NS}"
  export RUNTIME="${RUNTIME}"
  export IMAGE="${IMAGE}"

  export URUNC_BENCH_LABEL="${label}"
  export RESOURCE_GRANULARITY="both"
  export RESOURCE_START_SETTLE="1"
  export RESOURCE_START_SAMPLE_DELAYS="${RESOURCE_START_SAMPLE_DELAYS}"
  export RESOURCE_CLEANUP_SETTLE="1"
  export RESOURCE_CLEANUP_SAMPLE_DELAYS="${RESOURCE_CLEANUP_SAMPLE_DELAYS}"
  export NERDCTL_RUN_TIMEOUT_SEC="${NERDCTL_RUN_TIMEOUT_SEC:-600}"
  # We already reset explicitly in reset_ns(); avoid double reset inside urunc_bench.sh.
  export SKIP_RESET="1"

  # --tsv enables RESOURCE_SAMPLE=1 and writes RESULT_TSV.
  "${URUNC_BENCH_SH}" sequential "${SNAPSHOTTER}" "${N}" \
    --sample-every "${SAMPLE_EVERY}" \
    --tsv "${tsv}"
}

main() {
  echo "[config] NS=${NS} SNAPSHOTTER=${SNAPSHOTTER} RUNTIME=${RUNTIME}"
  echo "[config] IMAGE=${IMAGE}"
  echo "[config] N=${N} SAMPLE_EVERY=${SAMPLE_EVERY}"
  echo "[config] RESOURCE_START_SAMPLE_DELAYS=${RESOURCE_START_SAMPLE_DELAYS}"
  echo "[config] RESOURCE_CLEANUP_SAMPLE_DELAYS=${RESOURCE_CLEANUP_SAMPLE_DELAYS}"
  echo "[config] RESET_PRUNE_IMAGES=${RESET_PRUNE_IMAGES} PRUNE_IMAGES_REMOVE_IMAGES=${PRUNE_IMAGES_REMOVE_IMAGES} RESET_NS_SLEEP_SEC=${RESET_NS_SLEEP_SEC}"
  echo "[config] OUT_DIR=${OUT_DIR}"

  # Execution order can affect cache/allocator state; allow swapping order.
  # Values: "view,noview" (default) or "noview,view".
  local order
  order="${MODE_ORDER:-view,noview}"
  case "$order" in
    view,noview)
      run_one "view" "seq${N}_view" "${VIEW_TSV}"
      run_one "noview" "seq${N}_noview" "${NOVIEW_TSV}"
      ;;
    noview,view)
      run_one "noview" "seq${N}_noview" "${NOVIEW_TSV}"
      run_one "view" "seq${N}_view" "${VIEW_TSV}"
      ;;
    *)
      echo "unknown MODE_ORDER=${order} (expected view,noview or noview,view)" >&2
      exit 1
      ;;
  esac

  echo "[analyze] python3 ${ANALYZE_PY}"
  python3 "${ANALYZE_PY}" --n "${N}" --view "${VIEW_TSV}" --noview "${NOVIEW_TSV}" --out "${SUMMARY_JSON}"

  echo "[done] TSV:"
  echo "  view:   ${VIEW_TSV}"
  echo "  noview: ${NOVIEW_TSV}"
  echo "[done] summary: ${SUMMARY_JSON}"
}

main "$@"

