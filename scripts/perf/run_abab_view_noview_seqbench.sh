#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NS="${NS:-default}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
RUNTIME="${RUNTIME:-io.containerd.urunc.v2}"
IMAGE="${IMAGE:-harbor.nbfc.io/nubificus/urunc/busybox-qemu-linux-raw:latest}"

# per-mode sequential container count
N="${N:-50}"
# ABAB rounds (each round runs: noview then view)
ROUNDS="${ROUNDS:-5}"
OUT_TSV="${OUT_TSV:-/tmp/urunc-view-noview-abab.tsv}"

switch_view() {
  (
    cd "$REPO_ROOT"
    sudo make install
  )
}

switch_noview() {
  sudo cp /usr/local/bin/bak/* /usr/local/bin/
}

run_one() {
  local mode="$1" round="$2"
  local label
  label="abab_r${round}_${mode}"

  export URUNC_BENCH_NS="$NS"
  export RESOURCE_SAMPLE=1
  export RESULT_TSV="$OUT_TSV"
  export RESOURCE_FORMAT=v2
  export RESOURCE_GRANULARITY=both
  export RESOURCE_START_SETTLE=1
  export RESOURCE_START_SAMPLE_DELAYS="${RESOURCE_START_SAMPLE_DELAYS:-5,20,60}"
  export RESOURCE_CLEANUP_SETTLE=1
  export RESOURCE_CLEANUP_SAMPLE_DELAYS="${RESOURCE_CLEANUP_SAMPLE_DELAYS:-5,20,60}"
  export URUNC_BENCH_LABEL="$label"
  export IMAGE="$IMAGE"
  export RUNTIME="$RUNTIME"
  export NERDCTL_RUN_TIMEOUT_SEC="${NERDCTL_RUN_TIMEOUT_SEC:-180}"
  export BENCH_DEBUG="${BENCH_DEBUG:-1}"
  export BENCH_DEBUG_LOG="${BENCH_DEBUG_LOG:-/tmp/urunc-seqbench-debug.log}"

  "${SCRIPT_DIR}/urunc_bench.sh" sequential "$SNAPSHOTTER" "$N" --tsv "$OUT_TSV"
}

main() {
  echo "[abab] NS=$NS SNAPSHOTTER=$SNAPSHOTTER RUNTIME=$RUNTIME IMAGE=$IMAGE"
  echo "[abab] N=$N ROUNDS=$ROUNDS OUT_TSV=$OUT_TSV"

  # Warm-up (optional): one short no-view run to stabilize caches; does not write TSV.
  if [[ "${WARMUP:-1}" == "1" ]]; then
    echo "[abab] warmup: noview N=5 (no TSV)"
    switch_noview
    URUNC_BENCH_NS="$NS" IMAGE="$IMAGE" RUNTIME="$RUNTIME" \
      "${SCRIPT_DIR}/urunc_bench.sh" sequential "$SNAPSHOTTER" 5 >/dev/null
  fi

  local r
  for r in $(seq 1 "$ROUNDS"); do
    echo "[abab] round $r/$ROUNDS: noview"
    switch_noview
    run_one noview "$r"

    echo "[abab] round $r/$ROUNDS: view"
    switch_view
    run_one view "$r"
  done

  echo "[abab] done: $OUT_TSV"
}

main "$@"

