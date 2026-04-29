#!/usr/bin/env bash

set -euo pipefail

NAMESPACE="${NAMESPACE:-default}"
IMAGE="${IMAGE:-harbor.nbfc.io/nubificus/urunc/nginx-qemu-linux-raw:latest}"
RUNTIME="${RUNTIME:-io.containerd.urunc.v2}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
COUNT="${COUNT:-4}"
PREFIX="${PREFIX:-svtest}"
KEEP_CONTAINERS="${KEEP_CONTAINERS:-0}"

TMPDIR="$(mktemp -d)"

cleanup() {
  rm -rf "${TMPDIR}"
  if [[ "${KEEP_CONTAINERS}" != "1" ]]; then
    for name in $(nerdctl -n "${NAMESPACE}" ps -a --format '{{.Names}}' | grep "^${PREFIX}-" || true); do
      nerdctl -n "${NAMESPACE}" rm -f "${name}" >/dev/null 2>&1 || true
    done
  fi
}
trap cleanup EXIT

now_ms() {
  date +%s%3N
}

run_one() {
  local name="$1"
  local outfile="$2"
  local start end

  start="$(now_ms)"
  nerdctl -n "${NAMESPACE}" run -d \
    --name "${name}" \
    --snapshotter "${SNAPSHOTTER}" \
    --runtime "${RUNTIME}" \
    "${IMAGE}" >/dev/null
  end="$(now_ms)"

  printf '%s %s\n' "${name}" "$((end - start))" > "${outfile}"
}

run_sequential() {
  local total_start total_end

  total_start="$(now_ms)"
  for i in $(seq 1 "${COUNT}"); do
    run_one "${PREFIX}-seq-${i}" "${TMPDIR}/seq-${i}.out"
  done
  total_end="$(now_ms)"

  echo "== Sequential =="
  sort "${TMPDIR}"/seq-*.out
  echo "TOTAL_MS $((total_end - total_start))"
  echo
}

run_concurrent() {
  local total_start total_end

  total_start="$(now_ms)"
  for i in $(seq 1 "${COUNT}"); do
    run_one "${PREFIX}-conc-${i}" "${TMPDIR}/conc-${i}.out" &
  done
  wait
  total_end="$(now_ms)"

  echo "== Concurrent =="
  sort "${TMPDIR}"/conc-*.out
  echo "TOTAL_MS $((total_end - total_start))"
  echo
}

echo "Namespace   : ${NAMESPACE}"
echo "Image       : ${IMAGE}"
echo "Runtime     : ${RUNTIME}"
echo "Snapshotter : ${SNAPSHOTTER}"
echo "Count       : ${COUNT}"
echo

run_sequential

if [[ "${KEEP_CONTAINERS}" != "1" ]]; then
  for name in $(nerdctl -n "${NAMESPACE}" ps -a --format '{{.Names}}' | grep "^${PREFIX}-seq-" || true); do
    nerdctl -n "${NAMESPACE}" rm -f "${name}" >/dev/null 2>&1 || true
  done
fi

run_concurrent

if findmnt -R /run/urunc/shared-views >/dev/null 2>&1; then
  echo "== Shared View Mounts =="
  findmnt -R /run/urunc/shared-views || true
fi
