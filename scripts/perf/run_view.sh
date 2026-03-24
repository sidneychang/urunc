#!/usr/bin/env bash
set -euo pipefail

# 镜像与环境可通过环境变量覆盖
NS="${NS:-urunc-bench}"
IMAGE="${IMAGE:-harbor.nbfc.io/nubificus/urunc/nginx-qemu-linux-raw:latest}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
RUNTIME="${RUNTIME:-io.containerd.uruncv.v2}"
RESET_SCRIPT="${RESET_SCRIPT:-$(dirname "$0")/reset_urunc_bench_state.sh}"

# 在 3G 内存机器上，默认少跑几次并适当间隔
RUNS="${RUNS:-10}"
SLEEP_BETWEEN_RUNS="${SLEEP_BETWEEN_RUNS:-2}"

echo "== view snapshot =="
echo "NS=$NS IMAGE=$IMAGE SNAPSHOTTER=$SNAPSHOTTER RUNTIME=$RUNTIME RUNS=$RUNS SLEEP=$SLEEP_BETWEEN_RUNS"

echo "[0/3] resetting benchmark namespace runtime state..."
NS="$NS" SNAPSHOTTER="$SNAPSHOTTER" "$RESET_SCRIPT"

echo "[1/3] ensuring image is present (pull if missing)..."
if ! sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "image not found locally, pulling..."
  sudo nerdctl -n "$NS" --snapshotter "$SNAPSHOTTER" pull "$IMAGE"
else
  echo "image already present locally, skipping pull"
fi

echo "[2/3] running containers..."
for i in $(seq 1 "$RUNS"); do
  echo "[$RUNTIME] run $i / $RUNS"
  /usr/bin/time -f "%e" \
    sudo nerdctl -n "$NS" run -d \
      --snapshotter "$SNAPSHOTTER" \
      --runtime "$RUNTIME" \
      "$IMAGE" > /dev/null
  cid="$(sudo nerdctl -n "$NS" ps -l -q)"
  if [ -n "$cid" ]; then
    sudo nerdctl -n "$NS" stop "$cid" > /dev/null
    sudo nerdctl -n "$NS" rm "$cid" > /dev/null
  fi
  sleep "$SLEEP_BETWEEN_RUNS"
done

echo "[3/3] resetting benchmark namespace runtime state..."
NS="$NS" SNAPSHOTTER="$SNAPSHOTTER" "$RESET_SCRIPT"
