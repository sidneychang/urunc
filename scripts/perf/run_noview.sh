#!/usr/bin/env bash
set -euo pipefail

# 镜像与环境可通过环境变量覆盖
IMAGE="${IMAGE:-harbor.nbfc.io/nubificus/urunc/nginx-qemu-linux-raw:latest}"
SNAPSHOTTER="${SNAPSHOTTER:-devmapper}"
# nerdctl 这里需要的是 runtime 插件 ID 或二进制完整路径
RUNTIME="${RUNTIME:-io.containerd.uruncnv.v2}"

# 在 3G 内存机器上，默认少跑几次并适当间隔
RUNS="${RUNS:-10}"
SLEEP_BETWEEN_RUNS="${SLEEP_BETWEEN_RUNS:-2}"

echo "== baseline (no view) =="
echo "IMAGE=$IMAGE SNAPSHOTTER=$SNAPSHOTTER RUNTIME=$RUNTIME RUNS=$RUNS SLEEP=$SLEEP_BETWEEN_RUNS"

echo "[1/2] ensuring image is present (pull if missing)..."
if ! sudo nerdctl --snapshotter "$SNAPSHOTTER" image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "image not found locally, pulling..."
  sudo nerdctl --snapshotter "$SNAPSHOTTER" pull "$IMAGE"
else
  echo "image already present locally, skipping pull"
fi

echo "[2/2] running containers..."
for i in $(seq 1 "$RUNS"); do
  echo "[$RUNTIME] run $i / $RUNS"
  /usr/bin/time -f "%e" \
    sudo nerdctl run -d \
      --snapshotter "$SNAPSHOTTER" \
      --runtime "$RUNTIME" \
      "$IMAGE" > /dev/null
  cid="$(sudo nerdctl ps -l -q)"
  if [ -n "$cid" ]; then
    sudo nerdctl stop "$cid" > /dev/null
    sudo nerdctl rm "$cid" > /dev/null
  fi
  sleep "$SLEEP_BETWEEN_RUNS"
done

