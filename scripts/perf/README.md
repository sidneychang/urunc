# urunc 性能基准脚本使用说明

本目录提供 **并发 / 顺序** 启动容器、**资源采样 TSV**、**namespace 清理** 等脚本。统一入口为 **`urunc_bench.sh`**；底层实现为 `run_*_bench.sh` 与 **`lib_bench_common.sh`**。

更细的**存储指标含义**（devmapper vs blockfile、thin pool、cleanup 残留）见 [STORAGE_METRICS.md](STORAGE_METRICS.md)。

---

## 前置条件

- 已安装并可执行：`nerdctl`、`ctr`、`sudo`（拉镜像、`du`、`lvs`/`dmsetup` 等）。
- 使用 urunc 运行时：`RUNTIME` 默认 `io.containerd.urunc.v2`（可用环境变量覆盖）。
- 换 **snapshotter**（devmapper ↔ blockfile）后，建议至少一次 **`--pull`** 或 `FORCE_PULL=1`，保证当前 snapshotter 下镜像已解包。

---

## 统一入口：`urunc_bench.sh`

在仓库根目录或 `scripts/perf` 下执行（路径按你的克隆位置调整）：

```bash
cd /path/to/urunc/scripts/perf
chmod +x urunc_bench.sh run_*.sh lib_bench_common.sh reset_*.sh  # 若尚未可执行
```

### 子命令一览

| 子命令 | 作用 |
|--------|------|
| `concurrent <devmapper\|blockfile> <1\|10\|32\|64>` | 并发一批启动容器，统计每次 `nerdctl run` 耗时 |
| `sequential <devmapper\|blockfile> <N>` | **顺序**启动 N 个容器（无并发），适合看随 N 增长的资源 |
| `reset <devmapper\|blockfile> [--ns <name>]` | 清理指定 NS 下的容器、task、shim、快照（见脚本内说明） |
| `flush <devmapper\|blockfile> [--ns <name>]` | 按叶节点顺序删光该 snapshotter 的快照（比 reset 更偏「只清快照图」） |
| `help` | 打印帮助 |

**命名空间：**

- `concurrent` / `sequential` 默认 **`URUNC_BENCH_NS=default`**（可用环境变量覆盖）。
- `reset` / `flush` 默认 **`NS=urunc-bench`**；若你压测用的是 `default`，请 **`--ns default`** 或 **`URUNC_BENCH_NS=default`**，与压测一致。

---

## 顺序压测（推荐做「内存 / 存储随 N 变化」）

```bash
export URUNC_BENCH_LABEL=my-build-nonview   # 写入 TSV，便于区分二进制版本
./urunc_bench.sh sequential devmapper 50 --tsv /tmp/seq-devmapper.tsv
```

常用可选参数（由 `urunc_bench.sh` 解析并传给顺序脚本）：

- `--sample-every K`：每启动 **K 个**容器才写一条资源行（默认每条都打点则不必设）。
- `--pull`：强制 `nerdctl pull`。

等价环境变量（直接跑 `run_sequential_start_bench.sh` 时）：

- `SEQUENTIAL_TOTAL`：由位置参数传入，一般不必单独 export。
- `SAMPLE_EVERY`：与 `--sample-every` 一致。

---

## 并发压测

```bash
./urunc_bench.sh concurrent blockfile 32 --pull --tsv /tmp/conc-bf.tsv
```

并发度只能是 **1、10、32、64** 之一（脚本内校验）。

---

## 资源采样 TSV（v2）

加 **`--tsv 文件`** 即打开采样（内部等价 `RESOURCE_SAMPLE=1`、`RESULT_TSV=...`）。

### 表头说明（v2）

包含：**系统 meminfo**、**containerd / shim / qemu RSS**、**snapshotter 插件目录 du**、**/run/containerd du**、**devmapper thin pool**（解析成功时）、**快照数 / 运行容器数**、**单次启动耗时 / 批耗时** 等。完整列名以运行生成的首行为准。

### 常用环境变量

| 变量 | 含义 |
|------|------|
| `RESOURCE_FORMAT=legacy` | 使用旧版 7 列 TSV（不推荐用于对比存储） |
| `RESOURCE_GRANULARITY=both\|coarse\|fine` | 粗（仅系统 meminfo）/ 细（进程+du）/ 两者 |
| `RESOURCE_SAMPLE_LIGHT=1` | 跳过对大目录的 `du`，加快采样 |
| `URUNC_BENCH_LABEL=...` | 实验标签，写入 TSV |
| `CONTAINERD_ROOT` | 默认 `/var/lib/containerd` |
| `CONTAINERD_RUN_ROOT` | 默认 `/run/containerd` |
| `DEVMAPPER_THIN_LV=vg/lv` | 手动指定 thin pool 的 `lvs` 对象（可选） |
| `DEVMAPPER_THIN_LV_AUTO=0` | 关闭自动解析 thin LV |
| `RESOURCE_CLEANUP_SETTLE=1` | cleanup **之后**再按延迟多点采样（见下） |
| `RESOURCE_CLEANUP_SAMPLE_DELAYS=5,15,30,60` | 从 cleanup 完成起算的**绝对秒数**，会依次增量 sleep 后追加 `*_settle_<秒>s` 行 |

### cleanup 后多次采样（观察异步回收）

在首条 `after_cleanup_*` 写入后，若设置：

```bash
export RESOURCE_CLEANUP_SETTLE=1
export RESOURCE_CLEANUP_SAMPLE_DELAYS=5,15,30,60
./urunc_bench.sh sequential blockfile 10 --tsv /tmp/with-settle.tsv
```

TSV 中会多出 `after_cleanup_sequential_10_settle_5s`、`_settle_15s` 等标签行，用于对比 **blockfile / devmapper 删除是否滞后**。

---

## reset 是否自动执行？

- **`concurrent`** / **`sequential`** 脚本在拉镜像、正式跑批之前会调用 **`reset_urunc_bench_state.sh`**（可用 **`SKIP_RESET=1`** 跳过）。
- 带 **`--tsv`** 时，**第一条**资源行往往在 **reset 之前**采集；若希望基线也是「干净环境」，可先手动执行一次 **`urunc_bench.sh reset ...`** 再跑压测。

---

## 仅清理环境

```bash
# 与 concurrent/sequential 使用 default NS 时一致：
./urunc_bench.sh reset devmapper --ns default
./urunc_bench.sh flush blockfile --ns default
```

快照删不干净时，可用 `flush`；更激进选项见 [STORAGE_METRICS.md](STORAGE_METRICS.md) 与 `reset_urunc_bench_state.sh` 内注释。

---

## view / no-view 黑盒存储验证

如果你现在不是靠不同 runtime ID 区分 `view` / `no-view`，而是靠：

- `view`：当前分支执行 `make install`
- `no-view`：`cp /usr/local/bin/bak/* /usr/local/bin/`

可以直接用：

```bash
cd /root/zxd/urunc
chmod +x scripts/perf/verify_view_noview_switch.sh
```

新的 [verify_view_noview_switch.sh](/root/zxd/urunc/scripts/perf/verify_view_noview_switch.sh) 不再把 `du monRootfs` 当主结论，而是同时采：

- host 侧 `/run` 的 `df -B1`
- host 侧 `/proc/meminfo` 中的 `Shmem`
- 可选的 `dmsetup status` / `lvs`（需要设置 `THIN_POOL`）
- 容器 mount namespace 内对 bundle、`rootfs`、`monRootfs` 的 `df` / `findmnt` / `mountinfo`

### 1. 单容器采样

`view`：

```bash
NS=default SNAPSHOTTER=devmapper \
  scripts/perf/verify_view_noview_switch.sh sample-one view
```

`no-view`：

```bash
NS=default SNAPSHOTTER=devmapper \
  scripts/perf/verify_view_noview_switch.sh sample-one noview
```

如果你还想一起看 thin-pool：

```bash
NS=default SNAPSHOTTER=devmapper THIN_POOL=<your-thin-pool> \
  scripts/perf/verify_view_noview_switch.sh sample-one view
```

### 2. A/B 黑盒对比

```bash
NS=default SNAPSHOTTER=devmapper RUNS=20 \
  scripts/perf/verify_view_noview_switch.sh ab
```

如果还要一起看 dm/lv：

```bash
NS=default SNAPSHOTTER=devmapper RUNS=20 THIN_POOL=<your-thin-pool> \
  scripts/perf/verify_view_noview_switch.sh ab
```

这个模式会按顺序做两轮：

- `no-view`：执行 `sudo cp /usr/local/bin/bak/* /usr/local/bin/`
- `view`：执行 `sudo make install`

每轮都会：

- 开始前自动清理
- 采样 host `/run` 与 `Shmem`
- 启动 `RUNS` 个容器
- 再采样一次 host 指标
- 选 1 个活容器进入其 mount namespace，用 shell 工具查看 bundle、`rootfs`、`monRootfs`
- 最后自动清理

如果你想换镜像、runtime 或运行次数，可以这样：

```bash
NS=default SNAPSHOTTER=devmapper RUNTIME=io.containerd.urunc.v2 \
IMAGE=harbor.nbfc.io/nubificus/urunc/nginx-qemu-linux-raw:latest \
RUNS=50 scripts/perf/verify_view_noview_switch.sh ab
```

---

## devmapper 专用（可选）

修改 `base_image_size` 等会动 containerd 配置，脚本通过环境变量控制，例如：

```bash
DEVMAPPER_BASE_IMAGE_SIZE=10GB APPLY_DEVMAPPER_BASE_IMAGE_SIZE=1 \
  ./urunc_bench.sh concurrent devmapper 1 --tsv /tmp/x.tsv
```

详见 `lib_bench_common.sh` 顶部注释。

---

## 直接调用底层脚本（不推荐新手）

```bash
NS=default SNAPSHOTTER=devmapper RESOURCE_SAMPLE=1 RESULT_TSV=/tmp/r.tsv \
  ./run_sequential_start_bench.sh --devmapper 20 --sample-every 5
```

需自行保证与 **`reset`/`flush` 使用同一 `NS`**。

---

## 故障排查简表

| 现象 | 建议 |
|------|------|
| TSV 里 `thin_*` 为空 | 检查 `pool_name` 与 `lvs` 是否一致；或设 `DEVMAPPER_THIN_LV=vg/lv`；非 LVM 时看 `thin_metrics_source` 是否为 `dmsetup` |
| blockfile cleanup 后 `snapshotter_root_bytes` 仍高 | 设 `RESOURCE_CLEANUP_SETTLE=1`；查 `ctr snapshots ls`；必要时 `flush` 或镜像 prune |
| 换 snapshotter 后镜像异常 | `FORCE_PULL=1` 或 `--pull` |

---

## 文件索引

| 文件 | 作用 |
|------|------|
| `urunc_bench.sh` | 统一入口 |
| `run_concurrent_start_bench.sh` | 并发基准 |
| `run_sequential_start_bench.sh` | 顺序基准 |
| `lib_bench_common.sh` | 资源采样 v2、sudo 代理、devmapper 辅助 |
| `reset_urunc_bench_state.sh` | 按 NS 清理运行时状态 |
| `flush_namespace_snapshots.sh` | 叶序删快照 |
| [STORAGE_METRICS.md](STORAGE_METRICS.md) | 存储指标与结论说明 |
