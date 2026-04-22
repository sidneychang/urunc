# view vs no-view（N=20）证据记录与口径说明（2026-04-20）

本文用于回答 maintainer 的三个问题：

1. `/run` 增长是否确实由 urunc 的 copy 行为导致？
2. view snapshot 是否引入可忽略的存储开销（尤其是 snapshotter/devmapper）？
3. 目前的采样方法与结论口径是否清晰、可复现？

---

## 1. 实验配置与采集方式

### 1.1 实验脚本与输出

- 脚本：`scripts/perf/run_view_noview_seq200_compare.sh`
- 本次参数：
  - `N=20`
  - `SAMPLE_EVERY=1`
  - `RESOURCE_START_SAMPLE_DELAYS=5,20,60`
  - `RESOURCE_CLEANUP_SAMPLE_DELAYS=5,20,60`
- 输出目录：`/tmp/urunc-view-noview-seq20-20260420-225510/`
  - `view_seq20.tsv`
  - `noview_seq20.tsv`
  - `summary.json`

### 1.2 运行命令

```bash
cd /root/zxd/urunc
OUT_DIR="/tmp/urunc-view-noview-seq20-20260420-225510"
N=20 SAMPLE_EVERY=1 OUT_DIR="$OUT_DIR" \
RESOURCE_START_SAMPLE_DELAYS=5,20,60 \
RESOURCE_CLEANUP_SAMPLE_DELAYS=5,20,60 \
bash scripts/perf/run_view_noview_seq200_compare.sh
```

### 1.3 关键采样点（用于结论）

- 基线：`before_sequential_20`
- 稳定点：`after_start_20_of_20_settle_60s`
- 清理验证：`after_cleanup_sequential_20_settle_60s`

采用 `settle_60s` 的原因：避免启动后短时间 transient 波动，把结论固定在稳定点。

---

## 2. 指标含义（本次用到的最关键列）

### 2.1 `/run` 与 copy 归因

- `run_tmpfs_used_bytes`：`df /run` 的 used（/run 通常为 tmpfs）。
- `mem_shmem_kb`：系统 `Shmem`，可反映 tmpfs/shmem 压力。
- `sample_monrootfs_du_x_bytes`：对一个运行中容器的 `bundle/monRootfs` 执行 `du -sx`，只统计同一文件系统（tmpfs）上的真实占用，不跟随 bind mount 跨文件系统。

其中 `sample_monrootfs_du_x_bytes` 是“是否发生了真实 copy 到 tmpfs”的直接证据。

### 2.2 snapshotter/devmapper 开销

- `thin_data_used_bytes` / `thin_meta_used_bytes`
- `thin_data_used_blocks` / `thin_meta_used_blocks`
- `thin_data_pct` / `thin_meta_pct`

这些反映 devmapper thin-pool 的真实数据区/元数据区使用变化，是判断 view 是否引入额外 snapshotter 存储开销的主指标。

---

## 3. 结果（稳定点 delta：stable - before）

数据来源：`/tmp/urunc-view-noview-seq20-20260420-225510/summary.json`

| 指标 | view | no-view | 结论 |
|---|---:|---:|---|
| `run_tmpfs_used_mib` delta | +1.2539 | +140.3906 | no-view 显著增加 /run(tmpfs) |
| `mem_shmem_mib` delta | +1.2578 | +140.3906 | no-view 显著增加 shmem |
| `thin_data_used_mib` delta | +20.0 | +20.0 | 二者一致 |
| `thin_meta_used_mib` delta | +0.2344 | +0.2344 | 二者一致 |
| `thin_data_used_blocks` delta | +320 | +320 | 二者一致 |
| `thin_meta_used_blocks` delta | +60 | +60 | 二者一致 |

简述：

- `/run` 与 `Shmem` 在 no-view 下明显高于 view，且量级一致（约 +140MiB）。
- devmapper thin-pool 的 data/meta 增量在 view/no-view 下一致，未观察到 view 额外开销。

### 3.1 原始数据摘录（raw values）

> 口径：`before_sequential_20` 与 `after_start_20_of_20_settle_60s` 两个采样点的原始值。  
> 下表直接摘录自 `summary.json` 的 `_raw` 字段（bytes/blocks/pct），并附增量。

| 指标 | view-before | view-stable | view-delta | no-view-before | no-view-stable | no-view-delta |
|---|---:|---:|---:|---:|---:|---:|
| `thin_data_used_bytes` | 114,622,464 | 135,593,984 | +20,971,520 | 114,622,464 | 135,593,984 | +20,971,520 |
| `thin_meta_used_bytes` | 18,104,320 | 18,350,080 | +245,760 | 18,104,320 | 18,350,080 | +245,760 |
| `thin_data_used_blocks` | 1,749 | 2,069 | +320 | 1,749 | 2,069 | +320 |
| `thin_meta_used_blocks` | 4,420 | 4,480 | +60 | 4,420 | 4,480 | +60 |
| `thin_data_pct` | 0.1068 | 0.1263 | +0.0195 | 0.1068 | 0.1263 | +0.0195 |
| `thin_meta_pct` | 0.1686 | 0.1709 | +0.0023 | 0.1686 | 0.1709 | +0.0023 |
| `run_tmpfs_used_bytes` | 2,744,320 | 4,059,136 | +1,314,816 | 2,744,320 | 149,954,560 | +147,210,240 |
| `mem_shmem_kb` | 11,952 | 13,240 | +1,288 | 11,956 | 155,716 | +143,760 |

备注：

- `thin_data_used_bytes` 增量 `20,971,520 bytes = 20.0 MiB`
- `thin_meta_used_bytes` 增量 `245,760 bytes = 0.234375 MiB`
- `run_tmpfs_used_bytes` 增量：
  - view: `1,314,816 bytes = 1.2539 MiB`
  - no-view: `147,210,240 bytes = 140.3906 MiB`

---

## 4. `monRootfs du -x` 走势（after_start_1..20）

从两份 TSV 提取 `after_start_i_of_20` 的 `sample_monrootfs_du_x_bytes`：

- view：`i=1..20` 均为 `4096 bytes`（约 0 MiB）
- no-view：`i=1..20` 均为 `7311360 bytes`（约 6.97 MiB）

### 4.1 原始数据逐轮摘录（after_start_1..20）

| i | view `sample_monrootfs_du_x_bytes` | no-view `sample_monrootfs_du_x_bytes` |
|---:|---:|---:|
| 1 | 4,096 | 7,311,360 |
| 2 | 4,096 | 7,311,360 |
| 3 | 4,096 | 7,311,360 |
| 4 | 4,096 | 7,311,360 |
| 5 | 4,096 | 7,311,360 |
| 6 | 4,096 | 7,311,360 |
| 7 | 4,096 | 7,311,360 |
| 8 | 4,096 | 7,311,360 |
| 9 | 4,096 | 7,311,360 |
| 10 | 4,096 | 7,311,360 |
| 11 | 4,096 | 7,311,360 |
| 12 | 4,096 | 7,311,360 |
| 13 | 4,096 | 7,311,360 |
| 14 | 4,096 | 7,311,360 |
| 15 | 4,096 | 7,311,360 |
| 16 | 4,096 | 7,311,360 |
| 17 | 4,096 | 7,311,360 |
| 18 | 4,096 | 7,311,360 |
| 19 | 4,096 | 7,311,360 |
| 20 | 4,096 | 7,311,360 |

说明：该列为单容器抽样值（不是所有容器求和），且 `du` 以文件系统块计量（4KiB 粒度）。因此 `4096` 仅表示“抽样容器无显著实体大文件 copy”，全局变化仍以 `run_tmpfs_used_bytes` 为主。

这说明：

- view 路径下，`monRootfs` 在 tmpfs 上几乎没有实体 payload copy。
- no-view 路径下，`monRootfs` 上存在稳定的实体 copy（约 6.97MiB/容器样本）。

该观察与代码路径一致：

- no-view：`prepareDMAsBlock()` -> `extractFilesFromBlock()` 复制 `unikernel/initrd/urunc.json` 到 `monRootfs`
- view：`bindViewFilesToMonRootfs()` 从 snapshot view bind-mount，不做 copy

---

## 5. 为什么这些数据可以回答 maintainer 的问题

### 问题 A：`/run` 增长是否来自 copy？

可以回答“是”：

- 同一次实验中，`run_tmpfs_used` 与 `Shmem` 同时在 no-view 显著上升（约 +140MiB），view 基本不变（约 +1.3MiB）。
- `sample_monrootfs_du_x_bytes` 在 view/no-view 间形成稳定分离（0MiB vs 6.97MiB）。
- 这条证据链直接对齐“copy 是否写入 tmpfs”。

### 问题 B：view snapshot 是否增加 snapshotter 存储开销？

本次稳定点结论是“未观察到额外开销”：

- thin-pool data/meta 的 bytes、blocks、pct 增量均一致。
- 说明 view 优化主要体现在 `/run(tmpfs)` copy 消除，而非增加 devmapper 存储。

### 问题 C：结果是否可复现、口径是否清晰？

可以复现：

- 有固定脚本、固定参数、固定输出目录与 `summary.json`。
- 采用 `settle_60s` 稳定点，减少瞬时采样偏差。

---

## 6. 当前结论边界与建议

- 该结论来自一轮 `N=20`，方向已经清晰且能回答当前问题。
- 为对外/评审更稳健，建议再补 1-2 轮（如 ABAB 或 swap order），保持同一稳定点口径（`settle_60s`）。
- 文档引用时，优先使用本文件中的“稳定点 delta”与 `monRootfs du -x` 证据，避免使用早期 transient 点。

---

## 7. 外部资料依据：为什么这些指标能测容器存储变化

本节给出指标设计的“外部可验证依据”，用于增强结论稳健性。

### 7.1 thin-pool 指标（`thin_*`）为什么能代表 snapshotter 存储变化

1) **Linux dm-thin 内核文档**明确 thin-pool 状态包含：

- `used metadata blocks/total metadata blocks`
- `used data blocks/total data blocks`

并给出 `thin-pool` target 的 status 语义。  
来源：Linux Kernel 文档（thin provisioning）  
<https://www.kernel.org/doc/html/v6.9/admin-guide/device-mapper/thin-provisioning.html>

2) **LVM 官方手册**明确：

- `Data%`（`data_percent`）= thin pool data LV 已使用比例
- `Meta%`（`metadata_percent`）= thin pool metadata LV 已使用比例

来源：`lvmthin(7)`  
<https://man7.org/linux/man-pages/man7/lvmthin.7.html>

3) **containerd devmapper 文档**明确 devmapper snapshotter 基于 dm-thin pool，且 `pool_name` 指向 `/dev/mapper` 下的 thin-pool，`root_path` 主要是 metadata 目录。  
因此，评估“snapshotter真实存储开销”应优先看 thin-pool data/meta 指标，而不仅是 plugin 目录 `du`。

来源：containerd docs（devmapper）  
<https://containerd.io/docs/main/snapshotters/devmapper/>

结论：`thin_data_used_*`、`thin_meta_used_*`、`thin_*_pct` 能直接反映 devmapper 后端的真实占用变化，足以用于比较 view/no-view 对 snapshotter 存储的影响。

### 7.1.1 在本仓库中的记录与计算方式（实现细节）

`thin_*` 指标由 `scripts/perf/lib_bench_common.sh` 的资源采样路径写入 TSV，核心逻辑如下：

1) **数据来源选择（本次实验）**

- 本次实验以 `dmsetup` 为唯一口径（`thin_metrics_source=dmsetup`）：
  - `dmsetup status <pool>` 提取 `data/meta used/total blocks`
  - `dmsetup table <pool>` 提取 data block 大小（扇区）

2) **关键计算**

- `thin_data_used_blocks` / `thin_meta_used_blocks`：来自 `dmsetup status` 原始 block 计数（或保留为空）
- `thin_data_used_bytes`（dmsetup 路径）：
  - `data_used_blocks * data_block_size_bytes`
  - `data_block_size_bytes = blocksize_sectors * 512`
- `thin_meta_used_bytes`（dmsetup 路径）：
  - `meta_used_blocks * meta_block_bytes`
  - 默认 `meta_block_bytes = 4096`（可由环境变量覆盖）
- `thin_data_pct` / `thin_meta_pct`（本次）：
  - 由 `dmsetup` 的 `used_blocks / total_blocks` 换算

3) **为什么能作为 devmapper snapshotter 下的存储占用（本次以 dmsetup 为准）**

- 本次实验 `thin_metrics_source` 为 `dmsetup`，因此结论口径仅基于 dm-thin pool 的 `dmsetup status/table` 数据，不混用其他来源。
- Linux kernel 文档定义了 thin-pool status 中 `used metadata blocks/total metadata blocks` 与 `used data blocks/total data blocks` 的语义；这是后端分配状态的直接信号。  
  来源：<https://www.kernel.org/doc/html/v6.9/admin-guide/device-mapper/thin-provisioning.html>
- `dmsetup(8)` 说明 `status`/`table` 是 device-mapper target 的状态与表项查询接口；本实验正是从这两个接口读取 blocks 与 block size。  
  来源：<https://man7.org/linux/man-pages/man8/dmsetup.8.html>
- containerd devmapper 文档明确 snapshotter 使用 device-mapper thin-pool；因此在该 snapshotter 下，dm-thin pool 的 data/meta 变化可作为存储占用变化依据。  
  来源：<https://containerd.io/docs/main/snapshotters/devmapper/>
- 本实验中 view/no-view 在 `thin_*` 的 bytes/blocks/pct 增量完全一致，说明两种模式在 devmapper 后端消耗等价；差异主要体现在 `/run(tmpfs)` copy 路径。

### 7.2 `/run` 与 `du -x` 为什么能代表 copy 写入 tmpfs

1) `df` 定义是“报告文件系统空间使用”；脚本使用 `df -B1 --output=size,used,avail /run`，因此 `run_tmpfs_used_bytes` 直接是 `/run` 这个文件系统的已用空间。

来源：`df(1)`  
<https://man7.org/linux/man-pages/man1/df.1.html>

2) `du -x`（`--one-file-system`）定义为“跳过不同文件系统上的目录”。  
脚本对 `monRootfs` 使用 `du -sx`，因此 `sample_monrootfs_du_x_bytes` 只统计 `monRootfs` 所在文件系统（本场景为 `/run` tmpfs）上的真实占用，不把 bind-mount 进来的其他文件系统数据算进去。


---

## 8. 给 maintainer 的回复（建议稿）

以下文字可直接用于 discussion/reply（不附原始 TSV，只给关键摘录和口径说明）。

### 8.1 English reply draft

Thanks for the feedback from the 15/04/2026 meeting. We aligned the evaluation with the requested scope (storage-focused, not process memory). Below is a unified update from the latest controlled `view vs no-view` run (`N=50`), using:
- baseline sample point: right before sequential startup begins
- stable sample point: after all 50 containers are up and then settled for 60 seconds

**Environment and run model**
- Host: Linux `6.11.0`
- Stack: containerd + urunc (`RUNTIME=io.containerd.urunc.v2`)
- Snapshotter: `devmapper`
- Image: `harbor.nbfc.io/nubificus/urunc/busybox-qemu-linux-raw:latest`
- Run model: single-round sequential comparison (`view` then `no-view`)

**How each metric is computed, and why we use it**
- `run_tmpfs_used_bytes`: direct filesystem used-bytes on `/run` from `df -B1` (`used` column).  
  Why: `/run` is tmpfs in this setup; copy payload written into runtime bundles is reflected here.
- `mem_shmem_kb`: `Shmem` from `/proc/meminfo`.  
  Why: cross-check for tmpfs/shmem pressure at system level; should move with `/run` when tmpfs-backed copy grows.
- `thin_data_used_blocks`, `thin_meta_used_blocks`: used dm-thin blocks from **`dmsetup status`** for the active thin-pool.  
  Why: this is the backend allocation truth for devmapper snapshotter.
- `thin_data_used_bytes = thin_data_used_blocks * data_block_bytes`, `thin_meta_used_bytes = thin_meta_used_blocks * meta_block_bytes`, with block sizes read via **`dmsetup table/status`** (`data_block_bytes=65536`, `meta_block_bytes=4096` in this run).  
  Why: byte-level comparison is easier to interpret than blocks while keeping dm-thin semantics.
- `sample_monrootfs_du_x_bytes`: run `du -sx` on one container's `monRootfs` (`-x` means same filesystem only).  
  Why: detects whether there is real payload materialized inside tmpfs-backed `monRootfs` (copy path), without counting bind-mounted data from other filesystems.

**Meaningful raw excerpts (stable - before in the same run)**

Column meanings:
- `view-before` / `no-view-before`: baseline value measured right before sequential startup
- `view-stable` / `no-view-stable`: stable value measured after all 50 containers are up and settled for 60 seconds
- `view-delta` / `no-view-delta`: computed as `stable - before` for the same mode

| Metric | view-before | view-stable | view-delta | no-view-before | no-view-stable | no-view-delta |
|---|---:|---:|---:|---:|---:|---:|
| `thin_data_used_bytes` | 114,622,464 | 167,051,264 | +52,428,800 | 114,622,464 | 167,051,264 | +52,428,800 |
| `thin_meta_used_bytes` | 18,104,320 | 18,718,720 | +614,400 | 18,104,320 | 18,718,720 | +614,400 |
| `thin_data_used_blocks` | 1,749 | 2,549 | +800 | 1,749 | 2,549 | +800 |
| `thin_meta_used_blocks` | 4,420 | 4,570 | +150 | 4,420 | 4,570 | +150 |
| `run_tmpfs_used_bytes` | 2,744,320 | 6,025,216 | +3,280,896 | 2,744,320 | 370,769,920 | +368,025,600 |
| `mem_shmem_kb` | 11,964 | 15,172 | +3,208 | 11,968 | 371,368 | +359,400 |

Additional raw excerpt during start progression (`after_start_i_of_50`):
- At `i=10`: view `run_tmpfs_used_mib=3.2461`, no-view `72.8125` (gap `69.5664`)
- At `i=20`: view `3.8711`, no-view `143.0078` (gap `139.1367`)
- At `i=30`: view `4.4961`, no-view `213.2031` (gap `208.7070`)
- At `i=40`: view `5.1211`, no-view `283.3984` (gap `278.2773`)
- At `i=50`: view `5.7461`, no-view `353.5938` (gap `347.8477`)

`sample_monrootfs_du_x_bytes` excerpt:
- view path: ~`4096` bytes
- no-view path: ~`7,311,360` bytes

Interpretation:
- `/run` and `Shmem` diverge strongly only in no-view, matching the copy-path hypothesis.
- dm-thin counters (`thin_*` blocks/bytes) are identical between view/no-view, so we do not observe additional devmapper thin-pool overhead from view.
- The `du -sx` split (`4096` vs `7,311,360`) matches bind-mount vs copy behavior in implementation.

For additional robustness, we can still add 1-2 rounds (ABAB or swapped order) with the same stable-point methodology.

### 8.2 中文回复稿（内部同步）

感谢 15/04/2026 会议反馈。我们已按“以存储评估为主（非进程内存）”完成一轮 `view vs no-view` 对照。以下口径统一基于最新 `N=50` 结果，采样点定义为：
- 基线采样点：顺序启动开始前
- 稳定采样点：50 个容器全部启动后再静置 60 秒

**实验环境与运行模型**
- 宿主机：Linux `6.11.0`
- 容器栈：containerd + urunc（`RUNTIME=io.containerd.urunc.v2`）
- snapshotter：`devmapper`
- 镜像：`harbor.nbfc.io/nubificus/urunc/busybox-qemu-linux-raw:latest`
- 运行模型：单轮顺序对照（`view` 与 `no-view` 各 1 次，顺序 `view -> no-view`）

**每个指标怎么计算，为什么用它**
- `run_tmpfs_used_bytes`：`df -B1 /run` 的 `used` 字段。  
  原因：本环境 `/run` 是 tmpfs，运行期 bundle 的 copy 写入会直接体现在这里。
- `mem_shmem_kb`：`/proc/meminfo` 的 `Shmem`。  
  原因：从系统内存角度交叉验证 tmpfs/shmem 压力，应与 `/run` 同向变化。
- `thin_data_used_blocks`、`thin_meta_used_blocks`：从 **`dmsetup status`** 获取 dm-thin pool 的已用 block。  
  原因：这是 devmapper snapshotter 后端真实分配状态。
- `thin_data_used_bytes = thin_data_used_blocks * data_block_bytes`，`thin_meta_used_bytes = thin_meta_used_blocks * meta_block_bytes`；block size 来自 **`dmsetup table/status`**（本次 `65536/4096`）。  
  原因：保留 dm-thin 语义，同时提供直观的字节级对比。
- `sample_monrootfs_du_x_bytes`：对某个容器的 `monRootfs` 执行 `du -sx`（`-x` 仅统计同一文件系统）。  
  原因：用于识别 tmpfs 上是否真的落了实体 copy，避免把跨文件系统 bind-mount 数据算进去。

**有意义的原始数据摘录（同一轮里 stable-before）**

列含义说明：
- `view-before` / `no-view-before`：对应模式在“顺序启动开始前”的原始值
- `view-stable` / `no-view-stable`：对应模式在“50 个容器全部启动并静置 60 秒后”的原始值
- `view-delta` / `no-view-delta`：同一模式下按 `stable - before` 计算得到的增量

| 指标 | view-before | view-stable | view-delta | no-view-before | no-view-stable | no-view-delta |
|---|---:|---:|---:|---:|---:|---:|
| `thin_data_used_bytes` | 114,622,464 | 167,051,264 | +52,428,800 | 114,622,464 | 167,051,264 | +52,428,800 |
| `thin_meta_used_bytes` | 18,104,320 | 18,718,720 | +614,400 | 18,104,320 | 18,718,720 | +614,400 |
| `thin_data_used_blocks` | 1,749 | 2,549 | +800 | 1,749 | 2,549 | +800 |
| `thin_meta_used_blocks` | 4,420 | 4,570 | +150 | 4,420 | 4,570 | +150 |
| `run_tmpfs_used_bytes` | 2,744,320 | 6,025,216 | +3,280,896 | 2,744,320 | 370,769,920 | +368,025,600 |
| `mem_shmem_kb` | 11,964 | 15,172 | +3,208 | 11,968 | 371,368 | +359,400 |

启动过程中的分段摘录（`after_start_i_of_50`）：
- i=10：view `run_tmpfs_used_mib=3.2461`，no-view `72.8125`（差 `69.5664`）
- i=20：view `3.8711`，no-view `143.0078`（差 `139.1367`）
- i=30：view `4.4961`，no-view `213.2031`（差 `208.7070`）
- i=40：view `5.1211`，no-view `283.3984`（差 `278.2773`）
- i=50：view `5.7461`，no-view `353.5938`（差 `347.8477`）

`sample_monrootfs_du_x_bytes` 摘录：
- view：约 `4096` bytes
- no-view：约 `7,311,360` bytes

结论解释：
- `/run` 与 `Shmem` 只在 no-view 明显抬升，符合 copy 路径预期。
- `thin_*`（blocks/bytes）在 view/no-view 下一致，未观察到 view 增加 devmapper thin-pool 开销。
- `du -sx` 的 `4096` vs `7,311,360` 与 bind-mount vs copy 路径一致。

若需进一步增强统计稳健性，可在同口径下增加 1-2 轮 ABAB 或 swap-order。

### 8.3 N=50 单轮复现实验（2026-04-20）

本节补充一次更大规模（`N=50`）单轮对照（`view` + `no-view` 各一次），用于验证 `N=20` 结论在更高并发容器数下是否保持一致。

#### 8.3.1 运行命令与输出

```bash
cd /root/zxd/urunc
N=50 SAMPLE_EVERY=1 \
RESOURCE_START_SAMPLE_DELAYS=5,20,60 \
RESOURCE_CLEANUP_SAMPLE_DELAYS=5,20,60 \
bash scripts/perf/run_view_noview_seq200_compare.sh
```

- 输出目录：`/tmp/urunc-view-noview-seq50-20260420-232748/`
- 关键文件：
  - `view_seq50.tsv`
  - `noview_seq50.tsv`
  - `summary.json`

#### 8.3.2 稳定点口径与结果（stable - before）

- baseline point: before sequential startup begins
- stable point: after all 50 containers are up and settled for 60 seconds

| 指标 | view delta | no-view delta | 结论 |
|---|---:|---:|---|
| `run_tmpfs_used_mib` | +3.1289 | +350.9766 | no-view 在 `/run(tmpfs)` 显著增加 |
| `mem_shmem_mib` | +3.1328 | +350.9766 | no-view 的 shmem 显著增加 |
| `thin_data_used_mib` | +50.0000 | +50.0000 | 二者一致 |
| `thin_meta_used_mib` | +0.5859 | +0.5859 | 二者一致 |
| `thin_data_used_blocks` | +800 | +800 | 二者一致 |
| `thin_meta_used_blocks` | +150 | +150 | 二者一致 |

补充观察（来源：`summary.json`）：

- `run_containerd_mib` 增量两侧接近：view `+600.9683`，no-view `+600.7331`。
- `after_start_i_of_50` 过程中，`run_tmpfs_used_mib` 随 i 增长持续分离（i=10/20/30/40/50 对应差值约 69.6/139.1/208.7/278.3/347.8 MiB）。
- `sample_monrootfs_du_x_bytes` 在 `noview` 路径维持 `7,311,360 bytes`（约 6.97 MiB）量级，`view` 路径保持 `4096 bytes` 级别，和 `N=20` 的模式一致。

结论：`N=50` 单轮结果与 `N=20` 结论一致，即收益主要来自消除 no-view 路径在 `/run(tmpfs)` 的 copy，而非将开销转移到 devmapper thin-pool。

