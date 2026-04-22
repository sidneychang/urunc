# devmapper thin-pool 指标校验与 view/no-view 归因实验记录

本文记录两件事：

1. **修正** `scripts/perf/lib_bench_common.sh` 中基于 `dmsetup status` 的 `thin_*_used_bytes` 计算口径（旧实现低估 1024×）。
2. 在 **view** 与 **no-view** 两种实现下做对照实验，验证 maintainer 的观点：**thin-pool（dm/lv）层面的存储开销应在稳定采样点上无差异**；观测到的 meta 差异主要来自 **采样时序（transient）**。

> 环境特征：本机 `containerd-pool` 为 dm-thin pool（`lvs` 未展示对应 thin-pool LV），因此 thin 指标来源为 `dmsetup`。

---

## 1. 背景：`dmsetup status` 的单位与旧实现错误

本机 `dmsetup status containerd-pool` 形如：

```text
0 209715200 thin-pool 0 4420/2621440 1749/1638400 - rw ... - 1024
```

其中：

- `4420/2621440`、`1749/1638400` 的单位是 **blocks**
- 最后一个字段 `1024` 是 **blocksize_sectors**（每个 block 包含 1024 个 512B 扇区）
- 默认 `sector_bytes = 512`

因此 blocks → bytes 的正确换算是：

\[
\text{bytes} = \text{blocks} \times \text{blocksize\_sectors} \times \text{sector\_bytes}
\]

### 1.1 旧实现的问题

旧实现把 `blocks` 误当成 `sectors`，只乘了 `512`，漏乘了 `blocksize_sectors(=1024)`，导致：

- `thin_data_used_bytes` / `thin_meta_used_bytes` **低估 1024×**，且旧实现用于解析 `dmsetup status` 时把 `data/meta` 的字段顺序也弄反了（导致 TS V 列语义互换）
- `thin_data_pct` / `thin_meta_pct` **不受影响**（仍然来自 used/total 的比值）

验证用的基线换算（直接从 `dmsetup status` 得到）：

```bash
sudo dmsetup status containerd-pool | awk '
{
  for (i=1;i<=NF;i++) if ($i=="thin-pool") { j=i; break }
  # Kernel status order is: <meta_used>/<meta_total> <data_used>/<data_total>
  split($(j+2), m, "/"); split($(j+3), d, "/");
  bs=$(NF); ss=512;
  du=d[1]*bs*ss; dt=d[2]*bs*ss;
  mu=m[1]*bs*ss; mt=m[2]*bs*ss;
  printf "data_blocks=%s/%s meta_blocks=%s/%s blocksize_sectors=%s\n", d[1],d[2],m[1],m[2],bs;
  printf "data_used_bytes=%0.f data_total_bytes=%0.f data_pct=%.6f\n", du, dt, 100*d[1]/d[2];
  printf "meta_used_bytes=%0.f meta_total_bytes=%0.f meta_pct=%.6f\n", mu, mt, 100*m[1]/m[2];
}'
```

该基线在当时输出为（示例）：

- `data_used_bytes=916979712`
- `meta_used_bytes=2317352960`
- `blocksize_sectors=1024`

而旧脚本同一时刻 TSV 中对应字段为：

- `thin_data_used_bytes=2263040`
- `thin_meta_used_bytes=895488`

两者比值均为 **1024**（硬证据）。

其中：`thin_data_used_bytes` 对应的是 `meta_used_bytes`（旧列与正确列语义互换），而 `thin_meta_used_bytes` 对应的是 `data_used_bytes`。

---

## 2. 修复：正确按 blocksize(sectors) 折算 bytes

修复位置：`scripts/perf/lib_bench_common.sh`

- 替换解析函数为 `_bench_dmsetup_thin_pool_bytes()`：
  - 读取 status 行末尾 `blocksize_sectors`
  - 将 `data/meta used/total` 从 blocks 换算到 bytes：`blocks * blocksize_sectors * sector_bytes`
- `_bench_devmapper_thin_metrics()` 中的 dmsetup 分支改为调用新函数

修复后，运行最小实验 `sequential devmapper 1` 的 `before_*` 行里：

- `thin_data_used_bytes` / `thin_meta_used_bytes` 与上面的 awk 直接换算 **完全一致**

这验证了 used_bytes 的口径已正确。

---

## 3. 旧数据如何解释（重要）

如果旧脚本输出的 `thin_*_used_bytes` 来自 `dmsetup`，且本机 `dmsetup status` 行末尾 `blocksize_sectors=1024`，则：

- **新口径 = 旧口径 × 1024**
- 两种实现的差值 `Δ` 也同样 **× 1024**

注意：这只修正 `used_bytes` 的绝对值；`pct` 一般无需修正。

---

## 4. view vs no-view：100 轮对照（TSV v2）

执行方式（按实际切换规则）：

- **view**：保持当前安装（当时为 view），直接跑 100
- **no-view**：`sudo cp /usr/local/bin/bak/* /usr/local/bin/` 后再跑 100

产物：

- view：`/tmp/urunc-devmapper-view-100.tsv`
- no-view：`/tmp/urunc-devmapper-noview-100.tsv`

观测到的主要结论（在 `after_start_100_of_100` 采样点）：

- `thin_data_used_bytes`：view 与 no-view **完全一致**
- `thin_meta_used_bytes`：存在小幅差异（no-view 更高，约 24MiB 量级）

这与 maintainer 的直觉一致（data 不变），但 meta 的单点差异需要进一步归因（见下一节）。

---

## 5. 归因实验：多点采样证明 meta 差异主要来自采样时序

为了区分「真实稳定差异」与「transient 采样时序差异」，设计了一个 **多点采样** 实验：

- 正确切换方式：
  - **view**：`sudo make install`
  - **no-view**：`sudo cp /usr/local/bin/bak/* /usr/local/bin/`
- 每轮：
  - reset
  - 启动 50 个容器（保持运行，不立刻 cleanup）
  - 在 after_start 时刻分别在 \(t=0/5/20/60s\) 读取：
    - `dmsetup status containerd-pool`（换算 data/meta bytes + blocks）
    - `ctr snapshots ls` 数量
    - `ctr leases ls` 数量
    - `urunc-shared` snapshot/lease 计数（验证 view-shared 机制确实存在）
  - cleanup + reset 后，再在 \(t=0/5/20/60s\) 读取一次，确认回到基线

### 5.1 关键观测

1) **view 轮确实引入了 shared view snapshot + lease**

- view after_start：`urunc_shared_snapshots=1`、`leases_total=1`、`leases_urunc_shared=1`
- no-view after_start：上述计数为 0

2) **data 一致**

两轮 `data_blocks` / `data_used_bytes` 在同一采样时刻一致。

3) **meta 在早期点会不同，但在 ≥20s 收敛一致**

示例（after_start）：

- view：`meta_blocks` 从 2279 → 2335 → 2399（20s 后稳定）
- no-view：`meta_blocks` 从 2331 → 2371 → 2399（20s 后稳定）

即：**最终稳定点（20s/60s）两者完全一致**。差异仅存在于“t=0、t=5”这种偏早的 transient 采样点。

4) **cleanup 后两轮均立刻回到同一基线并稳定**

排除了“残留资源导致长期偏高”的可能性。

### 5.2 归因结论

- 在 dm-thin 环境中，`dmsetup status` 的 `meta_blocks`（以及折算出的 meta_used_bytes）在启动容器后存在一个 **短时间增长/提交窗口**。
- view/no-view 会改变系统在这一窗口内的 **时间线与交错顺序**（view 轮多了 shared snapshot/lease/mount 等步骤），导致早期采样点可能出现“方向反直觉”的差异。
- 但在稳定采样点（本实验中约 20s 后），两者收敛一致；因此 maintainer 所说的“view/no-view 在 snapshotter 存储层面应无差异”在严格采样条件下成立。

---

## 6. 对 bench 的建议

为了避免将 transient 误当结论，建议：

- 在 `after_start` 增加 settle 多点采样（如 5/20/60s），或在报告中只使用稳定点；
- 保留 `thin_metrics_source`，并在 dmsetup 路径确保 `used_bytes` 使用 blocks→bytes 的正确换算；
- 将 `urunc-shared` snapshot/lease 的计数（或至少 snapshots_total/leases_total）记录到日志/TSV，便于讨论“view 机制是否生效、是否引入额外对象”。

---

## 7. view / no-view：内存归因（PSS + Shmem + 稳定采样）

前面的结论基本说明：在 dm-thin(devmapper) 后端上，**thin-pool 的 data/meta 在稳定采样点收敛一致**；因此如果要证明 view 机制“优化”，通常需要把收益落在 **tmpfs(/run) copy** 与 **内存（特别是 PSS）** 的可解释指标上，而不是“系统总内存观感”。

### 7.0 我们当前要证明的两件事（对齐 15/04 会议结论）

1. **/run 的增长确实来自 urunc 的 copy 行为（no-view）**，而不是 snapshotter 或其他系统组件的长期存储膨胀。
2. **view snapshot 的存储开销应接近 0（或可忽略）**，需要分别在：
   - **snapshotter 层（thin data/meta / snapshotter root）**
   - **/run tmpfs（containerd bundle/monRootfs 等）**
   做可复现的“稳定采样点”对照。

### 7.1 为什么要看 PSS（而不是只看 RSS / MemAvailable）

- `RSS` 会对共享页重复计入，难以判断“新增包/进程”带来的**净开销**。
- `MemAvailable`、`Cached`、`Slab` 容易被 page cache 与内核回收时序影响，用它做主结论会很不稳定。
- `PSS`（Proportional Set Size）来自 `/proc/<pid>/smaps_rollup`，对共享页按比例分摊，更适合做 view/no-view 的主对比指标。

### 7.2 TSV v2 新增列（资源采样）

已在 `scripts/perf/lib_bench_common.sh` 的 TSV v2 增加：

- `mem_shmem_kb`：`/proc/meminfo` 的 `Shmem`
- `containerd_main_pss_kb`：containerd 主进程 PSS
- `shim_urunc_pss_sum_kb`：所有 `containerd-shim-urunc-v2` 进程 PSS 汇总
- `qemu_pss_sum_kb`：所有 `qemu-system*` 进程 PSS 汇总
-
- `sample_monrootfs_du_x_bytes` / `sample_monrootfs_du_all_bytes`（新增）：
  - `sample_monrootfs_du_x_bytes`：对一个“正在运行的容器”的 `bundle/monRootfs` 做 `du -sx`（**只统计 monRootfs 同一文件系统上的字节数**，通常就是 `/run` 上的 tmpfs，用于证明“是否发生了真实 copy 占用 /run”）。
  - `sample_monrootfs_du_all_bytes`：对同一目录做普通 `du -s`（会跟随 bind mount 进入 snapshot view 等其他文件系统；**只能作为“文件是否可见”的 supporting signal**，不能用于衡量 /run 的真实占用）。
  - `sample_bundle_path`：该样本对应的 containerd bundle 路径（便于手工复核）。

并增加 **after_start 稳定点采样**（避免 transient）：在 `after_start_N_of_N` 之后可选写入：

- `after_start_N_of_N_settle_5s`
- `after_start_N_of_N_settle_20s`
- `after_start_N_of_N_settle_60s`

通过环境变量打开：

```bash
export RESOURCE_START_SETTLE=1
export RESOURCE_START_SAMPLE_DELAYS=5,20,60
```

### 7.3 ABAB（交替）实验脚本

为了避免“先后顺序/热态”偏差，同时避免切换二进制时出现 `text file busy`（必须保证上一批容器已 cleanup），提供了一个批次式 **ABAB** 顺序启动实验脚本：

- `scripts/perf/run_abab_view_noview_seqbench.sh`

默认行为：

- warmup：先跑一次短 `noview`（不写 TSV）
- 每轮：`noview -> view`（每个模式跑一次顺序 N 容器）
- 每次都会走 `urunc_bench.sh sequential ... --tsv <file>`，并带上 `URUNC_BENCH_LABEL=abab_r<round>_<mode>` 便于后处理
- after_start 与 after_cleanup 都会做 5/20/60s settle 采样

运行示例：

```bash
cd /root/zxd/urunc

# 默认 N=50, ROUNDS=5, OUT_TSV=/tmp/urunc-view-noview-abab.tsv
bash scripts/perf/run_abab_view_noview_seqbench.sh

# 自定义
N=100 ROUNDS=10 OUT_TSV=/tmp/abab.tsv \
  bash scripts/perf/run_abab_view_noview_seqbench.sh
```

### 7.4 TSV 自动分析（输出 ΔPSS/ΔShmem）

提供脚本读取 TSV，并以稳定点 `after_start_N_of_N_settle_60s` 与 `before_sequential_N` 做差：

```bash
python3 scripts/perf/analyze_abab_tsv.py /tmp/urunc-view-noview-abab.tsv --n 50
```

### 7.5 （填写结果）本机实验结论模板

本次在本机（devmapper + dmsetup thin-pool 指标）按 **ABAB** 跑了 2 轮（每轮 50 容器），用稳定点 `after_start_50_of_50_settle_60s` 相对 `before_sequential_50` 做差（Δ），并对两轮取均值：

| mode | ΔPSS_total(mean) | ΔRSS_total(mean) | ΔShmem(mean) | Δthin_data_used_bytes | Δthin_meta_used_bytes |
|---:|---:|---:|---:|---:|---:|
| no-view | ~4705 MiB | ~5330 MiB | **~351 MiB** | +78,643,200 | +419,430,400 |
| view | ~4747 MiB | ~5606 MiB | **~3.1 MiB** | +78,643,200 | +419,430,400 |

（以上由 `python3 scripts/perf/analyze_abab_tsv.py /tmp/urunc-view-noview-abab.tsv --n 50` 生成）

**结论（可引用）：**

- **view 显著降低 Shmem（约 351MiB → 3MiB）**。这与“no-view 在 /run tmpfs 内做 payload copy、view 通过 snapshot view bind-mount 避免 copy”的机制一致；因此 view 在“内存（tmpfs/shmem）”层面确实带来净收益。
- **PSS_total（containerd + shim + qemu）在 view 下略高（约 +41MiB/50 容器）**，量级约 \(< 1MiB/容器\)，相对 Shmem 的节省可忽略；但要强调：PSS 是更可靠的“进程私有/按共享分摊”指标，RSS 的差异不宜作为主结论。
- **thin-pool（data/meta used_bytes）在稳定采样点完全一致**（两种模式的 Δ 相同），与前文的“devmapper 存储层面 view/no-view 收敛一致”结论相符：view 的主要收益不在 thin-pool，而在 **/run tmpfs/shmem**。

> 进一步把“/run 增长来自 copy”做成硬证据时，优先引用 `sample_monrootfs_du_x_bytes`：
> - **no-view**：`du -x` 应显著增大（unikernel/initrd/urunc.json 真正落在 tmpfs）。
> - **view**：`du -x` 应保持很小（这些 payload 来自 snapshot view 的 bind-mount，tmpfs 只存少量目录/元数据）。

### 7.6 小规模（N=20）+ 多采样点（1/5/10/20/60s）

为了验证“绝大多数时间都更好”而不只看单个稳定点，额外跑了一个小规模实验：

- N=20
- ABAB rounds=3（noview 与 view 各 3 次）
- after_start 采样点：`settle_1s,5s,10s,20s,60s`
- TSV：`/tmp/urunc-view-noview-abab-n20.tsv`

用稳定点 `after_start_20_of_20_settle_60s` 相对 `before_sequential_20` 做差（Δ），均值为：

| mode | ΔPSS_total(mean) | ΔRSS_total(mean) | ΔShmem(mean) | Δthin_data_used_bytes | Δthin_meta_used_bytes |
|---:|---:|---:|---:|---:|---:|
| no-view | ~1888 MiB | ~2124 MiB | **~140.4 MiB** | +31,457,280 | +167,772,160 |
| view | ~1909 MiB | ~2241 MiB | **~1.3 MiB** | +31,457,280 | +167,772,160 |

关键观察：

- **从早期点开始（1s 起）view 的 Shmem 就显著更低**，并且一直保持到 60s（符合“避免 /run tmpfs copy”的机制预期）。
- PSS_total 仍呈现 view 略高、但量级远小于 Shmem 节省的现象。

