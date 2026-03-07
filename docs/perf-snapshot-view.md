## devmapper snapshot view 性能记录（view vs non‑view）

### 测试环境与前置条件

- **OS**: Ubuntu 22.04（内核 5.15）
- **containerd**: 使用 `devmapper` snapshotter
- **镜像**: `harbor.nbfc.io/nubificus/urunc/nginx-qemu-linux-raw:latest`
- **测量方式**: `/usr/bin/time -f "%e"` 包裹 `sudo nerdctl run -d ...`，统计单次容器启动耗时（unikernel 起 VM 冷启动）
- **脚本**:
  - **no view**: `scripts/perf/run_noview.sh`（`RUNTIME=io.containerd.uruncnv.v2`）
  - **view**: `scripts/perf/run_view.sh`（`RUNTIME=io.containerd.uruncv.v2`）
  - 公共参数：`SNAPSHOTTER=devmapper`, `RUNS=10`, `SLEEP=2`

### containerd 配置方式（与本次测试相关的部分）

- **devmapper snapshotter**（节选，自 `docs/installation.md`）：

```toml
[plugins.'io.containerd.snapshotter.v1.devmapper']
  pool_name = "containerd-pool"
  root_path = "/var/lib/containerd/io.containerd.snapshotter.v1.devmapper"
  base_image_size = "10GB"
  discard_blocks = true
  fs_type = "ext2"
```

- **urunc 相关 runtime（低层 runtime v2 入口）**：

```toml
[plugins.'io.containerd.runtime.v2.task'.io.containerd.uruncv.v2]
  # 使用 urunc 提供的 shim
  runtime_type = "io.containerd.urunc.v2"

[plugins.'io.containerd.runtime.v2.task'.io.containerd.uruncnv.v2]
  # 同样的 shim，只是禁用 snapshot view 的变体
  runtime_type = "io.containerd.urunc.v2"
```

- **（可选）CRI 层 urunc runtime（如需通过 Kubernetes 使用）**：

```toml
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.urunc]
  runtime_type = "io.containerd.urunc.v2"
  container_annotations = ["com.urunc.unikernel.*"]
  pod_annotations       = ["com.urunc.unikernel.*"]
  snapshotter           = "devmapper"
```

> 说明：benchmark 中直接用 `nerdctl --runtime io.containerd.urunc{v,nv}.v2 --snapshotter devmapper`，因此关键在于 `runtime.v2.task` 段和 devmapper 配置。

### 修改前的基线结果（初始 view 实现）

- **脚本参数**: `RUNS=10`, `SLEEP=2`
- **no view（io.containerd.uruncnv.v2）**：
  - 单次 run 耗时（秒）：`1.04, 1.33, 1.98, 2.29, 2.35, 2.70, 3.08, 3.15, 3.58, 3.72`
  - **粗略均值**: 约 2.5–2.6 s
- **view（io.containerd.uruncv.v2，启用 containerd snapshot view）**：
  - 单次 run 耗时（秒）：`4.84, 4.30, 4.47, 4.99, 5.07, 5.27, 5.62, 5.69, 6.41, 6.63`
  - **粗略均值**: 约 5.3–5.5 s
- **现象**：
  - view 路径比 non‑view 明显更慢（约 2 倍启动时延），
  - 瓶颈主要在 shim 端每次 `CreateSnapshotView` 都新建 containerd client + devmapper view + mount + lease 的固定开销。

### 修改后的结果（shim 复用 containerd client 之后）

修改点（提交 93bcb9b8 之后）：

- **在 `pkg/shiminject/inject.go` 中增加进程级共享 client**：
  - 使用 `sync.Once` 初始化一次 `containerd.New(...)`。
  - `CreateSnapshotView` 和 `CleanupSnapshotView` 都通过 `getContainerdClient()` 复用该 client。
  - 每次调用时用 `namespaces.WithNamespace(ctx, ...)` 在同一个 client 上切换 namespace。

在相同环境和脚本参数下重新跑：

- **no view（io.containerd.uruncnv.v2）**：
  - 单次 run 耗时（秒）：`0.89, 0.85, 0.85, 0.93, 0.99, 0.97, 0.89, 0.87, 0.96, 0.85`
  - **粗略均值**: 约 0.9 s
- **view（io.containerd.uruncv.v2）**：
  - 单次 run 耗时（秒）：`1.20, 1.29, 1.28, 1.18, 1.06, 1.09, 1.09, 1.24, 1.11, 1.09`
  - **粗略均值**: 约 1.1–1.2 s

### 当前结论（仅针对短生命周期 benchmark）

- **两条路径都因优化受益明显**：整体启动时延从数秒级下降到 ~1 秒级。
- **view 仍然略慢于 non‑view（~1.2 s vs ~0.9 s）**：
  - 减少了每次新建 client 的开销，但 devmapper `View + mount + lease` 本身仍是固定成本。
  - 对于这种“起容器就立刻停掉”的短生命周期测试，这部分固定开销仍然足以让 view 稍慢于 non‑view。

### view 实现的耗时拆解（基于一次典型 run 的日志）

通过在 shim 与 urunc 内部打点，观测到一次启容器过程中，**view 路径相比 no‑view 额外多出的关键步骤及耗时大致如下**（单位均为毫秒，来自一条代表性日志）：

- **shim / containerd 侧（`subsystem=shiminject`）**
  - `containerd.New`：约 **2 ms**
  - `ContainerService.Get`：约 **2 ms**
  - `SnapshotService.Stat`（devmapper 下用于找到 committed parent）：约 **0 ms**
  - **`SnapshotService.View`（创建只读 snapshot view）**：约 **128 ms**
  - `mount.All`（把 view 挂到 `/run/urunc/views/<containerID>`）：约 **9 ms**
  - `LeasesService.Create`（创建 GC 保护租约）：约 **9 ms**
  - `LeasesService.AddResource`（把 view 加到 lease 中）：约 **16 ms**
  - `config.json.update`（读取+修改+写回 bundle `config.json` 注入 view mount 路径）：约 **4 ms**
  - **小结**：上述步骤合计约 **170 ms** 的额外开销，其中 `SnapshotService.View` 是主因，其次是 lease 相关调用。

- **urunc 侧 view 专用路径（`subsystem=unikontainers`，仅在 `FromSnapshotView=true` 时发生）**
  - `bindViewFilesToMonRootfs`：从 view 里 **bind‑mount 出 unikernel/initrd/urunc.json 到 monitor rootfs**；
  - `copyMountfiles`：将所有 bind mount 内容复制到 active rootfs（语义与 no‑view 路径一致，只是现在不再复制 unikernel/initrd/urunc.json 本身）；
  - `prepareDMAsBlock`（仅 no‑view 路径会调用，用于真实复制那 3 个文件）：在一次代表性日志中，`extractFilesFromBlock` 的 `duration_ms` 约为 **13 ms**，`prepareDMAsBlock` 里记录到的 `extract_ms` 约为 **13 ms**、`unmount_ms` 约为 **1 ms**、`total_prepare_ms` 约为 **14 ms**；
  - `mount.Unmount(rfs.MountedPath)`：在 view 路径中卸载 active rootfs，以便复用其块设备；
  - `setupDev(rfs.MonRootfs, rfs.Path)`：在 monitor rootfs 里创建设备节点；
  - 对这些步骤分别打点后看到，**每步的耗时都显著低于 shim 侧的 `SnapshotService.View`**，整体加起来远小于上面约 170 ms 的控制面成本；并且根据当前这条日志，**我们通过 view 所避免的“3 个文件 copy + 卸载”的额外 CPU/IO 开销量级大约在十几毫秒左右**。

结合 benchmark 结果：

- `no-view` 平均 **~0.9 s**，`view` 平均 **~1.1–1.2 s**，差值 **0.2–0.3 s**；
- 其中约 **0.17 s** 可以直接由上面 “创建并使用 snapshot view” 的额外开销解释（主要是 devmapper 的 `SnapshotService.View`），
- **而被我们省掉的 unikernel/initrd/urunc.json 文件拷贝，在 SSD + page cache 场景下通常只需几十毫秒以内，因此很难抵消新增的 view 成本**。

### 冷缓存单次对比（drop_caches 后各跑一次）

为了大致估算 “完全冷缓存” 下 copy 与 view 的绝对开销，又在每次 run 前执行：

```bash
sudo sync
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
```

然后分别只跑 **一次**（同一镜像与 devmapper snapshotter）：

- **no view（`io.containerd.uruncnv.v2`）**：
  - 单次 run 耗时：`1.88 s`
- **view（`io.containerd.uruncv.v2`）**：
  - 单次 run 耗时：`1.89 s`

在这种近似“全冷启动”的情况下，两条路径的耗时几乎一致，说明：

- 冷缓存时，**磁盘 IO（镜像数据 + rootfs 读）主导了整体启动时间**，view 的额外控制面成本（~170 ms）被分页缓存缺失带来的 IO 抖动“淹没”掉了；
- 同时也侧面印证：**我们省掉的那几个文件 copy 在冷缓存场景下确实有成本，但和整个 VM/unikernel 冷启动 + devmapper IO 相比，占比依然不算大。**

### view 优化的真正适用场景讨论

从上述实验可以看出，在“非常短生命周期的容器 + 每次冷启动都立即 stop”的基准里，view 方案更多是：

- **换掉少量文件 copy，增加一次 devmapper view 创建 + 挂载 + 注解 +（可选）租约的控制面开销**，
- 在常见 SSD + 热 cache 的环境下，这会让启动时间略微变长（0.2–0.3 s）。

view 的优势更适合以下场景：

- **长生命周期或多次复用的容器 / sandbox**：
  - 我们不再为每个实例复制 unikernel/initrd/`urunc.json` 到新的 rootfs，而是一直从只读 view 里 bind 读取，
  - 对于大镜像或频繁滚动部署，可以显著减少 `/run` 等本地存储的写放大和碎片。
- **关注存储占用、GC 与一致性而不是纯启动延迟的环境**：
  - 所有副本共享同一个只读 snapshot view，避免“每个 unikernel 实例一份 artifact 副本”的空间浪费，
  - 当镜像层更新时，通过 snapshot view 可以更容易地保证读到的是一致的 committed snapshot，而不是某个正在变动的 active 设备。
- **后续可以进一步优化控制面的场景**：
  - 例如：重用一个进程级 `containerd.Client`、考虑复用或去掉 lease、在特定 snapshotter（非 devmapper）或某些 workload 下关闭 view，仅在“大镜像 + 多实例”才启用。

综合来说：

- **如果目标是极致的“单次冷启动延迟”**，no‑view 目前仍然略占优势；
- **如果更关心长期运行中的存储占用、复制次数和一致性**，view 通过去掉重复文件拷贝、统一从只读 snapshot 读数据，在这类场景下会更有价值。

### 关于测试稳定性和其它需要补充的测试

当前的基准主要是单机上的 `RUNS=10` 冷/热启动对比，结论已经足够解释 view 与 no‑view 在启动时延上的关系。但为了让结论在更广泛的环境下更稳健，后续可以考虑：

- **提升性能测试的稳定性**
  - 在同一台、尽量空闲的机器上测试，并固定 CPU 频率/电源策略（避免 Turbo / 省电模式带来的波动）。
  - 对每种配置跑更多轮次（例如 `RUNS=30` 或以上），统计 **中位数、p95**，而不是只看均值或单次结果。
  - 交错运行：`no-view → view → no-view → view ...`，避免“先后顺序”导致 cache / 预热偏差。
  - 将脚本中的 `sleep`、后台清理动作保持一致，并在需要时明确区分“冷缓存”（drop_caches）与“热缓存”（多次重复后）的场景。

- **从其它维度补充测试**
  - **不同镜像大小与类型**：不仅是 nginx‑qemu‑linux‑raw，小镜像/超大镜像（几百 MB+）分别测试，观察 view 在大镜像场景下对写放大/启动时间的影响是否更明显。
  - **不同 snapshotter**：对比 devmapper 与 blockfile（以及将来可能支持的其它 block‑based snapshotter），确认 view 行为和收益的一致性或差异。
  - **长生命周期 workload**：运行长时间存在的 unikernel（例如持续数分钟/小时的服务），统计：
    - `/run` 或其它本地路径上的额外文件写入大小；
    - devmapper thinpool 的使用情况随时间的变化；
    - 多次滚动部署 / 重启时是否减少了临时文件和碎片。
  - **压力与并发场景**：同时启动 N 个容器（如 5、10、50 个），比较：
    - view / no‑view 在并发启动时间上的差异；
    - devmapper thinpool、IOPS、延迟的变化；
    - 是否出现资源竞争或 GC 行为（特别是 view 的 lease 与 snapshot 回收）。
  - **健壮性与清理测试**：
    - 模拟异常退出（shim crash / containerd restart），验证 view 造成的 mount / snapshot 是否会被正确清理（依赖 shim 的 CleanupSnapshotView + containerd GC）。
    - 在 K8s/CRI 环境下测试：确保 CRI 层 runtime 配置下，view 与 no‑view 均能正确工作并清理资源。

这些补充测试有助于更全面地回答一个问题：**在真实生产工作负载中，引入 view 是否能显著改善存储行为和长期运行特性，而不仅仅是对“单次短启动时延”的微调。**

