## devmapper snapshot view 测试前环境清理说明

这份文档只保留测试前的环境清理方法，不保留历史实验数据。

### 目标

在每次启动测试前，尽量把和 `nerdctl + urunc + devmapper` 相关的旧状态清掉，避免上一次测试的容器、shim、task 或 snapshot 影响下一次结果。

### 推荐做法

每次测试前先运行：

```bash
NS=<benchmark-namespace> PRUNE_IMAGES=1 scripts/perf/reset_urunc_bench_state.sh
```

建议使用较短的 benchmark namespace，例如：

```bash
NS=ub PRUNE_IMAGES=1 scripts/perf/reset_urunc_bench_state.sh
```

`NS` 建议保持较短，是因为 `urunc` 在 `/run/containerd/...` 下创建 Unix socket，namespace 过长时可能触发路径长度问题。

### 清理脚本会做什么

[`scripts/perf/reset_urunc_bench_state.sh`](../scripts/perf/reset_urunc_bench_state.sh)
会清理以下内容：

- 指定 namespace 下的 `io.containerd.urunc.v2` 容器
- 指定 namespace 下的 task
- 指定 namespace 下残留的 `containerd-shim-urunc-v2`
- 这些 shim 的子进程
- 指定 namespace 下 `devmapper` snapshotter 的 active snapshot
- 当 `PRUNE_IMAGES=1` 时，也尝试删除该 namespace 下不再需要的 committed snapshot 与镜像引用

脚本最后会输出：

- `remaining_containers`
- `remaining_tasks`
- `remaining_shims`
- `remaining_snapshots`
- `global_dmsetup_devices`

这些字段可以作为“本轮测试前环境是否清干净”的快速检查项。

### 建议的测试前检查

执行 reset 后，建议至少确认：

```bash
ctr -n <benchmark-namespace> c ls
ctr -n <benchmark-namespace> tasks ls
ctr -n <benchmark-namespace> snapshots --snapshotter devmapper ls
dmsetup ls
```

理想状态是：

- benchmark namespace 下没有容器
- benchmark namespace 下没有 task
- benchmark namespace 下没有 `devmapper` snapshot
- `dmsetup ls` 只剩池本身和系统盘映射

### 相关脚本

- [`scripts/perf/reset_urunc_bench_state.sh`](../scripts/perf/reset_urunc_bench_state.sh)
- [`scripts/perf/run_noview.sh`](../scripts/perf/run_noview.sh)
- [`scripts/perf/run_view.sh`](../scripts/perf/run_view.sh)

其中 `run_noview.sh` 和 `run_view.sh` 已接入测试前后的 reset 流程，可作为后续重新整理 benchmark 时的基础脚本。
  - 首次 shared view 创建过重；
  - 后续请求即使不拿锁，也必须等待该 view 真正 ready。

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

### 2026-03-28 补充实验：main vs cherry-main 的并发 / warm 启动

本轮补充实验针对用户关心的两条构建路径：

- **no-view**：`main` 分支，执行 `go mod vendor && make install`
- **view**：`cherry-main` 分支，执行 `go mod vendor && make install`

实验约束与说明：

- 镜像仍为 `harbor.nbfc.io/nubificus/urunc/nginx-qemu-linux-raw:latest`
- snapshotter 仍为 `devmapper`
- 记录的是 `nerdctl run -d ...` 返回的 wall-clock 时间
- **镜像拉取不计入计时**：每轮开始前先确认镜像已在本地，避免把首次 pull 算进启动时间
- **并发启动**：同时发起 `N` 个 `nerdctl run -d`
- **warm 启动**：先启动 1 个 keeper 容器保持不退出，随后顺序启动 `N` 个容器，每个容器启动完成后立刻删除
- 由于该机器上存在活跃的 `k8s.io` namespace workload，本轮只清理 `default` namespace 中带测试前缀的容器，不做全局清理
- 如果某一档测试在 **5 分钟内没有完成**，则该档记为 `skipped`

#### 当前已确认可用的数据

##### no-view（main）

- **并发 10**
  - 批次总耗时：`32013 ms`
  - 单容器返回：`n=10, avg=23146.70 ms, min=13894 ms, max=32004 ms`
- **并发 32**
  - 批次总耗时：`105729 ms`
  - 单容器返回：`n=32, avg=70732.25 ms, min=31623 ms, max=105713 ms`
- **并发 64**
  - 批次总耗时：`218700 ms`
  - 单容器返回：`n=64, avg=146028.59 ms, min=64419 ms, max=218677 ms`
- **warm 10**
  - 单容器返回：`n=10, avg=5075.10 ms, min=3947 ms, max=6233 ms`
- **warm 32**
  - 单容器返回：`n=32, avg=4399.06 ms, min=4071 ms, max=5164 ms`

##### view（cherry-main）

- **并发 10**
  - 批次总耗时：`32034 ms`
  - 单容器返回：`n=10, avg=28105.10 ms, min=20851 ms, max=32024 ms`
- **并发 32**
  - 批次总耗时：`123588 ms`
  - 单容器返回：`n=32, avg=78475.84 ms, min=42105 ms, max=123575 ms`
- **并发 64**
  - 批次总耗时：`121784 ms`
  - 单容器返回：`n=64, avg=111487.77 ms, min=77008 ms, max=121761 ms`
- **warm 10**
  - 单容器返回：`n=10, avg=9920.00 ms, min=8845 ms, max=11334 ms`

#### 超时 / 部分完成的数据

- **no-view warm 64**
  - 5 分钟超时前完成 `50/64`
  - 已收样本平均：`4415.80 ms`
- **no-view 并发 128**
  - 5 分钟超时前完成 `75/128`
  - 已收样本平均：`209824.52 ms`
- **view 并发 128**
  - 5 分钟超时前完成 `76/128`
  - 已收样本平均：`216509.91 ms`
- **view warm 32**
  - 5 分钟超时前完成 `27/32`
  - 已收样本平均：`9246.59 ms`
- **view warm 64**
  - 5 分钟超时前完成 `26/64`
  - 已收样本平均：`9745.50 ms`

#### 当前对这些数据的解读边界

- `view` 的 **并发 64** 结果文件已经确认有 **64 个唯一容器名** 和 **64 个唯一容器 ID**，因此可以确认这 64 个 `run -d` 都返回完成了
- 但是，**不能仅凭当前这批数据就下结论说 view 在 64 并发下明显优于 no-view**
- 原因是：
  - `view 64` 这轮是在后半段用更严格、单配置、5 分钟封顶的流程跑出来的
  - `no-view 64` 则来自更早一轮较“脏”的批量跑法，中间经历过并发清理与残留干扰
- 因此，当前更稳妥的表述应是：
  - **观察到** `view 64` 的 `run -d` 返回时间低于当前手里的 `no-view 64`
  - 但要把它上升为结论，仍需要在更隔离、更一致的环境里重跑 `no-view 64` 做严格对照

#### 当前缺失项

- `view warm 128`
- `view` 路径在本轮规模下的 phase/timestamp 归因分析

后续如果能在不承载现有 `k8s.io` workload 的隔离节点上继续测试，建议优先补齐：

- `view warm 10 / 32 / 64`
- `no-view 64` 的干净重跑
- `URUNC_PROFILE_STARTUP=1` 下的 `view` 并发与 warm 拆解

### 2026-03-28 补充实验（二）：每轮前全量清理 urunc/devmapper 运行态后的 view warm

在用户确认“可以影响当前 k8s/urunc workload”后，又补做了一轮更激进的
`cherry-main / view` warm 启动实验。和上面的测试相比，这一轮在**每个测试前**
都执行了更强的 reset：

- `systemctl stop kubelet`
- 删除 `default` 与 `k8s.io` namespace 中所有 `io.containerd.urunc.v2` 容器
- 杀掉宿主机上残留的 `containerd-shim-urunc-v2`、`urunc`、`/opt/urunc/bin/qemu-system`、`firecracker`、`cloud-hypervisor`
- 卸载 `/run/urunc/*` 下的挂载
- `systemctl restart containerd`
- 确认镜像已在本地后，再开始计时

测试结束后，已重新执行：

```bash
sudo systemctl start kubelet
```

#### full-reset 下的 view warm 结果

- **warm 10**
  - 单容器返回：`n=10, avg=4350.80 ms, median=4257.50 ms, min=4037 ms, max=4708 ms`
- **warm 32**
  - 单容器返回：`n=32, avg=3811.16 ms, median=4345.00 ms, min=496 ms, max=11450 ms`
- **warm 64**
  - 单容器返回：`n=64, avg=4296.70 ms, median=4224.50 ms, min=3869 ms, max=6433 ms`

#### 对这轮 full-reset warm 结果的观察

- 和前一轮“不做全量 reset”的 `view warm 10`（约 `9.9 s`）相比，这一轮 `warm 10`
  明显下降到约 `4.35 s`
- 这说明在当前机器上，**测试前遗留的 urunc/containerd/devmapper 运行态**会显著影响 warm 启动结果
- `warm 32` 中出现了一组异常低值（`496/502/515/531/545 ms`），因此这一档不能只看均值：
  - 均值：`3811.16 ms`
  - 中位数：`4345.00 ms`
- 从中位数看，`warm 10 / 32 / 64` 三档在这轮 full-reset 条件下都大致落在 **4.2–4.3 s**
  左右，没有随着规模线性恶化
