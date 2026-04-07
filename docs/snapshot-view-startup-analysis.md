# snapshot view 启动时延分析与替代方案

本文基于当前仓库里的 `perf-snapshot-view` 记录、`urunc` 现有实现，以及
`containerd` `devmapper` snapshotter 的源码行为，分析下面两个问题：

- 为什么在 `devmapper` 下启用 `snapshot view` 之后，启动延迟反而更差；
- 如果目标是提升启动速度，`snapshot view` 是否有帮助；如果没有，是否存在
  overhead 更小、同时又能避免 `unikernel` / `initrd` / `urunc.json`
  重复写入的替代方案。

---

## 1. 结论摘要

### 1.1 当前实现下，snapshot view 不会提升单次启动速度

对当前实现而言，`snapshot view` 在 `devmapper` 下更像是一个
“减少重复拷贝”的机制，而不是一个“缩短 create 热路径”的机制。

原因是：

- 它在 shim 的 `Create` 热路径里额外引入了：
  - `SnapshotService.View`
  - `mount.All`
  - lease 维护
  - `config.json` 注解注入
- 但 `urunc` 侧并没有因此省掉 block rootfs 的主流程：
  - 仍然要处理 guest rootfs block device；
  - 仍然要复制 OCI bind mounts；
  - 仍然要 `umount` active rootfs；
  - 仍然要 `setupDev(...)`。

也就是说，当前 `view` 实际只替代了：

- 从容器 rootfs 里复制 `unikernel`
- 从容器 rootfs 里复制 `initrd`
- 从容器 rootfs 里复制 `urunc.json`

但新增的是一个更重的 `devmapper view` 控制面开销。

### 1.2 变慢的核心不在锁，而在 devmapper 的 View 本身

从 profile 结果看：

- 串行场景下，主要固定成本是 `SnapshotService.View`；
- 并发场景下，即使锁竞争消失，后续请求仍然要等待首个 `View()` 完成。

这说明真正的根因不是“锁没优化好”，而是：

- 在 `devmapper` 下，`View()` 本身就是一个重操作；
- 只要它发生在启动关键路径里，就会吞掉 view 想省下来的文件复制收益。

### 1.3 如果目标是“低 overhead 避免写放大”，更好的方向是 artifact cache

如果主要目标是避免每个容器都重复写入：

- `unikernel`
- `initrd`
- `urunc.json`

那么比 `SnapshotService.View` 更合适的方案通常是：

- 基于 committed parent snapshot 做一个本地只读 artifact cache；
- 首次从 active rootfs 复制一次，后续同镜像容器直接复用缓存目录；
- 用本地 refcount / GC 管理生命周期；
- 不再为这个目的调用 containerd `View()`。

这样可以保留“避免重复写入”的收益，同时把额外控制面开销降到远低于
`devmapper View` 的程度。

---

## 2. 现象：view 比 no-view 更慢

当前仓库中的两类记录都指向同一个结论。

### 2.1 端到端 view vs no-view 对比

在 `docs/perf-view-vs-noview.md` 中：

- `no-view` 首次约 `0.84s`，后续约 `1.22s` 到 `1.36s`；
- `view` 首次约 `1.08s`，后续约 `1.35s` 到 `1.59s`。

这说明：

- `view` 路径并没有让端到端 create 更快；
- 它还带来了一个稳定的小幅额外成本。

### 2.2 profile 结果指出成本落在 view 控制面

在 `docs/perf-snapshot-view.md` 的 profiling 数据中：

- 串行时：
  - `shiminject.shared_view.snapshot_view` 平均大约在几十到一百多毫秒；
  - `shiminject.shared_view.mount.total` 也是稳定固定成本；
  - 锁等待基本为 `0`。
- 并发时：
  - 真正执行 `SnapshotService.View` 的通常只有 `1` 个请求；
  - 但其余请求会等待第一个请求把 shared view 做好；
  - 即使把锁范围缩小，等待只是从 `lock.wait` 变成了 `wait_for_ready`。

因此，从实验上已经能确认：

- 慢的不是“重复创建 view”；
- 慢的也不主要是“用户态锁”；
- 慢的是“首个 devmapper view 创建 + 挂载”本身。

---

## 3. 当前代码实际做了什么

这一节只以当前代码为准。

### 3.1 shim 在 Create 前创建 shared snapshot view

`pkg/shimtask/wrapper.go` 里的 `Create(...)` 会先调用：

- `shiminject.CreateSnapshotView(ctx, bundle, containerID)`

然后再进入底层 `TaskService.Create(...)`。

也就是说，snapshot view 是明确发生在容器 create 热路径里的。

### 3.2 CreateSnapshotView 的额外动作

`pkg/shiminject/inject.go` 里的 `CreateSnapshotView(...)` 当前会做这些事：

1. 新建 containerd client；
2. 解析容器的 `SnapshotKey` 和 `Snapshotter`；
3. 对 `devmapper` 向上找 committed parent snapshot；
4. 创建或确保 lease；
5. 通过 shared-view 目录锁 / marker 协调并发；
6. 调用 `SnapshotService.View(...)` 创建 view；
7. 调用 `mount.All(...)` 把 view 挂到本地目录；
8. 在 `users/` 下登记当前容器；
9. 改写 bundle 下的 `config.json`，注入
   `com.urunc.snapshot.view.mount_path`。

这些步骤中，真正重的是第 6 和第 7 步；其余步骤通常都只是小开销。

### 3.3 urunc 侧并没有因为 view 而跳过主链路

`pkg/unikontainers/rootfs.go` 与 `pkg/unikontainers/block.go` 中的行为是：

- 当存在 `com.urunc.snapshot.view.mount_path` 时：
  - `urunc` 仍然把 active rootfs block device 当作 guest rootfs；
  - snapshot view 只被用来读取 `unikernel` / `initrd` / `urunc.json`；
  - 通过 `bindViewFilesToMonRootfs(...)` 把这几个文件 bind 到 monitor rootfs。
- 之后仍然会：
  - `copyMountfiles(...)`
  - `mount.Unmount(rfs.MountedPath)`
  - `setupDev(...)`

换句话说，当前 `view` 的收益只有：

- 不再把镜像里的 `unikernel` / `initrd` / `urunc.json` 复制到 monitor rootfs。

它没有改变：

- active snapshot 的创建；
- guest rootfs block 准备；
- mount files 处理；
- monitor rootfs 准备。

---

## 4. containerd / devmapper 侧为什么 View 会重

结合 `containerd` `v1.7.30` 的 `devmapper` 实现，可以更清楚地理解这件事。

### 4.1 对 devmapper 来说，View 不是“轻量句柄”

在 `containerd` 的 `snapshots/devmapper/snapshotter.go` 中：

- `View(...)` 会进入 `createSnapshot(..., snapshots.KindView, ...)`；
- 对有 parent 的 snapshot，会调用 `CreateSnapshotDevice(...)`。

这意味着在 `devmapper` 下，`View()` 并不是一个单纯的元数据查询，而是会：

- 分配新的 thin snapshot device；
- 创建对应元数据；
- 激活该设备；
- 最终再返回可挂载的 mount 信息。

### 4.2 CreateSnapshotDevice 会 suspend / snapshot / activate / resume

在 `snapshots/devmapper/pool_device.go` 中，`CreateSnapshotDevice(...)`
的关键流程包括：

- 查询 base device 元数据；
- 在必要时 suspend base device；
- 创建 snapshot thin device；
- 激活新设备；
- 最后 resume base device。

对 `device-mapper thin` 来说，这些步骤本来就不是轻量操作。

因此，一个合理的推断是：

- 你们在 profile 里看到的 `SnapshotService.View` 数十到数百毫秒级固定成本，
  本质上对应的是 `devmapper` 的 snapshot device 创建与激活流程；
- 这不是 `urunc` 自己在用户态加几行缓存就能完全消掉的成本；
- 只要 `View()` 落在启动关键路径，它就很难比“复制几个文件”更便宜。

---

## 5. 为什么当前 snapshot view 反而更慢

综合实验与代码，可把原因拆成三层。

### 5.1 它增加的是“重控制面”，省掉的是“轻数据面”

当前 `view` 路径额外增加：

- devmapper `View()`
- mount
- lease
- config 注入

而省掉的主要只有：

- `unikernel`
- `initrd`
- `urunc.json`

这三份文件的复制。

在 SSD + page cache 较热的常见场景下，这三份文件的复制通常只是一段有限的
顺序读写；而 `devmapper View()` 则包含 metadata transaction、thin snapshot
创建、device activate 等更重的动作。

结果就是：

- `view` 省掉的部分，小于它新增的部分；
- 所以端到端启动更慢。

### 5.2 view 没有改变主导启动时间的那部分链路

你们自己的 `perf-view-vs-noview` 文档已经指出：

- containerd create
- devmapper active snapshot 创建
- CreateRuntime hooks
- network / device setup

这些才是端到端时延里的大头，而它们与 `view` 复用关系不大。

当前实现中，snapshot view 只作用在：

- “monitor 侧如何拿到 unikernel/initrd/config 文件”

它并没有改变：

- “guest rootfs block 是怎么准备的”

因此它天然很难成为启动时延的决定性优化点。

### 5.3 并发下，等待只是换了形态

对 shared view 做并发优化之后：

- 锁持有变少了；
- 串行路径确实更好看了；

但并发总时间没明显变好，因为：

- 其他请求仍然必须等待首个请求把 shared view 真正做好。

也就是说：

- 你们已经把“锁竞争”这个表面问题处理掉了；
- 剩下暴露出来的，就是“首个 `View()` 本身太重”这个根因。

---

## 6. 从提升启动速度的角度，snapshot view 是否有帮助

答案需要区分“按需创建”还是“预热复用”。

### 6.1 按需创建：基本没有帮助

如果像当前实现这样，在容器 `Create` 里现做：

- resolve snapshot
- `SnapshotService.View`
- `mount.All`

那么从启动速度角度看，通常不会有帮助，尤其是在 `devmapper` 下。

原因是：

- 首次 `View()` 的固定成本已经足以抵消甚至超过复制那几份文件的收益；
- 并发时还会把其他请求拖着一起等。

### 6.2 预热 / 预创建：可能有一点帮助，但帮助有限

如果 shared view 能在启动关键路径之外提前准备好，例如：

- 镜像 pull / unpack 之后异步预热；
- 第一个容器之外的后台任务预创建；
- 长生命周期 view 常驻；

那么后续容器启动时就不必再支付 `View()` 的首个成本。

这种情况下，snapshot view 的潜在收益会变成：

- 避免重复复制 `unikernel` / `initrd` / `urunc.json`；
- 只保留 bind mount 级别的轻量操作。

但即便如此，它能改善的仍只是启动链路中的一小段，因为：

- active rootfs block 准备仍然还在；
- hook、网络、设备等成本仍然还在。

所以它最多是：

- 一个小优化；

而不是：

- 启动速度的主优化方向。

---

## 7. 如果目标是低 overhead 避免写放大，更推荐什么

我更推荐一个不依赖 `SnapshotService.View()` 的方案：

- **基于 committed snapshot 的 artifact cache**

### 7.1 基本思路

为每个：

- `(snapshotter, namespace, committed-parent-snapshot)`

建立一个本地缓存目录，例如：

```text
/run/urunc/artifacts/<key>/
  ├── unikernel
  ├── initrd
  └── urunc.json
```

流程如下：

1. 容器 create 时，仍使用 containerd 已经准备好的 active rootfs mount；
2. 解析出该容器所属的 committed parent snapshot；
3. 检查本地 artifact cache 是否已存在；
4. 如果不存在：
   - 从 active rootfs 复制一次 `unikernel` / `initrd` / `urunc.json`；
   - 写入共享 cache；
5. 如果存在：
   - 直接从 cache bind-mount 到 monitor rootfs；
6. 用本地 refcount 或后台 GC 管理清理。

### 7.2 它为什么比 snapshot view 更适合当前目标

这个方案的优点是：

- 避免了 `SnapshotService.View` 的高固定成本；
- 首次只需复制一次，后续同镜像容器不再重复写；
- 不需要额外 containerd lease；
- 不需要额外挂一个 shared snapshot mount；
- 并发控制只需本地文件锁，复杂度更低。

从目标匹配度上看，它更像是：

- 一个“文件 artifact 去重缓存”；

而不是：

- 一个“额外把 rootfs 重新做成只读 view”的机制。

### 7.3 需要注意的点

这个方案要处理几个边界：

- key 必须绑定到 committed snapshot，而不是 active snapshot；
- 如果 `urunc.json` 含有每容器动态差异，就不能无脑全共享；
- cache 目录的生命周期要有 refcount 或定期 GC；
- 需要处理首个创建者与并发复用者之间的同步。

但这些问题都比把 `devmapper View()` 放进 create 热路径更可控。

---

## 8. 关于 qemu / monitor 文件的额外说明

需要单独说明一点：

- `qemu` 主程序、本机库目录、`/usr/share/qemu`、`/usr/share/seabios`
  等 monitor 侧文件，在当前实现里主要已经是通过 `fileFromHost(..., withCopy=false)`
  bind-mount 进 monitor rootfs；
- 它们不是这次 `snapshot view` 能否提速的主要矛盾。

因此当前讨论的“写放大”重点，主要还是镜像里的：

- `unikernel`
- `initrd`
- `urunc.json`

而不是 host 上的 qemu 二进制本体。

---

## 9. 建议

### 9.1 如果优先级是“启动更快”

建议：

- 保持 `no-view` 或等价路径为默认热路径；
- 不要把 `devmapper View()` 放进容器 create 的关键路径。

### 9.2 如果优先级是“减少重复写入”

建议：

- 引入基于 committed snapshot 的本地 artifact cache；
- 首次复制一次，后续按镜像复用；
- 用 bind mount 提供给 monitor rootfs。

### 9.3 如果两者都想兼顾

建议优先级：

1. 先做 artifact cache；
2. 如果未来仍需要严格的只读共享 rootfs 视图，再把 snapshot view 作为可选的、
   预热式优化，而不是默认热路径。

---

## 10. 参考

### 仓库内实现与实验记录

- `docs/perf-snapshot-view.md`
- `docs/perf-view-vs-noview.md`
- `pkg/shimtask/wrapper.go`
- `pkg/shiminject/inject.go`
- `pkg/unikontainers/rootfs.go`
- `pkg/unikontainers/block.go`

### containerd 相关源码与文档

- `containerd` `devmapper` 包文档：  
  <https://pkg.go.dev/github.com/containerd/containerd/snapshots/devmapper>
- `containerd` `v1.7.30` `devmapper snapshotter` 源码：  
  <https://github.com/containerd/containerd/blob/v1.7.30/snapshots/devmapper/snapshotter.go>
- `containerd` `v1.7.30` `devmapper pool device` 源码：  
  <https://github.com/containerd/containerd/blob/v1.7.30/snapshots/devmapper/pool_device.go>
- `containerd` snapshotter 接口定义：  
  <https://raw.githubusercontent.com/containerd/containerd/main/core/snapshots/snapshotter.go>

