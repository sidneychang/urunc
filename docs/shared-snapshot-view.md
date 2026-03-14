## urunc 共享 snapshot view 设计说明

### 背景

在最初的实现中，urunc 的 shim 在每个容器的 `Create` 阶段都会：

- 调用 containerd 的 `SnapshotService.View`，基于容器 rootfs 的 snapshot 创建一个只读 snapshot view；
- 将该 view 挂载到 `/run/urunc/views/<containerID>`；
- 把该挂载路径通过注解 `com.urunc.snapshot.view.mount_path` 写回 bundle 的 `config.json`，供 urunc 在 guest 启动时使用；
- 在容器 `Delete` 阶段通过 `CleanupSnapshotView` 负责卸载 mount、删除 snapshot view、删除 lease。

这个模型有两个显著问题：

- **每个容器一个 view**：即使多个容器共享同一个镜像 snapshot，也会为每个容器在 containerd 里单独创建一个 snapshot view 和 lease；
- **view 生命周期强绑定 containerd**：view 作为 containerd 的 snapshot 对象存在，其生命周期主要由 shim + containerd GC 管理，不利于在 urunc 侧“按镜像 / snapshot 复用”。

新的设计目标是：

- 复用 view：对于同一个 `(snapshotter, namespace, snapshotKey)`，多个 urunc 容器共享同一份只读视图；
- view 不再是 containerd 创建的 snapshot 对象，而是 **urunc 自己管理的本地挂载目录**；
- 生命周期主要由 shim 和 urunc 管理，与 containerd snapshot 的生命周期保持松耦合。

---

### 共享 view 的整体思路

新的实现仍然遵守原来的“契约”：

- **urunc 只依赖一个注解**：`com.urunc.snapshot.view.mount_path`；
- 注解的值是一个 **只读 rootfs 视图目录**，urunc 仅从这里 bind-mount `unikernel` / `initrd` / `urunc.json` 到 monitor rootfs；
- urunc 不再也从未依赖任何 containerd 的 view key、lease ID 等信息。

区别在于：

- 注解中提供的路径不再是 `/run/urunc/views/<containerID>` 这种“一容器一目录”的路径；
- 而是一个按 snapshot 聚合的共享目录：

```text
/run/urunc/shared-views/<viewID>/data
```

这里的 `<viewID>` 是根据 snapshot 元数据计算出来的一个稳定标识，使得“相同 snapshot / 不同容器”可以共享同一份 view。

---

### 共享 view 的标识与目录结构

#### viewID 的计算

`viewID` 的构造在 `pkg/shiminject/inject.go` 中完成，逻辑为：

- 从 containerd 读取容器的：
  - `snapshotter`（如 `devmapper`、`blockfile` 等）；
  - `snapshotKey`；
  - 当前 namespace（通过 `namespaces.NamespaceRequired(ctx)`）；
- 然后拼出：

```go
viewID := fmt.Sprintf("%s_%s_%s", snapshotter, ns, snapshotKey)
```

只要 `(snapshotter, namespace, snapshotKey)` 一致，所有容器都会得到同一个 `viewID`，从而复用同一个 view 挂载。

#### 目录布局

共享 view 的根目录定义为：

```go
const (
    sharedViewsRoot       = "/run/urunc/shared-views"
    sharedViewsDataDir    = "data"
    sharedViewsUsersDir   = "users"
    sharedViewsLockSuffix = ".lock"
)
```

每个 `viewID` 对应的目录结构为：

```text
/run/urunc/shared-views/<viewID>/
  ├── data/          # 挂载 snapshot 的只读视图目录
  ├── users/         # 引用计数目录，每个活跃容器一个 marker 文件
  └── <viewID>.lock  # 进程间互斥锁文件，防止并发准备/清理冲突
```

- `data/`：真正给 urunc 暴露的 view 路径，注解 `com.urunc.snapshot.view.mount_path` 指向的就是这个目录；
- `users/`：每个使用该 view 的容器会在这里创建一个空文件（文件名为 `containerID`），作为引用计数；
- `<viewID>.lock`：在对该 view 做“初始化或清理”操作时，通过 `flock` 实现跨进程互斥。

---

### CreateSnapshotView：创建 / 复用共享 view

函数入口仍是：

- `pkg/shiminject/inject.go` 中的 `CreateSnapshotView(ctx, bundle, containerID string)`；
- 在 `pkg/shimtask/wrapper.go` 中的 `TaskService.Create` 会调用它，和之前一样。

新的实现步骤概括如下：

1. **获取 namespace 与 containerd client（复用 client）**
   - 从 `ctx` 中拿到 namespace（必须存在）；
   - 从环境变量 `CONTAINERD_ADDRESS` 或默认 `/run/containerd/containerd.sock` 构造地址；
   - 使用一个 `sync.Once` 的进程级共享 client：

   ```go
   func getContainerdClient(address, ns string) (*containerd.Client, error) {
       clientOnce.Do(func() {
           clientInst, clientErr = containerd.New(address, containerd.WithDefaultNamespace(ns))
           ...
       })
       return clientInst, clientErr
   }
   ```

2. **读取容器的 snapshot 信息**
   - `ContainerService.Get(ctx, containerID)`；
   - 取出 `SnapshotKey` / `Snapshotter`；
   - 如二者为空，则说明没有使用 snapshot（例如本地 rootfs），直接返回 `(nil, nil)`，不创建 view。

3. **根据 snapshot 信息构造共享 view**

   - 按前述规则计算 `viewID`；
   - 计算路径：

   ```go
   viewBase   := filepath.Join(sharedViewsRoot, viewID)
   viewDataDir := filepath.Join(viewBase, sharedViewsDataDir)
   viewUsersDir := filepath.Join(viewBase, sharedViewsUsersDir)
   lockPath  := viewBase + sharedViewsLockSuffix
   ```

   - 创建 `sharedViewsRoot` 目录；
   - `flock(lockPath)`，串行化对该 `viewID` 的所有操作；
   - 确保 `viewUsersDir` 存在。

4. **首次挂载 snapshot（只在 `data/` 不存在时）**

   - 如果 `viewDataDir` 不存在：
     - 创建 `viewDataDir`；
     - 获取 snapshotter 对象：`ss := client.SnapshotService(snapshotter)`；
     - 对 devmapper，优先用 committed parent snapshot：
       - `info := ss.Stat(ctx, snapshotKey)`；
       - 若 `info.Parent != ""`，则将 `snapshotKey` 切换为 `info.Parent`；
     - 调用 `ss.Mounts(ctx, snapshotKey)` 获取 snapshot 的挂载信息；
     - 使用 `mount.All(mounts, viewDataDir)` 将该 snapshot 挂载到 `data/` 目录。

   - 如果 `viewDataDir` 已存在，则说明这个 snapshot 的共享 view 已经初始化过，直接复用即可。

5. **增加引用计数（users 目录）**

   - 在 `viewUsersDir` 下创建一个以 `containerID` 命名的文件：

   ```go
   userMarker := filepath.Join(viewUsersDir, containerID)
   os.WriteFile(userMarker, []byte(time.Now().Format(time.RFC3339Nano)), 0644)
   ```

   - 这个文件本身的内容不重要，关键是它的存在表示“该容器正在使用此 view”。

6. **向 bundle 的 config.json 注入 view 挂载路径**

   - 读取 `bundle/config.json` 到 `specs.Spec`；
   - 在 `spec.Annotations` 中设置：

   ```go
   spec.Annotations["com.urunc.snapshot.view.mount_path"] = viewDataDir
   ```

   - 再写回 `config.json`。

7. **返回 SnapshotViewInfo 给 shim 包装器**

   ```go
   return &SnapshotViewInfo{
       ViewID:      viewID,
       MountPath:   viewDataDir,
       Snapshotter: snapshotter,
       Namespace:   ns,
       ContainerID: containerID,
   }, nil
   ```

   shim 的 `wrapper.Create` 会把这个结构体保存在内存中，用于后续 Delete 阶段的清理。

---

### CleanupSnapshotView：基于引用计数的本地清理

清理函数仍然是：

- `pkg/shiminject/inject.go` 中的 `CleanupSnapshotView(ctx context.Context, info *SnapshotViewInfo) error`；
- 在 `pkg/shimtask/wrapper.go` 的 `Delete` 和 `Create` 失败回滚路径中调用。

新的实现逻辑：

1. **检查 ViewID 与路径**
   - 如果 `info == nil` 或 `info.ViewID == ""`，直接返回。

2. **加锁（同一个 viewID 串行清理）**

   - 通过 `flock(viewBase + ".lock")` 获取独占锁；
   - 计算 `viewBase` / `viewDataDir` / `viewUsersDir` 路径。

3. **删除当前容器的用户 marker（减引用）**

   - 如果 `info.ContainerID` 非空，则删除：

   ```go
   userMarker := filepath.Join(viewUsersDir, info.ContainerID)
   _ = os.Remove(userMarker)
   ```

4. **检查是否还有其它用户**

   - 读取 `viewUsersDir` 内的条目：
   - 如果 `ReadDir` 出错或剩余条目数量大于 0，则说明仍有容器在使用这个 view：
     - 不进行进一步清理，直接返回。

5. **最后一个引用释放：卸载并删除目录**

   - 当 `users/` 目录为空时：
     - 如果 `viewDataDir` 存在，则调用 `mount.Unmount(viewDataDir, 0)` 卸载挂载；
     - 删除整个 `viewBase` 目录（包括 `data/`、`users/` 和 `.lock` 文件）。

通过这种方式，实现了一个简单的“文件系统级引用计数”：

- 只要至少有一个容器的 `users/<containerID>` 文件存在，对应的 snapshot view 就会一直保持已挂载状态；
- 当最后一个容器删除自己的 marker 后，view 会被自动卸载并清理。

---

### 与 containerd / urunc 的关系与兼容性

#### 和 containerd 的关系

- 新实现 **不再调用 `SnapshotService.View` 创建新的 snapshot view 对象**，也不再创建或删除任何 containerd lease；
- 只使用：
  - `ContainerService.Get` 读取容器元数据；
  - `SnapshotService.Stat`（devmapper 下选择 committed parent）；
  - `SnapshotService.Mounts` 获取现有 snapshot 的挂载信息；
  - `mount.All` 在本地挂载该 snapshot 到 urunc 自己的目录；
- containerd 的 snapshot 生命周期仍然由容器本身和 containerd GC 决定，urunc 的 shared view 只是对同一个 snapshot 的一个额外挂载视图，不会改变 GC 语义。

#### 和 urunc 的关系（兼容性）

urunc 侧的逻辑主要集中在：

- `pkg/unikontainers/rootfs.go`：
  - 通过注解 `com.urunc.snapshot.view.mount_path` 判断是否启用 snapshot view 路径；
  - 使用该路径做 logging 和可选的 `getBlockDeviceFromMount`（用于观测）；
  - 把 `FromSnapshotView` / `SnapshotView.MountPath` 塞进 `RootfsParams`。
- `pkg/unikontainers/block.go`：
  - `bindViewFilesToMonRootfs(viewMountPath, monRootfs, ...)`：从 `viewMountPath` 下 bind-mount `unikernel` / `initrd` / `urunc.json` 到 monitor rootfs；
  - 其余 block 设备处理逻辑保持不变。

由于新的实现仍然：

- 使用**相同的注解 key**：`com.urunc.snapshot.view.mount_path`；
- 保证注入的路径是一个可读的只读目录，里边包含完整的 container rootfs（或至少包含 `unikernel` / `initrd` / `urunc.json`）；

因此：

- **urunc 端无需任何代码修改** 即可兼容共享 view 的新实现；
- 之前的行为只是“每个容器一个 view mount 目录”，现在则是“多个容器指向同一个共享 view 目录”，对 urunc 的使用方式没有语义差异。

---

### 异常与后续优化思路

#### 异常情况

- 如果某个容器在 Delete 阶段异常退出，导致 `CleanupSnapshotView` 没有被调用：
  - 最坏的结果是在 `/run/urunc/shared-views/<viewID>/users/` 中遗留一个 marker 文件；
  - 对其它容器不会有影响，只是该 view 不会被自动清理；
  - 可以通过运维脚本或后续的后台 GC 来扫描“长时间无人使用 / 无对应容器存在”的 view，并做补偿清理。

#### 可选的后续优化

- **后台 GC**（非当前改动的一部分，只是建议）：
  - 周期性扫描 `/run/urunc/shared-views`；
  - 判断 `users/` 是否为空，或所有 marker 对应的 containerID 是否已经在 containerd 中不存在；
  - 对满足条件的 view 调用同样的清理逻辑（Unmount + RemoveAll）。

- **更细粒度的 viewID**：
  - 目前以 `(snapshotter, namespace, snapshotKey)` 为 key，一般已经足够；
  - 如未来需要按镜像 digest 或某些特定参数再细分 view，可以在 `viewID` 计算规则里附加更多信息（例如镜像 digest、某些 urunc 注解等）。

---

### 小结

本次改动把 snapshot view 的实现从：

- “**每个容器一个 containerd snapshot view + lease**”

迁移到了：

- “**按 snapshot 共享的本地挂载目录，由 urunc shim 自己管理生命周期**”。

核心变化包括：

- 新增 `/run/urunc/shared-views/<viewID>/{data,users}` 目录结构与文件锁机制；
- 使用 `SnapshotService.Mounts` + `mount.All` 挂载现有 snapshot，而不再创建额外的 snapshot view 对象；
- 通过 `users/` 目录下的 per-container marker 文件实现简单的引用计数，最后一个容器删除时自动卸载并清理 view 目录；
- 保持对 urunc 侧的接口（注解 key 和语义）不变，从而做到无感知地完成 view 复用改造。

