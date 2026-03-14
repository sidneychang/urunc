## urunc 共享 snapshot view 设计（多镜像 / 多容器 / 可控回收）

### 1. 背景与问题

当前已有实现（见 `shared-snapshot-view.md` 与 `pkg/shiminject/inject.go`）已经完成：

- 在 shim `Create` 阶段为容器创建 / 复用一个 **共享的只读 view 挂载目录**：
  - 路径形如 `/run/urunc/shared-views/<viewID>/data`；
  - `<viewID>` 由 `(snapshotter, namespace, snapshotKey)` 计算而来；
  - 通过注解 `com.urunc.snapshot.view.mount_path` 写回 bundle `config.json` 供 urunc 使用；
- 在 `Delete` 阶段通过 `CleanupSnapshotView` 结合 `users/` 目录实现 **本地引用计数**，在最后一个容器删除时卸载并删除 view 挂载目录。

已经解决的问题：

- 同一容器 rootfs snapshot 的多个容器不会重复创建多个 view，而是复用同一个本地 view 挂载；
- view 的本地挂载目录（`/run/urunc/shared-views/...`）的生命周期由 urunc 明确控制，不依赖 containerd 的 GC。

尚待进一步精细化的问题：

- **多个镜像 / 多版本**：如何精确地定义“哪些容器应共享同一个 view”，而不会因为镜像升级、版本差异导致错误复用；
- **底层 snapshot 与 lease 管理**：在不破坏 containerd 自身 GC 语义的前提下，是否可以利用 lease 更好地表达“是否有 urunc view 在使用某个 snapshot”。

本文给出一个自洽的设计（与你当前的想法一致）：  

> **同一镜像（更准确说，同一 root snapshot）的多个容器共享同一个 view；  
> 仅当最后一个使用该镜像的容器被移除后，才清理 view 相关资源；  
> view 的创建/删除时机完全由 urunc 自己控制，而不依赖 containerd 的 GC。**

---

### 2. 核心抽象：以 root snapshot 为粒度的 view 复用

#### 2.1 viewKey 定义

对于每个容器，shim 通过 containerd client 取得：

- `snapshotter`：例如 `devmapper`、`blockfile` 等；
- `snapshotKey`：容器 rootfs 对应的 snapshot key；
- `namespace`：containerd namespace。

对于 devmapper 这类块设备 snapshotter，还会通过：

- `SnapshotService.Stat(snapshotKey)` 找到 committed parent，作为最终的 `rootSnapshotKey`。

**定义 viewKey：**

```text
viewKey = (snapshotter, namespace, rootSnapshotKey)
viewID  = hash(viewKey) 或 fmt.Sprintf("%s_%s_%s", snapshotter, namespace, rootSnapshotKey)
```

语义：

- **同一个 viewKey（viewID）**：
  - 视为“同一个镜像版本”的 rootfs；
  - 所有 urunc 容器应共享一个 view；
- **不同的 viewKey**：
  - 不同镜像或不同版本；
  - 各自拥有独立的 view 生命周期。

这满足：

- **多个容器 / 同一镜像**：多容器复用同一 view；
- **多个镜像 / 多版本**：不同镜像（不同 root snapshot）互不干扰。

#### 2.2 view 的物理形态

每个 viewKey 对应一个在 host 上的目录：

```text
/run/urunc/shared-views/<viewID>/
  ├── data/         # view 挂载点（只读 rootfs 视图）
  ├── users/        # 本地 refcount：每个容器一个 marker 文件
  └── <viewID>.lock # flock 锁文件，用于并发创建 / 清理串行化
```

- `data/`：
  - 使用 `SnapshotService.Mounts(rootSnapshotKey)` + `mount.All(...)` 挂载 snapshot；
  - 或者将来挂载一个“专用 view‑snapshot”（见后文）。
- `users/`：
  - 每个使用该 view 的 urunc 容器在这里写一个 `users/<containerID>` 文件；
  - 文件内容可以是时间戳等，无强语义要求，存在即表示“该容器引用了这个 view”；
  - 通过 `ReadDir(users)` 的条目数量实现本地引用计数。
- `<viewID>.lock`：
  - 用 `syscall.Flock` 实现对该 viewKey 的所有“首创 / 清理”操作串行化；
  - 避免多个 shim 并发创建 / 删除时产生竞态。

---

### 3. 生命周期：多容器 / 多镜像 / 多版本

#### 3.1 第一次使用某个镜像（view 创建）

在 shim 的 `Create` 调用 `CreateSnapshotView` 时：

1. 解析容器信息：
   - `snapshotter` / `snapshotKey` / `namespace`；
   - 对 devmapper 使用 committed parent 作为 `rootSnapshotKey`。
2. 计算 `viewKey` → `viewID`，并构造路径：

```go
viewBase    = filepath.Join("/run/urunc/shared-views", viewID)
viewDataDir = filepath.Join(viewBase, "data")
viewUsers   = filepath.Join(viewBase, "users")
lockPath    = viewBase + ".lock"
```

3. 对 `lockPath` 调用 `flock` 获取独占锁；
4. 创建 `viewBase` & `users` 目录（如果不存在）；
5. 当 `data/` 不存在时：
   - 创建 `data/`；
   - 使用 `SnapshotService.Mounts(rootSnapshotKey)` 取得挂载信息；
   - 调 `mount.All(mounts, viewDataDir)` 将该 snapshot 挂载到 `data/`；
   - 可选：如果 snapshotter 支持，以只读方式挂载或通过 remount + `MS_RDONLY` 保证只读。
6. 在 `users/` 下创建 `users/<containerID>` 文件作为引用标记；
7. 读取 `bundle/config.json`，将：

```json
"com.urunc.snapshot.view.mount_path": "/run/urunc/shared-views/<viewID>/data"
```

写入 `spec.Annotations` 后回写文件。

> 结果：  
> **对于某个 root snapshot 的第一个容器，完成 view 创建；  
> 后续容器只需复用 `data/` 挂载并增加自己的 marker。**

#### 3.2 同一镜像更多容器（view 复用）

后续容器的 `Create`：

1. 通过同样的方式得到同一个 `viewKey`；
2. 加锁后发现 `data/` 已存在：
   - 不需要再次调用 `SnapshotService.Mounts` / `mount.All`；
3. 在 `users/` 下为当前 `containerID` 创建 marker；
4. 注入同一条 `com.urunc.snapshot.view.mount_path`（指向同一个 `data/`）。

> 结果：  
> **所有同一镜像（同一 root snapshot）的容器共用一个 view，符合“首次创建、后续复用”的预期。**

#### 3.3 容器删除（引用减少）

在容器 `Delete` 时：

1. urunc 侧先按已有逻辑：
   - 卸载从 view bind 到 monitor rootfs 的 mount（避免引用 devmapper 设备）；
2. shim 的 `CleanupSnapshotView`：
   - 通过 `SnapshotViewInfo` 取得 `ViewID`、`ContainerID` 等；
   - 加锁；  
   - 删除 `users/<containerID>` marker（本地 refcount -= 1）；
   - 如果 `users/` 中仍有其他文件：
     - 说明还有容器在用该镜像的 view → 不做进一步清理；
   - 如果 `users/` 已空：
     - 说明这是最后一个容器 → 进入最终清理。

#### 3.4 最后一个容器删除（view 回收）

当 `users/` 目录为空时，本地 refcount = 0：

- 必做：
  - `mount.Unmount(viewDataDir, 0)`；
  - `os.RemoveAll(viewBase)` 删除 `/run/urunc/shared-views/<viewID>` 整棵树；
- 如果后续引入 lease / view‑snapshot：
  - 此处还可以安全地删除对该 viewKey 专用的 lease 或 view‑snapshot（见下节）。

> 结果：  
> **对某个镜像 root snapshot 而言：  
> 第一个容器创建 view，最后一个容器回收 view，中间容器只改变 refcount。  
> view 删除时机完全由 urunc 控制，与 containerd GC 解耦。**

---

### 4. 多镜像 / 多版本下的行为

有了 `viewKey = (snapshotter, namespace, rootSnapshotKey)`，多镜像/多版本自然被区分开来：

- **不同镜像或不同版本**：
  - 它们在 containerd 中会对应不同的 root snapshotKey；
  - 于是 viewKey 不同 → `viewID` 不同 → 对应不同的 `/run/urunc/shared-views/<viewID>`；
  - 各自的 view 生命周期由自己的容器集决定，互不影响。

- **同一镜像 / 同一版本 / 多容器**：
  - 同一个 root snapshotKey → 相同 viewKey；
  - 所有容器共用一个 view 挂载，同进同退；
  - 满足“多个容器使用同一镜像时复用 view”这一核心 idea。

需要注意的一点是：

- 我们 **不假设不同版本的镜像可以共用同一个 view**，即：
  - 不会把两个不同 root snapshotKey 映射到同一个 viewKey；
  - 因为 `unikernel/initrd/urunc.json` 极有可能随着镜像版本变化而变化；
  - 否则会出现“新版本容器读到旧版本 kernel/initrd”的错误。

如果未来希望做更激进的跨版本去重，可以另外按 `unikernel/initrd/urunc.json` 的内容 hash 做二级缓存，但那是后续更复杂的优化，不在本设计的主路径中。

---

### 5. lease / view‑snapshot 的可选增强方案

在上述设计中，本地 view 的创建/删除完全由 urunc 控制；  
containerd 对底层 snapshot 的 GC 仍然基于自己的引用/lease 逻辑。  

在这个基础上，可以考虑两种增强：

#### 5.1 方案 A：只用 lease 让 containerd 感知“是否有 urunc view 在用”

**思路：**

- 对每个 `viewKey` 创建一个 lease（或共享一个带 label 的 lease），只负责表达：
  - “当前是否有 urunc view 容器在使用这个 root snapshotKey”。

**行为：**

- 当 `users/` 从 **0 → 1**：
  - `LeasesService.Create` 一个 lease（或引用已有 lease）；
  - `LeasesService.AddResource(lease, {Type: "snapshots/<snapshotter>", ID: rootSnapshotKey})`，把 snapshot 挂到这个 lease 上；
- 当 `users/` 从 **1 → 0**：
  - 删除该 lease（`LeasesService.Delete`）；
  - 释放 GC 保护，让 containerd 在确认没有其他引用后自行决定何时删除 snapshot。

**好处：**

- urunc 不负责“最终删除 snapshot 的时机”，只负责“告诉 containerd 我还在/不在使用这个 snapshot”；
- containerd 的 GC 不会在 urunc view 使用期间错误删除 snapshot；
- 删除时机对 urunc 来说明确、可控，但又不破坏 containerd 的整体 snapshot/GC 语义。

**实现可行性分析：**

- API 与依赖都已存在（旧实现中使用过 `LeasesService`），代码上只需：
  - 为 `SnapshotViewInfo` 或一个 map 增加 `LeaseID`；
  - 在 `CreateSnapshotView` / `CleanupSnapshotView` 增加很少量逻辑；
- 性能：
  - lease 的创建/删除只在 refcount 0→1 / 1→0 时发生，即“首次使用 view / 最后一个容器删除”两个时刻，开销可忽略；
- 风险：
  - 不会触碰 snapshot 数据，仅影响 GC 逻辑；
  - 出错时最坏情况是 leak 一个 lease（可以由运维工具清理），不会影响正在运行的容器。

**结论：**

- 方案 A 是在当前实现之上的一个小步增强，**实现难度低，可行性高，适合作为短期可落地的改进**。

#### 5.2 方案 B：view‑snapshot + lease（专用 snapshot 层）

**思路（更隔离但更复杂）：**

- 不直接挂载 `rootSnapshotKey`，而是从它派生一个专门给 view 用的 snapshot，例如：

```text
viewSnapKey = "<rootSnapshotKey>-urunc-view"
```

- 第一次使用 `viewKey` 时：
  - 调用 `Prepare/Commit` 或等价 API 创建 `viewSnapKey`；
  - 给它打上 `com.urunc.view=1` 之类的 label；
  - 创建 lease，并把 `viewSnapKey` 掛到该 lease；
  - `SnapshotService.Mounts(viewSnapKey)` + `mount.All(..., data/)`。

- 当 `users/` 为 0（最后一个容器删除）时：
  - `mount.Unmount(data/)`；
  - `SnapshotService.Remove(viewSnapKey)` 删除此专用 snapshot；
  - `LeasesService.Delete(lease)` 删除 lease。

**优点：**

- 将“view 专用的 snapshot 层”与原始 snapshot 完全隔离，原始 snapshot 仍可由镜像管理和其他 runtime 使用；
- urunc 在 refcount=0 时可以完全清理 view 专用的 snapshot 层，避免累积多余层。

**实现可行性与风险：**

- 不同 snapshotter 的 `Prepare/Commit` 语义和开销不同，需要分别验证；
- 对 devmapper 等块设备 snapshotter，需要确认不会对 thin pool / 元数据造成额外负担；
- 必须处理多 shim 并发首创 `viewSnapKey` 的竞态，增加代码复杂度。

**结论：**

- 方案 B 技术上可行，但实现和测试工作量较大，适合作为中长期优化，而非第一阶段目标。

---

### 6. 设计整体可行性结论

结合当前代码库与依赖，可以得出：

- **当前已实现的 shared‑view + 本地 refcount 方案**：
  - 已支持按 `(snapshotter, namespace, rootSnapshotKey)` 粒度复用 view；
  - 对于“多个容器使用同一镜像时可以复用 view，直到所有容器删除后才清理 view 资源”的需求，已经满足；
  - 删除时机完全由 urunc 自己掌控（最后一个容器 Delete → 立即回收本地 view）。

- **在此基础上的增强方向**：
  - **短期可行（方案 A）**：在首次使用 / 最后一次使用 view 时，通过 lease 把“是否有 urunc view 使用该 snapshot”告知 containerd；
    - 不改变现有接口和行为，只增强 GC 语义，风险低、易回滚；
  - **中长期优化（方案 B）**：为 view 专门创建一层 snapshot（view‑snapshot），在 refcount=0 时连同 lease 一起删除；
    - 能实现更彻底的 view 层清理，但实现和验证成本高。

总结为一句话：

> **按 root snapshot 粒度管理共享 view + 本地 refcount 决定创建/删除时机，是一个在多镜像、多容器场景下既合理又可实现的设计；  
> 在这个基础上，逐步引入 lease（以及可能的 view‑snapshot）可以进一步优化与 containerd GC 的协同，而不会打乱现有流程。**

