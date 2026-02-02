## 文档标题

**urunc + containerd devmapper 视图快照（view snapshot）方案调研与问题列表**

---

### 一、问题背景与动机

- **场景**  
  在 `urunc` 与 block‑based snapshotter（如 `devmapper`、`blockfile`）配合使用时，可以把 containerd 的快照当作块设备直接传给 unikernel guest。  
  典型运行方式示例（使用 devmapper snapshotter）：

  ```bash
  time sudo nerdctl run -d --snapshotter devmapper \
    --runtime io.containerd.urunc.v2 \
    harbor.nbfc.io/nubificus/urunc/nginx-qemu-linux-raw:latest
  ```

- **当前问题**  
  在使用 devmapper 作为 snapshotter 时：
  - urunc 需要访问 **guest kernel、initrd（可选）、`urunc.json`** 这些文件；
  - 这些文件位于 **容器 rootfs** 中；
  - 为了把 block‑based rootfs 设备直接交给 unikernel guest，必须先从容器 rootfs 中把这些文件拷贝出来，再 `umount` 容器 rootfs，把块设备传给 guest；
  - 这引入了 **额外的文件复制和 I/O 开销**，也会在 `/run` 等目录堆积临时文件。

- **优化思路：view snapshot（只读视图）**  
  - 利用 containerd 提供的 **只读快照（view snapshot）**：
    - 为容器 rootfs 创建一个只读 view snapshot；
    - 将这个 snapshot 单独挂载；
    - 直接在该只读挂载点读取 unikernel binary、initrd 和 `urunc.json`，而不是复制文件；
  - 理论上：
    - view snapshot 本身不产生额外数据，只是把读取请求转发到底层层；
    - 因此 **几乎没有额外存储开销**；
    - 还能避免临时文件和复制逻辑，简化 runtime 流程。

---

### 二、当前实现概览（基于最近两个 commit）

#### 1. 传统路径：不使用 view snapshot

对应 `pkg/unikontainers/block.go` 中的逻辑：

- **文件复制与卸载路径**
  - 使用 `prepareDMAsBlock` / `extractFilesFromBlock`：
    - 把 unikernel binary、initrd、`urunc.json` 从容器 rootfs 拷贝到一个新的临时目录（monitor rootfs）；
    - 然后 `mount.Unmount(rootfsPath)` 卸载 devmapper snapshot 的挂载点；
    - 最后调用 `setupDev` 把块设备暴露给 guest。
  - 这一路径存在显式的 `FIXME`，指出：
    - 该方式会在 `/run` 积累大量 unikernel binary、initrd 和 `urunc.json` 文件；
    - 带来额外 I/O 和存储使用。

#### 2. 新路径：使用预先创建的 snapshot view

在最近的改动中，引入了 **“从 snapshot view 读取文件”** 的支持，主要涉及：

- **`pkg/unikontainers/rootfs.go`：rootfs 选择逻辑**
  - `tryContainerBlockRootfs`：
    - 通过注解 `annotSnapshotViewMountPath` 读取 view mount 路径 `viewMountPath`；
    - 调用 `getMountInfo(rs.cntrRootfs)`，获取当前容器 rootfs 挂载点对应的块设备 `activeRootfsDevice.Source`；
    - 为日志可观测性，可选调用 `getBlockDeviceFromMount(viewMountPath)` 获取 view mount 的块设备（仅用于记录）；
    - 构造 `RootfsParams`：
      - `Type = "block"`；
      - `Path = activeRootfsDevice.Source`（交给 guest 的块设备）；
      - `MountedPath = rs.cntrRootfs`（活动 rootfs 挂载点）；
      - `FromSnapshotView = true`；
      - `SnapshotView = { MountPath: viewMountPath, BlockDevice: viewBlockDevice }`。

  - 重要的一点：  
    **urunc 本身不再直接与 containerd 通信创建 snapshot**，只消费由 shim 预先创建并挂载好的 view snapshot，通过注解拿到路径。

- **`pkg/unikontainers/block.go`：block‑based rootfs 设置逻辑**
  - `handleBlockBasedRootfs`：
    - 区分显式 block 文件与“容器 rootfs 作为 block 设备”：
      - `rfs.MountedPath == ""`：显式 block 文件（例如 `/rootfs.ext2`）；
      - `rfs.MountedPath != ""`：容器 rootfs 本身是块设备（devmapper/blockfile）。
    - 对于容器 rootfs 作为块设备的情况，调用 `handleCntrRootfsAsBlock`。

  - `handleCntrRootfsAsBlock`：
    - **有 snapshot view 时（`FromSnapshotView == true`）：**
      - 调用 `bindViewFilesToMonRootfs(viewMountPath, monRootfs, unikernelPath, initrdPath, uruncJSONFilename)`：
        - 在只读 view mount 上找到 unikernel/initrd/`urunc.json`；
        - 把这些文件 bind‑mount 到 monitor rootfs 中的对应路径（不复制，不写磁盘）；
      - 调用 `copyMountfiles(rfs.MountedPath, mounts)`：
        - 把所有 bind mounts 对应的内容复制进活动 rootfs；
      - `mount.Unmount(rfs.MountedPath)` 卸载活动 rootfs 的挂载；
      - `setupDev(rfs.MonRootfs, rfs.Path)` 把块设备暴露给 guest；
      - 最终用 `BlockDevParams{ Source: rfs.Path, MountPoint: "/", ID: "rootfs" }` 表示 guest rootfs。

    - **无 snapshot view 时：**
      - 沿用旧逻辑：
        - `copyMountfiles` 把 bind mounts 内容复制进 rootfs；
        - 使用 `prepareDMAsBlock` + `extractFilesFromBlock` 复制 unikernel/initrd/`urunc.json` 然后卸载 rootfs；
        - 再将块设备交给 guest。

---

### 三、本项目目标（与 Expected Outcome 对齐）

- **目标 1：文档**  
  - 输出一份说明：
    - containerd 中 block‑based snapshot（devmapper / blockfile）的基本概念；
    - snapshot 及 view snapshot 创建和管理相关的 API（gRPC / Go client 的典型调用路径）；
    - 与 urunc 使用场景相关的约束和注意事项。

- **目标 2：实现**  
  - 在 urunc 体系中实现：
    - 能够为容器 rootfs 请求并挂载 **只读 view snapshot**；
    - 让 urunc 能从该 view snapshot 中直接读取 guest kernel、initrd、`urunc.json` 等文件；
    - 在不破坏现有行为的前提下，对 devmapper / blockfile 等 block‑based snapshotter 透明工作。

- **目标 3：评估**  
  - 设计实验并完成：
    - **性能对比**：view snapshot 方案 vs 传统 copy+umount 方案（如启动时间、I/O 量等）；
    - **存储开销**：view snapshot 是否确实“几乎没有额外存储开销”（thinpool 使用量、blockfile scratch 占用等）；
    - **限制分析**：识别在不同 unikernel 类型、不同 workload 下该方案的适用性和局限。

---

### 四、需要与导师讨论的问题（按类别整理）
Open questions: image build & layer‑aware snapshots
Do we need to change the current urunc image build tool?
Today, urunc images are built with our own tooling. If we want to reliably “pick the layer that contains the unikernel artifacts”, do we need to:
enforce that the guest kernel, initrd and urunc.json always live in a dedicated layer (e.g. a base layer), and
add explicit annotations/labels on that layer so that the runtime can identify it?
Or is a simpler “rootfs‑level view snapshot” (no per‑layer selection) good enough for this project, so that we don’t touch the image build pipeline at all?
Is it feasible (and supported) to create a view snapshot directly from a single image layer?
In containerd, snapshots are addressed by snapshot keys and form a parent chain. Each image layer usually corresponds to one read‑only snapshot in the snapshotter.
Technically, we could create a new read‑only snapshot/view with parent = <snapshot for that layer> instead of using the top rootfs snapshot. Is this pattern supported and recommended by containerd and the devmapper snapshotter?
If we only mount the snapshot corresponding to that layer (and its parents) rather than the final merged rootfs:
are there any issues with whiteouts and file visibility (missing files from upper layers)?
is it acceptable if all unikernel artifacts (kernel/initrd/urunc.json) are guaranteed to live entirely in that lower layer?
How can we safely map from an OCI image layer to the corresponding snapshot in the devmapper snapshotter?
Do we need to rely on containerd’s internal naming conventions (which is brittle),
or should we introduce explicit metadata/annotations during image build so that the runtime can resolve “the layer that contains the unikernel” to a concrete snapshot key?
#### 4.1 containerd / block‑based snapshot / view snapshot 相关

- **快照模型与语义**
  - **Q1**：在 containerd 的快照模型中，devmapper / blockfile 的 **“active snapshot” 与 “view snapshot（只读视图）”** 的正式语义是什么？  
    - view snapshot 是否总是只读、无写时 COW？
  - **Q2**：对同一容器 rootfs 创建额外的 view snapshot，在 devmapper 中是否会带来额外的元数据或空间开销？  
    - 有无推荐的使用模式或数量限制？

- **API 选择与调用路径**
  - **Q3**：如果 runtime 侧需要创建 view snapshot 并挂载，推荐通过哪一层 API 完成？  
    - 直接使用 containerd gRPC / Go client？  
    - 还是建议把这部分逻辑统一收敛在 shim 中，由 shim 与 containerd 交互？
  - **Q4**：创建 view snapshot 的标准流程是什么？
    - snapshot 名称如何命名（包含 container ID / task ID）？  
    - 是否需要为 view snapshot 增加特定 label / annotation（例如 `kind=view`）以便区分？

- **devmapper vs blockfile 的差异**
  - **Q5**：在 devmapper 和 blockfile snapshotter 下，view snapshot 的行为是否完全一致？  
    - 是否有一些只在 devmapper 上推荐、在 blockfile 上不适合的模式？

- **生命周期与回收**
  - **Q6**：view snapshot 的生命周期是否应该严格与 container 生命周期绑定？  
    - 容器退出时立刻删除，还是可以保留一段时间以做调试或复用？
  - **Q7**：容器多次重启时，view snapshot 更推荐：
    - 每次重新创建临时 view，  
    - 还是创建一个长期存在的 view 并进行复用？

#### 4.2 urunc / shim 职责划分与接口设计

- **职责边界**
  - **Q8**：当前实现中，**view snapshot 的创建和挂载由 shim 完成，urunc 只消费注解和 mountpoint**。  
    - 从架构上看，这是否是希望长期保持的模式？  
    - 还是本项目希望尝试“让 urunc 或 shim‑urunc 直接调用 containerd 的 snapshot API”？
  - **Q9**：在整个栈中，哪一层更适合作为 **snapshot 生命周期的 owner**（创建 / 挂载 / 删除）？
    - containerd 配置层、  
    - shim 层、  
    - 还是 urunc 二进制本身？

- **注解与对外接口稳定性**
  - **Q10**：我们目前使用 `annotSnapshotViewMountPath` 传递 snapshot view 的挂载路径。  
    - 这个 key 的命名和语义是否需要文档化，作为长期稳定接口？  
    - 是否需要增加更多注解：view snapshot ID、只读标记、关联的父 snapshot ID 等？
  - **Q11**：如果将来需要多个不同用途的 view snapshot（例如一个给监控/调试，一个给 unikernel），注解和 API 应该如何设计以便扩展？

- **回退策略 / 错误路径**
  - **Q12**：如果创建或挂载 view snapshot 失败，目前代码会回退到“直接 block rootfs + 文件复制”的传统路径。  
    - 在实验阶段，你更倾向：  
      - 保留这种自动 fallback 行为，保证功能可用；  
      - 还是在 view snapshot 失败时直接报错，方便尽早发现配置问题？

#### 4.3 当前实现中的细节与改进空间

- **文件复制残留问题**
  - **Q13**：在 snapshot view 路径中，我们已经避免了 unikernel/initrd/`urunc.json` 的复制，但仍然对 bind mounts 执行 `copyMountfiles`。  
    - 是否还有必要复制 bind mount 的内容？  
    - 能否通过为 guest 暴露额外的 block 或 shared‑fs（9pfs/virtiofs）来替代复制？

- **块设备选择与可观测性**
  - **Q14**：现在 guest 实际使用的是 **活动 rootfs 的块设备**，view snapshot 主要用于文件读取和日志观测。  
    - 是否有场景需要 guest 直接使用 view 对应的块设备？  
    - devmapper 视角下，这两者在隔离性或性能上有何差异？

- **边界情况与健壮性**
  - **Q15**：`getMountInfo(rs.cntrRootfs)` 有可能失败，导致无法走 block 方案。  
    - 在真实部署中，这类失败可能来自哪些因素（例如 mount namespace、挂载传播配置等）？  
    - 是否需要在文档中明确列出这些前置条件？
  - **Q16**：view mount 是只读的，但我们通过 bind‑mount 将其中的文件暴露给 monitor rootfs。  
    - 是否还需要在 urunc 侧强制设置额外的只读标志，避免某些意外写入？

- **不同 unikernel 类型的适配**
  - **Q17**：当前 devmapper/blockfile 主要服务于 Rumprun / Linux guest 的 block‑based rootfs。  
    - 对 Unikraft / Mirage / Linux / Rumprun 等不同 guest，是否有不同的推荐 rootfs 模式（initrd、block、9pfs、virtiofs）？  
    - view snapshot 方案是否优先服务某几种 guest 类型？

#### 4.4 评估与实验设计

- **评估指标**
  - **Q18**：性能评估主要关注哪些指标？
    - 容器启动到 guest 应用 ready 的时间？  
    - 启动阶段的 I/O 读写量？  
    - 还是整体 CPU / 内存占用？
  - **Q19**：存储评估如何度量？
    - devmapper thinpool usage（before/after）？  
    - blockfile scratch 文件大小变化？  
    - 还是 snapshot 元数据数量和大小？

- **基线场景**
  - **Q20**：默认基线是否确定为“当前实现的 copy+umount 路径”？  
    - 是否需要增加 overlayfs + initrd/9pfs 作为额外对比基线？
  - **Q21**：希望评估哪些典型 workload？
    - 小镜像（例如 nginx‑alpine）、大镜像、包含大量小文件的镜像等。

- **工具与可观测性**
  - **Q22**：是否有推荐复用的脚本或工具（例如 `script/performance` 下的工具），用于采集时延、I/O 和容量数据？  
    - 是否需要在 urunc 或 shim 中增加额外日志 / metrics（例如“是否走 snapshot view 路径”、“view 创建/挂载耗时”）？

#### 4.5 项目范围、里程碑与沟通方式

- **范围与优先级**
  - **Q23**：这次 mentorship 中，文档、实现、评估三部分的大致优先级如何？  
    - 是否希望实现部分只覆盖 devmapper，还是同时覆盖 blockfile？
  - **Q24**：对于 block‑based snapshotter，当前阶段是否可以只 focus 在 devmapper，上线后再考虑 blockfile？

- **里程碑划分**
  - **Q25**：是否可以把项目分为三个阶段：
    - 阶段 1：containerd snapshot / view API 与语义调研 + 文档；  
    - 阶段 2：urunc/shim 最小可用实现（MVP）；  
    - 阶段 3：系统化评估与文档收尾。  
    - 导师希望在哪些阶段参与 review / pair‑programming？

- **沟通与协作方式**
  - **Q26**：日常沟通的节奏与渠道如何安排？  
    - 是否采用每周一次同步会议 + GitHub issue / PR 评论的方式协作？
  - **Q27**：代码开发时，导师更倾向于：
    - 早期就开 WIP PR，频繁 push，随时讨论；  
    - 还是等到功能基本成型后再提交较完整的 PR？

---

### 五、kick‑off 会议建议结构（可选）

- **1）简要说明背景和当前实现现状**  
  用第 1、2 节内容，用 5–10 分钟说明 motivation 和现有实现两条路径。

- **2）确认项目目标与范围**  
  基于第 3 节，确认文档/实现/评估各自的深度与优先级。

- **3）围绕第 4 节问题进行讨论**  
  优先讨论：
  - containerd view snapshot 语义与 API（4.1）；  
  - urunc/shim 职责边界（4.2）；  
  - 评估指标与基线（4.4）。

- **4）确定下一步里程碑和沟通方式**  
  明确第一个里程碑要交付什么，以及之后的会议/反馈节奏。

---

## 附录 A：如何通过 bunny 构建 `urunc` 可运行的 OCI 镜像（详细版）

本附录回答一个实践问题：**如何用 [bunny](https://github.com/nubificus/bunny) 把一个 unikernel（以及可选的 initrd / block rootfs）打包成一个 `urunc` 能直接运行的 OCI 镜像**。

### A.1 先厘清两类镜像：`urunc-deploy` vs “unikernel OCI 镜像”

- **`urunc-deploy` 镜像（本仓库 `deployment/urunc-deploy/`）**  
  作用是把 `urunc`、`containerd-shim-urunc-v2` 以及所需的 hypervisor（二进制）安装到 Kubernetes 节点上（DaemonSet 方式）。  
  它是“安装运行时”的镜像，**不是你要运行的应用/unikernel 镜像**。

- **unikernel OCI 镜像（本文重点）**  
  这是一个“像容器镜像一样的包”，里面放了 unikernel 的二进制、initrd（可选）、以及 `urunc` 运行需要的元数据。  
  `urunc` 作为 OCI runtime 会拉取该镜像并启动 unikernel。

### A.2 `urunc` 需要哪些元数据：annotations 与 `/urunc.json`

`urunc` 可以从两处拿到 unikernel 的运行参数：

1. **OCI image annotations（推荐）**：由镜像的 `LABEL` 写入（最终变成 image annotations）。  
2. **rootfs 中的 `/urunc.json`（兜底）**：当上层运行时（尤其是 Docker 生态中的一些路径）不透传 image annotations 到底层 runtime 时，`urunc` 会回退读取 rootfs 根目录下的 `urunc.json`。  

在 `bunny` 的 Dockerfile-like 语法中：**所有 `LABEL` 会同时写入 annotations，并写入一个特殊的 `/urunc.json`**（便于兜底）。

#### A.2.1 常用字段（最小可运行集合）

以下字段基本上决定了 `urunc` 如何启动 guest：

- `com.urunc.unikernel.unikernelType`：unikernel 框架类型  
  支持值：`unikraft` / `rumprun` / `mirage`
- `com.urunc.unikernel.hypervisor`：使用哪个 VMM/monitor  
  支持值：`qemu` / `firecracker` / `spt` / `hvt`
- `com.urunc.unikernel.binary`：unikernel 二进制在容器 rootfs 内的路径  
  例如：`/unikernel/kernel`
- `com.urunc.unikernel.cmdline`：传给 unikernel 的命令行字符串  
  例如：`nginx -c /nginx/conf/nginx.conf`

常见可选字段：

- `com.urunc.unikernel.initrd`：initrd 路径（如果你的 guest 以 initrd 方式启动 rootfs）  
  例如：`/unikernel/initrd`
- `com.urunc.unikernel.mountRootfs`：是否请求 `urunc` 将镜像 rootfs（容器 rootfs）挂载/暴露给 guest  
  值通常写成字符串 `"true"` / `"false"`（因为来源是 annotations）
- `com.urunc.unikernel.block`、`com.urunc.unikernel.blkMntPoint`：以 block 镜像形式附加 rootfs/数据盘时使用

### A.3 bunny 的两种输入格式：Dockerfile-like 与 `bunnyfile`

`bunny` 依托 BuildKit（LLB）工作，支持两类输入文件：

- **Dockerfile-like（常用来“打包已构建产物”）**  
  优点：和 Dockerfile 类似，上手快；用 `COPY` 放文件，用 `LABEL` 写 annotations。  
  限制：文档中明确该语法主要用于**打包 pre-built unikernel**，不负责构建 unikernel 本身。

- **`bunnyfile`（YAML）**  
  优点：语义更贴近“unikernel 产物 + 平台信息（framework/monitor/arch）+ rootfs/kernal 来源”；也支持从 OCI 镜像复用内置文件。  
  适合：你希望用同一套规范描述“从本地文件/从远端 OCI 镜像取 kernel/initrd/rootfs，然后生成可运行镜像”。

两者都依赖 BuildKit frontend 机制：**文件首行必须声明 syntax**，例如：

```Dockerfile
#syntax=harbor.nbfc.io/nubificus/bunny:latest
```

> 说明：这行的意义是告诉 buildkit/docker：“用 bunny 这个 frontend 来解析后面的文件内容”，而不是用默认 Dockerfile frontend。

### A.4 方案 1：用 Dockerfile-like（`Containerfile`）打包本地 unikernel 产物

这是最直接、最常见的路径：你已经有 `kernel`、`initrd`（可选）等产物，只需要打进 OCI 并写好 LABEL。

#### A.4.1 目录准备（示例）

假设当前目录包含：

- `kernel`：unikernel 二进制
- `rootfs.cpio`：initrd（cpio 格式）  

你可以把它们按你喜欢的路径放进镜像 rootfs；推荐统一放在 `/unikernel/` 下，避免和应用文件冲突。

#### A.4.2 示例 `Containerfile`（unikraft + qemu + initrd）

```Dockerfile
#syntax=harbor.nbfc.io/nubificus/bunny:latest
FROM scratch

COPY kernel /unikernel/kernel
COPY rootfs.cpio /unikernel/initrd

LABEL "com.urunc.unikernel.binary"="/unikernel/kernel"
LABEL "com.urunc.unikernel.initrd"="/unikernel/initrd"
LABEL "com.urunc.unikernel.cmdline"="nginx -c /nginx/conf/nginx.conf"
LABEL "com.urunc.unikernel.unikernelType"="unikraft"
LABEL "com.urunc.unikernel.hypervisor"="qemu"
LABEL "com.urunc.unikernel.mountRootfs"="false"
```

#### A.4.3 构建与推送

```bash
# 构建
docker build -f Containerfile -t <registry>/<repo>:<tag> .

# 推送
docker push <registry>/<repo>:<tag>
```

> 经验建议：tag 推荐包含 `framework`、`monitor`、`arch`、版本号/commit，便于排障与复现，例如：  
> `<tag>=unikraft-qemu-amd64-<app>-<gitsha>`

### A.5 方案 2：用 `bunnyfile` 打包本地产物（或同时描述平台信息）

当你希望把“平台参数（framework/monitor/arch）”写得更结构化，推荐 `bunnyfile`。

#### A.5.1 示例 `bunnyfile`（本地 kernel + 本地 initrd）

```yaml
#syntax=harbor.nbfc.io/nubificus/bunny:latest
version: v0.1

platforms:
  framework: unikraft
  monitor: qemu
  architecture: x86

rootfs:
  from: local
  path: rootfs.cpio

kernel:
  from: local
  path: kernel

cmdline: nginx -c /nginx/conf/nginx.conf
```

构建：

```bash
docker build -f bunnyfile -t <registry>/<repo>:<tag> .
```

### A.6 方案 3：复用一个“已有 OCI 镜像里”的 unikernel，给它补齐 `urunc` 元数据

当你拿到的镜像已经包含 kernel/initrd/rootfs（例如来自某个 catalog），但没有 `urunc` 所需 annotations（或上层不透传）时，可以用 bunny 生成一个“可被 urunc 运行的新镜像”。

#### A.6.1 示例：从 `unikraft.org/nginx:1.15` 取 kernel 并补齐元数据

```yaml
#syntax=harbor.nbfc.io/nubificus/bunny:latest
version: v0.1

platforms:
  framework: unikraft
  monitor: qemu
  architecture: x86

kernel:
  from: unikraft.org/nginx:1.15
  path: /unikraft/bin/kernel

cmdline: "nginx -c /nginx/conf/nginx.conf"
```

构建：

```bash
docker build -f bunnyfile -t <registry>/<repo>:<tag> .
```

### A.7 运行与验证（用 containerd/nerdctl）

在已安装 `urunc` runtime 的节点上（例如你通过 `urunc-deploy` 完成安装后），建议优先用 `nerdctl` 验证：

```bash
sudo nerdctl run --rm \
  --runtime io.containerd.urunc.v2 \
  <registry>/<repo>:<tag>
```

如果你在评估 block-based snapshotter（如 devmapper / blockfile），可以指定 snapshotter：

```bash
sudo nerdctl run --rm \
  --snapshotter devmapper \
  --runtime io.containerd.urunc.v2 \
  <registry>/<repo>:<tag>
```

> 说明：`urunc` 的一些优化路径（如 view snapshot 读取 `urunc.json`/kernel/initrd）依赖 containerd snapshotter 与 shim 的配合；因此在该类环境里，用 `nerdctl + containerd` 的路径验证更贴近真实部署。

### A.8 常见踩坑与排查建议

- **镜像 annotations 没有透传**  
  现象：你用 Docker/某些高层工具跑，`urunc` 看不到 annotations。  
  处理：确保镜像 rootfs 根目录存在 `/urunc.json`（bunny 的 LABEL 路径通常会生成）；运行时尽量走 containerd/nerdctl 路径。

- **`com.urunc.unikernel.binary` 路径不对**  
  现象：启动时报找不到 kernel/unikernel binary。  
  处理：检查 `COPY` 目的路径与 `LABEL com.urunc.unikernel.binary` 是否一致；建议固定放 `/unikernel/` 下。

- **hypervisor/monitor 与产物不匹配**  
  现象：例如把需要 Solo5-hvt 的产物标成 qemu，启动失败。  
  处理：确认 `com.urunc.unikernel.hypervisor` 与实际产物格式一致（mirage/solo5 常见 `hvt`，unikraft 常见 `qemu`/`firecracker`）。

- **架构不匹配（amd64/arm64）**  
  现象：镜像在不同节点架构运行失败。  
  处理：对每个架构分别构建并发布（或用 buildx 构建多架构 manifest）；并让镜像 tag/manifest 反映架构。

### A.9 推荐的最小交付物（你可以直接复制改名）

为了让团队协作时“路径/语义一致”，建议在每个 unikernel 项目目录里至少包含：

- `Containerfile` 或 `bunnyfile`（二选一）
- `kernel`（以及 `rootfs.cpio`/block image，如适用）
- 一份 `README` 说明如何构建/运行（包含 `nerdctl run --runtime io.containerd.urunc.v2 ...` 示例）

---

## 附录 B：view snapshot 方案 vs 传统 copy+umount 方案性能评估设计

本附录给出一套可复现的实验方案，用于对比：

- **传统方案（baseline）**：不使用 view snapshot，从 devmapper active rootfs 挂载点 **copy `kernel/initrd/urunc.json` 到 monitor rootfs**，然后 `umount` 并把块设备交给 guest；
- **view 方案（current）**：shim 为容器 rootfs 创建只读 **view snapshot**，在 host 上挂载到 `/run/urunc/views/<cid>`，`urunc` 从 view 中 **bind‑mount `kernel/initrd/urunc.json` 到 monitor rootfs**，不再复制这些文件。

目标是从 **启动时延、I/O 读写量、存储占用** 三个维度，量化两种方案的差异。

### B.1 对比方式：两套二进制 + 两个 runtime 名称

为避免在单一二进制中引入测试开关，本方案通过 **分别编译“无 view 版”和“有 view 版”**，并在 containerd 中注册成两个不同的 runtime 名称：

- **无 view 版（baseline，假设 runtime 名为 `urunc_noview`）**
  - 基于未引入 view 的实现（或在专门分支中移除 view 相关改动），编译：
    - `urunc-noview`
    - `containerd-shim-urunc-v2-noview`
  - 安装到：
    - `/usr/local/bin/urunc-noview`
    - `/usr/local/bin/containerd-shim-urunc-v2-noview`
  - 行为：**不调用 containerd snapshot API 创建 view，也不向 `config.json` 注入 `annotSnapshotViewMountPath`**，始终走 `copy + prepareDMAsBlock + extractFilesFromBlock` 的旧路径。

- **有 view 版（current，假设 runtime 名为 `urunc_view`）**
  - 基于当前 `main`（包含 view + bind‑mount 实现），编译：
    - `urunc-view`
    - `containerd-shim-urunc-v2-view`
  - 安装到：
    - `/usr/local/bin/urunc-view`
    - `/usr/local/bin/containerd-shim-urunc-v2-view`
  - 行为：shim 在 `CreateSnapshotView` 中创建只读 view，并将 mount path 写入 `config.json` 注解；`urunc` 读取 `annotSnapshotViewMountPath`，在 `handleCntrRootfsAsBlock` 中走 **view + bind‑mount** 分支。

在 containerd 的 `config.toml`（通常为 `/etc/containerd/config.toml`）中，注册两个 runtime：

```toml
[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.urunc_noview]
  runtime_type = "io.containerd.urunc.v2"
  privileged_without_host_devices = true
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.urunc_noview.options]
    BinaryName = "/usr/local/bin/containerd-shim-urunc-v2-noview"

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.urunc_view]
  runtime_type = "io.containerd.urunc.v2"
  privileged_without_host_devices = true
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.urunc_view.options]
    BinaryName = "/usr/local/bin/containerd-shim-urunc-v2-view"
```

重启 containerd 后，即可通过 `--runtime` 选择不同方案：

- **无 view：**

  ```bash
  sudo nerdctl run --rm \
    --snapshotter devmapper \
    --runtime urunc_noview \
    <image>
  ```

- **有 view：**

  ```bash
  sudo nerdctl run --rm \
    --snapshotter devmapper \
    --runtime urunc_view \
    <image>
  ```

除 `--runtime` 名字外，所有参数（镜像、snapshotter、节点配置）保持一致，以保证对比公平。

### B.2 实验矩阵：测哪些组合

建议从以下几个基本维度组合实验：

- **镜像特性**
  - 小镜像：文件少、总体几十 MB，例如 nginx‑alpine 风格；
  - 中等镜像：典型 unikraft/nginx 镜像；
  - 大镜像：包含较多静态资源 / 大量小文件的镜像。

- **启动场景**
  - 冷启动：重启机器或重启 containerd 后，首次启动容器（页缓存尽量为空）；
  - 热启动：在同一节点上反复启动同一镜像（多次 `nerdctl run`），测试 cache 命中下的差异。

- **负载强度**
  - 单实例：一次只启动 1 个容器；
  - 并发实例：同时启动 N（例如 10、50）个同镜像实例，观察放大效应。

一个最小可行的实验矩阵示例：

- 小镜像 × 冷启动 × 单实例
- 小镜像 × 热启动 × 50 实例
- 中等镜像 × 冷启动 × 单实例
- 中等镜像 × 热启动 × 50 实例

在每个点上，分别对 `urunc_noview` 和 `urunc_view`：

- 独立运行多次（例如 30 次），收集时延分布（平均、P50、P90、P99）；
- 记录同样的 I/O 和存储指标（见下一节）。

### B.3 评估指标与采集方式

#### B.3.1 启动时延

**目标：** 对比“传统方案 vs view 方案”的 **端到端启动时间**。

- **简单测量**：在外层用 `/usr/bin/time` 包裹 `nerdctl run`：

  ```bash
  for i in $(seq 1 30); do
    /usr/bin/time -f "%e" \
      sudo nerdctl run --rm \
        --snapshotter devmapper \
        --runtime urunc_noview \
        <image> > /dev/null
  done
  ```

  对 `urunc_view` 重复同样流程，仅切换 `--runtime`。

- **更精细的阶段划分（可选）**：
  - 利用 `urunc` 现有的 metrics 埋点（例如 `TS15`~`TS18`），从日志中推算：
    - containerd 创建 task → `urunc Exec` 开始；
    - `Exec` 开始 → guest 应用 ready（可通过 guest 日志中某句特征 log 标记）。
  - 这样可以单独量化“view 创建 + bind‑mount”引入的额外时延。

#### B.3.2 I/O 读写量（磁盘 / 块设备）

**目标：** 衡量旧路径中对 `kernel/initrd/urunc.json` 的 file copy 对 I/O 的影响。

可选方式：

- 使用 `iostat -dx 1` 在实验期间采集磁盘读写吞吐，在每组测试开始前清零计数；
- 或使用 `pidstat -d 1 -p <urunc/shim pid>` 粗略观察 `urunc` / shim 的读写量；
- 针对 devmapper thinpool：
  - 在每组实验前后，通过 `dmsetup status <pool>` / `lvdisplay` 记录使用块数；
  - 对比 `urunc_noview` 与 `urunc_view` 在运行 N 个容器后 thinpool 使用量的增量。

预期：

- `urunc_noview`：由于从 active rootfs 复制 `kernel/initrd/urunc.json`，启动阶段磁盘写入与 thinpool 使用都会明显随容器数线性增长；
- `urunc_view`：view snapshot 自身不复制数据，bind‑mount 不写盘，thinpool 使用增量应接近 0（仅有少量元数据变化）。

#### B.3.3 存储占用（/run 下临时文件、bundle 大小）

**目标：** 对比两种方案在 `/run` 下生成的临时数据规模。

方法：

- 确认 container bundle 根路径（例如 `/run/containerd/io.containerd.runtime.v2.task/k8s.io`）；
- 运行一批容器后，对各容器 bundle，特别是 `monRootfs` 目录做 `du`：

  ```bash
  sudo du -sh /run/containerd/io.containerd.runtime.v2.task/k8s.io/* | sort -h
  sudo du -sh /run/containerd/io.containerd.runtime.v2.task/k8s.io/<container-id>/monRootfs
  ```

预期：

- baseline（无 view）：`monRootfs` 下会实际持有一份 `kernel/initrd/urunc.json` 文件，尺寸约等于它们之和；
- view 方案：`monRootfs` 对这几个路径是 bind‑mount 到 view，不额外占用 data blocks，`du -sh` 的结果应显著更小。

### B.4 建议的脚本结构

为方便重复实验，建议在仓库中增加一个简单的 performance 脚本目录，例如 `scripts/perf/`：

- `scripts/perf/run_noview.sh`：
  - 固定使用 `--runtime urunc_noview`；
  - 接受参数：镜像名、运行次数、并发度等；
  - 外层用 `/usr/bin/time` 统计启动时间。
- `scripts/perf/run_view.sh`：
  - 与 `run_noview.sh` 同构，只是 `--runtime` 换成 `urunc_view`。

在实验时：

- 保证两套脚本的参数（镜像、snapshotter、节点配置）完全一致，仅切换 runtime 名；
- 在独立终端运行 `iostat` / `pidstat` / `dmsetup status` 等工具，按 B.3 小节记录 I/O 与空间数据；
- 对每组（noview vs view）跑足够次数（建议每个点至少 30 次），计算启动时延分布，并与 I/O / 存储观察结果对照分析。

通过以上设计，可以系统化地量化 **view snapshot + bind‑mount 方案** 相对于传统 **copy+umount 方案** 在性能和资源占用上的收益，并且评估结果可在不同节点 / 镜像组合上重复验证。
