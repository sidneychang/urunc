本文档介绍如何使用 `nerdctl`（基于 `containerd`）运行 `urunc` 容器，并说明本仓库近期引入的 **RO snapshot view（只读视图）** 机制在运行时如何自动生效与如何验证。

## 背景：什么是 RO view（只读视图）

当你使用 `--runtime io.containerd.urunc.v2` 启动容器时，`containerd-shim-urunc-v2` 会在 **Create** 阶段（best-effort）为该容器的 rootfs 创建一个 **read-only snapshot view**，并把它挂载到宿主机：

- `/run/urunc/views/<containerID>`

然后 shim 会把该只读挂载路径注入到 bundle 的 OCI `config.json` annotation：

- `com.urunc.snapshot.view.mount_path=/run/urunc/views/<containerID>`

`urunc` 在启动时只读取这条 annotation 来消费 RO view（不再直接去连 containerd），用于从只读视图里读取 unikernel/initrd/`urunc.json` 等文件，从而避免拷贝、降低 IO 放大；若 view 创建失败，则会自动回退到原有路径，不影响容器启动（只是不使用该优化）。

## 前置条件

- 已安装并运行 `containerd`
- 已安装 `nerdctl`
- 已安装 `urunc` 与 `containerd-shim-urunc-v2`
- 宿主机具备你要使用的 monitor 所需能力（例如 KVM、TAP、对应 VMM 二进制等；具体依赖见 `docs/installation.md` / `docs/hypervisor-support.md`）

### 1) 快速检查二进制是否齐全

```bash
command -v containerd ctr nerdctl runc urunc containerd-shim-urunc-v2
urunc --version
containerd --version
nerdctl --version
```

> 说明：有些环境里 `urunc` 可能是一个包装脚本（例如转发到 `urunc.default --debug`），这是正常的。

### 2) 检查 containerd 是否在运行

```bash
systemctl is-active containerd
```

### 3) 确认 containerd 已配置 urunc runtime

打开 `/etc/containerd/config.toml`，确保存在 `urunc` runtime 配置，且 `runtime_type` 为：

- `io.containerd.urunc.v2`

示例（不同 containerd 版本/发行版的 section 名称可能略有差异，以你实际配置为准）：

```toml
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.urunc]
  runtime_type = "io.containerd.urunc.v2"
  container_annotations = ["com.urunc.unikernel.*"]
  pod_annotations = ["com.urunc.unikernel.*"]
  # snapshotter 可按需设置：overlayfs / devmapper / blockfile ...
  snapshotter = "devmapper"
```

修改后需要重启 containerd：

```bash
sudo systemctl restart containerd
```

## 权限说明：为什么需要 sudo

多数发行版默认将 containerd socket 设置为 `root:root 0660` 或更严格，因此普通用户会遇到：

- `permission denied`（连接 `/run/containerd/containerd.sock` 时）

这种情况下最简单的方式是 **直接使用 sudo 运行 nerdctl/ctr**：

```bash
sudo nerdctl ps
```

### （可选）启用非 root 访问

如果你希望非 root 用户直接使用 `nerdctl/ctr`，可以通过给 containerd 的 gRPC socket 配置一个 group（并把用户加入该组）来实现。不同发行版/版本的配置方式略有差异，请结合你的安全策略谨慎设置；相关思路是：

- 在 `/etc/containerd/config.toml` 的 `[grpc]` 里设置 `gid = <某个非 0 的组 id>`
- 重启 containerd
- 确保 socket 的 group 与权限允许该组读写
- 将你的用户加入该组并重新登录

## 使用 nerdctl 运行 urunc

下面示例都显式指定 `--runtime io.containerd.urunc.v2`。

### 示例 A：使用默认 snapshotter（通常是 overlayfs）

```bash
sudo nerdctl run --rm -ti \
  --runtime io.containerd.urunc.v2 \
  harbor.nbfc.io/nubificus/urunc/nginx-qemu-unikraft-initrd:latest
```

### 示例 B：使用 devmapper snapshotter（块设备快照）

```bash
sudo nerdctl run -d \
  --snapshotter devmapper \
  --runtime io.containerd.urunc.v2 \
  harbor.nbfc.io/nubificus/urunc/nginx-qemu-unikraft-initrd:latest
  # harbor.nbfc.io/nubificus/urunc/redis-hvt-rumprun-raw:latest
```

### 如何配置 devmapper（使用 thinpool）

使用 `--snapshotter devmapper` 前，需要先在宿主机上创建 devmapper thinpool 并让 containerd 在重启后能自动恢复。步骤如下（依赖 `bc`，可先执行 `sudo apt install -y bc`）。

**1. 安装脚本并创建 thinpool**

```bash
# 若已有 urunc 仓库
cd /path/to/urunc
sudo mkdir -p /usr/local/bin/scripts
sudo cp script/dm_create.sh /usr/local/bin/scripts/dm_create.sh
sudo cp script/dm_reload.sh /usr/local/bin/scripts/dm_reload.sh
sudo chmod 755 /usr/local/bin/scripts/dm_create.sh
sudo chmod 755 /usr/local/bin/scripts/dm_reload.sh

# 创建 thinpool（会在 /var/lib/containerd/io.containerd.snapshotter.v1.devmapper 下生成 data/meta 等）
sudo /usr/local/bin/scripts/dm_create.sh
```

**2. 开机自动恢复 thinpool（推荐）**

重启后 thinpool 会消失，需要先恢复再启动 containerd。可用 systemd 服务在开机时执行 `dm_reload.sh`（脚本末尾会 `systemctl restart containerd`）：

```bash
sudo mkdir -p /usr/local/lib/systemd/system
sudo cp script/dm_reload.service /usr/local/lib/systemd/system/dm_reload.service
sudo chmod 644 /usr/local/lib/systemd/system/dm_reload.service
sudo systemctl daemon-reload
sudo systemctl enable dm_reload.service
```

无 systemd 时，需在每次重启后手动执行：`sudo /usr/local/bin/scripts/dm_reload.sh`。

**3. 在 containerd 中启用 devmapper snapshotter**

在 `/etc/containerd/config.toml` 中增加或修改（containerd v2.x 示例）：

```toml
[plugins.'io.containerd.snapshotter.v1.devmapper']
  pool_name = "containerd-pool"
  root_path = "/var/lib/containerd/io.containerd.snapshotter.v1.devmapper"
  base_image_size = "10GB"
  discard_blocks = true
  fs_type = "ext2"
```

然后重启 containerd：

```bash
sudo systemctl restart containerd
```

**4. 验证**

```bash
sudo ctr plugin ls | grep devmapper
# 应看到：io.containerd.snapshotter.v1  devmapper  linux/amd64  ok
```

更细的说明见 [installation.md - Block-based snapshotters](../installation.md#setting-and-configuring-devmapper)。

## 如何验证 RO view 是否生效

RO view 是 shim 的实现细节，**你不需要额外参数开启**。你可以用下面方式做“可观测性”验证。

### 1) RO view 在哪里？怎么看到？

RO view 的挂载点路径是固定的：

- **目录**：`/run/urunc/views/<容器ID>`
- **容器 ID**：与 `nerdctl ps` / `nerdctl ps -a` 里的 **CONTAINER ID** 一致（可用短 ID，如 `65a`）。

**操作步骤：**

```bash
# 先看当前（或全部）容器，记下 CONTAINER ID
sudo nerdctl ps
# 或包含已停止的： sudo nerdctl ps -a

# RO view 就在下面，每个运行过的容器一个子目录（容器删除后会被 shim 清理掉）
sudo ls -la /run/urunc/views

# 若已有容器在跑，例如 ID 为 65a，则 view 路径为：
sudo ls -la /run/urunc/views/65a
```

该目录下是**只读**的 rootfs 视图，可直接从里面读 unikernel、initrd、`urunc.json` 等文件，无需拷贝。

### 2) 验证 view 是否真的被创建并挂载

在容器运行期间执行：

```bash
sudo ls -la /run/urunc/views
```

若能看到以容器 ID 命名的子目录，说明 RO view 已创建并挂载；进入该目录即可看到与镜像 rootfs 一致的文件布局。

### 3) 查看日志（推荐）

shim 在创建/挂载 view 时会输出类似 “created and mounted snapshot view …” 的日志。日志落点取决于你的部署方式（systemd/journald、containerd 日志等）。在 systemd 环境里通常可以用：

```bash
sudo journalctl -u containerd --no-pager -n 200
```

> 说明：即使创建 view 失败，shim 也会 best-effort 继续 Create，urunc 仍可能正常启动，只是不会使用该优化。

### 4) RO view 和 “committed snapshot” 的关系（devmapper）

在使用 **devmapper** 时，当前容器的 snapshot 通常有一个**父层（parent）**，即已经 commit 的只读层。shim 创建 RO view 时的行为是：

- **RO view 的来源**：优先用该容器的 **committed parent snapshot**（父层）调用 snapshotter 的 `View()`，得到只读视图并挂到 `/run/urunc/views/<容器ID>`。若从 parent 建 view 失败，会回退为用当前（active）snapshot 建 view。
- **也就是说**：**committed snapshot 被用来创建 RO view**，view 里的内容就是父层的内容；urunc 只从 view 里读 unikernel/initrd/urunc.json 等，不做文件拷贝。
- **给 guest 的 rootfs 块设备**：仍是**当前 active（可写）层**对应的块设备，不是 view。view 仅用于在宿主机上“只读读文件”，guest 的磁盘仍是可写的 active 层。

### 5) 为什么 `findmnt` / `mount` 里看到两个 devmapper 设备？（两个都被用了吗？）

使用 devmapper 且启用了 RO view 时，同一个容器会对应**两个** thin 卷（例如 `containerd-pool-snap-16` 和 `containerd-pool-snap-17`），**两个都在用**，分工不同：

| 设备 | 挂载点 | 读写 | 用途 |
|------|--------|------|------|
| 例如 `containerd-pool-snap-17` | `/run/urunc/views/<容器ID>` | **ro**（只读） | RO view：shim 创建的只读视图，urunc 从这里读 unikernel/initrd/urunc.json，不拷贝文件。 |
| 例如 `containerd-pool-snap-16` | `/run/containerd/.../rootfs` | **rw**（可写） | 容器 rootfs（active 层）：runc 挂给容器进程的根目录，也是传给 guest 的 rootfs 块设备。 |

所以：**snap-17（ro）** = 只读 view；**snap-16（rw）** = 容器的可写层 + guest 的磁盘。编号 16/17 由 devmapper 分配，不同机器或不同容器会不同。

## 常见问题排查

### 1) `permission denied` 连接 containerd.sock

- **现象**：`ctr` / `nerdctl` 报无法连接 `/run/containerd/containerd.sock`，permission denied
- **处理**：用 `sudo nerdctl ...`；或参考上文“启用非 root 访问”调整 socket 权限

### 2) `runtime io.containerd.urunc.v2 not found`

- **原因**：containerd 未配置 urunc runtime，或未重启
- **处理**：检查 `/etc/containerd/config.toml` 是否包含 `runtime_type = "io.containerd.urunc.v2"` 并重启 containerd

### 3) RO view 未生成

- **可能原因**：
  - snapshotter 不支持 view
  - 容器没有 SnapshotKey/Snapshotter（某些特殊场景）
  - 创建 view 失败（例如 devmapper 需要从 committed parent view，或 mount 权限问题）
- **结论**：RO view 是优化项，失败应当自动回退；优先看 `journalctl -u containerd` 获取原因

### 4) 内核报 `Buffer I/O error on dev dm-X`（devmapper）

- **现象**：`dmesg` 或 `journalctl -k` 里反复出现 `Buffer I/O error on dev dm-1`、`dm-3` 等。
- **常见原因**：
  1. **只做了 `nerdctl stop`，没有做 `nerdctl rm`**  
     `nerdctl stop` 只停止容器进程，**不会**删除容器和其 snapshot，devmapper 的 thin 卷（对应 dm-X）仍被 containerd 占用。若之后 thinpool 被重建或重启后未正确执行 `dm_reload`，内核或 udev 可能仍访问已无效的 dm 设备，就会报 I/O 错误。
  2. **重启后未恢复 thinpool**  
     重启后 thinpool 会消失，若未在 containerd 启动前执行 `dm_reload.sh`（或未启用 `dm_reload.service`），containerd 可能仍引用旧的 dm 设备号，导致对已不存在的设备做 I/O。
- **正确做法**：
  - 停止并**删除**容器时使用：`sudo nerdctl rm -f <容器ID或名>`（或先 `nerdctl stop` 再 `nerdctl rm`），这样 containerd 会删除 snapshot 并释放对应 dm 设备。
  - 若希望“停掉即删”，可用：`sudo nerdctl run --rm ...`，容器退出后会自动 rm。
  - 重启后确保 thinpool 存在：启用并运行 `dm_reload.service`，或在每次重启后执行 `sudo /usr/local/bin/scripts/dm_reload.sh`。
- **已出现大量 I/O 错误时**：可先 `sudo nerdctl rm -f $(sudo nerdctl ps -aq)` 清理已停止的容器，再重启 containerd；若之前未正确做 dm_reload，执行一次 `sudo /usr/local/bin/scripts/dm_reload.sh` 后再用。

