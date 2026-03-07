### urunc devmapper snapshot view 实现前后对比

本文对比 `urunc` 在 **使用 devmapper 作为 snapshotter** 时，围绕 “容器 rootfs 作为块设备” 这一条路径，在 **引入 snapshot view 之前** 和 **当前实现（带 view）** 的差异。重点放在：

- 如何处理 **unikernel binary / initrd / `urunc.json`** 这三个关键文件；
- 如何处理 **bind mounts**（例如 `/etc/resolv.conf`、ConfigMap/Secret、volume 等）；
- 为什么在有 snapshot view 的情况下，仍然保留了 `copyMountfiles` 的逻辑。

---

### 1. 总体流程对比：旧实现 vs 新实现

#### 1.1 旧实现（无 snapshot view）

核心代码在旧版 `block.go` 和 `rootfs.go` 中：

```1:120:pkg/unikontainers/block.go
// extractUnikernelFromBlock moves unikernel binary, initrd and urunc.json
// files from old rootfsPath to newRootfsPath
// FIXME: This approach fills up /run with unikernel binaries, initrds and urunc.json
// files for each unikernel we run
func extractFilesFromBlock(rootfsPath string, newRootfsPath string, unikernel string, uruncJSON string, initrd string) error {
	// ...
}

// prepareDMAsBLock copies the files needed for the unikernel boot (e.g.
// unikernel binary, initrd file) and the urunc.json file in a new temporary
// directory. Then it unmounts the devmapper device and renames the temporary
// directory as the container rootfs. This is needed to keep the same paths
// for the unikernel files.
func prepareDMAsBlock(rootfsPath string, newRootfsPath string, unikernel string, uruncJSON string, initrd string) error {
	// extract unikernel
	// FIXME: This approach fills up /run with unikernel binaries and
	// urunc.json files for each unikernel instance we run
	err := extractFilesFromBlock(rootfsPath, newRootfsPath, unikernel, uruncJSON, initrd)
	if err != nil {
		return err
	}
	// unmount block device
	// FIXME: umount and rm might need some retries
	err = mount.Unmount(rootfsPath)
	if err != nil {
		return err
	}

	return nil
}

func handleCntrRootfsAsBlock(rfs types.RootfsParams, unikernelType string, unikernelPath string, uruncJSONFilename string, initrdPath string, mounts []specs.Mount) (types.BlockDevParams, error) {
	err := copyMountfiles(rfs.MountedPath, mounts)
	if err != nil {
		return types.BlockDevParams{}, err
	}

	err = prepareDMAsBlock(rfs.MountedPath, rfs.MonRootfs, unikernelPath, uruncJSONFilename, initrdPath)
	if err != nil {
		return types.BlockDevParams{}, err
	}

	err = setupDev(rfs.MonRootfs, rfs.Path)
	if err != nil {
		return types.BlockDevParams{}, err
	}

	mp := "/"
	if unikernelType == "rumprun" {
		mp = "/data"
	}

	return types.BlockDevParams{
		Source:     rfs.Path,
		MountPoint: mp,
		ID:         "rootfs",
	}, nil
}
```

旧路径下，“容器 rootfs 作为块设备” 的关键步骤是：

- 在 `rootfs.go` 的 `chooseRootfs` 中，选出容器 rootfs 作为 block rootfs：

```60:103:pkg/unikontainers/rootfs.go
// tryContainerBlockRootfs checks if container rootfs can be used as a block device
// for guest's rootfs
func (rs *rootfsSelector) tryContainerBlockRootfs() (types.RootfsParams, bool) {
	if !rs.unikernel.SupportsBlock() {
		return types.RootfsParams{}, false
	}

	rootFsDevice, err := getMountInfo(rs.cntrRootfs)
	if err != nil {
		uniklog.Errorf("failed to get container's rootfs mount info: %v", err)
		return types.RootfsParams{}, false
	}

	if !rs.unikernel.SupportsFS(rootFsDevice.FsType) {
		return types.RootfsParams{}, false
	}

	return newRootfsResult("block", rootFsDevice.Source, rs.cntrRootfs, rs.cntrRootfs), true
}
```

- `handleBlockBasedRootfs` 将 `rfs.MountedPath != ""` 的情况交给 `handleCntrRootfsAsBlock`：

```180:210:pkg/unikontainers/block.go
func handleBlockBasedRootfs(rfs types.RootfsParams, ukernel types.Unikernel, unikernelType string, unikernelPath string, uruncJSONFilename string, initrdPath string, mounts []specs.Mount) ([]types.BlockDevParams, error) {
	// ...
	if rfs.MountedPath == "" {
		rootfsBlock, err = handleExplicitBlockImage(rfs.Path, "/")
	} else {
		rootfsBlock, err = handleCntrRootfsAsBlock(rfs, unikernelType, unikernelPath, uruncJSONFilename, initrdPath, mounts)
	}
	// ...
}
```

- `handleCntrRootfsAsBlock` 旧实现的语义：

  - 先对所有 bind mounts 调用 `copyMountfiles(rfs.MountedPath, mounts)`，把 bind 源的内容复制进当前 active rootfs 的挂载点；
  - 再调用 `prepareDMAsBlock(rfs.MountedPath, rfs.MonRootfs, ...)`：
    - 将 **unikernel binary / initrd / `urunc.json` 从容器 rootfs 拷贝到 `monRootfs`**；
    - 然后 `umount(rootfsPath)` 卸载容器 rootfs 的 devmapper 设备；
  - 最后调用 `setupDev(rfs.MonRootfs, rfs.Path)` 在 monitor rootfs 里创建设备节点，把卸载后的块设备暴露给 guest。

> 结果：**每个 unikernel 实例都会在 `/run` 下生成一份 unikernel / initrd / `urunc.json` 的副本**（`monRootfs` 目录），并且有明显的 FIXME 提示会堆垃圾。

#### 1.2 新实现（带 snapshot view）

在当前实现中，`block.go` 和 `rootfs.go` 被扩展，以支持 **shim 预先创建的 snapshot view**。核心变化如下。

1. 从 `rootfs.go` 里开始，用 annotation 感知 shim 注入的 view 挂载点：

```101:151:pkg/unikontainers/rootfs.go
// tryContainerBlockRootfs checks if container rootfs can be used as a block device
// for guest's rootfs. It first tries to use a snapshot view if enabled, otherwise
// falls back to the direct block device approach.
func (rs *rootfsSelector) tryContainerBlockRootfs() (types.RootfsParams, bool) {
	if !rs.unikernel.SupportsBlock() {
		return types.RootfsParams{}, false
	}

	// Try snapshot view optimization if the shim has already created and
	// mounted a view for us. Unlike the previous implementation, urunc no
	// longer talks to containerd directly; it only consumes annotations and
	// mountpoints prepared by the shim.
	viewMountPath := rs.annot[annotSnapshotViewMountPath]

	if viewMountPath != "" {
		// 先使用 snapshot view
		// ...
		result := newRootfsResult("block", activeRootfsDevice.Source, rs.cntrRootfs, rs.cntrRootfs)
		result.FromSnapshotView = true
		result.SnapshotView = &types.SnapshotViewResult{
			MountPath:   viewMountPath,
			BlockDevice: viewBlockDevice,
		}
		return result, true
	}

	// Original logic: use the container rootfs block device directly
	// ...
	return newRootfsResult("block", rootFsDevice.Source, rs.cntrRootfs, rs.cntrRootfs), true
}
```

2. 在 `block.go` 里，新增了 `bindViewFilesToMonRootfs`，并对 `handleCntrRootfsAsBlock` 进行分支扩展：

```85:131:pkg/unikontainers/block.go
// bindViewFilesToMonRootfs bind-mounts unikernel, initrd, and urunc.json from the
// snapshot view into the monitor rootfs so the VMM can read them directly
// (no copy, no storage overhead). Used when FromSnapshotView is true.
func bindViewFilesToMonRootfs(viewMountPath, monRootfs, unikernelPath, initrdPath, uruncJSON string) error {
	// ...
}
```

```238:353:pkg/unikontainers/block.go
func handleCntrRootfsAsBlock(rfs types.RootfsParams, unikernelType string, unikernelPath string, uruncJSONFilename string, initrdPath string, mounts []specs.Mount) (types.BlockDevParams, error) {
	if rfs.FromSnapshotView {
		// 使用 snapshot view 的路径：不再复制 unikernel/initrd/urunc.json
		err := bindViewFilesToMonRootfs(rfs.SnapshotView.MountPath, rfs.MonRootfs, unikernelPath, initrdPath, uruncJSONFilename)
		if err != nil {
			return types.BlockDevParams{}, err
		}

		// bind mounts 仍然需要 copy 到 active rootfs，再 umount active rootfs
		err = copyMountfiles(rfs.MountedPath, mounts)
		if err != nil {
			return types.BlockDevParams{}, err
		}
		if err := mount.Unmount(rfs.MountedPath); err != nil {
			return types.BlockDevParams{}, err
		}

		// setupDev(rfs.MonRootfs, rfs.Path) 与旧路径相同
		// 返回的 block 仍然是 active rootfs 对应的设备
		return types.BlockDevParams{
			Source:     rfs.Path,
			MountPoint: mp, // "/" 或 "/data"
			ID:         "rootfs",
		}, nil
	}

	// 否则，走原来的 copy+unmount 路径（见 1.1）
	// ...
}
```

> 结果：  
> - **有 view 时**：unikernel binary / initrd / `urunc.json` 不再从 active rootfs 复制到 `monRootfs`，而是从只读 view 上 bind‑mount 到 `monRootfs`；  
> - **无 view 时**：仍然走旧的 `prepareDMAsBlock` 路径，行为与之前保持兼容。

---

### 2. 对三个关键文件的处理差异

#### 2.1 旧实现：实拷贝到 `monRootfs`

旧版 `extractFilesFromBlock` 和 `prepareDMAsBlock` 中，对三个文件的处理是实拷贝：

```120:170:pkg/unikontainers/block.go
// extractUnikernelFromBlock moves unikernel binary, initrd and urunc.json
// files from old rootfsPath to newRootfsPath
// FIXME: This approach fills up /run with unikernel binaries, initrds and urunc.json
// files for each unikernel we run
func extractFilesFromBlock(rootfsPath string, newRootfsPath string, unikernel string, uruncJSON string, initrd string) error {
	currentUnikernelPath := filepath.Join(rootfsPath, unikernel)
	targetUnikernelPath := filepath.Join(newRootfsPath, unikernel)
	// moveFile + 目标目录创建
	// ...

	if initrd != "" {
		// 同样的 moveFile 到 newRootfsPath
	}

	currentConfigPath := filepath.Join(rootfsPath, uruncJSON)
	err = moveFile(currentConfigPath, newRootfsPath)
	if err != nil {
		return fmt.Errorf("Could not move %s to %s: %w", currentConfigPath, newRootfsPath, err)
	}

	return nil
}

func prepareDMAsBlock(rootfsPath string, newRootfsPath string, unikernel string, uruncJSON string, initrd string) error {
	// extract unikernel
	// FIXME: This approach fills up /run with unikernel binaries and
	// urunc.json files for each unikernel instance we run
	err := extractFilesFromBlock(rootfsPath, newRootfsPath, unikernel, uruncJSON, initrd)
	if err != nil {
		return err
	}

	// unmount block device
	err = mount.Unmount(rootfsPath)
	if err != nil {
		return err
	}

	return nil
}
```

- **来源**：容器 rootfs 的挂载点 `rootfsPath`（devmapper active snapshot）；
- **目标**：新的 monitor rootfs 目录 `newRootfsPath`（即 bundle 下的 `monRootfs`）；
- **副作用**：
  - 每个 unikernel 实例在 `/run/containerd/.../<bundle>/monRootfs/` 下拥有一份独立的 unikernel binary、initrd、`urunc.json` 副本；
  - 对于 devmapper 这种 block‑based snapshotter，这会在 `/run` 和 thinpool 上形成明显的写放大。

#### 2.2 新实现：从只读 view bind‑mount

新实现中，三个文件的处理从“复制”变为“从只读 view bind‑mount 出来”：

```85:131:pkg/unikontainers/block.go
// bindViewFilesToMonRootfs bind-mounts unikernel, initrd, and urunc.json from the
// snapshot view into the monitor rootfs so the VMM can read them directly
// (no copy, no storage overhead). Used when FromSnapshotView is true.
func bindViewFilesToMonRootfs(viewMountPath, monRootfs, unikernelPath, initrdPath, uruncJSON string) error {
	norm := func(p string) string { return strings.TrimPrefix(filepath.Clean(p), "/") }
	files := []struct{ src, target string }{
		{filepath.Join(viewMountPath, unikernelPath), norm(unikernelPath)},
		{filepath.Join(viewMountPath, uruncJSON), norm(uruncJSON)},
	}
	if initrdPath != "" {
		files = append(files, struct{ src, target string }{
			filepath.Join(viewMountPath, initrdPath), norm(initrdPath),
		})
	}

	for _, f := range files {
		dstPath := filepath.Join(monRootfs, f.target)
		dstDir := filepath.Dir(dstPath)
		if err := bindMountFile(f.src, dstDir, dstPath, 0, unix.MS_BIND|unix.MS_PRIVATE, false); err != nil {
			return fmt.Errorf("bind view %s -> monRootfs/%s: %w", f.src, f.target, err)
		}
	}
	return nil
}
```

在 `handleCntrRootfsAsBlock` 中，只在 `FromSnapshotView == true` 时调用该函数：

```238:305:pkg/unikontainers/block.go
if rfs.FromSnapshotView {
	// Using snapshot view: bind-mount unikernel/initrd/urunc.json from view into
	// monitor rootfs so we read directly (no copy, no storage overhead).
	err := bindViewFilesToMonRootfs(rfs.SnapshotView.MountPath, rfs.MonRootfs, unikernelPath, initrdPath, uruncJSONFilename)
	if err != nil {
		return types.BlockDevParams{}, err
	}
	// ...
}
```

这样：

- **源数据** 仍然来自 devmapper 的 snapshot 链（view 通常挂的是 parent snapshot），active/rootfs 对应的设备与 view 共享 parent；
- monitor 只通过 `monRootfs` 上的 bind mount 来读这些文件，不再在本地目录产生额外副本；
- **卸载 active rootfs 后，guest 使用的块设备是 active snapshot 对应的设备**，与 view 所在的只读设备分离。

---

### 3. 为什么有 view 时仍然需要 `copyMountfiles`

从旧实现到新实现，`copyMountfiles` 的功能基本保持不变，只是调用位置在 view 分支里做了更细致的日志和注释：

```203:215:pkg/unikontainers/block.go
func copyMountfiles(targetPath string, mounts []specs.Mount) error {
	for _, m := range mounts {
		if m.Type != "bind" {
			continue
		}
		err := fileFromHost(targetPath, m.Source, m.Destination, 0, true)
		if (err != nil) && !errors.Is(err, ErrCopyDir) {
			return err
		}
	}
	return nil
}
```

在 snapshot view 分支中，仍然对 active rootfs（`rfs.MountedPath`）调用该函数：

```255:268:pkg/unikontainers/block.go
// We must NOT copy bind-mounted files into the snapshot view mount (read-only).
// Instead, copy them into the active container rootfs mount (rfs.MountedPath),
// then unmount it before passing its block device to the guest (same semantics
// as the original copy+unmount path, but without copying unikernel artifacts).
uniklog.Debug("Snapshot view path: copying bind-mount contents into active rootfs before unmount")
copyStart := time.Now()
err = copyMountfiles(rfs.MountedPath, mounts)
if err != nil {
	return types.BlockDevParams{}, err
}
```

原因在于：

- **我们解决的问题只针对那三个“镜像里就有的文件”**（unikernel / initrd / `urunc.json`），这些数据本来就在 devmapper snapshot 链上，active 和 view 共享同一 parent，可以通过 view 直接读取；
- **而 bind mounts 提供的内容（resolv.conf、Secret、ConfigMap、hostPath volume 等）**：
  - 往往来自完全不同的设备或目录，不在 snapshot 链里；
  - 在容器进程看来，它们是叠加在 rootfs 挂载点上的附加视图；
  - 如果我们不在 umount 前把这些内容“抄”进 active rootfs 对应的文件系统，那么：
    - 卸载 bind mounts 后，active snapshot 对应的块设备里不会包含这些文件；
    - guest 看到的 rootfs 就不再等价于原来的 “container rootfs + bind mounts” 视图。

因此：

- **snapshot view 仅仅用来避免复制 unikernel/initrd/`urunc.json` 这三个文件**；
- **对于 bind mounts，仍然需要一次 `copyMountfiles` 将其内容 materialize 到 active rootfs 中**，这样才能在卸载 rootfs 挂载点、把块设备交给 guest 之后，保持 guest 侧看到的文件树与容器原始语义一致。

---

### 4. shim 与 urunc 之间的分工变化（简述）

虽然本页主要比较 `urunc` 内部逻辑，但引入 snapshot view 后，shim 与 `urunc` 的分工也有所调整：

- **旧实现**：
  - `urunc` 自己需要与 containerd 交互、理解 snapshot 结构（或者完全依赖 active rootfs 的挂载）；
  - 所有 “view / snapshot” 相关的控制面逻辑都在 `urunc` 侧。

- **新实现**：
  - shim 负责调用 containerd 的 `SnapshotService.View`，创建只读 snapshot view，并挂到固定目录（如 `/run/urunc/views/<containerID>`）；
  - shim 将该挂载点通过 annotation `com.urunc.snapshot.view.mount_path` 注入 `config.json`；
  - `urunc` 在 `tryContainerBlockRootfs` 里只消费这个注解，不再直接操作 containerd；
  - 具体的文件读取优化由 `bindViewFilesToMonRootfs` 在 `block.go` 中完成。

这种分工带来的效果是：

- `urunc` 核心逻辑更专注于“如何把已经存在的 view 和 active snapshot 组合成 guest rootfs”；
- 控制面（view 的创建、lease 管理等）留在 shim + containerd 这条路径，便于统一治理与观测。

---

### 5. 小结

- **旧实现**：
  - 使用 `prepareDMAsBlock` + `extractFilesFromBlock`：
    - 把 unikernel binary / initrd / `urunc.json` 从容器 rootfs 拷贝到 bundle 下的 `monRootfs`；
    - 然后卸载 active rootfs，将其对应的块设备直接给 guest；
  - 同时用 `copyMountfiles` 将 bind mounts 的内容复制进 active rootfs，使块设备内的视图接近容器视角。

- **新实现（带 snapshot view）**：
  - `tryContainerBlockRootfs` 通过 annotation 感知 shim 创建的 snapshot view，并设置 `FromSnapshotView = true`；
  - 有 view 时：
    - 使用 `bindViewFilesToMonRootfs` 从只读 view 中 bind‑mount 出 unikernel/initrd/`urunc.json` 到 `monRootfs`，**不再复制这些文件**；
    - 仍然在 active rootfs 上执行一次 `copyMountfiles`，然后卸载 active rootfs，并将其块设备给 guest；
  - 无 view 时：
    - 完全回退到旧路径（含文件复制），行为与之前保持兼容。

整体来说，新实现**精确地把“避免复制”的优化限制在那三个关键文件上**，同时保持了容器 rootfs + bind mounts 语义在 guest 侧的一致性，并将与 containerd 的控制面交互下沉到 shim 层完成。

