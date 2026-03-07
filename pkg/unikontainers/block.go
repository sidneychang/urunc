// Copyright (c) 2023-2026, Nubificus LTD
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unikontainers

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/moby/sys/mount"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	"github.com/urunc-dev/urunc/pkg/unikontainers/types"
)

var ErrMountpoint = errors.New("no FS is mounted in this mountpoint")

// getMountInfo checks if the path (given as argument) is a mountpoint
// looking at /proc/self/mountinfo.
// If the path is indeed a mount point then getMountInfo stores and returns
// the respective info in a BlockDevParams struct.
// If the path is not a mount point (not present in /proc/self/mountinfo)
// then getMountInfo returns an empty BlockDevParams struct and ErrMountpoint error.
func getMountInfo(path string) (types.BlockDevParams, error) {
	selfProcMountInfo := "/proc/self/mountinfo"

	file, err := os.Open(selfProcMountInfo)
	if err != nil {
		return types.BlockDevParams{}, fmt.Errorf("failed to open mountinfo: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " - ")
		if len(parts) != 2 {
			return types.BlockDevParams{}, fmt.Errorf("invalid mountinfo line in /proc/self/mountinfo")
		}

		fields := strings.Fields(parts[0])
		if len(fields) < 5 || fields[4] != path {
			continue
		}
		fields = strings.Fields(parts[1])
		if len(fields) < 2 {
			continue
		}
		uniklog.WithFields(logrus.Fields{
			"mounted at": path,
			"device":     fields[1],
			"fstype":     fields[0],
		}).Debug("Found container rootfs mount")

		return types.BlockDevParams{
			Source:     fields[1],
			FsType:     fields[0],
			MountPoint: path,
			ID:         "",
		}, nil
	}

	return types.BlockDevParams{}, ErrMountpoint
}

// bindViewFilesToMonRootfs bind-mounts unikernel, initrd, and urunc.json from the
// snapshot view into the monitor rootfs so the VMM can read them directly
// (no copy, no storage overhead). Used when FromSnapshotView is true.
func bindViewFilesToMonRootfs(viewMountPath, monRootfs, unikernelPath, initrdPath, uruncJSON string) error {
	start := time.Now()
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

	uniklog.WithFields(logrus.Fields{
		"view_mount": viewMountPath,
		"mon_rootfs": monRootfs,
		"files":      len(files),
	}).Info("Bind-mounting unikernel/initrd/urunc.json from snapshot view into monitor rootfs (no copy)")

	for _, f := range files {
		// We cannot reuse fileFromHost here because snapshot views are mounted
		// read-only (devmapper requirement). fileFromHost() unconditionally tries
		// to chmod/chown the destination path after bind-mounting, which fails
		// with EROFS on a read-only filesystem. For snapshot views we only need
		// a bind mount without touching permissions.
		dstPath := filepath.Join(monRootfs, f.target)
		dstDir := filepath.Dir(dstPath)
		if err := bindMountFile(f.src, dstDir, dstPath, 0, unix.MS_BIND|unix.MS_PRIVATE, false); err != nil {
			return fmt.Errorf("bind view %s -> monRootfs/%s: %w", f.src, f.target, err)
		}

		uniklog.WithFields(logrus.Fields{
			"src":    f.src,
			"target": filepath.Join(monRootfs, f.target),
		}).Debug("Bind-mounted file from view")
	}
	uniklog.WithFields(logrus.Fields{
		"view_mount":   viewMountPath,
		"mon_rootfs":   monRootfs,
		"files":        len(files),
		"duration_ms":  time.Since(start).Milliseconds(),
	}).Info("Finished bind-mounting view files into monitor rootfs")
	return nil
}

// extractUnikernelFromBlock moves unikernel binary, initrd and urunc.json
// files from old rootfsPath to newRootfsPath
// FIXME: This approach fills up /run with unikernel binaries, initrds and urunc.json
// files for each unikernel we run
func extractFilesFromBlock(rootfsPath string, newRootfsPath string, unikernel string, uruncJSON string, initrd string) error {
	uniklog.WithFields(logrus.Fields{
		"src": rootfsPath, "dst": newRootfsPath,
		"unikernel": unikernel, "initrd": initrd, "urunc_json": uruncJSON,
	}).Debug("Extracting unikernel/initrd/urunc.json from block rootfs (copy)")

	currentUnikernelPath := filepath.Join(rootfsPath, unikernel)
	targetUnikernelPath := filepath.Join(newRootfsPath, unikernel)
	targetUnikernelDir, _ := filepath.Split(targetUnikernelPath)
	err := moveFile(currentUnikernelPath, targetUnikernelDir)
	if err != nil {
		return fmt.Errorf("Could not move %s to %s: %w", currentUnikernelPath, targetUnikernelPath, err)
	}
	uniklog.WithField("path", targetUnikernelPath).Debug("Copied unikernel")

	if initrd != "" {
		currentInitrdPath := filepath.Join(rootfsPath, initrd)
		targetInitrdPath := filepath.Join(newRootfsPath, initrd)
		targetInitrdDir, _ := filepath.Split(targetInitrdPath)
		err = moveFile(currentInitrdPath, targetInitrdDir)
		if err != nil {
			return fmt.Errorf("Could not move %s to %s: %w", currentInitrdPath, targetInitrdPath, err)
		}
		uniklog.WithField("path", targetInitrdPath).Debug("Copied initrd")
	}

	currentConfigPath := filepath.Join(rootfsPath, uruncJSON)
	err = moveFile(currentConfigPath, newRootfsPath)
	if err != nil {
		return fmt.Errorf("Could not move %s to %s: %w", currentConfigPath, newRootfsPath, err)
	}
	uniklog.WithField("path", filepath.Join(newRootfsPath, uruncJSON)).Debug("Copied urunc.json")
	return nil
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

	uniklog.
		WithField("rootfs_path", rootfsPath).
		WithField("mon_rootfs_path", newRootfsPath).
		Info("Prepared monitor rootfs from block-based container rootfs (devmapper/blockfile snapshot)")

	uniklog.WithField("rootfs_path", rootfsPath).Info("Unmounting block-based container rootfs to reuse device for unikernel")
	// unmount block device
	// FIXME: umount and rm might need some retries
	err = mount.Unmount(rootfsPath)
	if err != nil {
		return err
	}
	uniklog.WithField("rootfs_path", rootfsPath).Info("Unmounted block-based container rootfs")

	return nil
}

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

func handleExplicitBlockImage(blockImg string, mountPoint string) (types.BlockDevParams, error) {
	if blockImg == "" {
		return types.BlockDevParams{}, nil
	}

	if mountPoint == "" {
		return types.BlockDevParams{}, fmt.Errorf("annotation for block device was set without a mountpoint")
	}

	id := ""
	if mountPoint == "/" {
		id = "rootfs"
	}

	return types.BlockDevParams{
		Source:     blockImg,
		MountPoint: mountPoint,
		ID:         id,
	}, nil
}

func handleCntrRootfsAsBlock(rfs types.RootfsParams, unikernelType string, unikernelPath string, uruncJSONFilename string, initrdPath string, mounts []specs.Mount) (types.BlockDevParams, error) {
	if rfs.FromSnapshotView {
		start := time.Now()
		// Using snapshot view: bind-mount unikernel/initrd/urunc.json from view into
		// monitor rootfs so we read directly (no copy, no storage overhead).
		uniklog.
			WithField("mounted_path", rfs.MountedPath).
			WithField("block_device", rfs.Path).
			WithField("mon_rootfs", rfs.MonRootfs).
			Info("Setting up container rootfs as block device for guest using snapshot view (no file copy needed)")

		uniklog.Debug("Snapshot view path: bind-mounting unikernel/initrd/urunc.json from view mount into monitor rootfs")
		err := bindViewFilesToMonRootfs(rfs.SnapshotView.MountPath, rfs.MonRootfs, unikernelPath, initrdPath, uruncJSONFilename)
		if err != nil {
			return types.BlockDevParams{}, err
		}

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
		uniklog.WithFields(logrus.Fields{
			"rootfs_path":  rfs.MountedPath,
			"duration_ms":  time.Since(copyStart).Milliseconds(),
		}).Info("Snapshot view path: copied bind-mount contents into active rootfs")

		uniklog.WithField("rootfs_path", rfs.MountedPath).Info("Snapshot view path: unmounting active container rootfs to reuse device for unikernel")
		umountStart := time.Now()
		if err := mount.Unmount(rfs.MountedPath); err != nil {
			return types.BlockDevParams{}, err
		}
		uniklog.WithFields(logrus.Fields{
			"rootfs_path":  rfs.MountedPath,
			"duration_ms":  time.Since(umountStart).Milliseconds(),
		}).Info("Snapshot view path: unmounted active container rootfs")

		uniklog.WithField("block_device", rfs.Path).Debug("Snapshot view path: setting up block device in monitor rootfs")
		setupStart := time.Now()
		err = setupDev(rfs.MonRootfs, rfs.Path)
		if err != nil {
			return types.BlockDevParams{}, err
		}
		uniklog.WithFields(logrus.Fields{
			"block_device": rfs.Path,
			"duration_ms":  time.Since(setupStart).Milliseconds(),
		}).Info("Snapshot view path: finished setting up block device in monitor rootfs")

		mp := "/"
		if unikernelType == "rumprun" {
			mp = "/data"
		}

		uniklog.WithFields(logrus.Fields{
			"block_device": rfs.Path,
			"guest_mount":  mp,
			"duration_ms":  time.Since(start).Milliseconds(),
		}).Info("Snapshot view block setup complete; guest rootfs will use view block device")
		return types.BlockDevParams{
			Source:     rfs.Path,
			MountPoint: mp,
			ID:         "rootfs",
		}, nil
	}

	// Original logic: copy files and unmount original rootfs
	uniklog.
		WithField("mounted_path", rfs.MountedPath).
		WithField("block_device", rfs.Path).
		WithField("mon_rootfs", rfs.MonRootfs).
		Info("Setting up container rootfs as block device for guest (block-based snapshotter, copy+unmount path)")

	uniklog.Debug("Block path (no snapshot view): copying mount files")
	err := copyMountfiles(rfs.MountedPath, mounts)
	if err != nil {
		return types.BlockDevParams{}, err
	}

	uniklog.WithFields(logrus.Fields{
		"rootfs_path": rfs.MountedPath,
		"mon_rootfs":  rfs.MonRootfs,
		"unikernel":   unikernelPath,
		"initrd":      initrdPath,
		"urunc_json":  uruncJSONFilename,
	}).Info("Block path: copying unikernel/initrd/urunc.json then unmounting container rootfs")
	err = prepareDMAsBlock(rfs.MountedPath, rfs.MonRootfs, unikernelPath, uruncJSONFilename, initrdPath)
	if err != nil {
		return types.BlockDevParams{}, err
	}

	uniklog.Debug("Block path: setting up block device in monitor rootfs")
	err = setupDev(rfs.MonRootfs, rfs.Path)
	if err != nil {
		return types.BlockDevParams{}, err
	}

	mp := "/"
	// NOTE: Rumprun does not allow us to mount
	// anything at '/'. As a result, we use the
	// /data mount point for Rumprun. For all the
	// other guests we use '/'.
	if unikernelType == "rumprun" {
		mp = "/data"
	}

	return types.BlockDevParams{
		Source:     rfs.Path,
		MountPoint: mp,
		ID:         "rootfs",
	}, nil
}

// Search all the mount entries in the container's config and
// find the ones that come from a block.
func getBlockVolumes(monRootfs string, mounts []specs.Mount, ukernel types.Unikernel) ([]types.BlockDevParams, error) {
	blkImgs := []types.BlockDevParams{}
	for i, m := range mounts {
		// We check only bind mounts
		if m.Type != "bind" {
			continue
		}
		// Get the information of the source path
		// from /proc/self/mountinfo
		mInfo, err := getMountInfo(m.Source)
		if errors.Is(err, ErrMountpoint) {
			// ErrMountpoint means we did not find any
			// such mount and hence we can skip it.
			continue
		}
		if err != nil {
			return nil, err
		}
		if ukernel.SupportsFS(mInfo.FsType) {
			err = mount.Unmount(mInfo.MountPoint)
			if err != nil {
				return nil, err
			}
			err = setupDev(monRootfs, mInfo.Source)
			if err != nil {
				return nil, err
			}
			mInfo.ID = fmt.Sprintf("vol%d", i)
			mInfo.MountPoint = m.Destination
			blkImgs = append(blkImgs, mInfo)
		}
	}

	return blkImgs, nil
}

func handleBlockBasedRootfs(rfs types.RootfsParams, ukernel types.Unikernel, unikernelType string, unikernelPath string, uruncJSONFilename string, initrdPath string, mounts []specs.Mount) ([]types.BlockDevParams, error) {
	var blockArgs []types.BlockDevParams
	var rootfsBlock types.BlockDevParams
	var err error

	// Determine if this is an explicit block file case vs container rootfs as block
	// Explicit block: uses a block file (e.g., /rootfs.ext2) inside the container (MountedPath == "")
	// Container rootfs as block: the container rootfs itself is the block device (MountedPath != "")
	if rfs.MountedPath == "" {
		// Explicit block file case: block file is accessed directly from container rootfs
		rootfsBlock, err = handleExplicitBlockImage(rfs.Path, "/")
	} else {
		// Container rootfs as block device case (may use snapshot view optimization)
		rootfsBlock, err = handleCntrRootfsAsBlock(rfs, unikernelType, unikernelPath, uruncJSONFilename, initrdPath, mounts)
	}

	if err != nil {
		return nil, err
	}
	rootfsBlock.ID = "rootfs"
	blockArgs = append(blockArgs, rootfsBlock)
	blockFromMounts, err := getBlockVolumes(rfs.MonRootfs, mounts, ukernel)
	if err != nil {
		return nil, err
	}
	blockArgs = append(blockArgs, blockFromMounts...)

	return blockArgs, nil
}
