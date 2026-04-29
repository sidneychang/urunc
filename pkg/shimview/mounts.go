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

package shimview

import (
	"context"
	"fmt"
	"os"

	"github.com/containerd/containerd/mount"
	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
)

func mountSharedViewMounts(target string, mounts []mount.Mount, createdTarget bool, action string) error {
	if err := mount.All(normalizeReadonlyMounts(mounts), target); err != nil {
		if createdTarget {
			_ = os.RemoveAll(target)
		}
		return fmt.Errorf("mount shared snapshot at %s for %s: %w", target, action, err)
	}
	return nil
}

func ensureSharedViewPrepared(
	ctx context.Context,
	client *containerdClients,
	ns, snapshotter, viewKey, snapshotKey string,
	paths sharedViewPaths,
) error {
	unlock, err := acquireSharedViewLock(paths.lockPath)
	if err != nil {
		return err
	}
	defer unlock()

	createdData, err := ensureSharedViewDirs(paths)
	if err != nil {
		return err
	}
	_, err = getOrCreateSharedViewMounts(ctx, client, ns, snapshotter, viewKey, snapshotKey)
	if err != nil && createdData {
		_ = os.RemoveAll(paths.dataDir)
	}
	return err
}

func newTemporarySharedViewMountpoint(paths sharedViewPaths) (string, error) {
	if _, err := ensureSharedViewDirs(paths); err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp(paths.dataDir, "mnt-")
	if err != nil {
		return "", fmt.Errorf("create temporary shared view mountpoint under %s: %w", paths.dataDir, err)
	}
	return dir, nil
}

func mountSharedViewAt(ctx context.Context, info *SnapshotViewInfo, target string) error {
	mounted, err := mountinfo.Mounted(target)
	if err != nil {
		log.WithError(err).WithField("path", target).Warn("failed to check mountpoint status while preparing shared view use")
	}
	if mounted {
		return nil
	}

	client, err := newContainerdClient()
	if err != nil {
		return err
	}
	defer client.Close()

	mounts, err := loadSharedViewMounts(ctx, client, info)
	if err != nil {
		return err
	}

	if err := mountSharedViewMounts(target, mounts, false, "artifact use"); err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(target, 0o755); mkErr == nil {
				return mountSharedViewMounts(target, mounts, false, "artifact use retry")
			}
		}
		return err
	}
	return nil
}

func unmountMountpoint(path string) error {
	if err := mount.Unmount(path, 0); err != nil {
		if os.IsNotExist(err) || err == unix.EINVAL {
			return nil
		}
		log.WithError(err).WithField("path", path).Warn("failed to unmount shared snapshot view")
		return err
	}
	return nil
}

func cleanupSharedViewMounts(paths sharedViewPaths) error {
	mounts, err := mountinfo.GetMounts(mountinfo.PrefixFilter(paths.dataDir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("list shared view mounts under %s: %w", paths.dataDir, err)
	}

	for i := len(mounts) - 1; i >= 0; i-- {
		if err := unmountMountpoint(mounts[i].Mountpoint); err != nil {
			return err
		}
	}
	return nil
}

func normalizeReadonlyMounts(mounts []mount.Mount) []mount.Mount {
	if len(mounts) == 0 {
		return mounts
	}

	normalized := make([]mount.Mount, len(mounts))
	for i, m := range mounts {
		opts := m.Options
		if len(opts) > 0 {
			newOpts := make([]string, 0, len(opts))
			for _, opt := range opts {
				if opt == "rw" {
					opt = "ro"
				}
				newOpts = append(newOpts, opt)
			}
			opts = newOpts
		}
		normalized[i] = mount.Mount{
			Type:    m.Type,
			Source:  m.Source,
			Target:  m.Target,
			Options: opts,
		}
	}

	return normalized
}
