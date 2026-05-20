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

// Per-container snapshot view: read-only mounts from shim-written urunc-view.json
// (kernel, initrd, urunc.json bind-copied from image root). Mount descriptors are
// persisted by the shim; this package does not dial containerd.

package unikontainers

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/mount"
	"golang.org/x/sys/unix"
)

const (
	perContainerViewStateFile       = "urunc-view.json"
	perContainerViewMountsStateFile = "urunc-view-mounts.json"
)

var errPerContainerViewNotPrepared = errors.New("snapshot view not prepared")

type perContainerViewState struct {
	ViewKey     string        `json:"view_key"`
	Snapshotter string        `json:"snapshotter"`
	Namespace   string        `json:"namespace"`
	Mounts      []mount.Mount `json:"mounts,omitempty"`
}

type perContainerViewMountsState struct {
	Targets []string `json:"targets,omitempty"`
}

// tryRunOnPerContainerView loads shim-written per-container view metadata from
// the bundle directory, mounts that snapshot view read-only on a temp
// directory, runs fn with that mount root, then unmounts and removes the temp dir.
//
// If no view was prepared (missing state file), returns (false, nil) so the
// caller can fall back to the legacy path.
//
// If state exists, returns (true, err) where err is from mounting, fn, or
// unmount (unmount failure is returned only when fn succeeded).
func tryRunOnPerContainerView(bundle string, fn func(mountRoot string) error) (ok bool, retErr error) {
	state, err := loadPerContainerViewState(bundle)
	if err != nil {
		if errors.Is(err, errPerContainerViewNotPrepared) {
			return false, nil
		}
		return false, err
	}

	mountpoint, err := mkTempSnapshotViewDir()
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(mountpoint)

	if err := mountPerContainerViewReadonly(state, mountpoint); err != nil {
		return false, err
	}
	defer func() {
		if uerr := unmountSnapshotViewTemp(mountpoint); uerr != nil {
			if retErr == nil {
				retErr = uerr
				return
			}
			uniklog.WithError(uerr).WithField("path", mountpoint).Warn("failed to unmount temporary snapshot view mount")
		}
	}()

	retErr = fn(mountpoint)
	return true, retErr
}

func loadPerContainerViewState(bundle string) (*perContainerViewState, error) {
	path := filepath.Join(bundle, perContainerViewStateFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errPerContainerViewNotPrepared
		}
		return nil, fmt.Errorf("read snapshot view state %s: %w", path, err)
	}

	var state perContainerViewState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot view state %s: %w", path, err)
	}
	return &state, nil
}

func mountPerContainerViewReadonly(state *perContainerViewState, target string) error {
	if len(state.Mounts) == 0 {
		return fmt.Errorf("snapshot view state %s has no mounts (recreate the container with an updated shim)", perContainerViewStateFile)
	}

	return mountReadonlySnapshotView(target, state.Mounts)
}

func mkTempSnapshotViewDir() (string, error) {
	dir, err := os.MkdirTemp("", "urunc-snapshot-view-")
	if err != nil {
		return "", fmt.Errorf("create temporary snapshot view mountpoint: %w", err)
	}
	return dir, nil
}

func unmountSnapshotViewTemp(path string) error {
	if err := mount.Unmount(path, 0); err != nil {
		if os.IsNotExist(err) || err == unix.EINVAL {
			return nil
		}
		uniklog.WithError(err).WithField("path", path).Warn("failed to unmount snapshot view")
		return err
	}
	return nil
}

func mountReadonlySnapshotView(target string, mounts []mount.Mount) error {
	if err := mount.All(mounts, target); err != nil {
		return fmt.Errorf("mount snapshot view at %s for boot file bind: %w", target, err)
	}

	return nil
}

func persistPerContainerViewMountsState(bundle string, targets []string) error {
	state := perContainerViewMountsState{Targets: targets}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal snapshot view mount state: %w", err)
	}

	path := filepath.Join(bundle, perContainerViewMountsStateFile)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write snapshot view mount state %s: %w", path, err)
	}

	return nil
}

func loadPerContainerViewMountsState(bundle string) (*perContainerViewMountsState, error) {
	path := filepath.Join(bundle, perContainerViewMountsStateFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read snapshot view mount state %s: %w", path, err)
	}

	var state perContainerViewMountsState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot view mount state %s: %w", path, err)
	}

	return &state, nil
}

func removePerContainerViewMountsState(bundle string) error {
	path := filepath.Join(bundle, perContainerViewMountsStateFile)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove snapshot view mount state %s: %w", path, err)
	}

	return nil
}

func cleanupPerContainerViewMounts(bundle string) error {
	state, err := loadPerContainerViewMountsState(bundle)
	if err != nil || state == nil {
		return err
	}

	for i := len(state.Targets) - 1; i >= 0; i-- {
		target := filepath.Clean(state.Targets[i])
		if err := unix.Unmount(target, unix.MNT_DETACH); err != nil {
			if err == unix.EINVAL || err == unix.ENOENT || os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to unmount snapshot view target %s: %w", target, err)
		}
	}

	return removePerContainerViewMountsState(bundle)
}

func rollbackPerContainerViewTargets(targets []string) {
	for i := len(targets) - 1; i >= 0; i-- {
		target := filepath.Clean(targets[i])
		if err := unix.Unmount(target, unix.MNT_DETACH); err != nil {
			if err == unix.EINVAL || err == unix.ENOENT || os.IsNotExist(err) {
				continue
			}
			uniklog.WithError(err).WithField("target", target).Warn("failed to roll back snapshot view bind mount")
		}
	}
}
