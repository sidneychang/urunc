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

// Snapshot view boot binds.
//
// # Problem
//
// Block rootfs passes the container image to the guest as a block device. Before
// that, urunc still needs kernel, initrd, and urunc.json on the host under
// monRootfs. The legacy path copies them out of the mounted bundle rootfs; the
// snapshot-view path bind-mounts them from a read-only containerd view instead.
//
// # Who owns what
//
//   - Shim (SnapshotViewAccessor): creates/removes the containerd snapshot VIEW,
//     persists mount descriptors in bundle config.json (AnnotSnapshotView).
//   - This file (urunc): temp mount view, bind boot files into monRootfs (see
//     blockRootfs.preSetup and rebindSnapshotViewBootAfterPrepareRoot in Exec).
//     Binds are not persisted for Delete cleanup; they are torn down when the monitor
//     mount namespace is destroyed.
//   - Urunc does not dial containerd for views.
//
// # Boot bind flow (prepareSnapshotViewBootBinds; block re-bind after prepareRoot in Exec)
//
//  1. loadSnapshotViewFromBundle — reads config.json from disk.
//  2. temp mount view, bind boot files into monRootfs, unmount temp view.
//  3. On failure before success return, defer rolls back binds from this attempt.
//
// # Teardown
//
//  Successful containers: boot binds are not persisted; they are released when the
//  monitor mount namespace is destroyed (no explicit umount on Delete).
//  Shim separately removes the containerd snapshot view.

package unikontainers

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/mount"
	"golang.org/x/sys/unix"
)

// AnnotSnapshotView is the config.json annotation key for shim-prepared snapshot views.
const AnnotSnapshotView = "com.urunc.internal.snapshot_view"

// snapshotViewState is the JSON shape stored in config.json AnnotSnapshotView.
type snapshotViewState struct {
	ViewKey         string        `json:"view_key"`
	LeaseID         string        `json:"lease_id,omitempty"`
	Snapshotter     string        `json:"snapshotter"`
	Namespace       string        `json:"namespace"`
	Mounts []mount.Mount `json:"mounts,omitempty"`
}

// PatchBundleSnapshotView writes snapshot view JSON into bundle config.json.
func PatchBundleSnapshotView(bundleDir, snapshotViewJSON string) error {
	configPath := filepath.Join(bundleDir, configFilename)
	fi, err := os.Stat(configPath)
	if err != nil {
		return fmt.Errorf("stat config.json: %w", err)
	}

	spec, err := loadSpec(bundleDir)
	if err != nil {
		return fmt.Errorf("load bundle spec: %w", err)
	}
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}
	if snapshotViewJSON == "" {
		delete(spec.Annotations, AnnotSnapshotView)
	} else {
		spec.Annotations[AnnotSnapshotView] = snapshotViewJSON
	}

	patched, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config.json: %w", err)
	}
	if err := os.WriteFile(configPath, patched, fi.Mode()); err != nil {
		return fmt.Errorf("write config.json: %w", err)
	}
	return nil
}

// ReadBundleSnapshotView returns the snapshot view JSON from bundle config.json.
func ReadBundleSnapshotView(bundleDir string) (string, error) {
	configPath := filepath.Join(bundleDir, configFilename)
	if _, err := os.Stat(configPath); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("stat config.json: %w", err)
	}

	spec, err := loadSpec(bundleDir)
	if err != nil {
		return "", err
	}
	if spec.Annotations == nil {
		return "", nil
	}
	return spec.Annotations[AnnotSnapshotView], nil
}

func parseSnapshotViewJSON(raw string) (*snapshotViewState, error) {
	if raw == "" {
		return nil, nil
	}
	var state snapshotViewState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot view state %s: %w", AnnotSnapshotView, err)
	}
	return &state, nil
}

// loadSnapshotViewStateFromBundle reads AnnotSnapshotView without requiring mounts.
func loadSnapshotViewStateFromBundle(bundleDir string) (*snapshotViewState, error) {
	raw, err := ReadBundleSnapshotView(bundleDir)
	if err != nil {
		return nil, err
	}
	return parseSnapshotViewJSON(raw)
}

// loadSnapshotViewFromBundle reads AnnotSnapshotView for the create/bind path (needs mounts).
func loadSnapshotViewFromBundle(bundleDir string) (*snapshotViewState, error) {
	state, err := loadSnapshotViewStateFromBundle(bundleDir)
	if err != nil || state == nil {
		return state, err
	}
	if len(state.Mounts) == 0 {
		return nil, fmt.Errorf("snapshot view state %s has no mounts (recreate the container with an updated shim)", AnnotSnapshotView)
	}
	return state, nil
}

func snapshotViewPreparedInBundle(bundleDir string) bool {
	raw, err := ReadBundleSnapshotView(bundleDir)
	return err == nil && raw != ""
}

func snapshotViewRelPath(p string) string {
	return strings.TrimPrefix(filepath.Clean(p), "/")
}

func bindBootArtifactsFromView(viewRoot, monRootfs, unikernelPath, initrdPath, uruncJSON string, bindTargets *[]string) error {
	files := []struct{ src, target string }{
		{filepath.Join(viewRoot, snapshotViewRelPath(unikernelPath)), snapshotViewRelPath(unikernelPath)},
		{filepath.Join(viewRoot, snapshotViewRelPath(uruncJSON)), snapshotViewRelPath(uruncJSON)},
	}
	if initrdPath != "" {
		files = append(files, struct{ src, target string }{
			filepath.Join(viewRoot, snapshotViewRelPath(initrdPath)), snapshotViewRelPath(initrdPath),
		})
	}

	for _, f := range files {
		dstPath := filepath.Join(monRootfs, f.target)
		dstDir := filepath.Dir(dstPath)
		if err := bindMountFile(f.src, dstDir, dstPath, 0, unix.MS_BIND, false); err != nil {
			rollbackSnapshotViewBinds(*bindTargets)
			*bindTargets = nil
			return fmt.Errorf("bind view %s -> monRootfs/%s: %w", f.src, f.target, err)
		}
		*bindTargets = append(*bindTargets, dstPath)
	}
	return nil
}

// rollbackSnapshotViewBinds unmounts boot binds from a failed prepareSnapshotViewBootBinds attempt.
func rollbackSnapshotViewBinds(targets []string) {
	for i := len(targets) - 1; i >= 0; i-- {
		if err := unmountSnapshotViewBind(targets[i]); err != nil {
			uniklog.WithError(err).WithField("target", filepath.Clean(targets[i])).Warn("failed to roll back snapshot view bind mount")
		}
	}
}

// prepareSnapshotViewBootBinds reads bundle config.json at call time (after shim Prepare).
func prepareSnapshotViewBootBinds(bundleDir, monRootfs, unikernelPath, initrdPath, uruncJSON string) (useView bool, err error) {
	state, err := loadSnapshotViewFromBundle(bundleDir)
	if err != nil {
		return false, err
	}
	if state == nil {
		return false, nil
	}

	var bindTargets []string
	keepBinds := false
	defer func() {
		if !keepBinds {
			rollbackSnapshotViewBinds(bindTargets)
		}
	}()

	mountpoint, err := os.MkdirTemp("", "urunc-snapshot-view-")
	if err != nil {
		return false, fmt.Errorf("create temporary snapshot view mountpoint: %w", err)
	}
	defer os.RemoveAll(mountpoint)

	if err := mount.All(state.Mounts, mountpoint); err != nil {
		uniklog.WithError(err).Warn("snapshot view unavailable; falling back to legacy boot file extraction")
		return false, nil
	}

	bindErr := bindBootArtifactsFromView(mountpoint, monRootfs, unikernelPath, initrdPath, uruncJSON, &bindTargets)

	uerr := mount.Unmount(mountpoint, 0)
	if uerr != nil && !os.IsNotExist(uerr) && uerr != unix.EINVAL {
		if bindErr == nil {
			bindErr = uerr
		} else {
			uniklog.WithError(uerr).WithField("path", mountpoint).Warn("failed to unmount temporary snapshot view mount")
		}
	}

	if bindErr != nil {
		if len(bindTargets) > 0 {
			return false, fmt.Errorf("snapshot view boot artifact bind completed but cleanup failed: %w", bindErr)
		}
		uniklog.WithError(bindErr).Warn("snapshot view unavailable; falling back to legacy boot file extraction")
		return false, nil
	}

	keepBinds = true
	return true, nil
}

func unmountSnapshotViewBind(target string) error {
	target = filepath.Clean(target)
	err := unix.Unmount(target, unix.MNT_DETACH)
	if err == nil || err == unix.EINVAL || err == unix.ENOENT || os.IsNotExist(err) {
		return nil
	}
	return fmt.Errorf("failed to unmount snapshot view bind %s: %w", target, err)
}
