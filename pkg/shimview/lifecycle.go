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
)

// OnCreatePinContainerReference makes container metadata own the prepared
// shared view reference at task Create time.
//
// If pinning fails, it rolls back any prepared shim-owned state so callers do
// not need to manually perform partial cleanup.
func OnCreatePinContainerReference(ctx context.Context, bundle string, info *SnapshotViewInfo) error {
	if info == nil {
		return nil
	}
	if err := PinSharedViewToContainer(ctx, info); err != nil {
		if rollbackErr := OnCreateRollback(ctx, bundle, info); rollbackErr != nil {
			return fmt.Errorf("pin shared snapshot view to container: %w (rollback also failed: %v)", err, rollbackErr)
		}
		return err
	}
	return nil
}

// OnCreateRollback undoes metadata and on-disk state created for a task whose
// TaskService.Create path failed after the shared view had already been prepared.
func OnCreateRollback(ctx context.Context, bundle string, info *SnapshotViewInfo) error {
	if info == nil {
		return DeleteSnapshotViewState(bundle)
	}

	var firstErr error
	if err := UnpinSharedViewFromContainer(ctx, info); err != nil {
		firstErr = err
	}
	if err := CleanupSnapshotView(ctx, info); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := DeleteSnapshotViewState(bundle); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// OnCreateFinalize releases short-lived host-side resources that are only
// needed while the shared view is being prepared and pinned.
func OnCreateFinalize(ctx context.Context, info *SnapshotViewInfo) error {
	if info == nil {
		return nil
	}
	return ReleaseSnapshotViewLease(ctx, info)
}

// OnDeleteReleaseContainerReference removes the container metadata reference
// and then reconciles shared-view state to reclaim the view when it becomes unused.
func OnDeleteReleaseContainerReference(ctx context.Context, info *SnapshotViewInfo) error {
	if info == nil {
		return nil
	}

	var firstErr error
	if err := UnpinSharedViewFromContainer(ctx, info); err != nil {
		firstErr = err
	}
	if err := CleanupSnapshotView(ctx, info); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// AttachSnapshotViewToContainer is kept as a compatibility wrapper.
func AttachSnapshotViewToContainer(ctx context.Context, bundle string, info *SnapshotViewInfo) error {
	return OnCreatePinContainerReference(ctx, bundle, info)
}

// RollbackSnapshotViewCreate is kept as a compatibility wrapper.
func RollbackSnapshotViewCreate(ctx context.Context, bundle string, info *SnapshotViewInfo) error {
	return OnCreateRollback(ctx, bundle, info)
}

// FinalizeSnapshotViewCreate is kept as a compatibility wrapper.
func FinalizeSnapshotViewCreate(ctx context.Context, info *SnapshotViewInfo) error {
	return OnCreateFinalize(ctx, info)
}

// DetachSnapshotViewFromContainer is kept as a compatibility wrapper.
func DetachSnapshotViewFromContainer(ctx context.Context, info *SnapshotViewInfo) error {
	return OnDeleteReleaseContainerReference(ctx, info)
}

// CleanupSnapshotView retries shared-view cleanup after a container-level
// lifecycle transition such as task delete or failed create rollback.
func CleanupSnapshotView(ctx context.Context, info *SnapshotViewInfo) error {
	if info == nil || info.SharedViewID == "" {
		return nil
	}

	paths := newSharedViewPaths(info.SharedViewID)

	unlock, err := acquireSharedViewLock(paths.lockPath)
	if err != nil {
		return err
	}
	defer unlock()

	return reconcileViewLocked(ctx, paths)
}

// UseSnapshotView mounts the shared view for a bundle on a private temporary
// host mountpoint just long enough for the caller to consume boot artifacts.
func UseSnapshotView(ctx context.Context, bundle string, use func(info *SnapshotViewInfo) error) error {
	info, err := LoadSnapshotViewState(bundle)
	if err != nil {
		return err
	}
	if info == nil || info.SharedViewID == "" {
		return nil
	}

	paths := newSharedViewPaths(info.SharedViewID)
	mountpoint, err := newTemporarySharedViewMountpoint(paths)
	if err != nil {
		return err
	}
	defer os.RemoveAll(mountpoint)

	if err := mountSharedViewAt(ctx, info, mountpoint); err != nil {
		return err
	}

	useInfo := *info
	useInfo.MountPath = mountpoint
	if err := use(&useInfo); err != nil {
		if unmountErr := unmountMountpoint(mountpoint); unmountErr != nil {
			log.WithError(unmountErr).WithField("path", mountpoint).Warn("failed to unmount temporary shared view mount after artifact use error")
		}
		return err
	}
	if err := unmountMountpoint(mountpoint); err != nil {
		return err
	}

	return nil
}
