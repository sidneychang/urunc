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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCleanupSharedViewIfUnusedSkipsReferencedView(t *testing.T) {
	paths := newTestSharedViewPaths(t)
	if !assert.NoError(t, os.MkdirAll(paths.dataDir, 0o755)) {
		return
	}

	meta := &sharedViewMeta{
		SnapshotViewInfo: SnapshotViewInfo{
			SharedViewID: "shared-view",
			ViewKey:      "view-key",
			Snapshotter:  "overlayfs",
			Namespace:    "testns",
			MountPath:    paths.dataDir,
		},
		LeaseID: "lease-id",
	}

	oldRefFn := sharedViewReferencedFn
	oldDeleteLeaseFn := deleteSharedViewLeaseFn
	t.Cleanup(func() {
		sharedViewReferencedFn = oldRefFn
		deleteSharedViewLeaseFn = oldDeleteLeaseFn
	})

	sharedViewReferencedFn = func(context.Context, *sharedViewMeta) (bool, error) {
		return true, nil
	}

	var leaseDeleted bool
	deleteSharedViewLeaseFn = func(context.Context, *sharedViewMeta) error {
		leaseDeleted = true
		return nil
	}

	err := cleanupSharedViewIfUnused(context.Background(), meta, paths)
	if !assert.NoError(t, err) {
		return
	}

	assert.False(t, leaseDeleted, "expected referenced shared view to skip cleanup")
}

func TestCleanupSharedViewIfUnusedTriggersCleanupSteps(t *testing.T) {
	paths := newTestSharedViewPaths(t)
	if !assert.NoError(t, os.MkdirAll(paths.dataDir, 0o755)) {
		return
	}

	meta := &sharedViewMeta{
		SnapshotViewInfo: SnapshotViewInfo{
			SharedViewID: "shared-view",
			ViewKey:      "view-key",
			Snapshotter:  "overlayfs",
			Namespace:    "testns",
			MountPath:    paths.dataDir,
		},
		LeaseID: "lease-id",
	}

	var leaseDeleted, mountsCleaned, snapshotRemoved bool
	oldRefFn := sharedViewReferencedFn
	oldDeleteLeaseFn := deleteSharedViewLeaseFn
	oldCleanupMountsFn := cleanupSharedViewMountsFn
	oldRemoveSnapshotFn := removeSharedViewSnapshotFn
	t.Cleanup(func() {
		sharedViewReferencedFn = oldRefFn
		deleteSharedViewLeaseFn = oldDeleteLeaseFn
		cleanupSharedViewMountsFn = oldCleanupMountsFn
		removeSharedViewSnapshotFn = oldRemoveSnapshotFn
	})

	sharedViewReferencedFn = func(context.Context, *sharedViewMeta) (bool, error) {
		return false, nil
	}
	deleteSharedViewLeaseFn = func(context.Context, *sharedViewMeta) error {
		leaseDeleted = true
		return nil
	}
	cleanupSharedViewMountsFn = func(sharedViewPaths) error {
		mountsCleaned = true
		return nil
	}
	removeSharedViewSnapshotFn = func(context.Context, *SnapshotViewInfo) error {
		snapshotRemoved = true
		return nil
	}

	err := cleanupSharedViewIfUnused(context.Background(), meta, paths)
	if !assert.NoError(t, err) {
		return
	}

	assert.True(t, leaseDeleted, "expected lease cleanup to run")
	assert.True(t, mountsCleaned, "expected mount cleanup to run")
	assert.True(t, snapshotRemoved, "expected snapshot cleanup to run")
}

func TestReconcileViewCleansUnreferencedSharedView(t *testing.T) {
	paths := newTestSharedViewPaths(t)
	if !assert.NoError(t, os.MkdirAll(paths.dataDir, 0o755)) {
		return
	}

	meta := &SnapshotViewInfo{
		SharedViewID: "shared-view",
		ViewKey:      "view-key",
		Snapshotter:  "overlayfs",
		Namespace:    "testns",
		MountPath:    paths.dataDir,
	}
	if !assert.NoError(t, SaveSharedViewMeta(paths, meta)) {
		return
	}

	var leaseDeleted bool
	oldRefFn := sharedViewReferencedFn
	oldDeleteLeaseFn := deleteSharedViewLeaseFn
	oldCleanupMountsFn := cleanupSharedViewMountsFn
	oldRemoveSnapshotFn := removeSharedViewSnapshotFn
	t.Cleanup(func() {
		sharedViewReferencedFn = oldRefFn
		deleteSharedViewLeaseFn = oldDeleteLeaseFn
		cleanupSharedViewMountsFn = oldCleanupMountsFn
		removeSharedViewSnapshotFn = oldRemoveSnapshotFn
	})

	sharedViewReferencedFn = func(context.Context, *sharedViewMeta) (bool, error) {
		return false, nil
	}
	deleteSharedViewLeaseFn = func(context.Context, *sharedViewMeta) error {
		leaseDeleted = true
		return nil
	}
	cleanupSharedViewMountsFn = func(sharedViewPaths) error { return nil }
	removeSharedViewSnapshotFn = func(context.Context, *SnapshotViewInfo) error { return nil }

	err := ReconcileView(context.Background(), meta.SharedViewID)
	if !assert.NoError(t, err) {
		return
	}

	assert.True(t, leaseDeleted, "expected orphaned shared view to trigger lease cleanup")
}

func newTestSharedViewPaths(t *testing.T) sharedViewPaths {
	t.Helper()

	oldRoot := sharedViewsRoot
	root := t.TempDir()
	sharedViewsRoot = root
	t.Cleanup(func() {
		sharedViewsRoot = oldRoot
	})

	return newSharedViewPaths("shared-view")
}
