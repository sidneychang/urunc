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
	"errors"
	"fmt"
	"os"
)

var (
	deleteSharedViewLeaseFn    = deleteSharedViewLease
	cleanupSharedViewMountsFn  = cleanupSharedViewMounts
	removeSharedViewSnapshotFn = removeSharedViewSnapshot
	sharedViewReferencedFn     = sharedViewReferenced
)

// ReconcileAllSharedViews performs a one-shot reconciliation pass for all shared
// views currently present on disk.
//
// This is a recovery helper (e.g. shim startup after crash/abrupt stop), not a
// primary lifecycle step for normal create/delete flows.
func ReconcileAllSharedViews(ctx context.Context) error {
	entries, err := os.ReadDir(sharedViewsRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read shared views root %s: %w", sharedViewsRoot, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if err := ReconcileSharedView(ctx, entry.Name()); err != nil {
			log.WithError(err).WithField("shared_view_id", entry.Name()).Warn("shared view reconcile failed")
		}
	}
	return nil
}

// ReconcileSharedView checks whether container metadata still references the shared
// view and retries cleanup for orphaned or previously failed state transitions.
//
// This is intentionally recovery-only; happy-path lifecycle uses explicit
// pin/unpin + cleanup entrypoints.
func ReconcileSharedView(ctx context.Context, sharedViewID string) error {
	if sharedViewID == "" {
		return nil
	}

	paths := newSharedViewPaths(sharedViewID)
	unlock, err := acquireSharedViewLock(paths.lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer unlock()

	return reconcileViewLocked(ctx, paths)
}

// ReconcileAllViews is kept as a compatibility wrapper.
func ReconcileAllViews(ctx context.Context) error {
	return ReconcileAllSharedViews(ctx)
}

// ReconcileView is kept as a compatibility wrapper.
func ReconcileView(ctx context.Context, sharedViewID string) error {
	return ReconcileSharedView(ctx, sharedViewID)
}

func reconcileViewLocked(ctx context.Context, paths sharedViewPaths) error {
	meta, err := LoadSharedViewMeta(paths)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	return cleanupSharedViewIfUnused(ctx, meta, paths)
}

func cleanupSharedViewIfUnused(ctx context.Context, meta *sharedViewMeta, paths sharedViewPaths) error {
	if meta == nil {
		return nil
	}

	referenced, err := sharedViewReferencedFn(ctx, meta)
	if err != nil {
		return err
	}
	if referenced {
		return nil
	}

	var firstErr error
	if err := deleteSharedViewLeaseFn(ctx, meta); err != nil {
		firstErr = err
	}
	if err := cleanupSharedViewMountsFn(paths); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := removeSharedViewSnapshotFn(ctx, &meta.SnapshotViewInfo); err != nil && firstErr == nil {
		firstErr = err
	}

	if firstErr != nil {
		return firstErr
	}

	if err := os.RemoveAll(paths.base); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove shared view base %s: %w", paths.base, err)
	}
	if err := os.Remove(paths.lockPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove shared view lock file %s: %w", paths.lockPath, err)
	}

	return nil
}
