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

	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/namespaces"
	"google.golang.org/grpc/metadata"
)

func cleanupCreateFailure(ctx context.Context, info *SnapshotViewInfo, paths sharedViewPaths) error {
	unlock, err := acquireSharedViewLock(paths.lockPath)
	if err != nil {
		return err
	}
	defer unlock()

	return cleanupSharedViewIfUnused(ctx, newSharedViewMeta(info), paths)
}

// CreateSnapshotView prepares the shared view snapshot itself and persists
// shim-owned state in the bundle sidecar.
//
// Primary lifecycle semantics:
//   - bundle sidecar state is the runtime source of truth per container
//   - container metadata pin/unpin is handled by task lifecycle helpers
//   - lease created here is short-lived and released right after successful
//     task Create (OnCreateFinalize)
//
// Reconcile is intentionally not part of this primary create path; it is a
// recovery mechanism invoked at shim startup and selected failure/stop paths.
func CreateSnapshotView(ctx context.Context, bundle, containerID string) (*SnapshotViewInfo, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, fmt.Errorf("namespace required: %w", err)
	}

	client, err := newContainerdClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	snapshotKey, snapshotter, err := resolveSnapshotKey(ctx, client, ns, containerID)
	if err != nil {
		return nil, err
	}
	if snapshotKey == "" {
		return nil, nil
	}

	sharedViewID := fmt.Sprintf("%s_%s_%s", snapshotter, ns, snapshotKey)
	viewKey := "urunc-shared-" + sharedViewID
	paths := newSharedViewPaths(sharedViewID)

	leaseID := sharedViewLeaseID + sharedViewID
	_, err = client.leases.Create(withNamespace(ctx, ns), &leasesapi.CreateRequest{ID: leaseID})
	err = grpcErr(err)
	if err != nil && !errdefs.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create shared view lease %s: %w", leaseID, err)
	}
	ctx = metadata.AppendToOutgoingContext(withNamespace(ctx, ns), "containerd-lease", leaseID)

	if err := os.MkdirAll(sharedViewsRoot, 0755); err != nil {
		return nil, fmt.Errorf("create shared views root %s: %w", sharedViewsRoot, err)
	}

	if err := ensureSharedViewPrepared(
		ctx, client, ns, snapshotter, viewKey, snapshotKey, paths,
	); err != nil {
		return nil, err
	}

	info := &SnapshotViewInfo{
		SharedViewID: sharedViewID,
		ViewKey:      viewKey,
		MountPath:    paths.dataDir,
		Snapshotter:  snapshotter,
		Namespace:    ns,
		ContainerID:  containerID,
	}

	unlock, err := acquireSharedViewLock(paths.lockPath)
	if err != nil {
		return nil, err
	}
	if err := SaveSharedViewMeta(paths, info); err != nil {
		unlock()
		if cerr := cleanupCreateFailure(ctx, info, paths); cerr != nil {
			log.WithError(cerr).Warn("failed to clean up shared view after metadata persistence failure")
			return nil, fmt.Errorf("persist shared view metadata: %w (cleanup also failed: %v)", err, cerr)
		}
		return nil, fmt.Errorf("persist shared view metadata: %w", err)
	}
	unlock()

	if err := SaveSnapshotViewState(bundle, info); err != nil {
		if cerr := CleanupSnapshotView(ctx, info); cerr != nil {
			log.WithError(cerr).Warn("failed to clean up shared view after state persistence failure")
			return nil, fmt.Errorf("persist snapshot view state: %w (cleanup also failed: %v)", err, cerr)
		}
		return nil, fmt.Errorf("persist snapshot view state: %w", err)
	}

	return info, nil
}
