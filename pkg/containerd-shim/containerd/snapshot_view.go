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

// Snapshot view: read-only container rootfs views for boot-artifact access.
//
// When mountRootfs passes the container snapshot as a block device to the guest,
// urunc must read kernel/initrd/urunc.json from that rootfs before attach. A view
// snapshot avoids copying those files into monRootfs. This path applies only when:
//   - [snapshot_view].enabled in /etc/urunc/config.toml (host opt-in)
//   - container uses a block-based snapshotter (devmapper or blockfile)
//   - shim ChooseRootfs selected container block rootfs (type block, MountedPath set)
//
// Create order: injectMissingAnnotations → inner TaskService.Create →
// chooseGuestRootfs → SnapshotViewAccessor.ShouldPrepare → Prepare.
// chooseGuestRootfs runs after the inner create mounts the bundle rootfs and
// persists rootfs params in config.json; Prepare writes AnnotSnapshotView there too.
//
// Delete paths:
//   - TaskService.Delete (graceful): load bundle config.json, inner Delete, then CleanupLoaded.
//   - shim binary "delete" (dead shim): uruncShimManager.Stop → CleanupSnapshotViewFromBundle
//     before runc manager.Stop; does not go through TaskService.

package containerd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	cntrtypes "github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/mount"
	"github.com/sirupsen/logrus"
	"github.com/urunc-dev/urunc/pkg/unikontainers"
	"github.com/urunc-dev/urunc/pkg/unikontainers/types"
	"google.golang.org/grpc/metadata"
)

const (
	snapshotViewKeyPrefix   = "urunc-view-"
	snapshotViewLeasePrefix = "urunc-snapshot-view-"
)

var (
	ErrSnapshotViewNotPrepared = errors.New("snapshot view not prepared")
	snapshotViewLog            = logrus.WithField("subsystem", "containerd-shim-snapshot-view")
)

type snapshotViewState struct {
	ViewKey         string        `json:"view_key"`
	LeaseID         string        `json:"lease_id"`
	Snapshotter     string        `json:"snapshotter"`
	Namespace       string        `json:"namespace"`
	Mounts []mount.Mount `json:"mounts,omitempty"`
}

// SnapshotViewCleanupState is bundle-persisted snapshot view state loaded
// before Delete removes the containerd view.
type SnapshotViewCleanupState struct {
	state *snapshotViewState
}

// SnapshotViewAccessor provides access to containerd resources for urunc
// snapshot views and their bundle-persisted state.
//
// In the create path, it uses containerID, snapshotter and snapshotKey to
// create a read-only snapshot view and persist AnnotSnapshotView in bundle config.json.
//
// In the delete path, it only needs namespace, snapshots client and leases
// client. Cleanup is driven by the snapshotViewState loaded from the bundle,
// rather than by prepare-time fields.
type SnapshotViewAccessor struct {
	namespace   string
	containerID string
	snapshotter string
	snapshotKey string
	snapshots   snapshotsapi.SnapshotsClient
	leases      leasesapi.LeasesClient
}

// NewSnapshotViewAccessor returns an accessor with namespace and containerd
// clients. When the session container uses a block-based snapshotter, prepare-
// time fields are also set; otherwise those fields remain empty and ShouldPrepare
// returns false.
func NewSnapshotViewAccessor(s *Session) *SnapshotViewAccessor {
	a := &SnapshotViewAccessor{
		namespace: s.namespace,
		snapshots: s.snapshotsClient(),
		leases:    s.leasesClient(),
	}
	ctr := s.GetContainer()
	if ctr != nil && ctr.GetSnapshotKey() != "" && isBlockSnapshotViewSnapshotter(ctr.GetSnapshotter()) {
		a.containerID = s.containerID
		a.snapshotter = ctr.GetSnapshotter()
		a.snapshotKey = ctr.GetSnapshotKey()
	}
	return a
}

// ShouldPrepare reports whether Prepare should run. Pass the RootfsParams
// returned by post-create chooseGuestRootfs.
func (a *SnapshotViewAccessor) ShouldPrepare(rootfs types.RootfsParams) bool {
	if a == nil ||
		a.snapshotter == "" ||
		a.snapshotKey == "" ||
		!isBlockSnapshotViewSnapshotter(a.snapshotter) ||
		rootfs.Type != "block" ||
		rootfs.MountedPath == "" {
		return false
	}

	uruncCfg, cfgErr := unikontainers.LoadUruncConfig(unikontainers.UruncConfigPath)
	if cfgErr != nil {
		snapshotViewLog.WithError(cfgErr).Warn("failed to load urunc config; snapshot view disabled")
		return false
	}
	return uruncCfg.SnapshotView.Enabled
}

func isBlockSnapshotViewSnapshotter(snapshotter string) bool {
	switch snapshotter {
	case "devmapper", "blockfile":
		return true
	default:
		return false
	}
}

// Prepare creates a containerd snapshot view and writes AnnotSnapshotView into bundle config.json.
// Call only when ShouldPrepare returned true.
func (a *SnapshotViewAccessor) Prepare(ctx context.Context, bundle string) error {
	if a == nil {
		return fmt.Errorf("snapshot view accessor is nil")
	}

	snapshotKey, err := a.resolveCommittedSnapshotBase(ctx, a.snapshotter, a.snapshotKey)
	if err != nil {
		return err
	}

	state := &snapshotViewState{
		ViewKey:     snapshotViewKeyPrefix + a.containerID,
		LeaseID:     snapshotViewLeasePrefix + a.containerID,
		Snapshotter: a.snapshotter,
		Namespace:   a.namespace,
	}

	nsCtx := withNamespace(ctx, a.namespace)
	if _, err := a.leases.Create(nsCtx, &leasesapi.CreateRequest{ID: state.LeaseID}); err != nil {
		err = containerdErr(err)
		if err != nil && !errdefs.IsAlreadyExists(err) {
			return fmt.Errorf("create snapshot view lease %s: %w", state.LeaseID, err)
		}
	}

	leaseCtx := metadata.AppendToOutgoingContext(nsCtx, "containerd-lease", state.LeaseID)
	mounts, err := a.createSnapshotView(leaseCtx, state.ViewKey, snapshotKey)
	if err != nil {
		if cleanupErr := a.cleanupViewLease(ctx, state); cleanupErr != nil {
			snapshotViewLog.WithError(cleanupErr).Warn("failed to clean up snapshot view lease after prepare failure")
		}
		return err
	}
	state.Mounts = mounts

	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal snapshot view state: %w", err)
	}
	if err := unikontainers.PatchBundleSnapshotView(bundle, string(encoded)); err != nil {
		if cleanupErr := a.cleanupResources(ctx, state); cleanupErr != nil {
			snapshotViewLog.WithError(cleanupErr).Warn("failed to clean up snapshot view after state persistence failure")
			return fmt.Errorf("persist snapshot view state: %w (cleanup also failed: %v)", err, cleanupErr)
		}
		return fmt.Errorf("persist snapshot view state: %w", err)
	}

	return nil
}

func (a *SnapshotViewAccessor) statSnapshot(ctx context.Context, snapshotter, key string) (parent string, committed bool, err error) {
	resp, err := a.snapshots.Stat(withNamespace(ctx, a.namespace), &snapshotsapi.StatSnapshotRequest{
		Snapshotter: snapshotter,
		Key:         key,
	})
	if err = containerdErr(err); err != nil {
		return "", false, err
	}
	info := resp.GetInfo()
	if info == nil {
		return "", false, fmt.Errorf("stat snapshot %s (%s): empty info", key, snapshotter)
	}
	return info.GetParent(), info.GetKind() == snapshotsapi.Kind_COMMITTED, nil
}

func (a *SnapshotViewAccessor) resolveCommittedSnapshotBase(ctx context.Context, snapshotter, snapshotKey string) (string, error) {
	parent, committed, err := a.statSnapshot(ctx, snapshotter, snapshotKey)
	if err != nil {
		return "", fmt.Errorf("stat snapshot %s (%s): %w", snapshotKey, snapshotter, err)
	}
	if committed {
		return snapshotKey, nil
	}
	if parent == "" {
		return snapshotKey, nil
	}

	current := parent
	for {
		parent, committed, err = a.statSnapshot(ctx, snapshotter, current)
		if err != nil {
			return "", fmt.Errorf("stat snapshot %s (%s parent walk): %w", current, snapshotter, err)
		}
		if committed {
			return current, nil
		}
		if parent == "" {
			return "", fmt.Errorf("%s snapshot %s has no committed parent in chain", snapshotter, snapshotKey)
		}
		current = parent
	}
}

func (a *SnapshotViewAccessor) createSnapshotView(ctx context.Context, viewKey, parentKey string) ([]mount.Mount, error) {
	nsCtx := withNamespace(ctx, a.namespace)
	viewResp, err := a.snapshots.View(nsCtx, &snapshotsapi.ViewSnapshotRequest{
		Snapshotter: a.snapshotter,
		Key:         viewKey,
		Parent:      parentKey,
	})
	if err = containerdErr(err); err == nil {
		return protoMountsToMounts(viewResp.GetMounts()), nil
	}
	if !errdefs.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create snapshot view %s from %s: %w", viewKey, parentKey, err)
	}

	mountsResp, err := a.snapshots.Mounts(nsCtx, &snapshotsapi.MountsRequest{
		Snapshotter: a.snapshotter,
		Key:         viewKey,
	})
	if err = containerdErr(err); err != nil {
		return nil, fmt.Errorf("create snapshot view %s from %s: %w", viewKey, parentKey, err)
	}
	return protoMountsToMounts(mountsResp.GetMounts()), nil
}

func protoMountsToMounts(mm []*cntrtypes.Mount) []mount.Mount {
	out := make([]mount.Mount, len(mm))
	for i, m := range mm {
		out[i] = mount.Mount{
			Type:    m.Type,
			Source:  m.Source,
			Target:  m.Target,
			Options: m.Options,
		}
	}
	return out
}

// LoadCleanupState reads snapshot view state from bundle config.json.
func (a *SnapshotViewAccessor) LoadCleanupState(bundle string) (*SnapshotViewCleanupState, error) {
	raw, err := unikontainers.ReadBundleSnapshotView(bundle)
	if err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, ErrSnapshotViewNotPrepared
	}
	var state snapshotViewState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot view state %s: %w", unikontainers.AnnotSnapshotView, err)
	}
	return &SnapshotViewCleanupState{state: &state}, nil
}

// CleanupLoaded removes containerd snapshot view resources using previously loaded state.
func (a *SnapshotViewAccessor) CleanupLoaded(ctx context.Context, state *SnapshotViewCleanupState) error {
	if a == nil || state == nil || state.state == nil {
		return nil
	}
	return a.cleanupResources(ctx, state.state)
}

func (a *SnapshotViewAccessor) namespaceFor(state *snapshotViewState) string {
	if state != nil && state.Namespace != "" {
		return state.Namespace
	}
	return a.namespace
}

func (a *SnapshotViewAccessor) cleanupResources(ctx context.Context, state *snapshotViewState) error {
	if err := a.cleanupViewSnapshot(ctx, state); err != nil {
		return err
	}
	return a.cleanupViewLease(ctx, state)
}

func (a *SnapshotViewAccessor) cleanupViewSnapshot(ctx context.Context, state *snapshotViewState) error {
	if state == nil || state.ViewKey == "" || state.Snapshotter == "" {
		return nil
	}
	_, err := a.snapshots.Remove(withNamespace(ctx, a.namespaceFor(state)), &snapshotsapi.RemoveSnapshotRequest{
		Snapshotter: state.Snapshotter,
		Key:         state.ViewKey,
	})
	if err = containerdErr(err); err != nil && !errdefs.IsNotFound(err) {
		snapshotViewLog.WithError(err).Warn("failed to remove snapshot view from containerd")
		return err
	}
	return nil
}

func (a *SnapshotViewAccessor) cleanupViewLease(ctx context.Context, state *snapshotViewState) error {
	if state == nil || state.LeaseID == "" {
		return nil
	}
	_, err := a.leases.Delete(withNamespace(ctx, a.namespaceFor(state)), &leasesapi.DeleteRequest{ID: state.LeaseID})
	if err = containerdErr(err); err != nil && !errdefs.IsNotFound(err) {
		snapshotViewLog.WithError(err).Warn("failed to remove snapshot view lease from containerd")
		return err
	}
	return nil
}

