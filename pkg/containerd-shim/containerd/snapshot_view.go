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
//   - shim ChooseRootfs selected container block rootfs (type block, MountedPath set),
//     or mountRootfs=true when ChooseRootfs was skipped (runtime will select rootfs)
//
// Create order: injectMissingAnnotations → chooseGuestRootfs → SnapshotView.ShouldPrepare → Prepare.
// chooseGuestRootfs persists rootfs params in config.json; PrepareSnapshotView writes urunc-view.json.

package containerd

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	cntrtypes "github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/mount"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	"github.com/urunc-dev/urunc/pkg/unikontainers"
	urunctypes "github.com/urunc-dev/urunc/pkg/unikontainers/types"
	"google.golang.org/grpc/metadata"
)

const (
	snapshotViewStateFilename = "urunc-view.json"
	snapshotViewLeasePrefix   = "urunc-snapshot-view-"
	annotMountRootfs          = "com.urunc.unikernel.mountRootfs"
)

var (
	ErrSnapshotViewNotPrepared = errors.New("snapshot view not prepared")
	snapshotViewLog            = logrus.WithField("subsystem", "containerd-shim-snapshot-view")
)

type snapshotViewState struct {
	ViewKey     string        `json:"view_key"`
	LeaseID     string        `json:"lease_id"`
	Snapshotter string        `json:"snapshotter"`
	Namespace   string        `json:"namespace"`
	Mounts      []mount.Mount `json:"mounts,omitempty"`
}

// SnapshotViewCleanupState is bundle-persisted snapshot view state loaded
// before Delete tears down runtime-owned bind mounts or bundle files.
type SnapshotViewCleanupState struct {
	state *snapshotViewState
}

// GuestRootfsChoice carries the outcome of shim chooseGuestRootfs for snapshot view gating.
// Chosen is true when rootfs params were selected and written to bundle config.json.
type GuestRootfsChoice struct {
	Params urunctypes.RootfsParams
	Chosen bool
}

// SnapshotView holds only the containerd resources needed to prepare or clean
// up a per-container snapshot view.
type SnapshotView struct {
	namespace   string
	containerID string
	snapshotter string
	snapshotKey string
	snapshots   snapshotsapi.SnapshotsClient
	leases      leasesapi.LeasesClient
}

// NewSnapshotView returns a snapshot view accessor when the session container
// uses a block-based snapshotter. Returns nil when snapshot view does not apply.
func NewSnapshotView(s *Session) *SnapshotView {
	ctr := s.GetContainer()
	if ctr == nil || ctr.GetSnapshotKey() == "" || !isBlockSnapshotViewSnapshotter(ctr.GetSnapshotter()) {
		return nil
	}
	return &SnapshotView{
		namespace:   s.namespace,
		containerID: s.containerID,
		snapshotter: ctr.GetSnapshotter(),
		snapshotKey: ctr.GetSnapshotKey(),
		snapshots:   s.snapshotsClient(),
		leases:      s.leasesClient(),
	}
}

// NewSnapshotViewCleanup returns a minimal accessor for Delete-time cleanup
// using bundle-persisted view state.
func NewSnapshotViewCleanup(s *Session) *SnapshotView {
	return &SnapshotView{
		namespace: s.namespace,
		snapshots: s.snapshotsClient(),
		leases:    s.leasesClient(),
	}
}

// ShouldPrepare reports whether Prepare should run.
// Pass the GuestRootfsChoice returned by chooseGuestRootfs; mountRootfs is read from the
// bundle only when Chosen is false (choice skipped).
func (v *SnapshotView) ShouldPrepare(bundle string, choice GuestRootfsChoice) (bool, error) {
	if v == nil {
		return false, nil
	}
	if !snapshotViewEnabledInConfig() {
		return false, nil
	}

	if choice.Chosen {
		return unikontainers.RootfsNeedsContainerSnapshotView(choice.Params), nil
	}

	mountRootfs, err := mountRootfsEnabledFromBundle(bundle)
	if err != nil {
		return false, err
	}
	return mountRootfs, nil
}

func snapshotViewEnabledInConfig() bool {
	uruncCfg, cfgErr := unikontainers.LoadUruncConfig(unikontainers.UruncConfigPath)
	if cfgErr != nil {
		snapshotViewLog.WithError(cfgErr).Warn("failed to load urunc config; snapshot view disabled")
		return false
	}
	return uruncCfg.SnapshotView.Enabled
}

func mountRootfsEnabledFromBundle(bundle string) (bool, error) {
	cfgPath := filepath.Join(bundle, "config.json")
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		return false, fmt.Errorf("read bundle config: %w", err)
	}

	var spec specs.Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return false, fmt.Errorf("parse bundle config: %w", err)
	}
	if spec.Annotations == nil {
		return false, nil
	}

	v := normalizeBoolString(spec.Annotations[annotMountRootfs])
	if v == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("parse %s %q: %w", annotMountRootfs, v, err)
	}
	return enabled, nil
}

func isBlockSnapshotViewSnapshotter(snapshotter string) bool {
	switch snapshotter {
	case "devmapper", "blockfile":
		return true
	default:
		return false
	}
}

func normalizeBoolString(v string) string {
	if v == "" {
		return ""
	}
	if _, err := strconv.ParseBool(v); err == nil {
		return v
	}
	if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
		if _, err := strconv.ParseBool(string(decoded)); err == nil {
			return string(decoded)
		}
	}
	return v
}

// Prepare creates a containerd snapshot view and writes urunc-view.json into the bundle.
// Call only when ShouldPrepare returned true for the same bundle and choice.
func (v *SnapshotView) Prepare(ctx context.Context, bundle string, choice GuestRootfsChoice) error {
	if v == nil {
		return fmt.Errorf("snapshot view accessor is nil")
	}

	need, err := v.ShouldPrepare(bundle, choice)
	if err != nil {
		return err
	}
	if !need {
		return fmt.Errorf("Prepare called but snapshot view is not applicable")
	}

	snapshotKey, err := v.resolveCommittedSnapshotBase(ctx, v.snapshotter, v.snapshotKey)
	if err != nil {
		return err
	}

	viewToken := perContainerViewToken(v.snapshotter, v.namespace, v.containerID, snapshotKey)
	state := &snapshotViewState{
		ViewKey:     "urunc-view-" + viewToken,
		LeaseID:     snapshotViewLeasePrefix + viewToken,
		Snapshotter: v.snapshotter,
		Namespace:   v.namespace,
	}

	nsCtx := withNamespace(ctx, v.namespace)
	if _, err := v.leases.Create(nsCtx, &leasesapi.CreateRequest{ID: state.LeaseID}); err != nil {
		err = containerdErr(err)
		if err != nil && !errdefs.IsAlreadyExists(err) {
			return fmt.Errorf("create snapshot view lease %s: %w", state.LeaseID, err)
		}
	}

	leaseCtx := metadata.AppendToOutgoingContext(nsCtx, "containerd-lease", state.LeaseID)
	mounts, err := v.createSnapshotView(leaseCtx, state.ViewKey, snapshotKey)
	if err != nil {
		if cleanupErr := v.cleanupViewLease(ctx, state); cleanupErr != nil {
			snapshotViewLog.WithError(cleanupErr).Warn("failed to clean up snapshot view lease after prepare failure")
		}
		return err
	}
	state.Mounts = mounts

	if err := saveSnapshotViewState(bundle, state); err != nil {
		if cleanupErr := v.cleanupResources(ctx, bundle, state); cleanupErr != nil {
			snapshotViewLog.WithError(cleanupErr).Warn("failed to clean up snapshot view after state persistence failure")
			return fmt.Errorf("persist snapshot view state: %w (cleanup also failed: %v)", err, cleanupErr)
		}
		return fmt.Errorf("persist snapshot view state: %w", err)
	}

	return nil
}

func (v *SnapshotView) statSnapshot(ctx context.Context, snapshotter, key string) (parent string, committed bool, err error) {
	resp, err := v.snapshots.Stat(withNamespace(ctx, v.namespace), &snapshotsapi.StatSnapshotRequest{
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

func (v *SnapshotView) resolveCommittedSnapshotBase(ctx context.Context, snapshotter, snapshotKey string) (string, error) {
	parent, committed, err := v.statSnapshot(ctx, snapshotter, snapshotKey)
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
		parent, committed, err = v.statSnapshot(ctx, snapshotter, current)
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

func (v *SnapshotView) createSnapshotView(ctx context.Context, viewKey, parentKey string) ([]mount.Mount, error) {
	nsCtx := withNamespace(ctx, v.namespace)
	viewResp, err := v.snapshots.View(nsCtx, &snapshotsapi.ViewSnapshotRequest{
		Snapshotter: v.snapshotter,
		Key:         viewKey,
		Parent:      parentKey,
	})
	if err = containerdErr(err); err == nil {
		return protoMountsToMounts(viewResp.GetMounts()), nil
	}
	if !errdefs.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create snapshot view %s from %s: %w", viewKey, parentKey, err)
	}

	mountsResp, err := v.snapshots.Mounts(nsCtx, &snapshotsapi.MountsRequest{
		Snapshotter: v.snapshotter,
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

func perContainerViewToken(snapshotter, namespace, containerID, snapshotKey string) string {
	sum := sha256.Sum256([]byte(snapshotter + "\x00" + namespace + "\x00" + containerID + "\x00" + snapshotKey))
	return hex.EncodeToString(sum[:])
}

// Cleanup removes containerd view resources and bundle state.
func (v *SnapshotView) Cleanup(ctx context.Context, bundle string) error {
	if v == nil {
		return nil
	}

	state, err := v.LoadCleanupState(bundle)
	if err != nil {
		if errors.Is(err, ErrSnapshotViewNotPrepared) {
			return nil
		}
		return err
	}

	return v.CleanupLoaded(ctx, bundle, state)
}

// LoadCleanupState reads snapshot view state from the bundle so callers can
// keep enough information to clean up after runtime Delete removes bundle files.
func (v *SnapshotView) LoadCleanupState(bundle string) (*SnapshotViewCleanupState, error) {
	state, err := loadSnapshotViewState(bundle)
	if err != nil {
		return nil, err
	}
	return &SnapshotViewCleanupState{state: state}, nil
}

// CleanupLoaded removes containerd snapshot view resources using previously
// loaded state.
func (v *SnapshotView) CleanupLoaded(ctx context.Context, bundle string, state *SnapshotViewCleanupState) error {
	if v == nil || state == nil || state.state == nil {
		return nil
	}
	return v.cleanupResources(ctx, bundle, state.state)
}

func (v *SnapshotView) cleanupResources(ctx context.Context, bundle string, state *snapshotViewState) error {
	if err := v.cleanupViewSnapshot(ctx, state); err != nil {
		return err
	}
	if err := v.cleanupViewLease(ctx, state); err != nil {
		return err
	}
	return deleteSnapshotViewState(bundle)
}

func (v *SnapshotView) cleanupViewSnapshot(ctx context.Context, state *snapshotViewState) error {
	if state == nil || state.ViewKey == "" || state.Snapshotter == "" {
		return nil
	}
	_, err := v.snapshots.Remove(withNamespace(ctx, v.namespace), &snapshotsapi.RemoveSnapshotRequest{
		Snapshotter: state.Snapshotter,
		Key:         state.ViewKey,
	})
	if err = containerdErr(err); err != nil && !errdefs.IsNotFound(err) {
		snapshotViewLog.WithError(err).Warn("failed to remove snapshot view from containerd")
		return err
	}
	return nil
}

func (v *SnapshotView) cleanupViewLease(ctx context.Context, state *snapshotViewState) error {
	if state == nil || state.LeaseID == "" {
		return nil
	}
	_, err := v.leases.Delete(withNamespace(ctx, v.namespace), &leasesapi.DeleteRequest{ID: state.LeaseID})
	if err = containerdErr(err); err != nil && !errdefs.IsNotFound(err) {
		snapshotViewLog.WithError(err).Warn("failed to remove snapshot view lease from containerd")
		return err
	}
	return nil
}

func saveSnapshotViewState(bundle string, state *snapshotViewState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal snapshot view state: %w", err)
	}
	path := filepath.Join(bundle, snapshotViewStateFilename)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write snapshot view state %s: %w", path, err)
	}
	return nil
}

func loadSnapshotViewState(bundle string) (*snapshotViewState, error) {
	path := filepath.Join(bundle, snapshotViewStateFilename)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrSnapshotViewNotPrepared
		}
		return nil, fmt.Errorf("read snapshot view state %s: %w", path, err)
	}
	var state snapshotViewState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot view state %s: %w", path, err)
	}
	return &state, nil
}

func deleteSnapshotViewState(bundle string) error {
	path := filepath.Join(bundle, snapshotViewStateFilename)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove snapshot view state %s: %w", path, err)
	}
	return nil
}
