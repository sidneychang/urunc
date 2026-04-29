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
	"net"
	"os"

	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/mount"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	defaultContainerd       = "/run/containerd/containerd.sock"
	sharedViewGCLabelSuffix = "/urunc-shared-view"
)

type containerdClients struct {
	conn       *grpc.ClientConn
	snapshots  snapshotsapi.SnapshotsClient
	leases     leasesapi.LeasesClient
	containers containersapi.ContainersClient
}

func containerdAddress() string {
	if addr := os.Getenv("CONTAINERD_ADDRESS"); addr != "" {
		return addr
	}
	return defaultContainerd
}

func withNamespace(ctx context.Context, ns string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "containerd-namespace", ns)
}

func grpcErr(err error) error {
	if err == nil {
		return nil
	}
	return errdefs.FromGRPC(err)
}

func (c *containerdClients) containersClient() containersapi.ContainersClient {
	if c.containers == nil {
		c.containers = containersapi.NewContainersClient(c.conn)
	}
	return c.containers
}

func newContainerdClient() (*containerdClients, error) {
	addr := containerdAddress()
	conn, err := grpc.NewClient(
		"unix://"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableServiceConfig(),
		grpc.WithDisableRetry(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("containerd grpc client: %w", err)
	}
	return &containerdClients{
		conn:      conn,
		snapshots: snapshotsapi.NewSnapshotsClient(conn),
		leases:    leasesapi.NewLeasesClient(conn),
	}, nil
}

func (c *containerdClients) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func sharedViewGCLabel(snapshotter string) string {
	return fmt.Sprintf("containerd.io/gc.ref.snapshot.%s%s", snapshotter, sharedViewGCLabelSuffix)
}

func getContainer(ctx context.Context, client *containerdClients, ns, containerID string) (*containersapi.Container, error) {
	resp, err := client.containersClient().Get(withNamespace(ctx, ns), &containersapi.GetContainerRequest{ID: containerID})
	err = grpcErr(err)
	if err != nil {
		return nil, err
	}
	return resp.GetContainer(), nil
}

func PinSharedViewToContainer(ctx context.Context, info *SnapshotViewInfo) error {
	if info == nil || info.ContainerID == "" || info.Namespace == "" || info.Snapshotter == "" || info.ViewKey == "" {
		return nil
	}

	client, err := newContainerdClient()
	if err != nil {
		return fmt.Errorf("connect to containerd for shared view pin: %w", err)
	}
	defer client.Close()

	ctr, err := getContainer(ctx, client, info.Namespace, info.ContainerID)
	if err != nil {
		return fmt.Errorf("get container %s for shared view pin: %w", info.ContainerID, err)
	}
	if ctr == nil {
		return fmt.Errorf("container %s not found for shared view pin", info.ContainerID)
	}

	labelKey := sharedViewGCLabel(info.Snapshotter)
	labels := ctr.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	if labels[labelKey] == info.ViewKey {
		return nil
	}
	labels[labelKey] = info.ViewKey
	ctr.Labels = labels

	_, err = client.containersClient().Update(withNamespace(ctx, info.Namespace), &containersapi.UpdateContainerRequest{
		Container:  ctr,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels." + labelKey}},
	})
	err = grpcErr(err)
	if err != nil {
		return fmt.Errorf("update container %s shared view pin: %w", info.ContainerID, err)
	}
	return nil
}

func UnpinSharedViewFromContainer(ctx context.Context, info *SnapshotViewInfo) error {
	if info == nil || info.ContainerID == "" || info.Namespace == "" || info.Snapshotter == "" {
		return nil
	}

	client, err := newContainerdClient()
	if err != nil {
		return fmt.Errorf("connect to containerd for shared view unpin: %w", err)
	}
	defer client.Close()

	ctr, err := getContainer(ctx, client, info.Namespace, info.ContainerID)
	err = grpcErr(err)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get container %s for shared view unpin: %w", info.ContainerID, err)
	}
	if ctr == nil {
		return nil
	}

	labelKey := sharedViewGCLabel(info.Snapshotter)
	labels := ctr.GetLabels()
	if len(labels) == 0 {
		return nil
	}
	if _, ok := labels[labelKey]; !ok {
		return nil
	}

	delete(labels, labelKey)
	ctr.Labels = labels

	_, err = client.containersClient().Update(withNamespace(ctx, info.Namespace), &containersapi.UpdateContainerRequest{
		Container:  ctr,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	err = grpcErr(err)
	if err != nil {
		return fmt.Errorf("update container %s shared view unpin: %w", info.ContainerID, err)
	}
	return nil
}

func sharedViewReferenced(ctx context.Context, meta *sharedViewMeta) (bool, error) {
	if meta == nil || meta.Namespace == "" || meta.Snapshotter == "" || meta.ViewKey == "" {
		return false, nil
	}

	client, err := newContainerdClient()
	if err != nil {
		return false, fmt.Errorf("connect to containerd for shared view reference check: %w", err)
	}
	defer client.Close()

	resp, err := client.containersClient().List(withNamespace(ctx, meta.Namespace), &containersapi.ListContainersRequest{})
	err = grpcErr(err)
	if err != nil {
		return false, fmt.Errorf("list containers for shared view reference check: %w", err)
	}

	labelKey := sharedViewGCLabel(meta.Snapshotter)
	for _, ctr := range resp.GetContainers() {
		if ctr == nil {
			continue
		}
		if ctr.GetLabels()[labelKey] == meta.ViewKey {
			return true, nil
		}
	}
	return false, nil
}

func ReleaseSnapshotViewLease(ctx context.Context, info *SnapshotViewInfo) error {
	if info == nil || info.SharedViewID == "" {
		return nil
	}
	return deleteSharedViewLease(ctx, newSharedViewMeta(info))
}

func deleteSharedViewLease(ctx context.Context, meta *sharedViewMeta) error {
	if meta == nil || meta.LeaseID == "" || meta.Namespace == "" {
		return nil
	}

	client, err := newContainerdClient()
	if err != nil {
		return fmt.Errorf("connect to containerd for lease cleanup: %w", err)
	}
	defer client.Close()

	_, err = client.leases.Delete(withNamespace(ctx, meta.Namespace), &leasesapi.DeleteRequest{ID: meta.LeaseID})
	err = grpcErr(err)
	if err != nil && !errdefs.IsNotFound(err) {
		log.WithError(err).Warn("failed to remove shared view lease from containerd")
		return err
	}
	return nil
}

func needsCommittedSnapshotBase(snapshotter string) bool {
	switch snapshotter {
	case "devmapper", "blockfile":
		return true
	default:
		return false
	}
}

func resolveSnapshotKey(ctx context.Context, client *containerdClients, ns, containerID string) (snapshotKey, snapshotter string, err error) {
	fastResolved := false
	for _, cand := range []string{"devmapper", "overlayfs", "blockfile"} {
		_, serr := client.snapshots.Stat(withNamespace(ctx, ns), &snapshotsapi.StatSnapshotRequest{
			Snapshotter: cand,
			Key:         containerID,
		})
		if grpcErr(serr) == nil {
			snapshotKey = containerID
			snapshotter = cand
			fastResolved = true
			break
		}
	}

	if !fastResolved {
		ctr, gerr := getContainer(ctx, client, ns, containerID)
		if gerr != nil {
			return "", "", fmt.Errorf("get container %s: %w", containerID, gerr)
		}
		if ctr == nil {
			return "", "", fmt.Errorf("container %s not found in response", containerID)
		}
		snapshotKey = ctr.GetSnapshotKey()
		snapshotter = ctr.GetSnapshotter()
	}
	if snapshotKey == "" || snapshotter == "" {
		return "", "", nil
	}

	if needsCommittedSnapshotBase(snapshotter) {
		infoResp, serr := client.snapshots.Stat(withNamespace(ctx, ns), &snapshotsapi.StatSnapshotRequest{
			Snapshotter: snapshotter,
			Key:         snapshotKey,
		})
		if serr != nil {
			return "", "", fmt.Errorf("stat snapshot %s (%s): %w", snapshotKey, snapshotter, grpcErr(serr))
		}
		current := infoResp.GetInfo().GetParent()
		if current == "" {
			return snapshotKey, snapshotter, nil
		}

		for {
			pinfoResp, serr := client.snapshots.Stat(withNamespace(ctx, ns), &snapshotsapi.StatSnapshotRequest{
				Snapshotter: snapshotter,
				Key:         current,
			})
			if serr != nil {
				return "", "", fmt.Errorf("stat snapshot %s (%s parent walk): %w", current, snapshotter, grpcErr(serr))
			}
			pinfo := pinfoResp.GetInfo()
			if pinfo == nil {
				return "", "", fmt.Errorf("stat snapshot %s (%s parent walk): empty info", current, snapshotter)
			}

			if pinfo.GetKind() == snapshotsapi.Kind_COMMITTED {
				snapshotKey = current
				break
			}
			if pinfo.GetParent() == "" {
				return "", "", fmt.Errorf("%s snapshot %s has no committed parent in chain", snapshotter, snapshotKey)
			}
			current = pinfo.GetParent()
		}
	}

	return snapshotKey, snapshotter, nil
}

func toMounts(mm []*types.Mount) []mount.Mount {
	mounts := make([]mount.Mount, len(mm))
	for i, m := range mm {
		mounts[i] = mount.Mount{
			Type:    m.Type,
			Source:  m.Source,
			Target:  m.Target,
			Options: m.Options,
		}
	}
	return mounts
}

func getOrCreateSharedViewMounts(ctx context.Context, client *containerdClients, ns, snapshotter, viewKey, snapshotKey string) ([]mount.Mount, error) {
	viewResp, err := client.snapshots.View(withNamespace(ctx, ns), &snapshotsapi.ViewSnapshotRequest{
		Snapshotter: snapshotter,
		Key:         viewKey,
		Parent:      snapshotKey,
	})
	err = grpcErr(err)
	if err == nil {
		return toMounts(viewResp.GetMounts()), nil
	}
	if !errdefs.IsAlreadyExists(err) {
		return nil, fmt.Errorf("failed to create shared view snapshot %s from %s: %w", viewKey, snapshotKey, err)
	}

	mountsResp, err := client.snapshots.Mounts(withNamespace(ctx, ns), &snapshotsapi.MountsRequest{
		Snapshotter: snapshotter,
		Key:         viewKey,
	})
	err = grpcErr(err)
	if err != nil {
		return nil, fmt.Errorf("failed to get mounts for shared view snapshot %s: %w", viewKey, err)
	}
	return toMounts(mountsResp.GetMounts()), nil
}

func loadSharedViewMounts(ctx context.Context, client *containerdClients, info *SnapshotViewInfo) ([]mount.Mount, error) {
	mountsResp, err := client.snapshots.Mounts(withNamespace(ctx, info.Namespace), &snapshotsapi.MountsRequest{
		Snapshotter: info.Snapshotter,
		Key:         info.ViewKey,
	})
	err = grpcErr(err)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, fmt.Errorf("shared view snapshot %s missing while preparing artifact use", info.ViewKey)
		}
		return nil, fmt.Errorf("get mounts for shared view snapshot %s: %w", info.ViewKey, err)
	}
	return toMounts(mountsResp.GetMounts()), nil
}

func removeSharedViewSnapshot(ctx context.Context, info *SnapshotViewInfo) error {
	if info.ViewKey == "" || info.Snapshotter == "" {
		return nil
	}

	client, err := newContainerdClient()
	if err != nil {
		return fmt.Errorf("connect to containerd for snapshot cleanup: %w", err)
	}
	defer client.Close()

	_, err = client.snapshots.Remove(withNamespace(ctx, info.Namespace), &snapshotsapi.RemoveSnapshotRequest{
		Snapshotter: info.Snapshotter,
		Key:         info.ViewKey,
	})
	err = grpcErr(err)
	if err != nil && !errdefs.IsNotFound(err) {
		log.WithError(err).Warn("failed to remove shared view snapshot from containerd")
		return err
	}
	return nil
}

