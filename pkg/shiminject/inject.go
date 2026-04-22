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

package shiminject

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/namespaces"
	"github.com/moby/sys/mountinfo"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	// NOTE: Only the shared mount path is injected into OCI annotations.
	// All other data are shim-internal and kept in-memory for cleanup.
	annotViewMntPath  = "com.urunc.snapshot.view.mount_path"
	configFilename    = "config.json"
	defaultContainerd = "/run/containerd/containerd.sock"
	sharedViewsRoot   = "/run/urunc/shared-views"
	sharedViewsData   = "data"
	sharedViewsUsers  = "users"
	sharedViewsLock   = ".lock"
	sharedViewsCreate = ".creating"
	sharedViewLeaseID = "urunc-shared-view-"
	sharedViewWait    = 10 * time.Millisecond
	sharedViewTimeout = 5 * time.Second
)

var log = logrus.WithField("subsystem", "shiminject")

// SnapshotViewInfo describes the shared snapshot view for a container.
// Kept in-memory by the shim wrapper; used for cleanup on Delete.
type SnapshotViewInfo struct {
	// SharedViewID identifies the shared mount (based on snapshotKey).
	SharedViewID string
	// ViewKey is the containerd snapshot name for the shared view.
	ViewKey string
	// MountPath is the shared data directory all containers bind-mount files from.
	MountPath   string
	Snapshotter string
	Namespace   string
	ContainerID string
}

// sharedViewPaths holds filesystem paths for a shared view entry.
type sharedViewPaths struct {
	base       string
	dataDir    string
	usersDir   string
	lockPath   string
	createPath string
}

// newSharedViewPaths computes paths for the given sharedViewID.
func newSharedViewPaths(sharedViewID string) sharedViewPaths {
	base := filepath.Join(sharedViewsRoot, sharedViewID)
	return sharedViewPaths{
		base:       base,
		dataDir:    filepath.Join(base, sharedViewsData),
		usersDir:   filepath.Join(base, sharedViewsUsers),
		lockPath:   base + sharedViewsLock,
		createPath: base + sharedViewsCreate,
	}
}

// containerdAddress returns the containerd socket address.
func containerdAddress() string {
	if addr := os.Getenv("CONTAINERD_ADDRESS"); addr != "" {
		return addr
	}
	return defaultContainerd
}

type containerdClients struct {
	conn       *grpc.ClientConn
	snapshots  snapshotsapi.SnapshotsClient
	leases     leasesapi.LeasesClient
	containers containersapi.ContainersClient
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

// newContainerdClient dials containerd gRPC and creates only required service clients.
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

// acquireSharedViewLock opens (or creates) the per-shared-view lock file and
// acquires an exclusive flock. The returned unlock func must be deferred.
func acquireSharedViewLock(lockPath string) (func(), error) {
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open shared view lock %s: %w", lockPath, err)
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("flock %s: %w", lockPath, err)
	}
	unlock := func() {
		if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_UN); err != nil {
			log.WithError(err).Warn("failed to unlock shared view lock")
		}
		_ = lockFile.Close()
	}
	return unlock, nil
}

// resolveSnapshotKey fetches the container record from containerd and returns
// the effective snapshot key and snapshotter. For devmapper it transparently
// substitutes the committed parent snapshot when one exists.
// Both return values are empty strings when the container carries no snapshot.
func resolveSnapshotKey(ctx context.Context, client *containerdClients, ns, containerID string) (snapshotKey, snapshotter string, err error) {
	// Fast path: for devmapper and some snapshotters, active snapshot key equals
	// container ID. Probe common snapshotters first to avoid Containers.Get path.
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
		ctrResp, gerr := client.containersClient().Get(withNamespace(ctx, ns), &containersapi.GetContainerRequest{ID: containerID})
		if gerr != nil {
			return "", "", fmt.Errorf("get container %s: %w", containerID, grpcErr(gerr))
		}
		ctr := ctrResp.GetContainer()
		if ctr == nil {
			return "", "", fmt.Errorf("container %s not found in response", containerID)
		}
		snapshotKey = ctr.GetSnapshotKey()
		snapshotter = ctr.GetSnapshotter()
	}
	if snapshotKey == "" || snapshotter == "" {
		return "", "", nil
	}

	// For devmapper (and similar block snapshotters), the container's SnapshotKey
	// typically refers to an active snapshot whose parent is the committed image
	// layer. Shared read-only views must be based on a committed parent snapshot,
	// otherwise the snapshotter rejects the View() call with "parent ... is not
	// committed snapshot". Walk up the parent chain until we find a committed
	// snapshot and use that as the effective key for the shared view.
	if snapshotter == "devmapper" {
		// For devmapper, the container's snapshotKey may itself already be
		// committed, but its parent (which devmapper validates when creating
		// a view) can still be an active snapshot. The View base must have a
		// committed parent, so we walk *from the container snapshot's parent*
		// upwards until we find a committed snapshot and use that as the base.
		infoResp, serr := client.snapshots.Stat(withNamespace(ctx, ns), &snapshotsapi.StatSnapshotRequest{
			Snapshotter: snapshotter,
			Key:         snapshotKey,
		})
		if serr != nil {
			return "", "", fmt.Errorf("stat snapshot %s (devmapper): %w", snapshotKey, grpcErr(serr))
		}
		current := infoResp.GetInfo().GetParent()
		if current == "" {
			// No parent chain to walk; fall back to the original key.
			return snapshotKey, snapshotter, nil
		}

		for {
			pinfoResp, serr := client.snapshots.Stat(withNamespace(ctx, ns), &snapshotsapi.StatSnapshotRequest{
				Snapshotter: snapshotter,
				Key:         current,
			})
			if serr != nil {
				return "", "", fmt.Errorf("stat snapshot %s (devmapper parent walk): %w", current, grpcErr(serr))
			}
			pinfo := pinfoResp.GetInfo()
			if pinfo == nil {
				return "", "", fmt.Errorf("stat snapshot %s (devmapper parent walk): empty info", current)
			}

			if pinfo.GetKind() == snapshotsapi.Kind_COMMITTED {
				snapshotKey = current
				break
			}

			if pinfo.GetParent() == "" {
				return "", "", fmt.Errorf("devmapper snapshot %s has no committed parent in chain", snapshotKey)
			}
			current = pinfo.GetParent()
		}
	}

	return snapshotKey, snapshotter, nil
}

// ensureSharedViewDirs creates the users and data subdirectories.
// It reports whether the data directory was freshly created.
// Caller must hold the shared view lock before calling.
func ensureSharedViewDirs(paths sharedViewPaths) (createdData bool, err error) {
	if err := os.MkdirAll(paths.usersDir, 0755); err != nil {
		return false, fmt.Errorf("create shared view users dir %s: %w", paths.usersDir, err)
	}
	if _, err := os.Stat(paths.dataDir); err != nil {
		if !os.IsNotExist(err) {
			return false, fmt.Errorf("stat shared view data dir %s: %w", paths.dataDir, err)
		}
		if err := os.MkdirAll(paths.dataDir, 0755); err != nil {
			return false, fmt.Errorf("create shared view data dir %s: %w", paths.dataDir, err)
		}
		return true, nil
	}
	return false, nil
}

func setSharedViewCreating(paths sharedViewPaths, containerID string) error {
	return os.WriteFile(paths.createPath, []byte(containerID), 0600)
}

func clearSharedViewCreating(paths sharedViewPaths) error {
	if err := os.Remove(paths.createPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func sharedViewCreateInProgress(paths sharedViewPaths) bool {
	_, err := os.Stat(paths.createPath)
	return err == nil
}

func waitForSharedViewReady(paths sharedViewPaths) error {
	start := time.Now()

	for {
		mounted, err := mountinfo.Mounted(paths.dataDir)
		if err == nil && mounted {
			return nil
		}
		if !sharedViewCreateInProgress(paths) {
			return nil
		}
		if time.Since(start) >= sharedViewTimeout {
			return fmt.Errorf("timed out waiting for shared view %s to become ready", paths.dataDir)
		}
		time.Sleep(sharedViewWait)
	}
}

// getOrCreateSharedViewMounts returns the OS mount list for viewKey,
// creating the snapshot view in containerd when it does not already exist.
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
		mounts := toMounts(viewResp.GetMounts())
		return mounts, nil
	}
	if !errdefs.IsAlreadyExists(err) {
		return nil, fmt.Errorf("failed to create shared view snapshot %s from %s: %w", viewKey, snapshotKey, err)
	}
	// View already exists — fetch its mounts.
	mountsResp, err := client.snapshots.Mounts(withNamespace(ctx, ns), &snapshotsapi.MountsRequest{
		Snapshotter: snapshotter,
		Key:         viewKey,
	})
	err = grpcErr(err)
	if err != nil {
		return nil, fmt.Errorf("failed to get mounts for shared view snapshot %s: %w", viewKey, err)
	}
	mounts := toMounts(mountsResp.GetMounts())
	return mounts, nil
}

// mountSharedView ensures the shared snapshot view is mounted at paths.dataDir.
// If it is already mounted the function is a no-op.
// createdData indicates whether the caller freshly created the directory;
// on mount failure it is used to decide whether to clean up.
func mountSharedView(ctx context.Context, client *containerdClients, ns, snapshotter, viewKey, snapshotKey string, paths sharedViewPaths, createdData bool) error {
	mounted, mntErr := mountinfo.Mounted(paths.dataDir)
	if mntErr != nil {
		log.WithError(mntErr).Warn("failed to check mountpoint status")
	}
	if mounted {
		return nil
	}

	mounts, err := getOrCreateSharedViewMounts(ctx, client, ns, snapshotter, viewKey, snapshotKey)
	if err != nil {
		return err
	}

	// Force read-only, noatime mounts for the shared view to avoid accidental
	// writes and atime-induced I/O on the parent device.
	for i := range mounts {
		opts := make([]string, 0, len(mounts[i].Options)+2)
		hasRO := false
		for _, o := range mounts[i].Options {
			if o == "rw" {
				continue
			}
			if o == "ro" {
				hasRO = true
			}
			opts = append(opts, o)
		}
		if !hasRO {
			opts = append(opts, "ro")
		}
		hasNoAtime := false
		for _, o := range opts {
			if o == "noatime" {
				hasNoAtime = true
				break
			}
		}
		if !hasNoAtime {
			opts = append(opts, "noatime")
		}
		mounts[i].Options = opts
	}

	if err := mount.All(mounts, paths.dataDir); err != nil {
		log.WithError(err).Warn("mount shared snapshot view failed")
		if createdData {
			_ = os.RemoveAll(paths.dataDir)
		}
		return fmt.Errorf("mount shared snapshot at %s: %w", paths.dataDir, err)
	}
	return nil
}

// registerContainerUser writes a marker file for containerID under users/,
// recording that this container is using the shared view.
// On failure, if cleanupMount is true the freshly mounted view is cleaned up.
func registerContainerUser(containerID string, paths sharedViewPaths, cleanupMount bool) error {
	userMarker := filepath.Join(paths.usersDir, containerID)
	if err := os.WriteFile(userMarker, []byte(time.Now().Format(time.RFC3339Nano)), 0600); err != nil {
		if cleanupMount {
			if uerr := mount.Unmount(paths.dataDir, 0); uerr != nil {
				log.WithError(uerr).Warn("failed to unmount shared view after marker write failure")
			}
			_ = os.RemoveAll(paths.base)
		}
		return fmt.Errorf("create user marker %s: %w", userMarker, err)
	}
	return nil
}

// injectViewPathToConfig adds the shared snapshot view mount path into the
// bundle's config.json OCI annotations so urunc can locate unikernel files
// via bind mounts without talking to containerd directly.
func injectViewPathToConfig(bundle, mountPath string) error {
	configPath := filepath.Join(bundle, configFilename)
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", configPath, err)
	}

	// Memory-lean path: patch only top-level "annotations" instead of
	// unmarshalling the full OCI spec struct.
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}

	annotations := make(map[string]string)
	if raw, ok := root["annotations"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &annotations); err != nil {
			return fmt.Errorf("unmarshal config.annotations: %w", err)
		}
	}
	// Fast path: avoid rewrite/allocation churn when value is already set.
	if annotations[annotViewMntPath] == mountPath {
		return nil
	}
	annotations[annotViewMntPath] = mountPath

	annBytes, err := json.Marshal(annotations)
	if err != nil {
		return fmt.Errorf("marshal config.annotations: %w", err)
	}
	root["annotations"] = annBytes

	// Use compact marshal to reduce transient allocation pressure in shim hot path.
	out, err := json.Marshal(root)
	if err != nil {
		return fmt.Errorf("marshal config with view mount annotation: %w", err)
	}
	if err := os.WriteFile(configPath, out, 0600); err != nil {
		return fmt.Errorf("write %s: %w", configPath, err)
	}
	return nil
}

func ensureSharedViewReadyAndRegistered(
	ctx context.Context,
	client *containerdClients,
	ns, snapshotter, viewKey, snapshotKey string,
	paths sharedViewPaths,
	containerID string,
) error {
	createdData := false
	shouldCreate := false

	unlock, err := acquireSharedViewLock(paths.lockPath)
	if err != nil {
		return err
	}

	createdData, err = ensureSharedViewDirs(paths)
	if err != nil {
		unlock()
		return err
	}

	mounted, mErr := mountinfo.Mounted(paths.dataDir)
	if mErr != nil {
		log.WithError(mErr).Warn("failed to check mountpoint status while deciding shared view create/reuse")
	}

	switch {
	case mounted:
		// Fast path: the shared view is already mounted; just register under lock.
	case sharedViewCreateInProgress(paths):
		unlock()
		if err := waitForSharedViewReady(paths); err != nil {
			return err
		}

		unlock, err = acquireSharedViewLock(paths.lockPath)
		if err != nil {
			return err
		}

		mounted, mErr = mountinfo.Mounted(paths.dataDir)
		if mErr != nil {
			log.WithError(mErr).Warn("failed to re-check mountpoint status after waiting for shared view creator")
		}
		if !mounted && !sharedViewCreateInProgress(paths) {
			if err := setSharedViewCreating(paths, containerID); err != nil {
				unlock()
				return fmt.Errorf("take over shared view create in progress %s: %w", paths.createPath, err)
			}
			shouldCreate = true
			unlock()

			if err := mountSharedView(ctx, client, ns, snapshotter, viewKey, snapshotKey, paths, createdData); err != nil {
				relock, lerr := acquireSharedViewLock(paths.lockPath)
				if lerr == nil {
					if cerr := clearSharedViewCreating(paths); cerr != nil {
						log.WithError(cerr).Warn("failed to clear shared view creating marker after takeover mount failure")
					}
					relock()
				}
				return err
			}

			unlock, err = acquireSharedViewLock(paths.lockPath)
			if err != nil {
				return err
			}
		}
	default:
		if err := setSharedViewCreating(paths, containerID); err != nil {
			unlock()
			return fmt.Errorf("mark shared view create in progress %s: %w", paths.createPath, err)
		}
		shouldCreate = true
		unlock()

		if err := mountSharedView(ctx, client, ns, snapshotter, viewKey, snapshotKey, paths, createdData); err != nil {
			relock, lerr := acquireSharedViewLock(paths.lockPath)
			if lerr == nil {
				if cerr := clearSharedViewCreating(paths); cerr != nil {
					log.WithError(cerr).Warn("failed to clear shared view creating marker after mount failure")
				}
				relock()
			}
			return err
		}

		unlock, err = acquireSharedViewLock(paths.lockPath)
		if err != nil {
			return err
		}
	}

	if err := registerContainerUser(containerID, paths, shouldCreate); err != nil {
		if shouldCreate {
			if cerr := clearSharedViewCreating(paths); cerr != nil {
				log.WithError(cerr).Warn("failed to clear shared view creating marker after user registration failure")
			}
		}
		unlock()
		return err
	}

	if shouldCreate {
		if err := clearSharedViewCreating(paths); err != nil {
			unlock()
			return fmt.Errorf("clear shared view creating marker %s: %w", paths.createPath, err)
		}
	}

	unlock()
	return nil
}

// CreateSnapshotView creates or reuses a shared read-only snapshot view for
// the container's image layer. Only one device mount is created per unique
// snapshotKey; all containers sharing the same image reuse that mount and
// each registers itself as a user via a marker file.
//
// The shared mount path is injected into the bundle's config.json so urunc
// can bind-mount individual files from it into each container without
// requiring any per-container device operations.
//
// If the container has no SnapshotKey/Snapshotter, the function is a no-op
// and returns (nil, nil).
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

	// snapshotKey: the containerd key identifying the container's root filesystem
	//              snapshot; used to create or reuse the shared read-only view.
	// snapshotter: the snapshotter backend type (e.g. overlayfs, devmapper);
	//              determines which snapshotter service to operate on and drives
	//              the devmapper-specific parent resolution path.
	snapshotKey, snapshotter, err := resolveSnapshotKey(ctx, client, ns, containerID)
	if err != nil {
		return nil, err
	}
	// Container has no associated snapshot (e.g. special containers); skip view creation.
	if snapshotKey == "" {
		return nil, nil
	}

	// sharedViewID is based on snapshotKey so all containers from the same
	// image layer reuse the same mount point.
	sharedViewID := fmt.Sprintf("%s_%s_%s", snapshotter, ns, snapshotKey)
	viewKey := "urunc-shared-" + sharedViewID
	paths := newSharedViewPaths(sharedViewID)

	// Ensure a containerd lease exists for this shared view so that GC keeps
	// the underlying snapshot device alive while any container is using it.
	leaseID := sharedViewLeaseID + sharedViewID
	_, err = client.leases.Create(withNamespace(ctx, ns), &leasesapi.CreateRequest{ID: leaseID})
	err = grpcErr(err)
	if err != nil && !errdefs.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create shared view lease %s: %w", leaseID, err)
	}
	ctx = metadata.AppendToOutgoingContext(withNamespace(ctx, ns), "containerd-lease", leaseID)

	// Ensure root directory exists before acquiring the lock file inside it.
	if err := os.MkdirAll(sharedViewsRoot, 0755); err != nil {
		return nil, fmt.Errorf("create shared views root %s: %w", sharedViewsRoot, err)
	}

	if err := ensureSharedViewReadyAndRegistered(
		ctx, client, ns, snapshotter, viewKey, snapshotKey, paths, containerID,
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

	if err := injectViewPathToConfig(bundle, paths.dataDir); err != nil {
		if cerr := CleanupSnapshotView(ctx, info); cerr != nil {
			log.WithError(cerr).Warn("failed to clean up shared view after config injection failure")
			return nil, fmt.Errorf("inject snapshot view path into config: %w (cleanup also failed: %v)", err, cerr)
		}
		return nil, fmt.Errorf("inject snapshot view path into config: %w", err)
	}

	return info, nil
}

// CleanupSnapshotView removes this container's user marker. When no more
// containers reference the shared view, it unmounts the device and removes
// the shared view snapshot from containerd.
func CleanupSnapshotView(ctx context.Context, info *SnapshotViewInfo) error {
	if info == nil || info.SharedViewID == "" {
		return nil
	}

	paths := newSharedViewPaths(info.SharedViewID)

	// Acquire lock to serialize user-count check and cleanup.
	unlock, err := acquireSharedViewLock(paths.lockPath)
	if err != nil {
		return err
	}
	defer unlock()

	removeContainerUserMarker(info.ContainerID, paths.usersDir)

	return removeSharedViewIfUnused(ctx, info, paths)
}

// removeContainerUserMarker deletes this container's marker file.
func removeContainerUserMarker(containerID, usersDir string) {
	if containerID == "" {
		return
	}
	userMarker := filepath.Join(usersDir, containerID)
	if err := os.Remove(userMarker); err != nil && !os.IsNotExist(err) {
		log.WithError(err).Warn("failed to remove shared view user marker")
	}
}

// removeSharedViewIfUnused checks the users directory; if empty it unmounts
// the shared view and removes the containerd snapshot and all directories.
func removeSharedViewIfUnused(ctx context.Context, info *SnapshotViewInfo, paths sharedViewPaths) error {
	entries, err := os.ReadDir(paths.usersDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read shared view users dir %s: %w", paths.usersDir, err)
	}
	if len(entries) > 0 {
		return nil
	}

	if sharedViewCreateInProgress(paths) {
		return nil
	}

	// No users remain — unmount, delete snapshot, remove directories.
	var firstErr error

	if err := mount.Unmount(paths.dataDir, 0); err != nil && !os.IsNotExist(err) {
		log.WithError(err).Warn("failed to unmount shared snapshot view")
		firstErr = err
	}
	removeSharedViewSnapshot(ctx, info)

	if err := os.RemoveAll(paths.base); err != nil {
		log.WithError(err).Warn("failed to remove shared view directory")
		if firstErr == nil {
			firstErr = err
		}
	}
	if err := os.Remove(paths.lockPath); err != nil && !os.IsNotExist(err) {
		log.WithError(err).Warn("failed to remove shared view lock file")
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// removeSharedViewSnapshot removes the shared view snapshot from containerd.
// Best-effort: errors are logged as warnings.
func removeSharedViewSnapshot(ctx context.Context, info *SnapshotViewInfo) {
	if info.ViewKey == "" || info.Snapshotter == "" {
		return
	}

	// Attach the shared view lease to the context so snapshot removal is
	// performed within the same lease that protects the snapshot from GC.
	if info.SharedViewID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "containerd-lease", sharedViewLeaseID+info.SharedViewID)
	}

	client, err := newContainerdClient()
	if err != nil {
		log.WithError(err).Warn("failed to connect to containerd for snapshot cleanup")
		return
	}
	defer client.Close()

	_, err = client.snapshots.Remove(withNamespace(ctx, info.Namespace), &snapshotsapi.RemoveSnapshotRequest{
		Snapshotter: info.Snapshotter,
		Key:         info.ViewKey,
	})
	err = grpcErr(err)
	if err != nil {
		log.WithError(err).Warn("failed to remove shared view snapshot from containerd")
	} else if info.SharedViewID != "" {
		// Best-effort removal of the corresponding lease once the snapshot is
		// gone. This allows containerd to garbage-collect any remaining
		// metadata while ensuring the snapshot was protected during use.
		leaseID := sharedViewLeaseID + info.SharedViewID
		_, err := client.leases.Delete(withNamespace(ctx, info.Namespace), &leasesapi.DeleteRequest{ID: leaseID})
		err = grpcErr(err)
		if err != nil && !errdefs.IsNotFound(err) {
			log.WithError(err).Warn("failed to remove shared view lease from containerd")
		}
	}
}
