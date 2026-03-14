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
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/snapshots"
	"github.com/moby/sys/mountinfo"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
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
	sharedViewLeaseID = "urunc-shared-view-"
)

var log = logrus.WithField("subsystem", "shiminject")

// SnapshotViewInfo describes the shared snapshot view for a container.
// Kept in-memory by the shim wrapper; used for cleanup on Delete.
type SnapshotViewInfo struct {
	// SharedViewID identifies the shared mount (based on snapshotKey).
	SharedViewID string
	// ViewKey is the containerd snapshot name for the shared view.
	ViewKey     string
	// MountPath is the shared data directory all containers bind-mount files from.
	MountPath   string
	Snapshotter string
	Namespace   string
	ContainerID string
}

// sharedViewPaths holds filesystem paths for a shared view entry.
type sharedViewPaths struct {
	base     string
	dataDir  string
	usersDir string
	lockPath string
}

// newSharedViewPaths computes paths for the given sharedViewID.
func newSharedViewPaths(sharedViewID string) sharedViewPaths {
	base := filepath.Join(sharedViewsRoot, sharedViewID)
	return sharedViewPaths{
		base:     base,
		dataDir:  filepath.Join(base, sharedViewsData),
		usersDir: filepath.Join(base, sharedViewsUsers),
		lockPath: base + sharedViewsLock,
	}
}

// containerdAddress returns the containerd socket address.
func containerdAddress() string {
	if addr := os.Getenv("CONTAINERD_ADDRESS"); addr != "" {
		return addr
	}
	return defaultContainerd
}

// newContainerdClient dials containerd scoped to the given namespace.
func newContainerdClient(ns string) (*containerd.Client, error) {
	addr := containerdAddress()
	start := time.Now()
	client, err := containerd.New(addr, containerd.WithDefaultNamespace(ns))
	if err != nil {
		return nil, fmt.Errorf("containerd client: %w", err)
	}
	log.WithFields(logrus.Fields{
		"op":          "containerd.New",
		"address":     addr,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("containerd call completed")
	return client, nil
}

// acquireSharedViewLock opens (or creates) the per-shared-view lock file and
// acquires an exclusive flock. The returned unlock func must be deferred.
func acquireSharedViewLock(lockPath string) (*os.File, func(), error) {
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("open shared view lock %s: %w", lockPath, err)
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		_ = lockFile.Close()
		return nil, nil, fmt.Errorf("flock %s: %w", lockPath, err)
	}
	unlock := func() {
		if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_UN); err != nil {
			log.WithError(err).WithField("lock", lockPath).Warn("failed to unlock shared view lock")
		}
		_ = lockFile.Close()
	}
	return lockFile, unlock, nil
}

// resolveSnapshotKey fetches the container record from containerd and returns
// the effective snapshot key and snapshotter. For devmapper it transparently
// substitutes the committed parent snapshot when one exists.
// Both return values are empty strings when the container carries no snapshot.
func resolveSnapshotKey(ctx context.Context, client *containerd.Client, containerID string) (snapshotKey, snapshotter string, err error) {
	store := client.ContainerService()
	start := time.Now()
	ctr, err := store.Get(ctx, containerID)
	if err != nil {
		return "", "", fmt.Errorf("get container %s: %w", containerID, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "ContainerService.Get",
		"container":   containerID,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("containerd call completed")

	snapshotKey = ctr.SnapshotKey
	snapshotter = ctr.Snapshotter
	log.WithFields(logrus.Fields{
		"container":    containerID,
		"snapshot_key": snapshotKey,
		"snapshotter":  snapshotter,
	}).Info("resolved container snapshot info")

	if snapshotKey == "" || snapshotter == "" {
		log.WithFields(logrus.Fields{
			"container":    containerID,
			"snapshot_key": snapshotKey,
			"snapshotter":  snapshotter,
		}).Info("container has no SnapshotKey/Snapshotter, skipping shared snapshot view creation")
		return "", "", nil
	}

	// For devmapper (and similar block snapshotters), the container's SnapshotKey
	// typically refers to an active snapshot whose parent is the committed image
	// layer. Shared read-only views must be based on a committed parent snapshot,
	// otherwise the snapshotter rejects the View() call with "parent ... is not
	// committed snapshot". Walk up the parent chain until we find a committed
	// snapshot and use that as the effective key for the shared view.
	if snapshotter == "devmapper" {
		ss := client.SnapshotService(snapshotter)

		// For devmapper, the container's snapshotKey may itself already be
		// committed, but its parent (which devmapper validates when creating
		// a view) can still be an active snapshot. The View base must have a
		// committed parent, so we walk *from the container snapshot's parent*
		// upwards until we find a committed snapshot and use that as the base.
		info, serr := ss.Stat(ctx, snapshotKey)
		if serr != nil {
			return "", "", fmt.Errorf("stat snapshot %s (devmapper): %w", snapshotKey, serr)
		}
		current := info.Parent
		if current == "" {
			// No parent chain to walk; fall back to the original key.
			log.WithFields(logrus.Fields{
				"container":          containerID,
				"container_snapshot": snapshotKey,
			}).Info("devmapper container snapshot has no parent; using container snapshot as shared view base")
			return snapshotKey, snapshotter, nil
		}

		for {
			pinfo, serr := ss.Stat(ctx, current)
			if serr != nil {
				return "", "", fmt.Errorf("stat snapshot %s (devmapper parent walk): %w", current, serr)
			}

			log.WithFields(logrus.Fields{
				"container":          containerID,
				"candidate_base":     current,
				"kind":               pinfo.Kind,
				"parent":             pinfo.Parent,
				"container_snapshot": snapshotKey,
			}).Info("inspected devmapper snapshot while resolving shared view base")

			if pinfo.Kind == snapshots.KindCommitted {
				log.WithFields(logrus.Fields{
					"container":          containerID,
					"container_snapshot": snapshotKey,
					"view_base_snapshot": current,
				}).Info("resolved devmapper committed parent snapshot for shared view")
				snapshotKey = current
				break
			}

			if pinfo.Parent == "" {
				return "", "", fmt.Errorf("devmapper snapshot %s has no committed parent in chain", snapshotKey)
			}
			current = pinfo.Parent
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

// getOrCreateSharedViewMounts returns the OS mount list for viewKey,
// creating the snapshot view in containerd when it does not already exist.
func getOrCreateSharedViewMounts(ctx context.Context, client *containerd.Client, snapshotter, viewKey, snapshotKey string) ([]mount.Mount, error) {
	ss := client.SnapshotService(snapshotter)
	start := time.Now()
	mounts, err := ss.View(ctx, viewKey, snapshotKey)
	if err == nil {
		log.WithFields(logrus.Fields{
			"op":          "SnapshotService.View",
			"snapshotter": snapshotter,
			"view_key":    viewKey,
			"snapshot":    snapshotKey,
			"duration_ms": time.Since(start).Milliseconds(),
		}).Info("containerd call completed")
		return mounts, nil
	}
	if !errdefs.IsAlreadyExists(err) {
		return nil, fmt.Errorf("failed to create shared view snapshot %s from %s: %w", viewKey, snapshotKey, err)
	}
	// View already exists — fetch its mounts.
	mounts, err = ss.Mounts(ctx, viewKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get mounts for shared view snapshot %s: %w", viewKey, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "SnapshotService.Mounts",
		"snapshotter": snapshotter,
		"snapshot":    viewKey,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("containerd call completed")
	return mounts, nil
}

// buildMountErrorFields assembles diagnostic log fields when a mount fails.
func buildMountErrorFields(dataDir string, mounts []mount.Mount) logrus.Fields {
	fields := logrus.Fields{"mount_path": dataDir}
	if _, serr := os.Stat(dataDir); serr != nil {
		fields["target_stat_error"] = serr.Error()
	}
	for i, m := range mounts {
		if m.Source == "" {
			continue
		}
		if _, serr := os.Stat(m.Source); serr != nil {
			fields[fmt.Sprintf("source_%d_error", i)] = serr.Error()
		}
	}
	return fields
}

// mountSharedView ensures the shared snapshot view is mounted at paths.dataDir.
// If it is already mounted the function is a no-op.
// createdData indicates whether the caller freshly created the directory;
// on mount failure it is used to decide whether to clean up.
func mountSharedView(ctx context.Context, client *containerd.Client, snapshotter, viewKey, snapshotKey string, paths sharedViewPaths, createdData bool) error {
	mounted, mntErr := mountinfo.Mounted(paths.dataDir)
	if mntErr != nil {
		log.WithError(mntErr).WithField("path", paths.dataDir).Warn("failed to check mountpoint status")
	}
	if mounted {
		log.WithFields(logrus.Fields{
			"mount_path": paths.dataDir,
			"view_key":   viewKey,
		}).Info("shared snapshot view already mounted, reusing")
		return nil
	}

	mounts, err := getOrCreateSharedViewMounts(ctx, client, snapshotter, viewKey, snapshotKey)
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

	start := time.Now()
	if err := mount.All(mounts, paths.dataDir); err != nil {
		log.WithFields(buildMountErrorFields(paths.dataDir, mounts)).WithError(err).Warn("mount shared snapshot view failed")
		if createdData {
			_ = os.RemoveAll(paths.dataDir)
		}
		return fmt.Errorf("mount shared snapshot at %s: %w", paths.dataDir, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "mount.All",
		"mount_path":  paths.dataDir,
		"view_key":    viewKey,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("shared snapshot view mounted")
	return nil
}

// registerContainerUser writes a marker file for containerID under users/,
// recording that this container is using the shared view.
// On failure, if createdData is true the freshly mounted view is cleaned up.
func registerContainerUser(containerID string, paths sharedViewPaths, createdData bool) error {
	userMarker := filepath.Join(paths.usersDir, containerID)
	start := time.Now()
	if err := os.WriteFile(userMarker, []byte(time.Now().Format(time.RFC3339Nano)), 0644); err != nil {
		if createdData {
			if uerr := mount.Unmount(paths.dataDir, 0); uerr != nil {
				log.WithError(uerr).WithField("mount_path", paths.dataDir).Warn("failed to unmount shared view after marker write failure")
			}
			_ = os.RemoveAll(paths.base)
		}
		return fmt.Errorf("create user marker %s: %w", userMarker, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "marker.write",
		"marker":      userMarker,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("shared view user marker written")
	return nil
}

// injectViewPathToConfig adds the shared snapshot view mount path into the
// bundle's config.json OCI annotations so urunc can locate unikernel files
// via bind mounts without talking to containerd directly.
func injectViewPathToConfig(bundle, containerID, mountPath string) error {
	configPath := filepath.Join(bundle, configFilename)
	log.WithFields(logrus.Fields{
		"op":        "config.json.prepare",
		"bundle":    bundle,
		"config":    configPath,
		"view_path": mountPath,
	}).Info("preparing to update config.json with snapshot view annotation")

	start := time.Now()
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", configPath, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "config.json.read",
		"config":      configPath,
		"duration_ms": time.Since(start).Milliseconds(),
		"bytes":       len(data),
	}).Info("read config.json")

	var spec specs.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}
	spec.Annotations[annotViewMntPath] = mountPath

	out, err := json.MarshalIndent(&spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config with view mount annotation: %w", err)
	}
	start = time.Now()
	if err := os.WriteFile(configPath, out, 0644); err != nil {
		return fmt.Errorf("write %s: %w", configPath, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "config.json.update",
		"bundle":      bundle,
		"container":   containerID,
		"duration_ms": time.Since(start).Milliseconds(),
		"view_path":   mountPath,
	}).Info("injected snapshot view mount path into config.json")
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

	client, err := newContainerdClient(ns)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	// snapshotKey: the containerd key identifying the container's root filesystem
	//              snapshot; used to create or reuse the shared read-only view.
	// snapshotter: the snapshotter backend type (e.g. overlayfs, devmapper);
	//              determines which snapshotter service to operate on and drives
	//              the devmapper-specific parent resolution path.
	snapshotKey, snapshotter, err := resolveSnapshotKey(ctx, client, containerID)
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
	ls := client.LeasesService()
	if _, err := ls.Create(ctx, leases.WithID(leaseID)); err != nil && !errdefs.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create shared view lease %s: %w", leaseID, err)
	}
	ctx = leases.WithLease(ctx, leaseID)

	// Ensure root directory exists before acquiring the lock file inside it.
	if err := os.MkdirAll(sharedViewsRoot, 0755); err != nil {
		return nil, fmt.Errorf("create shared views root %s: %w", sharedViewsRoot, err)
	}

	_, unlock, err := acquireSharedViewLock(paths.lockPath)
	if err != nil {
		return nil, err
	}
	defer unlock()

	createdData, err := ensureSharedViewDirs(paths)
	if err != nil {
		return nil, err
	}

	// Mount the shared view once; subsequent containers reuse the existing mount.
	if err := mountSharedView(ctx, client, snapshotter, viewKey, snapshotKey, paths, createdData); err != nil {
		return nil, err
	}

	log.WithFields(logrus.Fields{
		"container":      containerID,
		"shared_view_id": sharedViewID,
		"view_key":       viewKey,
		"mount_path":     paths.dataDir,
		"snapshot_key":   snapshotKey,
		"snapshotter":    snapshotter,
		"namespace":      ns,
		"created_mount":  createdData,
	}).Info("shared snapshot view ready; container will bind-mount files from it")

	if err := registerContainerUser(containerID, paths, createdData); err != nil {
		return nil, err
	}

	if err := injectViewPathToConfig(bundle, containerID, paths.dataDir); err != nil {
		return nil, err
	}

	return &SnapshotViewInfo{
		SharedViewID: sharedViewID,
		ViewKey:      viewKey,
		MountPath:    paths.dataDir,
		Snapshotter:  snapshotter,
		Namespace:    ns,
		ContainerID:  containerID,
	}, nil
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
	_, unlock, err := acquireSharedViewLock(paths.lockPath)
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
		log.WithError(err).WithField("marker", userMarker).Warn("failed to remove shared view user marker")
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
		log.WithFields(logrus.Fields{
			"shared_view_id":  info.SharedViewID,
			"remaining_users": len(entries),
			"container":       info.ContainerID,
		}).Debug("shared view still in use by other containers, skipping teardown")
		return nil
	}

	// No users remain — unmount, delete snapshot, remove directories.
	var firstErr error

	if err := mount.Unmount(paths.dataDir, 0); err != nil && !os.IsNotExist(err) {
		log.WithError(err).WithField("mount_path", paths.dataDir).Warn("failed to unmount shared snapshot view")
		firstErr = err
	} else {
		log.WithField("mount_path", paths.dataDir).Info("unmounted shared snapshot view")
	}

	removeSharedViewSnapshot(ctx, info)

	if err := os.RemoveAll(paths.base); err != nil {
		log.WithError(err).WithField("view_base", paths.base).Warn("failed to remove shared view directory")
		if firstErr == nil {
			firstErr = err
		}
	}
	if err := os.Remove(paths.lockPath); err != nil && !os.IsNotExist(err) {
		log.WithError(err).WithField("lock", paths.lockPath).Warn("failed to remove shared view lock file")
		if firstErr == nil {
			firstErr = err
		}
	}

	log.WithFields(logrus.Fields{
		"shared_view_id": info.SharedViewID,
		"container":      info.ContainerID,
	}).Info("shared snapshot view fully cleaned up")
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
		ctx = leases.WithLease(ctx, sharedViewLeaseID+info.SharedViewID)
	}

	client, err := newContainerdClient(info.Namespace)
	if err != nil {
		log.WithError(err).WithFields(logrus.Fields{
			"view_key":    info.ViewKey,
			"snapshotter": info.Snapshotter,
		}).Warn("failed to connect to containerd for snapshot cleanup")
		return
	}
	defer client.Close()

	ss := client.SnapshotService(info.Snapshotter)
	if err := ss.Remove(ctx, info.ViewKey); err != nil {
		log.WithError(err).WithFields(logrus.Fields{
			"view_key":    info.ViewKey,
			"snapshotter": info.Snapshotter,
		}).Warn("failed to remove shared view snapshot from containerd")
	} else {
		log.WithFields(logrus.Fields{
			"view_key":    info.ViewKey,
			"snapshotter": info.Snapshotter,
		}).Info("removed shared view snapshot from containerd")

		// Best-effort removal of the corresponding lease once the snapshot is
		// gone. This allows containerd to garbage-collect any remaining
		// metadata while ensuring the snapshot was protected during use.
		if info.SharedViewID != "" {
			leaseID := sharedViewLeaseID + info.SharedViewID
			ls := client.LeasesService()
			if err := ls.Delete(ctx, leases.Lease{ID: leaseID}); err != nil && !errdefs.IsNotFound(err) {
				log.WithError(err).WithField("lease_id", leaseID).Warn("failed to remove shared view lease from containerd")
			} else if err == nil {
				log.WithField("lease_id", leaseID).Info("removed shared view lease from containerd")
			}
		}
	}
}
          