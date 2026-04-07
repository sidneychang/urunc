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
	// experiment toggle: when true, do not create a containerd client at all.
	disableSharedSnapshotViewCreate = false
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
func newContainerdClient(ns string) (*containerdClients, error) {
	addr := containerdAddress()
	start := time.Now()
	conn, err := grpc.NewClient(
		"unix://"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableServiceConfig(),
		grpc.WithDisableRetry(),
		grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("containerd grpc client: %w", err)
	}
	log.WithFields(logrus.Fields{
		"op":          "grpc.NewClient",
		"address":     addr,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Debug("containerd call completed")
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
func acquireSharedViewLock(lockPath string) (*os.File, func(), error) {
	waitStart := time.Now()
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		logPhaseDuration("shiminject.shared_view.lock.open", time.Since(waitStart), logrus.Fields{
			"lock_path": lockPath,
		}, err)
		return nil, nil, fmt.Errorf("open shared view lock %s: %w", lockPath, err)
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		_ = lockFile.Close()
		logPhaseDuration("shiminject.shared_view.lock.wait", time.Since(waitStart), logrus.Fields{
			"lock_path": lockPath,
		}, err)
		return nil, nil, fmt.Errorf("flock %s: %w", lockPath, err)
	}
	lockHeldAt := time.Now()
	logPhaseDuration("shiminject.shared_view.lock.wait", lockHeldAt.Sub(waitStart), logrus.Fields{
		"lock_path": lockPath,
	}, nil)
	unlock := func() {
		holdDur := time.Since(lockHeldAt)
		if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_UN); err != nil {
			log.WithError(err).WithField("lock", lockPath).Warn("failed to unlock shared view lock")
		}
		_ = lockFile.Close()
		logPhaseDuration("shiminject.shared_view.lock.hold", holdDur, logrus.Fields{
			"lock_path": lockPath,
		}, nil)
	}
	return lockFile, unlock, nil
}

// resolveSnapshotKey fetches the container record from containerd and returns
// the effective snapshot key and snapshotter. For devmapper it transparently
// substitutes the committed parent snapshot when one exists.
// Both return values are empty strings when the container carries no snapshot.
func resolveSnapshotKey(ctx context.Context, client *containerdClients, ns, containerID string) (snapshotKey, snapshotter string, err error) {
	timer := startPhaseTimer("shiminject.resolve_snapshot_key.total", logrus.Fields{
		"container": containerID,
	})
	defer func() { timer.done(err) }()
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
			log.WithFields(logrus.Fields{
				"container":    containerID,
				"snapshot_key": snapshotKey,
				"snapshotter":  snapshotter,
			}).Debug("resolved snapshot via fast stat(containerID) path")
			fastResolved = true
			break
		}
	}

	if !fastResolved {
		start := time.Now()
		ctrResp, gerr := client.containersClient().Get(withNamespace(ctx, ns), &containersapi.GetContainerRequest{ID: containerID})
		if gerr != nil {
			return "", "", fmt.Errorf("get container %s: %w", containerID, grpcErr(gerr))
		}
		log.WithFields(logrus.Fields{
			"op":          "ContainerService.Get",
			"container":   containerID,
			"duration_ms": time.Since(start).Milliseconds(),
		}).Debug("containerd call completed")
		ctr := ctrResp.GetContainer()
		if ctr == nil {
			return "", "", fmt.Errorf("container %s not found in response", containerID)
		}
		snapshotKey = ctr.GetSnapshotKey()
		snapshotter = ctr.GetSnapshotter()
	}
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
			log.WithFields(logrus.Fields{
				"container":          containerID,
				"container_snapshot": snapshotKey,
			}).Info("devmapper container snapshot has no parent; using container snapshot as shared view base")
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

			log.WithFields(logrus.Fields{
				"container":          containerID,
				"candidate_base":     current,
				"kind":               pinfo.GetKind(),
				"parent":             pinfo.GetParent(),
				"container_snapshot": snapshotKey,
			}).Info("inspected devmapper snapshot while resolving shared view base")

			if pinfo.GetKind() == snapshotsapi.Kind_COMMITTED {
				log.WithFields(logrus.Fields{
					"container":          containerID,
					"container_snapshot": snapshotKey,
					"view_base_snapshot": current,
				}).Info("resolved devmapper committed parent snapshot for shared view")
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
	timer := startPhaseTimer("shiminject.shared_view.ensure_dirs", logrus.Fields{
		"view_base": paths.base,
	})
	defer func() { timer.done(err) }()
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
	return os.WriteFile(paths.createPath, []byte(containerID), 0644)
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
	timer := startPhaseTimer("shiminject.shared_view.wait_for_ready", logrus.Fields{
		"mount_path": paths.dataDir,
		"marker":     paths.createPath,
	})
	start := time.Now()
	var retErr error
	defer func() { timer.done(retErr) }()

	for {
		mounted, err := mountinfo.Mounted(paths.dataDir)
		if err == nil && mounted {
			return nil
		}
		if !sharedViewCreateInProgress(paths) {
			return nil
		}
		if time.Since(start) >= sharedViewTimeout {
			retErr = fmt.Errorf("timed out waiting for shared view %s to become ready", paths.dataDir)
			return retErr
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
	start := time.Now()
	viewResp, err := client.snapshots.View(withNamespace(ctx, ns), &snapshotsapi.ViewSnapshotRequest{
		Snapshotter: snapshotter,
		Key:         viewKey,
		Parent:      snapshotKey,
	})
	err = grpcErr(err)
	if err == nil {
		mounts := toMounts(viewResp.GetMounts())
		log.WithFields(logrus.Fields{
			"op":          "SnapshotService.View",
			"snapshotter": snapshotter,
			"view_key":    viewKey,
			"snapshot":    snapshotKey,
			"duration_ms": time.Since(start).Milliseconds(),
		}).Debug("containerd call completed")
		logPhaseDuration("shiminject.shared_view.snapshot_view", time.Since(start), logrus.Fields{
			"snapshotter": snapshotter,
			"view_key":    viewKey,
			"snapshot":    snapshotKey,
			"view_op":     "create",
		}, nil)
		return mounts, nil
	}
	if !errdefs.IsAlreadyExists(err) {
		logPhaseDuration("shiminject.shared_view.snapshot_view", time.Since(start), logrus.Fields{
			"snapshotter": snapshotter,
			"view_key":    viewKey,
			"snapshot":    snapshotKey,
			"view_op":     "create",
		}, err)
		return nil, fmt.Errorf("failed to create shared view snapshot %s from %s: %w", viewKey, snapshotKey, err)
	}
	// View already exists — fetch its mounts.
	mountsResp, err := client.snapshots.Mounts(withNamespace(ctx, ns), &snapshotsapi.MountsRequest{
		Snapshotter: snapshotter,
		Key:         viewKey,
	})
	err = grpcErr(err)
	if err != nil {
		logPhaseDuration("shiminject.shared_view.snapshot_mounts", time.Since(start), logrus.Fields{
			"snapshotter": snapshotter,
			"view_key":    viewKey,
			"view_op":     "reuse",
		}, err)
		return nil, fmt.Errorf("failed to get mounts for shared view snapshot %s: %w", viewKey, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "SnapshotService.Mounts",
		"snapshotter": snapshotter,
		"snapshot":    viewKey,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Debug("containerd call completed")
	mounts := toMounts(mountsResp.GetMounts())
	logPhaseDuration("shiminject.shared_view.snapshot_mounts", time.Since(start), logrus.Fields{
		"snapshotter": snapshotter,
		"view_key":    viewKey,
		"view_op":     "reuse",
	}, nil)
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
func mountSharedView(ctx context.Context, client *containerdClients, ns, snapshotter, viewKey, snapshotKey string, paths sharedViewPaths, createdData bool) error {
	timer := startPhaseTimer("shiminject.shared_view.mount.total", logrus.Fields{
		"snapshotter": snapshotter,
		"view_key":    viewKey,
		"snapshot":    snapshotKey,
		"mount_path":  paths.dataDir,
	})
	var retErr error
	defer func() { timer.done(retErr) }()
	mounted, mntErr := mountinfo.Mounted(paths.dataDir)
	if mntErr != nil {
		log.WithError(mntErr).WithField("path", paths.dataDir).Warn("failed to check mountpoint status")
	}
	if mounted {
		log.WithFields(logrus.Fields{
			"mount_path": paths.dataDir,
			"view_key":   viewKey,
		}).Debug("shared snapshot view already mounted, reusing")
		logPhaseDuration("shiminject.shared_view.mount.reuse", 0, logrus.Fields{
			"mount_path": paths.dataDir,
			"view_key":   viewKey,
		}, nil)
		return nil
	}

	mounts, err := getOrCreateSharedViewMounts(ctx, client, ns, snapshotter, viewKey, snapshotKey)
	if err != nil {
		retErr = err
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
		retErr = err
		return fmt.Errorf("mount shared snapshot at %s: %w", paths.dataDir, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "mount.All",
		"mount_path":  paths.dataDir,
		"view_key":    viewKey,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Debug("shared snapshot view mounted")
	logPhaseDuration("shiminject.shared_view.mount.perform", time.Since(start), logrus.Fields{
		"mount_path": paths.dataDir,
		"view_key":   viewKey,
	}, nil)
	return nil
}

// registerContainerUser writes a marker file for containerID under users/,
// recording that this container is using the shared view.
// On failure, if createdData is true the freshly mounted view is cleaned up.
func registerContainerUser(containerID string, paths sharedViewPaths, createdData bool) error {
	timer := startPhaseTimer("shiminject.shared_view.register_user", logrus.Fields{
		"container":  containerID,
		"users_dir":  paths.usersDir,
		"mount_path": paths.dataDir,
	})
	var retErr error
	defer func() { timer.done(retErr) }()
	userMarker := filepath.Join(paths.usersDir, containerID)
	start := time.Now()
	if err := os.WriteFile(userMarker, []byte(time.Now().Format(time.RFC3339Nano)), 0644); err != nil {
		if createdData {
			if uerr := mount.Unmount(paths.dataDir, 0); uerr != nil {
				log.WithError(uerr).WithField("mount_path", paths.dataDir).Warn("failed to unmount shared view after marker write failure")
			}
			_ = os.RemoveAll(paths.base)
		}
		retErr = err
		return fmt.Errorf("create user marker %s: %w", userMarker, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "marker.write",
		"marker":      userMarker,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Debug("shared view user marker written")
	return nil
}

// injectViewPathToConfig adds the shared snapshot view mount path into the
// bundle's config.json OCI annotations so urunc can locate unikernel files
// via bind mounts without talking to containerd directly.
func injectViewPathToConfig(bundle, containerID, mountPath string) error {
	timer := startPhaseTimer("shiminject.config.inject_view_path.total", logrus.Fields{
		"bundle":      bundle,
		"container":   containerID,
		"mount_path":  mountPath,
		"config_path": filepath.Join(bundle, configFilename),
	})
	var retErr error
	defer func() { timer.done(retErr) }()
	configPath := filepath.Join(bundle, configFilename)
	log.WithFields(logrus.Fields{
		"op":        "config.json.prepare",
		"bundle":    bundle,
		"config":    configPath,
		"view_path": mountPath,
	}).Debug("preparing to update config.json with snapshot view annotation")

	start := time.Now()
	data, err := os.ReadFile(configPath)
	if err != nil {
		retErr = err
		return fmt.Errorf("read %s: %w", configPath, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "config.json.read",
		"config":      configPath,
		"duration_ms": time.Since(start).Milliseconds(),
		"bytes":       len(data),
	}).Debug("read config.json")

	// Memory-lean path: patch only top-level "annotations" instead of
	// unmarshalling the full OCI spec struct.
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		retErr = err
		return fmt.Errorf("unmarshal config: %w", err)
	}

	annotations := make(map[string]string)
	if raw, ok := root["annotations"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &annotations); err != nil {
			retErr = err
			return fmt.Errorf("unmarshal config.annotations: %w", err)
		}
	}
	// Fast path: avoid rewrite/allocation churn when value is already set.
	if annotations[annotViewMntPath] == mountPath {
		log.WithFields(logrus.Fields{
			"op":        "config.json.skip_update",
			"bundle":    bundle,
			"container": containerID,
			"view_path": mountPath,
		}).Debug("snapshot view mount path already present in config.json")
		return nil
	}
	annotations[annotViewMntPath] = mountPath

	annBytes, err := json.Marshal(annotations)
	if err != nil {
		retErr = err
		return fmt.Errorf("marshal config.annotations: %w", err)
	}
	root["annotations"] = annBytes

	// Use compact marshal to reduce transient allocation pressure in shim hot path.
	out, err := json.Marshal(root)
	if err != nil {
		retErr = err
		return fmt.Errorf("marshal config with view mount annotation: %w", err)
	}
	start = time.Now()
	if err := os.WriteFile(configPath, out, 0644); err != nil {
		retErr = err
		return fmt.Errorf("write %s: %w", configPath, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "config.json.update",
		"bundle":      bundle,
		"container":   containerID,
		"duration_ms": time.Since(start).Milliseconds(),
		"view_path":   mountPath,
	}).Debug("injected snapshot view mount path into config.json")
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
	timer := startPhaseTimer("shiminject.create_snapshot_view.total", logrus.Fields{
		"bundle":    bundle,
		"container": containerID,
	})
	var retErr error
	defer func() { timer.done(retErr) }()
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		retErr = err
		return nil, fmt.Errorf("namespace required: %w", err)
	}

	// Experiment mode: skip before creating containerd client to isolate
	// client-related memory overhead in shim.
	if disableSharedSnapshotViewCreate {
		log.WithFields(logrus.Fields{
			"container": containerID,
			"ns":        ns,
		}).Info("shared snapshot view creation disabled by experiment toggle (no containerd client)")
		return nil, nil
	}

	client, err := newContainerdClient(ns)
	if err != nil {
		retErr = err
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
		retErr = err
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
		retErr = err
		return nil, fmt.Errorf("create shared view lease %s: %w", leaseID, err)
	}
	logPhaseDuration("shiminject.shared_view.lease.ensure", 0, logrus.Fields{
		"lease_id":    leaseID,
		"container":   containerID,
		"snapshotter": snapshotter,
	}, nil)
	ctx = metadata.AppendToOutgoingContext(withNamespace(ctx, ns), "containerd-lease", leaseID)

	// Ensure root directory exists before acquiring the lock file inside it.
	if err := os.MkdirAll(sharedViewsRoot, 0755); err != nil {
		retErr = err
		return nil, fmt.Errorf("create shared views root %s: %w", sharedViewsRoot, err)
	}

	createdData := false
	shouldCreate := false
	{
		_, unlock, err := acquireSharedViewLock(paths.lockPath)
		if err != nil {
			retErr = err
			return nil, err
		}

		createdData, err = ensureSharedViewDirs(paths)
		if err != nil {
			unlock()
			retErr = err
			return nil, err
		}

		mounted, mErr := mountinfo.Mounted(paths.dataDir)
		if mErr != nil {
			log.WithError(mErr).WithField("path", paths.dataDir).Warn("failed to check mountpoint status while deciding shared view create/reuse")
		}

		switch {
		case mounted:
			// Fast path: the shared view is already mounted; just register under lock.
		case sharedViewCreateInProgress(paths):
			unlock()
			if err := waitForSharedViewReady(paths); err != nil {
				retErr = err
				return nil, err
			}

			_, unlock, err = acquireSharedViewLock(paths.lockPath)
			if err != nil {
				retErr = err
				return nil, err
			}

			mounted, mErr = mountinfo.Mounted(paths.dataDir)
			if mErr != nil {
				log.WithError(mErr).WithField("path", paths.dataDir).Warn("failed to re-check mountpoint status after waiting for shared view creator")
			}
			if !mounted && !sharedViewCreateInProgress(paths) {
				if err := setSharedViewCreating(paths, containerID); err != nil {
					unlock()
					retErr = fmt.Errorf("take over shared view create in progress %s: %w", paths.createPath, err)
					return nil, retErr
				}
				shouldCreate = true
				unlock()

				if err := mountSharedView(ctx, client, ns, snapshotter, viewKey, snapshotKey, paths, createdData); err != nil {
					_, relock, lerr := acquireSharedViewLock(paths.lockPath)
					if lerr == nil {
						if cerr := clearSharedViewCreating(paths); cerr != nil {
							log.WithError(cerr).WithField("marker", paths.createPath).Warn("failed to clear shared view creating marker after takeover mount failure")
						}
						relock()
					}
					retErr = err
					return nil, err
				}

				_, unlock, err = acquireSharedViewLock(paths.lockPath)
				if err != nil {
					retErr = err
					return nil, err
				}
			}
		default:
			if err := setSharedViewCreating(paths, containerID); err != nil {
				unlock()
				retErr = fmt.Errorf("mark shared view create in progress %s: %w", paths.createPath, err)
				return nil, retErr
			}
			shouldCreate = true
			unlock()

			if err := mountSharedView(ctx, client, ns, snapshotter, viewKey, snapshotKey, paths, createdData); err != nil {
				_, relock, lerr := acquireSharedViewLock(paths.lockPath)
				if lerr == nil {
					if cerr := clearSharedViewCreating(paths); cerr != nil {
						log.WithError(cerr).WithField("marker", paths.createPath).Warn("failed to clear shared view creating marker after mount failure")
					}
					relock()
				}
				retErr = err
				return nil, err
			}

			_, unlock, err = acquireSharedViewLock(paths.lockPath)
			if err != nil {
				retErr = err
				return nil, err
			}
		}

		if err := registerContainerUser(containerID, paths, createdData); err != nil {
			if shouldCreate {
				if cerr := clearSharedViewCreating(paths); cerr != nil {
					log.WithError(cerr).WithField("marker", paths.createPath).Warn("failed to clear shared view creating marker after user registration failure")
				}
			}
			unlock()
			retErr = err
			return nil, err
		}

		if shouldCreate {
			if err := clearSharedViewCreating(paths); err != nil {
				unlock()
				retErr = fmt.Errorf("clear shared view creating marker %s: %w", paths.createPath, err)
				return nil, retErr
			}
		}

		unlock()
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
	}).Debug("shared snapshot view ready; container will bind-mount files from it")
	if err := injectViewPathToConfig(bundle, containerID, paths.dataDir); err != nil {
		retErr = err
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
	timer := startPhaseTimer("shiminject.cleanup_snapshot_view.total", logrus.Fields{})
	if info != nil {
		timer = startPhaseTimer("shiminject.cleanup_snapshot_view.total", logrus.Fields{
			"container":      info.ContainerID,
			"shared_view_id": info.SharedViewID,
			"view_key":       info.ViewKey,
		})
	}
	var retErr error
	defer func() { timer.done(retErr) }()
	if info == nil || info.SharedViewID == "" {
		return nil
	}

	paths := newSharedViewPaths(info.SharedViewID)

	// Acquire lock to serialize user-count check and cleanup.
	_, unlock, err := acquireSharedViewLock(paths.lockPath)
	if err != nil {
		retErr = err
		return err
	}
	defer unlock()

	removeContainerUserMarker(info.ContainerID, paths.usersDir)

	retErr = removeSharedViewIfUnused(ctx, info, paths)
	return retErr
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
	timer := startPhaseTimer("shiminject.shared_view.remove_if_unused", logrus.Fields{
		"container":      info.ContainerID,
		"shared_view_id": info.SharedViewID,
	})
	var retErr error
	defer func() { timer.done(retErr) }()
	entries, err := os.ReadDir(paths.usersDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		retErr = err
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

	if sharedViewCreateInProgress(paths) {
		log.WithFields(logrus.Fields{
			"shared_view_id": info.SharedViewID,
			"container":      info.ContainerID,
			"marker":         paths.createPath,
		}).Debug("shared view create still in progress, skipping teardown")
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
	logPhaseDuration("shiminject.shared_view.unmount", 0, logrus.Fields{
		"mount_path":     paths.dataDir,
		"shared_view_id": info.SharedViewID,
	}, firstErr)

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
	retErr = firstErr
	return retErr
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

	client, err := newContainerdClient(info.Namespace)
	if err != nil {
		log.WithError(err).WithFields(logrus.Fields{
			"view_key":    info.ViewKey,
			"snapshotter": info.Snapshotter,
		}).Warn("failed to connect to containerd for snapshot cleanup")
		return
	}
	defer client.Close()

	_, err = client.snapshots.Remove(withNamespace(ctx, info.Namespace), &snapshotsapi.RemoveSnapshotRequest{
		Snapshotter: info.Snapshotter,
		Key:         info.ViewKey,
	})
	err = grpcErr(err)
	if err != nil {
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
			_, err := client.leases.Delete(withNamespace(ctx, info.Namespace), &leasesapi.DeleteRequest{ID: leaseID})
			err = grpcErr(err)
			if err != nil && !errdefs.IsNotFound(err) {
				log.WithError(err).WithField("lease_id", leaseID).Warn("failed to remove shared view lease from containerd")
			} else if err == nil {
				log.WithField("lease_id", leaseID).Info("removed shared view lease from containerd")
			}
		}
	}
}
