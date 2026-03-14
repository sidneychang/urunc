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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/namespaces"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const (
	// NOTE: Only mount path is injected into OCI annotations.
	// All other data (snapshot key, view ID, snapshotter, namespace, container)
	// are shim-internal implementation details and are kept in-memory for cleanup.
	annotViewMntPath  = "com.urunc.snapshot.view.mount_path"
	configFilename    = "config.json"
	defaultContainerd = "/run/containerd/containerd.sock"
	sharedViewsRoot   = "/run/urunc/shared-views"
	sharedViewsData   = "data"
	sharedViewsUsers  = "users"
	sharedViewsLock   = ".lock"
)

var log = logrus.WithField("subsystem", "shiminject")

func isMountPoint(path string) (bool, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		if fields[4] == path {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// SnapshotViewInfo describes the snapshot view created for a container.
// It is kept in-memory by the shim wrapper and is used for cleanup on Delete.
type SnapshotViewInfo struct {
	ViewID      string
	ViewKey     string
	MountPath   string
	Snapshotter string
	Namespace   string
	ContainerID string
	LeaseID     string
}

// CreateSnapshotView creates or reuses a shared read-only view for the
// container's rootfs using containerd's snapshot API, mounts it under
// /run/urunc/shared-views/<viewID>/data, and injects ONLY the view mount path
// into the bundle's config.json
// annotations so that urunc can consume it without talking to containerd.
//
// If the container has no SnapshotKey/Snapshotter, the function is a no-op and
// returns (nil, nil).
func CreateSnapshotView(ctx context.Context, bundle, containerID string) (*SnapshotViewInfo, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, fmt.Errorf("namespace required: %w", err)
	}

	address := os.Getenv("CONTAINERD_ADDRESS")
	if address == "" {
		address = defaultContainerd
	}

	start := time.Now()
	client, err := containerd.New(address, containerd.WithDefaultNamespace(ns))
	if err != nil {
		return nil, fmt.Errorf("containerd client: %w", err)
	}
	log.WithFields(logrus.Fields{
		"op":          "containerd.New",
		"address":     address,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("containerd call completed")
	defer client.Close()

	// Fetch snapshot key/snapshotter directly from containerd.
	store := client.ContainerService()
	start = time.Now()
	container, err := store.Get(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("get container %s: %w", containerID, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "ContainerService.Get",
		"container":   containerID,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("containerd call completed")
	snapshotKey := container.SnapshotKey
	snapshotter := container.Snapshotter
	if snapshotKey == "" || snapshotter == "" {
		log.WithFields(logrus.Fields{
			"container":    containerID,
			"snapshot_key": snapshotKey,
			"snapshotter":  snapshotter,
		}).Debug("CreateSnapshotView: container has no SnapshotKey/Snapshotter, skipping view creation")
		return nil, nil
	}

	ss := client.SnapshotService(snapshotter)
	if ss == nil {
		return nil, fmt.Errorf("snapshotter %s not found", snapshotter)
	}

	originalKey := snapshotKey
	// For devmapper, prefer committed parent snapshot when available.
	if snapshotter == "devmapper" {
		start = time.Now()
		info, err := ss.Stat(ctx, snapshotKey)
		if err == nil {
			log.WithFields(logrus.Fields{
				"op":          "SnapshotService.Stat",
				"snapshotter": snapshotter,
				"snapshot":    snapshotKey,
				"duration_ms": time.Since(start).Milliseconds(),
			}).Info("containerd call completed")
		}
		if err == nil && info.Parent != "" {
			snapshotKey = info.Parent
			log.WithFields(logrus.Fields{
				"container":     containerID,
				"original_key":  originalKey,
				"committed_key": snapshotKey,
			}).Debug("Using committed parent snapshot for view (devmapper requirement)")
		}
	}

	viewID := fmt.Sprintf("%s_%s_%s", snapshotter, ns, snapshotKey)
	viewBase := filepath.Join(sharedViewsRoot, viewID)
	viewDataDir := filepath.Join(viewBase, sharedViewsData)
	viewUsersDir := filepath.Join(viewBase, sharedViewsUsers)
	lockPath := viewBase + sharedViewsLock
	viewKey := "urunc-shared-" + viewID

	if err := os.MkdirAll(sharedViewsRoot, 0755); err != nil {
		return nil, fmt.Errorf("create shared views root %s: %w", sharedViewsRoot, err)
	}

	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open view lock %s: %w", lockPath, err)
	}
	defer lockFile.Close()

	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		return nil, fmt.Errorf("flock %s: %w", lockPath, err)
	}
	defer func() {
		if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_UN); err != nil {
			log.WithError(err).WithField("lock", lockPath).Warn("failed to unlock snapshot view lock")
		}
	}()

	if err := os.MkdirAll(viewUsersDir, 0755); err != nil {
		return nil, fmt.Errorf("create view users dir %s: %w", viewUsersDir, err)
	}

	createdView := false
	if _, err := os.Stat(viewDataDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat view data dir %s: %w", viewDataDir, err)
		}
		if err := os.MkdirAll(viewDataDir, 0755); err != nil {
			return nil, fmt.Errorf("create view data dir %s: %w", viewDataDir, err)
		}
		createdView = true
	}

	mounted, mntErr := isMountPoint(viewDataDir)
	if mntErr != nil {
		log.WithError(mntErr).WithField("path", viewDataDir).Warn("failed to check mountpoint status")
	}
	if !mounted {
		// Ensure a shared containerd view snapshot exists (create or reuse).
		var mounts []mount.Mount
		start = time.Now()
		mounts, err = ss.View(ctx, viewKey, snapshotKey)
		if err != nil {
			if !errdefs.IsAlreadyExists(err) {
				return nil, fmt.Errorf("failed to create shared view snapshot %s from %s: %w", viewKey, snapshotKey, err)
			}
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
		} else {
			log.WithFields(logrus.Fields{
				"op":          "SnapshotService.View",
				"snapshotter": snapshotter,
				"view_key":    viewKey,
				"snapshot":    snapshotKey,
				"duration_ms": time.Since(start).Milliseconds(),
			}).Info("containerd call completed")
		}

		start = time.Now()
		if err := mount.All(mounts, viewDataDir); err != nil {
			fields := logrus.Fields{
				"mount_path": viewDataDir,
			}
			if _, serr := os.Stat(viewDataDir); serr != nil {
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
			log.WithFields(fields).WithError(err).Warn("mount snapshot view failed")
			if createdView {
				_ = os.RemoveAll(viewDataDir)
			}
			return nil, fmt.Errorf("mount snapshot at %s: %w", viewDataDir, err)
		}
		log.WithFields(logrus.Fields{
			"op":          "mount.All",
			"mount_path":  viewDataDir,
			"view_key":    viewKey,
			"duration_ms": time.Since(start).Milliseconds(),
		}).Info("snapshot view mounted")
	} else {
		log.WithFields(logrus.Fields{
			"mount_path": viewDataDir,
			"view_key":   viewKey,
		}).Info("snapshot view already mounted")
	}

	log.WithFields(logrus.Fields{
		"container":    containerID,
		"view_id":      viewID,
		"view_key":     viewKey,
		"mount_path":   viewDataDir,
		"snapshot_key": snapshotKey,
		"snapshotter":  snapshotter,
		"namespace":    ns,
		"created_view": createdView,
	}).Info("shared snapshot view ready for container rootfs")

	// Protect shared view snapshot from GC with a per-container lease.
	var leaseID string
	if ls := client.LeasesService(); ls != nil {
		lease, lerr := ls.Create(ctx, leases.WithRandomID(), leases.WithLabels(map[string]string{
			"io.containerd.gc.root":         "true",
			"com.urunc.snapshot.view":       viewKey,
			"com.urunc.snapshot.view.ns":    ns,
			"com.urunc.snapshot.view.view":  viewID,
			"com.urunc.snapshot.view.snap":  snapshotKey,
			"com.urunc.snapshot.view.snapper": snapshotter,
		}))
		if lerr != nil {
			log.WithError(lerr).WithFields(logrus.Fields{
				"container": containerID,
				"view_key":  viewKey,
			}).Warn("failed to create lease for shared snapshot view; GC may clean it up early")
		} else {
			if rerr := ls.AddResource(ctx, lease, leases.Resource{
				ID:   viewKey,
				Type: "snapshots/" + snapshotter,
			}); rerr != nil {
				log.WithError(rerr).WithFields(logrus.Fields{
					"container": containerID,
					"view_key":  viewKey,
				}).Warn("failed to attach shared view snapshot to lease; GC may clean it up early")
			} else {
				leaseID = lease.ID
				log.WithFields(logrus.Fields{
					"container": containerID,
					"view_key":  viewKey,
					"lease_id":  leaseID,
				}).Debug("created lease and attached shared view snapshot")
			}
		}
	}

	userMarker := filepath.Join(viewUsersDir, containerID)
	start = time.Now()
	if err := os.WriteFile(userMarker, []byte(time.Now().Format(time.RFC3339Nano)), 0644); err != nil {
		if createdView {
			if uerr := mount.Unmount(viewDataDir, 0); uerr != nil {
				log.WithError(uerr).WithField("mount_path", viewDataDir).Warn("failed to unmount snapshot view after marker write failure")
			}
			_ = os.RemoveAll(viewBase)
		}
		return nil, fmt.Errorf("create view user marker %s: %w", userMarker, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "marker.write",
		"marker":      userMarker,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("snapshot view marker updated")

	// Inject ONLY the view mount path into config.json annotations so urunc can
	// bind-mount unikernel/initrd/urunc.json from there without any file copies.
	configPath := filepath.Join(bundle, configFilename)
	log.WithFields(logrus.Fields{
		"op":        "config.json.prepare",
		"bundle":    bundle,
		"config":    configPath,
		"view_path": viewDataDir,
	}).Info("preparing to update config.json with snapshot view annotation")
	start = time.Now()
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", configPath, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "config.json.read",
		"config":      configPath,
		"duration_ms": time.Since(start).Milliseconds(),
		"bytes":       len(data),
	}).Info("read config.json")
	var spec specs.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}
	spec.Annotations[annotViewMntPath] = viewDataDir
	out, err := json.MarshalIndent(&spec, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal config with view mount annotation: %w", err)
	}
	start = time.Now()
	if err := os.WriteFile(configPath, out, 0644); err != nil {
		return nil, fmt.Errorf("write %s: %w", configPath, err)
	}
	log.WithFields(logrus.Fields{
		"op":          "config.json.update",
		"bundle":      bundle,
		"container":   containerID,
		"duration_ms": time.Since(start).Milliseconds(),
		"view_path":   viewDataDir,
	}).Info("injected snapshot view mount path into config.json")

	return &SnapshotViewInfo{
		ViewID:      viewID,
		ViewKey:     viewKey,
		MountPath:   viewDataDir,
		Snapshotter: snapshotter,
		Namespace:   ns,
		ContainerID: containerID,
		LeaseID:     leaseID,
	}, nil
}

// CleanupSnapshotView unmounts and removes a shared view once no containers
// reference it. It is expected to be called from the shim's Delete path.
func CleanupSnapshotView(ctx context.Context, info *SnapshotViewInfo) error {
	if info == nil {
		return nil
	}

	if info.ViewID == "" {
		return nil
	}

	viewBase := filepath.Join(sharedViewsRoot, info.ViewID)
	viewDataDir := filepath.Join(viewBase, sharedViewsData)
	viewUsersDir := filepath.Join(viewBase, sharedViewsUsers)
	lockPath := viewBase + sharedViewsLock

	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open view lock %s: %w", lockPath, err)
	}
	defer lockFile.Close()

	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("flock %s: %w", lockPath, err)
	}
	defer func() {
		if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_UN); err != nil {
			log.WithError(err).WithField("lock", lockPath).Warn("failed to unlock snapshot view lock")
		}
	}()

	if info.ContainerID != "" {
		userMarker := filepath.Join(viewUsersDir, info.ContainerID)
		if err := os.Remove(userMarker); err != nil && !os.IsNotExist(err) {
			log.WithError(err).WithField("marker", userMarker).Warn("failed to remove snapshot view user marker")
		}
	}

	// Best-effort: delete the lease protecting the shared view snapshot.
	if info.LeaseID != "" {
		address := os.Getenv("CONTAINERD_ADDRESS")
		if address == "" {
			address = defaultContainerd
		}
		client, err := containerd.New(address, containerd.WithDefaultNamespace(info.Namespace))
		if err != nil {
			log.WithError(err).WithField("lease_id", info.LeaseID).Warn("failed to connect to containerd for lease cleanup")
		} else {
			defer client.Close()
			if ls := client.LeasesService(); ls != nil {
				if err := ls.Delete(ctx, leases.Lease{ID: info.LeaseID}); err != nil {
					log.WithError(err).WithField("lease_id", info.LeaseID).Warn("failed to delete snapshot view lease")
				} else {
					log.WithField("lease_id", info.LeaseID).Info("deleted snapshot view lease")
				}
			}
		}
	}

	entries, err := os.ReadDir(viewUsersDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read view users dir %s: %w", viewUsersDir, err)
	}
	if len(entries) > 0 {
		log.WithFields(logrus.Fields{
			"view_id":  info.ViewID,
			"users":    len(entries),
			"mount":    viewDataDir,
			"container": info.ContainerID,
		}).Debug("snapshot view still in use, skipping cleanup")
		return nil
	}

	var firstErr error
	if err := mount.Unmount(viewDataDir, 0); err != nil && !os.IsNotExist(err) {
		log.WithError(err).WithField("mount_path", viewDataDir).Warn("failed to unmount snapshot view")
		firstErr = err
	} else {
		log.WithField("mount_path", viewDataDir).Info("unmounted snapshot view")
	}

	if err := os.RemoveAll(viewBase); err != nil {
		log.WithError(err).WithField("view_base", viewBase).Warn("failed to remove snapshot view directory")
		if firstErr == nil {
			firstErr = err
		}
	}
	if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
		log.WithError(err).WithField("lock", lockPath).Warn("failed to remove snapshot view lock file")
		if firstErr == nil {
			firstErr = err
		}
	}

	log.WithFields(logrus.Fields{
		"view_id":  info.ViewID,
		"mount":    viewDataDir,
		"container": info.ContainerID,
	}).Info("cleaned up shared snapshot view")

	return firstErr
}
