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

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/namespaces"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
)

const (
	// NOTE: Only mount path is injected into OCI annotations.
	// All other data (snapshot key, view key, snapshotter, namespace) are
	// shim-internal implementation details and are kept in-memory for cleanup.
	annotViewMntPath  = "com.urunc.snapshot.view.mount_path"
	configFilename    = "config.json"
	defaultContainerd = "/run/containerd/containerd.sock"
)

var log = logrus.WithField("subsystem", "shiminject")

// SnapshotViewInfo describes the snapshot view created for a container.
// It is kept in-memory by the shim wrapper and is used for cleanup on Delete.
type SnapshotViewInfo struct {
	ViewKey     string
	MountPath   string
	Snapshotter string
	Namespace   string
}

// CreateSnapshotView creates a read-only snapshot view for the container's
// rootfs using containerd's snapshot API, mounts it under /run/urunc/views,
// and injects ONLY the view mount path into the bundle's config.json
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

	client, err := containerd.New(address, containerd.WithDefaultNamespace(ns))
	if err != nil {
		return nil, fmt.Errorf("containerd client: %w", err)
	}
	defer client.Close()

	// Fetch snapshot key/snapshotter directly from containerd.
	store := client.ContainerService()
	container, err := store.Get(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("get container %s: %w", containerID, err)
	}
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
		if info, err := ss.Stat(ctx, snapshotKey); err == nil && info.Parent != "" {
			snapshotKey = info.Parent
			log.WithFields(logrus.Fields{
				"container":     containerID,
				"original_key":  originalKey,
				"committed_key": snapshotKey,
			}).Debug("Using committed parent snapshot for view (devmapper requirement)")
		}
	}

	viewKey := fmt.Sprintf("%s-urunc-view", containerID)
	mounts, err := ss.View(ctx, viewKey, snapshotKey)
	if err != nil {
		// Try original key as a fallback in case parent-based view fails
		if snapshotKey != originalKey {
			log.WithError(err).WithFields(logrus.Fields{
				"container":    containerID,
				"parent_key":   snapshotKey,
				"original_key": originalKey,
			}).Debug("Failed to create view from parent, trying original snapshot key")
			snapshotKey = originalKey
			mounts, err = ss.View(ctx, viewKey, snapshotKey)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to create snapshot view %s from %s: %w", viewKey, snapshotKey, err)
		}
	}

	viewMountPath := filepath.Join("/run/urunc/views", containerID)
	if err := os.MkdirAll(viewMountPath, 0755); err != nil {
		_ = ss.Remove(ctx, viewKey)
		return nil, fmt.Errorf("failed to create view mount directory %s: %w", viewMountPath, err)
	}

	if err := mount.All(mounts, viewMountPath); err != nil {
		_ = os.RemoveAll(viewMountPath)
		_ = ss.Remove(ctx, viewKey)
		return nil, fmt.Errorf("failed to mount snapshot view at %s: %w", viewMountPath, err)
	}

	log.WithFields(logrus.Fields{
		"container":    containerID,
		"view_key":     viewKey,
		"mount_path":   viewMountPath,
		"snapshot_key": snapshotKey,
		"snapshotter":  snapshotter,
		"namespace":    ns,
	}).Info("created and mounted snapshot view for container rootfs")

	// Inject ONLY the view mount path into config.json annotations so urunc can
	// bind-mount unikernel/initrd/urunc.json from there without any file copies.
	configPath := filepath.Join(bundle, configFilename)
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", configPath, err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}
	spec.Annotations[annotViewMntPath] = viewMountPath
	out, err := json.MarshalIndent(&spec, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal config with view mount annotation: %w", err)
	}
	if err := os.WriteFile(configPath, out, 0644); err != nil {
		return nil, fmt.Errorf("write %s: %w", configPath, err)
	}

	return &SnapshotViewInfo{
		ViewKey:     viewKey,
		MountPath:   viewMountPath,
		Snapshotter: snapshotter,
		Namespace:   ns,
	}, nil
}

// CleanupSnapshotView unmounts the view mount and removes the snapshot view
// from containerd. It is expected to be called from the shim's Delete path.
func CleanupSnapshotView(ctx context.Context, info *SnapshotViewInfo) error {
	if info == nil {
		return nil
	}

	var firstErr error

	// Unmount and remove mount directory
	if info.MountPath != "" {
		if err := mount.Unmount(info.MountPath, 0); err != nil {
			log.WithError(err).WithField("mount_path", info.MountPath).Warn("failed to unmount snapshot view")
			if firstErr == nil {
				firstErr = err
			}
		} else {
			log.WithField("mount_path", info.MountPath).Info("unmounted snapshot view")
		}
		if err := os.RemoveAll(info.MountPath); err != nil {
			log.WithError(err).WithField("mount_path", info.MountPath).Warn("failed to remove snapshot view mount directory")
		}
	}

	if info.ViewKey != "" && info.Snapshotter != "" {
		address := os.Getenv("CONTAINERD_ADDRESS")
		if address == "" {
			address = defaultContainerd
		}
		client, err := containerd.New(address, containerd.WithDefaultNamespace(info.Namespace))
		if err != nil {
			log.WithError(err).Warn("failed to connect to containerd for snapshot view cleanup")
			return firstErr
		}
		defer client.Close()

		ss := client.SnapshotService(info.Snapshotter)
		if ss != nil {
			if err := ss.Remove(ctx, info.ViewKey); err != nil {
				log.WithError(err).WithField("view_key", info.ViewKey).Warn("failed to remove snapshot view from containerd")
				if firstErr == nil {
					firstErr = err
				}
			} else {
				log.WithField("view_key", info.ViewKey).Info("removed snapshot view from containerd")
			}
		}
	}

	return firstErr
}
