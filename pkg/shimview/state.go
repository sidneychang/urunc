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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

const (
	snapshotViewStateFilename = "urunc-view.json"
	sharedViewMetaFilename    = "meta.json"
	sharedViewsData           = "data"
	sharedViewsLock           = ".lock"
	sharedViewLeaseID         = "urunc-shared-view-"
)

var ErrSnapshotViewStateNotFound = errors.New("snapshot view state not found")
var log = logrus.WithField("subsystem", "shimview")
var sharedViewsRoot = "/run/urunc/shared-views"

// SnapshotViewInfo describes the shared snapshot view for a container.
// It is persisted both in the bundle sidecar and in shared view metadata.
type SnapshotViewInfo struct {
	// SharedViewID identifies the shared mount (based on snapshotKey).
	SharedViewID string `json:"shared_view_id"`
	// ViewKey is the containerd snapshot name for the shared view.
	ViewKey string `json:"view_key"`
	// MountPath is the shared data directory all containers bind-mount files from.
	MountPath   string `json:"artifact_root"`
	Snapshotter string `json:"snapshotter"`
	Namespace   string `json:"namespace"`
	ContainerID string `json:"container_id"`
}

// sharedViewPaths holds filesystem paths for a shared view entry.
type sharedViewPaths struct {
	base     string
	dataDir  string
	lockPath string
}

// newSharedViewPaths computes paths for the given sharedViewID.
func newSharedViewPaths(sharedViewID string) sharedViewPaths {
	base := filepath.Join(sharedViewsRoot, sharedViewID)
	return sharedViewPaths{
		base:     base,
		dataDir:  filepath.Join(base, sharedViewsData),
		lockPath: base + sharedViewsLock,
	}
}

type sharedViewMeta struct {
	SnapshotViewInfo
	LeaseID string `json:"lease_id"`
}

func newSharedViewMeta(info *SnapshotViewInfo) *sharedViewMeta {
	if info == nil {
		return nil
	}
	return &sharedViewMeta{
		SnapshotViewInfo: *info,
		LeaseID:          sharedViewLeaseID + info.SharedViewID,
	}
}

func snapshotViewStatePath(bundle string) string {
	return filepath.Join(bundle, snapshotViewStateFilename)
}

func sharedViewMetaPath(paths sharedViewPaths) string {
	return filepath.Join(paths.base, sharedViewMetaFilename)
}

func SaveSnapshotViewState(bundle string, info *SnapshotViewInfo) error {
	if info == nil {
		return nil
	}

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal snapshot view state: %w", err)
	}

	statePath := snapshotViewStatePath(bundle)
	if err := os.WriteFile(statePath, data, 0600); err != nil {
		return fmt.Errorf("write snapshot view state %s: %w", statePath, err)
	}
	return nil
}

func LoadSnapshotViewState(bundle string) (*SnapshotViewInfo, error) {
	statePath := snapshotViewStatePath(bundle)
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrSnapshotViewStateNotFound
		}
		return nil, fmt.Errorf("read snapshot view state %s: %w", statePath, err)
	}

	var info SnapshotViewInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot view state %s: %w", statePath, err)
	}
	return &info, nil
}

func DeleteSnapshotViewState(bundle string) error {
	statePath := snapshotViewStatePath(bundle)
	if err := os.Remove(statePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove snapshot view state %s: %w", statePath, err)
	}
	return nil
}

func SaveSharedViewMeta(paths sharedViewPaths, info *SnapshotViewInfo) error {
	if info == nil {
		return nil
	}

	meta := newSharedViewMeta(info)

	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal shared view metadata: %w", err)
	}

	metaPath := sharedViewMetaPath(paths)
	if err := os.WriteFile(metaPath, data, 0600); err != nil {
		return fmt.Errorf("write shared view metadata %s: %w", metaPath, err)
	}
	return nil
}

func LoadSharedViewMeta(paths sharedViewPaths) (*sharedViewMeta, error) {
	metaPath := sharedViewMetaPath(paths)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("read shared view metadata %s: %w", metaPath, err)
	}

	var meta sharedViewMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal shared view metadata %s: %w", metaPath, err)
	}
	return &meta, nil
}
