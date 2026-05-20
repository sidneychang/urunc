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

package unikontainers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/mount"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
)

func writeTestBundleSpec(t *testing.T, bundleDir string, annotations map[string]string) {
	t.Helper()
	spec := specs.Spec{
		Version: "1.0.0",
		Root:    &specs.Root{Path: "rootfs"},
		Linux:   &specs.Linux{},
		Annotations: annotations,
	}
	raw, err := json.MarshalIndent(spec, "", "  ")
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(bundleDir, configFilename), raw, 0o644))
}

func TestLoadSnapshotViewFromBundle_Missing(t *testing.T) {
	bundleDir := t.TempDir()
	writeTestBundleSpec(t, bundleDir, nil)

	state, err := loadSnapshotViewFromBundle(bundleDir)
	assert.NoError(t, err)
	assert.Nil(t, state)
}

func TestLoadSnapshotViewFromBundle_Present(t *testing.T) {
	bundleDir := t.TempDir()
	raw := snapshotViewState{
		ViewKey:     "urunc-view-test",
		Snapshotter: "devmapper",
		Mounts:      []mount.Mount{{Type: "bind", Source: "/src", Target: "/dst"}},
	}
	data, err := json.Marshal(raw)
	assert.NoError(t, err)
	writeTestBundleSpec(t, bundleDir, map[string]string{AnnotSnapshotView: string(data)})

	state, err := loadSnapshotViewFromBundle(bundleDir)
	assert.NoError(t, err)
	assert.Equal(t, raw.ViewKey, state.ViewKey)
}

func TestLoadSnapshotViewFromBundle_EmptyMounts(t *testing.T) {
	bundleDir := t.TempDir()
	raw := snapshotViewState{ViewKey: "v", Snapshotter: "devmapper", Mounts: nil}
	data, err := json.Marshal(raw)
	assert.NoError(t, err)
	writeTestBundleSpec(t, bundleDir, map[string]string{AnnotSnapshotView: string(data)})

	_, err = loadSnapshotViewFromBundle(bundleDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no mounts")
}

func TestPatchAndReadBundleSnapshotView(t *testing.T) {
	bundleDir := t.TempDir()
	writeTestBundleSpec(t, bundleDir, map[string]string{"com.urunc.unikernel.unikernelType": "rumprun"})

	viewJSON := `{"view_key":"urunc-view-test","lease_id":"lease","snapshotter":"devmapper","mounts":[{"type":"bind","source":"/a","target":"/b"}]}`
	assert.NoError(t, PatchBundleSnapshotView(bundleDir, viewJSON))

	got, err := ReadBundleSnapshotView(bundleDir)
	assert.NoError(t, err)
	assert.Equal(t, viewJSON, got)
}

