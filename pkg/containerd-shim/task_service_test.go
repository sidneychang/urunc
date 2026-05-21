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

package containerdshim

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	containerdShim "github.com/urunc-dev/urunc/pkg/containerd-shim/containerd"
	"github.com/urunc-dev/urunc/pkg/unikontainers"
)

func TestCleanupSnapshotViewFromBundle_NotPrepared(t *testing.T) {
	bundle := t.TempDir()
	if err := CleanupSnapshotViewFromBundle(context.Background(), "/nonexistent", "id", bundle); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCleanupSnapshotViewFromBundle_LoadsState(t *testing.T) {
	bundle := t.TempDir()
	view := map[string]any{
		"view_key":    "urunc-view-ctr-1",
		"lease_id":    "urunc-snapshot-view-ctr-1",
		"snapshotter": "devmapper",
		"namespace":   "default",
	}
	raw, err := json.Marshal(view)
	assert.NoError(t, err)

	spec := specs.Spec{
		Version: "1.0.0",
		Root:    &specs.Root{Path: "rootfs"},
		Linux:   &specs.Linux{},
		Annotations: map[string]string{
			unikontainers.AnnotSnapshotView: string(raw),
		},
	}
	specRaw, err := json.MarshalIndent(spec, "", "  ")
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(bundle, "config.json"), specRaw, 0o644))

	if _, err := (&containerdShim.SnapshotViewAccessor{}).LoadCleanupState(bundle); err != nil {
		t.Fatalf("LoadCleanupState: %v", err)
	}

	assert.Error(t, CleanupSnapshotViewFromBundle(context.Background(), "", "id", bundle))
}
