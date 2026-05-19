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

// Temporary shim-side copy of urunc #565 (inject missing image annotations into
// bundle config.json). Keep this file self-contained so it can be dropped or
// reconciled when #565 merges.
package containerdshim

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	shimcontainerd "github.com/urunc-dev/urunc/pkg/containerd-shim/containerd"
)

const uruncAnnotationPrefix = "com.urunc.unikernel."

func (s *taskService) injectMissingAnnotations(ctx context.Context, r *taskAPI.CreateTaskRequest, session *shimcontainerd.Session) error {
	configPath := filepath.Join(r.Bundle, "config.json")
	info, err := os.Stat(configPath)
	if err != nil {
		return fmt.Errorf("stat config.json: %w", err)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read config.json: %w", err)
	}

	var spec specs.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return fmt.Errorf("unmarshal config.json: %w", err)
	}
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}

	imageAnnots, err := session.ImageAnnotations(ctx, uruncAnnotationPrefix)
	if err != nil {
		return err
	}

	changed := false
	for key, value := range imageAnnots {
		if _, ok := spec.Annotations[key]; ok {
			continue
		}
		spec.Annotations[key] = base64.StdEncoding.EncodeToString([]byte(value))
		changed = true
	}
	if !changed {
		return nil
	}

	out, err := json.MarshalIndent(&spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config.json: %w", err)
	}
	if err := os.WriteFile(configPath, out, info.Mode()); err != nil {
		return fmt.Errorf("write config.json: %w", err)
	}

	return nil
}
