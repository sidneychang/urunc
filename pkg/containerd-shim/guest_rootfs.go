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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	containerdTypes "github.com/containerd/containerd/api/types"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	shimcontainerd "github.com/urunc-dev/urunc/pkg/containerd-shim/containerd"
	"github.com/urunc-dev/urunc/pkg/unikontainers"
)

var errGuestRootfsChoiceSkipped = errors.New("guest rootfs choice skipped")

// chooseGuestRootfs selects guest rootfs parameters before inner task Create and
// persists them in the bundle OCI spec for runtime Exec to consume.
func chooseGuestRootfs(r *taskAPI.CreateTaskRequest) (shimcontainerd.GuestRootfsChoice, error) {
	spec, mode, err := loadSpec(r.Bundle)
	if err != nil {
		return shimcontainerd.GuestRootfsChoice{}, err
	}
	log := logrus.WithFields(logrus.Fields{
		"container_id": r.ID,
		"bundle":       filepath.Clean(r.Bundle),
	})

	config, err := unikontainers.GetUnikernelConfigFromSpecAnnotations(spec)
	if err != nil {
		return shimcontainerd.GuestRootfsChoice{}, errGuestRootfsChoiceSkipped
	}

	annotations := config.Map()
	uruncCfg, cfgErr := unikontainers.LoadUruncConfig(unikontainers.UruncConfigPath)
	if cfgErr != nil {
		log.WithError(cfgErr).Warn("urunc shim: failed to load urunc config; using defaults for guest rootfs choice")
	}

	rootfsParams, err := unikontainers.ChooseRootfs(filepath.Clean(r.Bundle), spec.Root.Path, annotations, uruncCfg, rootfsMountsFromCreateTask(r.Rootfs))
	if err != nil {
		return shimcontainerd.GuestRootfsChoice{}, err
	}

	encoded, err := unikontainers.EncodeRootfsParams(rootfsParams)
	if err != nil {
		return shimcontainerd.GuestRootfsChoice{}, err
	}
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}
	spec.Annotations[unikontainers.RootfsParamsAnnotation()] = encoded
	log.WithFields(logrus.Fields{
		"rootfs_type": rootfsParams.Type,
		"rootfs_path": rootfsParams.Path,
		"mon_rootfs":  rootfsParams.MonRootfs,
	}).Info("urunc shim: wrote guest rootfs choice to bundle")

	if err := saveSpec(r.Bundle, spec, mode); err != nil {
		return shimcontainerd.GuestRootfsChoice{}, err
	}
	return shimcontainerd.GuestRootfsChoice{
		Params: rootfsParams,
		Chosen: true,
	}, nil
}

func rootfsMountsFromCreateTask(rootfs []*containerdTypes.Mount) []unikontainers.RootfsMount {
	mounts := make([]unikontainers.RootfsMount, 0, len(rootfs))
	for _, m := range rootfs {
		if m == nil {
			continue
		}
		mounts = append(mounts, unikontainers.RootfsMount{
			Type:   m.Type,
			Source: m.Source,
		})
	}
	return mounts
}

// loadSpec reads the OCI runtime spec (config.json) from the task bundle at CreateTask time.
// Callers need the full spec on disk (root path, annotations read/write); the CreateTask RPC does
// not include the OCI document. injectMissingAnnotations runs before chooseGuestRootfs;
// snapshot view preparation runs after chooseGuestRootfs in taskService.Create.
func loadSpec(bundle string) (*specs.Spec, os.FileMode, error) {
	configPath := filepath.Join(bundle, "config.json")
	info, err := os.Stat(configPath)
	if err != nil {
		return nil, 0, fmt.Errorf("stat config.json: %w", err)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, 0, fmt.Errorf("read config.json: %w", err)
	}

	var spec specs.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, 0, fmt.Errorf("unmarshal config.json: %w", err)
	}
	if spec.Root == nil {
		return nil, 0, fmt.Errorf("invalid OCI spec: root section is required")
	}

	return &spec, info.Mode(), nil
}

func saveSpec(bundle string, spec *specs.Spec, mode os.FileMode) error {
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config.json: %w", err)
	}

	configPath := filepath.Join(bundle, "config.json")
	return os.WriteFile(configPath, data, mode)
}
