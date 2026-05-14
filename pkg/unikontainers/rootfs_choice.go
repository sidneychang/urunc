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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/urunc-dev/urunc/pkg/unikontainers/hypervisors"
	"github.com/urunc-dev/urunc/pkg/unikontainers/types"
	"github.com/urunc-dev/urunc/pkg/unikontainers/unikernels"
)

const (
	rootfsChoiceAnnotationVersion = "1"

	annotRootfsChoiceVersion = "com.urunc.shim.rootfsChoice.version"
	annotRootfsChoiceParams  = "com.urunc.shim.rootfsChoice.params"
)

// RootfsChoiceInput contains the state required to choose the guest rootfs.
type RootfsChoiceInput struct {
	Bundle          string
	ContainerRootfs string
	Annotations     map[string]string
	UruncConfig     *UruncConfig
}

// ResolveRootfsPath resolves an OCI rootfs path against the bundle path using
// the same rules as the runtime path. It is exported so the shim can prepare the
// same rootfs selection input without duplicating path handling.
func ResolveRootfsPath(bundleDir string, rootfsPath string) (string, error) {
	return resolveAgainstBase(filepath.Clean(bundleDir), filepath.Clean(rootfsPath))
}

// ShouldMountContainerRootfs reports whether the mountRootfs annotation is enabled.
func ShouldMountContainerRootfs(annotations map[string]string) bool {
	selector := &rootfsSelector{annot: annotations}
	return selector.shouldMountContainerRootfs()
}

// ProbeContainerRootfsMountInfo checks whether mount information is available for rootfsPath.
func ProbeContainerRootfsMountInfo(rootfsPath string) error {
	_, err := getMountInfo(rootfsPath)
	return err
}

// RootfsParamsToAnnotations serializes a chosen rootfs into shim handoff annotations.
func RootfsParamsToAnnotations(params types.RootfsParams) (map[string]string, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal rootfs choice params: %w", err)
	}

	return map[string]string{
		annotRootfsChoiceVersion: rootfsChoiceAnnotationVersion,
		annotRootfsChoiceParams:  base64.StdEncoding.EncodeToString(data),
	}, nil
}

// RootfsParamsFromAnnotations deserializes shim handoff annotations if they exist.
func RootfsParamsFromAnnotations(annotations map[string]string) (types.RootfsParams, bool, error) {
	if annotations == nil {
		return types.RootfsParams{}, false, nil
	}

	version, hasVersion := annotations[annotRootfsChoiceVersion]
	encodedParams, hasParams := annotations[annotRootfsChoiceParams]
	if !hasVersion && !hasParams {
		return types.RootfsParams{}, false, nil
	}
	if !hasVersion || !hasParams {
		return types.RootfsParams{}, false, fmt.Errorf("incomplete shim rootfs choice annotations")
	}
	if version != rootfsChoiceAnnotationVersion {
		return types.RootfsParams{}, false, fmt.Errorf("unsupported shim rootfs choice annotation version %q", version)
	}

	data, err := base64.StdEncoding.DecodeString(encodedParams)
	if err != nil {
		return types.RootfsParams{}, false, fmt.Errorf("decode shim rootfs choice params: %w", err)
	}

	var params types.RootfsParams
	if err := json.Unmarshal(data, &params); err != nil {
		return types.RootfsParams{}, false, fmt.Errorf("unmarshal shim rootfs choice params: %w", err)
	}

	return params, true, nil
}

func (rs *rootfsSelector) tryShimRootfsChoice() (types.RootfsParams, bool) {
	params, ok, err := RootfsParamsFromAnnotations(rs.annot)
	if err != nil {
		uniklog.WithError(err).Warn("invalid shim-selected guest rootfs; falling back to runtime rootfs selection")
		return types.RootfsParams{}, false
	}
	if !ok {
		return types.RootfsParams{}, false
	}

	if err := ensureMonitorRootfsDir(params); err != nil {
		uniklog.WithError(err).Warn("failed to prepare shim-selected monitor rootfs; falling back to runtime rootfs selection")
		return types.RootfsParams{}, false
	}

	uniklog.WithField("rootfs type", params.Type).Debug("using shim-selected guest rootfs")
	return params, true
}

func monitorRootfsPath(bundle string) string {
	return filepath.Join(bundle, monitorRootfsDirName)
}

func ensureMonitorRootfsDir(params types.RootfsParams) error {
	if params.MonRootfs == "" || filepath.Base(params.MonRootfs) != monitorRootfsDirName {
		return nil
	}

	return os.MkdirAll(params.MonRootfs, 0o755)
}

// ChooseRootfs determines the best rootfs configuration based on available options.
// It is used by the shim to pre-compute the same rootfs decision that the runtime
// would otherwise make later. Runtime-specific filesystem preparation is still
// performed later by the rootfsBuilder flow.
func ChooseRootfs(input RootfsChoiceInput) (types.RootfsParams, error) {
	bundleDir := filepath.Clean(input.Bundle)
	rootfsDir := filepath.Clean(input.ContainerRootfs)
	if bundleDir == "" || bundleDir == "." {
		return types.RootfsParams{}, fmt.Errorf("bundle path is empty")
	}
	if rootfsDir == "" || rootfsDir == "." {
		return types.RootfsParams{}, fmt.Errorf("container rootfs path is empty")
	}

	annotations := input.Annotations
	if annotations == nil {
		annotations = make(map[string]string)
	}

	cfg := input.UruncConfig
	if cfg == nil {
		cfg = defaultUruncConfig()
	}

	unikernelType := annotations[annotType]
	unikernel, err := unikernels.New(unikernelType)
	if err != nil {
		return types.RootfsParams{}, err
	}

	vmmType := annotations[annotHypervisor]
	vmm, err := hypervisors.NewVMM(hypervisors.VmmType(vmmType), cfg.Monitors)
	if err != nil {
		return types.RootfsParams{}, err
	}

	virtiofsdConfig := cfg.ExtraBins["virtiofsd"]

	selector := &rootfsSelector{
		bundle:     bundleDir,
		cntrRootfs: rootfsDir,
		annot:      annotations,
		unikernel:  unikernel,
		vmm:        vmm,
		vfsdPath:   virtiofsdConfig.Path,
	}

	// Priority 1: Initrd
	result, ok := selector.tryInitrd()
	if ok {
		return result, nil
	}

	// Priority 2: Explicit block annotation
	result, ok = selector.tryExplicitBlock()
	if ok {
		return result, nil
	}

	// Priority 3 & 4: Container rootfs (block or shared-fs)
	result, ok = selector.tryContainerRootfs()
	if ok {
		result.MonRootfs = monitorRootfsPath(bundleDir)
		return result, nil
	}

	if selector.shouldMountContainerRootfs() {
		return types.RootfsParams{}, fmt.Errorf("can not use the container rootfs as the sandbox's guest rootfs through block or shared-fs")
	}

	uniklog.Info("no rootfs configured for guest")
	result.MonRootfs = rootfsDir

	return result, nil
}
