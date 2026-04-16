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

import "fmt"

// getBlockDeviceFromMount extracts the block device path from a mount point
// by reusing the existing getMountInfo function.
// NOTE: This is no longer used for creating/cleaning snapshot views (those are
// handled entirely by the shim via containerd). It is kept as a small helper
// in case we need to discover the backing device for an already-mounted view.
func getBlockDeviceFromMount(mountPath string) (string, error) {
	blockDev, err := getMountInfo(mountPath)
	if err != nil {
		return "", fmt.Errorf("failed to get mount info for %s: %w", mountPath, err)
	}

	if blockDev.Source == "" {
		return "", fmt.Errorf("mount source is empty for %s", mountPath)
	}

	return blockDev.Source, nil
}
