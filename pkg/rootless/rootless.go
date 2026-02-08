// Copyright (c) 2023-2026, Nubificus LTD
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Parts of this file have been taken from
// https://github.com/opencontainers/runc/blob/main/rootless_linux.go
// which comes with an Apache 2.0 license. For more information check runc's
// licence.
//
// Package rootless provides helpers for rootless container execution.
package rootless

import (
	"os"

	"github.com/moby/sys/userns"
)

// ShouldHonorXDGRuntimeDir reports whether the runtime should use XDG_RUNTIME_DIR
// for the default root directory (e.g. /run/user/UID/runc instead of /run/urunc).
// It returns true for non-root processes and for root inside a user namespace
// when not running as the "root" user (e.g. rootless Podman).
func ShouldHonorXDGRuntimeDir() bool {
	if os.Geteuid() != 0 {
		return true
	}
	if !userns.RunningInUserNS() {
		// euid == 0 in the initial ns (real root): use /run/urunc for backward compatibility.
		return false
	}
	// euid == 0 inside a user namespace (rootless): honor XDG_RUNTIME_DIR unless USER=root.
	u, ok := os.LookupEnv("USER")
	return !ok || u != "root"
}
