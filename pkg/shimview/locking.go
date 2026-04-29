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
	"fmt"
	"golang.org/x/sys/unix"
	"os"
)

// acquireSharedViewLock opens (or creates) the per-shared-view lock file and
// acquires an exclusive flock. The returned unlock func must be deferred.
func acquireSharedViewLock(lockPath string) (func(), error) {
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open shared view lock %s: %w", lockPath, err)
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("flock %s: %w", lockPath, err)
	}
	unlock := func() {
		if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_UN); err != nil {
			log.WithError(err).Warn("failed to unlock shared view lock")
		}
		_ = lockFile.Close()
	}
	return unlock, nil
}

// ensureSharedViewDirs creates the shared-view data directory tree.
// It reports whether the data directory was freshly created.
// Caller must hold the shared view lock before calling.
func ensureSharedViewDirs(paths sharedViewPaths) (createdData bool, err error) {
	if _, err := os.Stat(paths.dataDir); err != nil {
		if !os.IsNotExist(err) {
			return false, fmt.Errorf("stat shared view data dir %s: %w", paths.dataDir, err)
		}
		if err := os.MkdirAll(paths.dataDir, 0755); err != nil {
			return false, fmt.Errorf("create shared view data dir %s: %w", paths.dataDir, err)
		}
		return true, nil
	}
	return false, nil
}
