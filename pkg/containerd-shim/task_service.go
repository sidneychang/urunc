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
	"errors"
	"fmt"
	"path/filepath"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/log"
	"github.com/containerd/ttrpc"
	containerdShim "github.com/urunc-dev/urunc/pkg/containerd-shim/containerd"
)

// taskService is urunc's shim-side wrapper around containerd's runc task
// service. It wires annotation injection, guest rootfs precompute, and optional
// per-container snapshot views at task Create/Delete boundaries.
type taskService struct {
	taskAPI.TaskService

	containerdAddress string
	// stateRoot is the containerd runtime v2 state directory (parent of
	// per-namespace dirs). Derived at plugin init from the shim cwd; used to
	// resolve bundle paths on Delete when cwd may no longer be the bundle.
	stateRoot string
}

func (s *taskService) Create(ctx context.Context, r *taskAPI.CreateTaskRequest) (*taskAPI.CreateTaskResponse, error) {
	session, err := containerdShim.OpenSession(ctx, s.containerdAddress, r.ID)
	if err != nil {
		log.G(ctx).WithError(err).Warn("urunc(shim): failed to open containerd session")
	} else {
		defer func() {
			if err := session.Close(); err != nil {
				log.G(ctx).WithError(err).Warn("urunc(shim): failed to close containerd session")
			}
		}()
		if err := containerdShim.InjectUruncAnnotations(ctx, session, r.Bundle); err != nil {
			log.G(ctx).WithError(err).Warn("urunc(shim): failed to inject annotations to spec")
		}
	}

	resp, err := s.TaskService.Create(ctx, r)
	if err != nil {
		return resp, err
	}

	// ChooseRootfs after inner task Create so bundle rootfs is mounted;
	// params are persisted in bundle config.json for runtime Exec.
	rootfsChoice, err := chooseGuestRootfs(r)
	if err != nil {
		if errors.Is(err, errGuestRootfsChoiceSkipped) {
			log.G(ctx).WithError(err).Debug("urunc(shim): guest rootfs choice skipped")
			return resp, nil
		}
		log.G(ctx).WithError(err).Warn("urunc(shim): failed to choose guest rootfs")
		return nil, err
	}

	if session != nil {
		snapshotViewAccessor := containerdShim.NewSnapshotViewAccessor(session)
		if snapshotViewAccessor.ShouldPrepare(rootfsChoice) {
			if err := snapshotViewAccessor.Prepare(ctx, r.Bundle); err != nil {
				log.G(ctx).WithError(err).Warn("urunc(shim): failed to prepare snapshot view; falling back to legacy boot artifact extraction")
			}
		}
	}

	return resp, nil
}

func (s *taskService) Delete(ctx context.Context, r *taskAPI.DeleteRequest) (*taskAPI.DeleteResponse, error) {
	cleanupSnapshotView := s.snapshotViewCleanupAfterDelete(ctx, r)

	var session *containerdShim.Session
	if cleanupSnapshotView != nil {
		var err error
		session, err = containerdShim.OpenSession(ctx, s.containerdAddress, r.ID)
		if err != nil {
			log.G(ctx).WithError(err).Warn("urunc(shim): open containerd session for snapshot view cleanup failed")
			cleanupSnapshotView = nil
		} else {
			defer func() {
				if err := session.Close(); err != nil {
					log.G(ctx).WithError(err).Warn("urunc(shim): failed to close containerd session")
				}
			}()
		}
	}

	resp, err := s.TaskService.Delete(ctx, r)

	var cleanupErr error
	if cleanupSnapshotView != nil && session != nil {
		cleanupErr = cleanupSnapshotView(ctx, session)
	}

	if err != nil {
		if cleanupErr != nil {
			log.G(ctx).WithError(cleanupErr).Warn("urunc(shim): snapshot view cleanup also failed after failed Delete")
		}
		return resp, err
	}
	if cleanupErr != nil {
		return resp, cleanupErr
	}
	return resp, nil
}

func (s *taskService) RegisterTTRPC(server *ttrpc.Server) error {
	taskAPI.RegisterTaskService(server, s)
	return nil
}

func (s *taskService) bundlePathFor(ctx context.Context, containerID string) (string, error) {
	if s.stateRoot == "" {
		return "", fmt.Errorf("task service state root is empty (shim cwd layout assumption violated)")
	}
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return "", fmt.Errorf("namespace required: %w", err)
	}
	return filepath.Join(s.stateRoot, ns, containerID), nil
}

// snapshotViewCleanupAfterDelete loads bundle cleanup state before inner Delete
// and returns a callback to run after inner Delete. The callback expects an open
// containerd session owned by Delete.
func (s *taskService) snapshotViewCleanupAfterDelete(ctx context.Context, r *taskAPI.DeleteRequest) func(context.Context, *containerdShim.Session) error {
	if r.ExecID != "" {
		return nil
	}

	bundle, err := s.bundlePathFor(ctx, r.ID)
	if err != nil {
		log.G(ctx).WithError(err).Warn("urunc(shim): resolve bundle path during Delete failed")
		return func(context.Context, *containerdShim.Session) error { return err }
	}

	state, err := (&containerdShim.SnapshotViewAccessor{}).LoadCleanupState(bundle)
	if err != nil {
		if !errors.Is(err, containerdShim.ErrSnapshotViewNotPrepared) {
			log.G(ctx).WithError(err).Warn("urunc(shim): load snapshot view cleanup state during Delete failed")
		}
		if errors.Is(err, containerdShim.ErrSnapshotViewNotPrepared) {
			return nil
		}
		return func(context.Context, *containerdShim.Session) error { return err }
	}

	return func(ctx context.Context, session *containerdShim.Session) error {
		if err := cleanupSnapshotViewLoaded(ctx, state, session); err != nil {
			log.G(ctx).WithError(err).Warn("urunc(shim): delete snapshot view during Delete failed")
			return err
		}
		return nil
	}
}

// CleanupSnapshotViewFromBundle removes containerd snapshot view resources described
// in bundle config.json. Boot binds are released with the monitor mount namespace.
// Missing view state is not an error. Used on the shim "delete" binary path (uruncShimManager.Stop).
func CleanupSnapshotViewFromBundle(ctx context.Context, containerdAddress, containerID, bundle string) error {
	state, err := (&containerdShim.SnapshotViewAccessor{}).LoadCleanupState(bundle)
	if err != nil {
		if errors.Is(err, containerdShim.ErrSnapshotViewNotPrepared) {
			return nil
		}
		return err
	}

	session, err := containerdShim.OpenSession(ctx, containerdAddress, containerID)
	if err != nil {
		return err
	}
	defer func() {
		_ = session.Close()
	}()

	return cleanupSnapshotViewLoaded(ctx, state, session)
}

func cleanupSnapshotViewLoaded(ctx context.Context, state *containerdShim.SnapshotViewCleanupState, session *containerdShim.Session) error {
	snapshotViewAccessor := containerdShim.NewSnapshotViewAccessor(session)
	return snapshotViewAccessor.CleanupLoaded(ctx, state)
}
