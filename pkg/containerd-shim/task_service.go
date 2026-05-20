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
	"github.com/containerd/ttrpc"
	"github.com/sirupsen/logrus"
	shimcontainerd "github.com/urunc-dev/urunc/pkg/containerd-shim/containerd"
)

// taskService is urunc's shim-side wrapper around containerd's runc task
// service. It wires annotation injection, per-container snapshot views, and
// guest rootfs precompute at task Create/Delete boundaries.
type taskService struct {
	taskAPI.TaskService

	containerdAddress string
	// stateRoot is the containerd runtime v2 state directory (parent of
	// per-namespace dirs). Derived at plugin init from the shim cwd; used to
	// resolve bundle paths on Delete when cwd may no longer be the bundle.
	stateRoot string
}

func (s *taskService) Create(ctx context.Context, r *taskAPI.CreateTaskRequest) (*taskAPI.CreateTaskResponse, error) {
	session, err := shimcontainerd.OpenSession(ctx, s.containerdAddress, r.ID)
	if err != nil {
		logrus.WithError(err).WithField("container_id", r.ID).Warn("urunc shim: failed to open containerd session")
	} else {
		defer session.Close()
	}

	// #565: merge image metadata into bundle config.json when spec lacks urunc keys.
	if session != nil {
		if err := s.injectMissingAnnotations(ctx, r, session); err != nil {
			logrus.WithError(err).WithField("container_id", r.ID).Warn("urunc shim: failed to inject missing annotations")
		}
	}

	// #684: ChooseRootfs before snapshot view so view tracks the selected guest rootfs path.
	rootfsChoice, err := chooseGuestRootfs(r)
	if err != nil {
		if errors.Is(err, errGuestRootfsChoiceSkipped) {
			logrus.WithField("container_id", r.ID).Debug("urunc shim: guest rootfs choice skipped")
		} else {
			logrus.WithError(err).Warn("urunc shim: failed to choose guest rootfs")
			return nil, err
		}
	}

	var snapshotView *shimcontainerd.SnapshotView
	if session != nil {
		snapshotView = shimcontainerd.NewSnapshotView(session)
	}

	var needView bool
	if snapshotView != nil {
		var gateErr error
		needView, gateErr = snapshotView.ShouldPrepare(r.Bundle, rootfsChoice)
		if gateErr != nil {
			return nil, gateErr
		}
	}
	if needView {
		if err := snapshotView.Prepare(ctx, r.Bundle, rootfsChoice); err != nil {
			return nil, err
		}
	}

	resp, err := s.TaskService.Create(ctx, r)
	if err != nil && needView && snapshotView != nil {
		if cleanupErr := snapshotView.Cleanup(ctx, r.Bundle); cleanupErr != nil {
			logrus.WithError(cleanupErr).Warn("urunc shim: cleanup snapshot view after failed Create failed")
		}
	}
	return resp, err
}

func (s *taskService) Delete(ctx context.Context, r *taskAPI.DeleteRequest) (*taskAPI.DeleteResponse, error) {
	cleanupSnapshotView := s.snapshotViewCleanupAfterDelete(ctx, r)

	resp, err := s.TaskService.Delete(ctx, r)
	cleanupSnapshotView(ctx)
	return resp, err
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

func (s *taskService) snapshotViewCleanupAfterDelete(ctx context.Context, r *taskAPI.DeleteRequest) func(context.Context) {
	if r.ExecID != "" {
		return func(context.Context) {}
	}

	bundle, err := s.bundlePathFor(ctx, r.ID)
	if err != nil {
		logrus.WithError(err).Warn("urunc shim: resolve bundle path during Delete failed")
		return func(context.Context) {}
	}

	session, err := shimcontainerd.OpenSession(ctx, s.containerdAddress, r.ID)
	if err != nil {
		logrus.WithError(err).Warn("urunc shim: open containerd session for snapshot view cleanup failed")
		return func(context.Context) {}
	}

	snapshotView := shimcontainerd.NewSnapshotViewCleanup(session)
	state, err := snapshotView.LoadCleanupState(bundle)
	if err != nil {
		if !errors.Is(err, shimcontainerd.ErrSnapshotViewNotPrepared) {
			logrus.WithError(err).Warn("urunc shim: load snapshot view cleanup state during Delete failed")
		}
		_ = session.Close()
		return func(context.Context) {}
	}

	return func(ctx context.Context) {
		defer session.Close()
		if err := snapshotView.CleanupLoaded(ctx, bundle, state); err != nil {
			logrus.WithError(err).Warn("urunc shim: delete snapshot view during Delete failed")
		}
	}
}
