//go:build linux

/*
   Copyright The containerd Authors.
   Copyright (c) 2023-2026, Nubificus LTD

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package shimwrap

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/ttrpc"
	"github.com/sirupsen/logrus"
	"github.com/urunc-dev/urunc/pkg/shimview"
)

var log = logrus.StandardLogger()

// taskService wraps the runc task service and delegates shared-view lifecycle
// transitions to shimview at task Create/Delete boundaries.
type taskService struct {
	taskAPI.TaskService

	stateRoot string
}

func (w *taskService) Create(ctx context.Context, r *taskAPI.CreateTaskRequest) (*taskAPI.CreateTaskResponse, error) {
	view, err := shimview.CreateSnapshotView(ctx, r.Bundle, r.ID)
	if err != nil {
		// Best-effort by design: CreateSnapshotView is an optimization path for
		// block-rootfs boot artifact access. If it fails, task creation should
		// still proceed using the legacy extraction path in urunc runtime.
		log.WithError(err).Warn("create snapshot view failed, continuing without")
	}
	if view != nil {
		if err := shimview.OnCreatePinContainerReference(ctx, r.Bundle, view); err != nil {
			log.WithError(err).Warn("attach shared snapshot view to container failed")
			return nil, err
		}
	}

	resp, err := w.TaskService.Create(ctx, r)
	if err != nil {
		if view != nil {
			if rollbackErr := shimview.OnCreateRollback(ctx, r.Bundle, view); rollbackErr != nil {
				log.WithError(rollbackErr).Warn("rollback shared snapshot view after failed Create failed")
			}
		}
		return resp, err
	}

	if view != nil {
		if err := shimview.OnCreateFinalize(ctx, view); err != nil {
			// Lease finalization is intentionally best-effort. Failing here should
			// not fail task create after the wrapped task service succeeded.
			log.WithError(err).Warn("finalize shared snapshot view after Create failed")
		}
	}

	return resp, nil
}

func (w *taskService) Delete(ctx context.Context, r *taskAPI.DeleteRequest) (*taskAPI.DeleteResponse, error) {
	var view *shimview.SnapshotViewInfo
	if r.ExecID == "" {
		bundle, berr := w.bundlePathFor(ctx, r.ID)
		if berr != nil {
			log.WithError(berr).Warn("resolve bundle path during Delete failed")
		} else {
			var lerr error
			view, lerr = shimview.LoadSnapshotViewState(bundle)
			if lerr != nil && !errors.Is(lerr, shimview.ErrSnapshotViewStateNotFound) {
				log.WithError(lerr).Warn("load snapshot view state during Delete failed")
			}
		}
	}

	resp, err := w.TaskService.Delete(ctx, r)
	if err != nil || r.ExecID != "" {
		return resp, err
	}

	if view != nil {
		if err := shimview.OnDeleteReleaseContainerReference(ctx, view); err != nil {
			// Detach/reconcile is cleanup-only at this point; task delete already
			// succeeded in the wrapped service so we log and continue.
			log.WithError(err).Warn("detach shared snapshot view during Delete failed")
		}
	}
	return resp, err
}

func (w *taskService) RegisterTTRPC(server *ttrpc.Server) error {
	taskAPI.RegisterTaskService(server, w)
	return nil
}

func (w *taskService) bundlePathFor(ctx context.Context, containerID string) (string, error) {
	if w.stateRoot == "" {
		return "", fmt.Errorf("wrapper state root is empty")
	}
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return "", fmt.Errorf("namespace required: %w", err)
	}
	return filepath.Join(w.stateRoot, ns, containerID), nil
}
