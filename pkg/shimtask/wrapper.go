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

package shimtask

import (
	"context"
	"sync"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	"github.com/containerd/ttrpc"
	"github.com/sirupsen/logrus"
	"github.com/urunc-dev/urunc/pkg/shiminject"
)

var log = logrus.StandardLogger()

// wrapper wraps the runc task service and creates a snapshot view
// before Create (best-effort) and cleans it up on Delete.
type wrapper struct {
	taskAPI.TaskService

	mu    sync.Mutex
	views map[string]*shiminject.SnapshotViewInfo
}

func (w *wrapper) Create(ctx context.Context, r *taskAPI.CreateTaskRequest) (*taskAPI.CreateTaskResponse, error) {
	view, err := shiminject.CreateSnapshotView(ctx, r.Bundle, r.ID)
	if err != nil {
		log.WithError(err).Warn("create snapshot view failed, continuing without")
	}

	resp, err := w.TaskService.Create(ctx, r)
	if err != nil {
		if view != nil {
			// Best-effort cleanup if inner Create fails.
			if cerr := shiminject.CleanupSnapshotView(ctx, view); cerr != nil {
				log.WithError(cerr).Warn("cleanup snapshot view after failed Create also failed")
			}
		}
		return resp, err
	}

	if view != nil {
		w.mu.Lock()
		if w.views == nil {
			w.views = make(map[string]*shiminject.SnapshotViewInfo)
		}
		w.views[r.ID] = view
		w.mu.Unlock()
	}

	return resp, nil
}

func (w *wrapper) Delete(ctx context.Context, r *taskAPI.DeleteRequest) (*taskAPI.DeleteResponse, error) {
	resp, err := w.TaskService.Delete(ctx, r)
	if err != nil || r.ExecID != "" {
		return resp, err
	}

	w.mu.Lock()
	view := w.views[r.ID]
	delete(w.views, r.ID)
	w.mu.Unlock()

	if view != nil {
		if cerr := shiminject.CleanupSnapshotView(ctx, view); cerr != nil {
			log.WithError(cerr).Warn("cleanup snapshot view during Delete failed")
		}
	}
	return resp, err
}

func (w *wrapper) RegisterTTRPC(server *ttrpc.Server) error {
	taskAPI.RegisterTaskService(server, w)
	return nil
}
