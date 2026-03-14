//go:build linux

/*
   Copyright The containerd Authors.

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
	"time"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	"github.com/containerd/ttrpc"
	"github.com/sirupsen/logrus"
	"github.com/urunc-dev/urunc/pkg/shiminject"
)

var log = logrus.WithField("subsystem", "urunc-task")

// wrapper wraps the runc task service and creates a snapshot view
// before Create (best-effort) and cleans it up on Delete.
type wrapper struct {
	taskAPI.TaskService
	viewInfo *shiminject.SnapshotViewInfo
}

func (w *wrapper) Create(ctx context.Context, r *taskAPI.CreateTaskRequest) (*taskAPI.CreateTaskResponse, error) {
	start := time.Now()
	view, err := shiminject.CreateSnapshotView(ctx, r.Bundle, r.ID)
	log.WithFields(logrus.Fields{
		"op":          "CreateSnapshotView",
		"id":          r.ID,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("shim step completed")
	if err != nil {
		log.WithError(err).WithField("id", r.ID).Warn("create snapshot view failed, continuing without")
	} else {
		w.viewInfo = view
	}

	start = time.Now()
	resp, err := w.TaskService.Create(ctx, r)
	log.WithFields(logrus.Fields{
		"op":          "inner.Create",
		"id":          r.ID,
		"duration_ms": time.Since(start).Milliseconds(),
	}).Info("shim step completed")
	if err != nil && w.viewInfo != nil {
		// Best-effort cleanup if inner Create fails
		if cerr := shiminject.CleanupSnapshotView(ctx, w.viewInfo); cerr != nil {
			log.WithError(cerr).WithField("id", r.ID).Warn("cleanup snapshot view after failed Create also failed")
		}
	}
	return resp, err
}

func (w *wrapper) Delete(ctx context.Context, r *taskAPI.DeleteRequest) (*taskAPI.DeleteResponse, error) {
	resp, err := w.TaskService.Delete(ctx, r)
	if w.viewInfo != nil {
		if cerr := shiminject.CleanupSnapshotView(ctx, w.viewInfo); cerr != nil {
			log.WithError(cerr).WithField("id", r.ID).Warn("cleanup snapshot view during Delete failed")
		}
		// avoid double cleanup
		w.viewInfo = nil
	}
	return resp, err
}

func (w *wrapper) RegisterTTRPC(server *ttrpc.Server) error {
	taskAPI.RegisterTaskService(server, w)
	return nil
}
