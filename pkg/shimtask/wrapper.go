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

	taskAPI "github.com/containerd/containerd/api/runtime/task/v2"
	ptypes "github.com/containerd/containerd/protobuf/types"
	"github.com/containerd/ttrpc"
	"github.com/sirupsen/logrus"
	"github.com/urunc-dev/urunc/pkg/shiminject"
)

var log = logrus.WithField("subsystem", "urunc-task")

// wrapper wraps the runc task service and creates a snapshot view
// before Create (best-effort) and cleans it up on Delete.
type wrapper struct {
	inner    taskAPI.TaskService
	viewInfo *shiminject.SnapshotViewInfo
}

func (w *wrapper) Create(ctx context.Context, r *taskAPI.CreateTaskRequest) (*taskAPI.CreateTaskResponse, error) {
	view, err := shiminject.CreateSnapshotView(ctx, r.Bundle, r.ID)
	if err != nil {
		log.WithError(err).WithField("id", r.ID).Warn("create snapshot view failed, continuing without")
	} else {
		w.viewInfo = view
	}

	resp, err := w.inner.Create(ctx, r)
	if err != nil && w.viewInfo != nil {
		// Best-effort cleanup if inner Create fails
		if cerr := shiminject.CleanupSnapshotView(ctx, w.viewInfo); cerr != nil {
			log.WithError(cerr).WithField("id", r.ID).Warn("cleanup snapshot view after failed Create also failed")
		}
	}
	return resp, err
}

func (w *wrapper) Start(ctx context.Context, r *taskAPI.StartRequest) (*taskAPI.StartResponse, error) {
	return w.inner.Start(ctx, r)
}
func (w *wrapper) Delete(ctx context.Context, r *taskAPI.DeleteRequest) (*taskAPI.DeleteResponse, error) {
	resp, err := w.inner.Delete(ctx, r)
	if w.viewInfo != nil {
		if cerr := shiminject.CleanupSnapshotView(ctx, w.viewInfo); cerr != nil {
			log.WithError(cerr).WithField("id", r.ID).Warn("cleanup snapshot view during Delete failed")
		}
		// avoid double cleanup
		w.viewInfo = nil
	}
	return resp, err
}
func (w *wrapper) Pids(ctx context.Context, r *taskAPI.PidsRequest) (*taskAPI.PidsResponse, error) {
	return w.inner.Pids(ctx, r)
}
func (w *wrapper) Pause(ctx context.Context, r *taskAPI.PauseRequest) (*ptypes.Empty, error) {
	return w.inner.Pause(ctx, r)
}
func (w *wrapper) Resume(ctx context.Context, r *taskAPI.ResumeRequest) (*ptypes.Empty, error) {
	return w.inner.Resume(ctx, r)
}
func (w *wrapper) Checkpoint(ctx context.Context, r *taskAPI.CheckpointTaskRequest) (*ptypes.Empty, error) {
	return w.inner.Checkpoint(ctx, r)
}
func (w *wrapper) Kill(ctx context.Context, r *taskAPI.KillRequest) (*ptypes.Empty, error) {
	return w.inner.Kill(ctx, r)
}
func (w *wrapper) Exec(ctx context.Context, r *taskAPI.ExecProcessRequest) (*ptypes.Empty, error) {
	return w.inner.Exec(ctx, r)
}
func (w *wrapper) ResizePty(ctx context.Context, r *taskAPI.ResizePtyRequest) (*ptypes.Empty, error) {
	return w.inner.ResizePty(ctx, r)
}
func (w *wrapper) CloseIO(ctx context.Context, r *taskAPI.CloseIORequest) (*ptypes.Empty, error) {
	return w.inner.CloseIO(ctx, r)
}
func (w *wrapper) Update(ctx context.Context, r *taskAPI.UpdateTaskRequest) (*ptypes.Empty, error) {
	return w.inner.Update(ctx, r)
}
func (w *wrapper) Wait(ctx context.Context, r *taskAPI.WaitRequest) (*taskAPI.WaitResponse, error) {
	return w.inner.Wait(ctx, r)
}
func (w *wrapper) Stats(ctx context.Context, r *taskAPI.StatsRequest) (*taskAPI.StatsResponse, error) {
	return w.inner.Stats(ctx, r)
}
func (w *wrapper) Connect(ctx context.Context, r *taskAPI.ConnectRequest) (*taskAPI.ConnectResponse, error) {
	return w.inner.Connect(ctx, r)
}
func (w *wrapper) Shutdown(ctx context.Context, r *taskAPI.ShutdownRequest) (*ptypes.Empty, error) {
	return w.inner.Shutdown(ctx, r)
}
func (w *wrapper) State(ctx context.Context, r *taskAPI.StateRequest) (*taskAPI.StateResponse, error) {
	return w.inner.State(ctx, r)
}

func (w *wrapper) RegisterTTRPC(server *ttrpc.Server) error {
	taskAPI.RegisterTaskService(server, w)
	return nil
}

