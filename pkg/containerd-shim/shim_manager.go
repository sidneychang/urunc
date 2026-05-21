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
	"os"

	"github.com/containerd/containerd/runtime/v2/runc/manager"
	"github.com/containerd/containerd/runtime/v2/shim"
	"github.com/containerd/log"
)

// grpcAddressEnv matches containerd's shim.Command env (runtime/v2/shim/shim.go).
const grpcAddressEnv = "GRPC_ADDRESS"

type uruncShimManager struct {
	inner shim.Manager
	name  string
}

// NewUruncShimManager returns a shim.Manager that wraps the runc v2 manager and
// cleans up urunc snapshot views on the containerd "delete" binary path.
func NewUruncShimManager(runtime string) shim.Manager {
	return &uruncShimManager{
		inner: manager.NewShimManager(runtime),
		name:  runtime,
	}
}

func (m *uruncShimManager) Name() string {
	return m.name
}

func (m *uruncShimManager) Start(ctx context.Context, id string, opts shim.StartOpts) (string, error) {
	return m.inner.Start(ctx, id, opts)
}

// Stop is invoked when containerd runs the shim binary with the "delete"
// subcommand (dead shim cleanup). That path does not use TaskService.Delete,
// so snapshot views must be removed here before runc tears down the bundle.
func (m *uruncShimManager) Stop(ctx context.Context, id string) (shim.StopStatus, error) {
	bundle, err := os.Getwd()
	if err != nil {
		log.G(ctx).WithError(err).Warn("urunc(shim): getwd during delete failed")
	} else if address := os.Getenv(grpcAddressEnv); address == "" {
		log.G(ctx).Warn("urunc(shim): GRPC_ADDRESS unset during delete; snapshot view cleanup skipped")
	} else if err := CleanupSnapshotViewFromBundle(ctx, address, id, bundle); err != nil {
		log.G(ctx).WithError(err).Warn("urunc(shim): snapshot view cleanup during delete failed")
	}

	return m.inner.Stop(ctx, id)
}
