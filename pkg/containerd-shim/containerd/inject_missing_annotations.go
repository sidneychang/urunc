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

// Temporary containerd helpers for urunc #565 (read urunc keys from image metadata).
package containerd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	imagesapi "github.com/containerd/containerd/api/services/images/v1"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/images"
	"github.com/containerd/platforms"
	imageSpec "github.com/opencontainers/image-spec/specs-go/v1"
)

// ImageAnnotations returns image config labels and manifest annotations with
// the provided prefix. Manifest annotations take precedence over config labels.
func (s *Session) ImageAnnotations(ctx context.Context, prefix string) (map[string]string, error) {
	imageRef := s.container.GetImage()
	if imageRef == "" {
		return nil, fmt.Errorf("container %q has empty image ref", s.containerID)
	}

	imageResp, err := s.imagesClient().Get(withNamespace(ctx, s.namespace), &imagesapi.GetImageRequest{Name: imageRef})
	if err != nil {
		return nil, fmt.Errorf("get image %s: %w", imageRef, containerdErr(err))
	}

	return s.imageAnnotations(ctx, imageResp.Image.Target, prefix)
}

func (s *Session) imageAnnotations(ctx context.Context, target *types.Descriptor, prefix string) (map[string]string, error) {
	ctx = withNamespace(ctx, s.namespace)
	contentClient := s.contentClient()

	manifestDesc, err := manifestDescriptor(ctx, contentClient, target)
	if err != nil {
		return nil, err
	}

	manifestRaw, err := readBlob(ctx, contentClient, manifestDesc.Digest, manifestDesc.Size)
	if err != nil {
		return nil, fmt.Errorf("read manifest blob: %w", err)
	}
	var manifest imageSpec.Manifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	configRaw, err := readBlob(ctx, contentClient, manifest.Config.Digest.String(), manifest.Config.Size)
	if err != nil {
		return nil, fmt.Errorf("read image config blob: %w", err)
	}
	var imageConfig imageSpec.Image
	if err := json.Unmarshal(configRaw, &imageConfig); err != nil {
		return nil, fmt.Errorf("unmarshal image config: %w", err)
	}

	annotations := make(map[string]string)
	for key, value := range imageConfig.Config.Labels {
		if strings.HasPrefix(key, prefix) {
			annotations[key] = value
		}
	}
	for key, value := range manifest.Annotations {
		if strings.HasPrefix(key, prefix) {
			annotations[key] = value
		}
	}

	return annotations, nil
}

func manifestDescriptor(
	ctx context.Context,
	contentClient contentapi.ContentClient,
	target *types.Descriptor,
) (*types.Descriptor, error) {
	if images.IsManifestType(target.MediaType) {
		return target, nil
	}

	if !images.IsIndexType(target.MediaType) {
		return nil, fmt.Errorf("unsupported image target media type: %s", target.MediaType)
	}

	indexRaw, err := readBlob(ctx, contentClient, target.Digest, target.Size)
	if err != nil {
		return nil, fmt.Errorf("read image index blob: %w", err)
	}

	var index imageSpec.Index
	if err := json.Unmarshal(indexRaw, &index); err != nil {
		return nil, fmt.Errorf("unmarshal image index: %w", err)
	}

	matcher := platforms.DefaultStrict()
	for _, manifest := range index.Manifests {
		if manifest.Platform == nil {
			continue
		}
		if matcher.Match(*manifest.Platform) {
			return &types.Descriptor{
				MediaType: manifest.MediaType,
				Digest:    manifest.Digest.String(),
				Size:      manifest.Size,
			}, nil
		}
	}

	return nil, fmt.Errorf("no matching manifest found in image index for platform %s", platforms.Format(platforms.DefaultSpec()))
}

func readBlob(ctx context.Context, contentClient contentapi.ContentClient, digest string, size int64) ([]byte, error) {
	stream, err := contentClient.Read(ctx, &contentapi.ReadContentRequest{
		Digest: digest,
		Size:   size,
	})
	if err != nil {
		return nil, containerdErr(err)
	}

	var raw []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, containerdErr(err)
		}
		raw = append(raw, resp.Data...)
	}

	return raw, nil
}
