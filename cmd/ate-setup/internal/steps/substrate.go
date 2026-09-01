// Copyright 2026 Google LLC
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

package steps

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// AteClient creates a client connected to ate-api-server.
func (e *Env) AteClient(ctx context.Context) (*ateclient.Client, error) {
	return ateclient.NewClient(ctx, e.Cfg.Kubeconfig, e.Cfg.Context, "", "", false)
}

// EnsureAtespace ensures the specified atespace exists in the substrate store.
func (e *Env) EnsureAtespace(ctx context.Context, name string) error {
	client, err := e.AteClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer client.Close()

	_, err = client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{
				Name: name,
			},
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}
		if _, getErr := client.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{
			Atespace: &ateapipb.ObjectRef{Name: name},
		}); getErr == nil {
			return nil
		}
		return fmt.Errorf("failed to create atespace %q: %w", name, err)
	}
	return nil
}

// CreateSubstrateActorTemplate parses a resolved YAML manifest and creates the ActorTemplate via the ate API.
func (e *Env) CreateSubstrateActorTemplate(ctx context.Context, manifest []byte) error {
	jsonData, err := yaml.YAMLToJSON(manifest)
	if err != nil {
		return fmt.Errorf("invalid YAML for actor template: %w", err)
	}
	if string(jsonData) == "null" {
		return fmt.Errorf("actor template manifest is empty")
	}
	template := &ateapipb.ActorTemplate{}
	if err := protojson.Unmarshal(jsonData, template); err != nil {
		return fmt.Errorf("unmarshaling actor template proto: %w", err)
	}

	client, err := e.AteClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer client.Close()

	_, err = client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: template,
	})
	if err != nil {
		atespace := template.GetMetadata().GetAtespace()
		name := template.GetMetadata().GetName()
		if status.Code(err) == codes.AlreadyExists {
			log.Stepf("actor template %s/%s already exists; keeping it (delete the demo to replace it)", atespace, name)
			return nil
		}
		if _, getErr := client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
			ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		}); getErr == nil {
			log.Stepf("actor template %s/%s already exists; keeping it (delete the demo to replace it)", atespace, name)
			return nil
		}
		return fmt.Errorf("failed to create actor template %s/%s: %w", atespace, name, err)
	}
	return nil
}

// WaitSubstrateActorTemplateReady blocks until the substrate ActorTemplate's golden snapshot is ready.
func (e *Env) WaitSubstrateActorTemplateReady(ctx context.Context, atespace, name string, timeout time.Duration) error {
	client, err := e.AteClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to ate-api-server: %w", err)
	}
	defer client.Close()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
			ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		})
		if err == nil && resp != nil {
			goldenSnapshotStatus := resp.GetStatus().GetGoldenSnapshotStatus()
			if goldenSnapshotStatus != nil {
				if goldenSnapshotStatus.GetGoldenSnapshot().GetName() != "" {
					return nil
				}
				if errMsg := goldenSnapshotStatus.GetErrorMessage(); errMsg != "" {
					return fmt.Errorf("actor template %s/%s failed: %s", atespace, name, errMsg)
				}
			}
		}

		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("timed out waiting for actor template %s/%s golden snapshot", atespace, name)
}

// DeleteSubstrateActorTemplate deletes an ActorTemplate resource.
func (e *Env) DeleteSubstrateActorTemplate(ctx context.Context, atespace, name string) error {
	client, err := e.AteClient(ctx)
	if err != nil {
		log.Warnf("could not connect to ate-api-server: %v", err)
		return nil
	}
	defer client.Close()

	if _, err := client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
	}); err != nil {
		log.Stepf("actor template %s/%s not deleted (may not exist: %v)", atespace, name, err)
	}
	return nil
}

// DeleteAtespace deletes an atespace.
func (e *Env) DeleteAtespace(ctx context.Context, name string) error {
	client, err := e.AteClient(ctx)
	if err != nil {
		log.Warnf("could not connect to ate-api-server: %v", err)
		return nil
	}
	defer client.Close()

	if _, err := client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: name},
	}); err != nil {
		log.Stepf("atespace %s not deleted (may not exist or is not empty: %v)", name, err)
	}
	return nil
}
