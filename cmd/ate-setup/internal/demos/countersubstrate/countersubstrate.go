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

// Package countersubstrate registers the substrate-resource variant of the
// counter demo, covering both gVisor and micro-VM sandboxes.
package countersubstrate

import (
	"context"
	"time"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

type demoCounterSubstrate struct {
	name          string
	description   string
	poolManifest  string
	tmplManifest  string
	atespace      string
	pool          string
	templateName  string
	goldenTimeout time.Duration
}

func (d *demoCounterSubstrate) Name() string          { return d.name }
func (d *demoCounterSubstrate) Description() string   { return d.description }
func (d *demoCounterSubstrate) Flags(*pflag.FlagSet)  {}
func (d *demoCounterSubstrate) TemplatePath() string  { return d.poolManifest }

func (d *demoCounterSubstrate) Deploy(ctx context.Context, e *steps.Env) error {
	log.Stepf("%s_deploy (%s/%s)", d.name, d.atespace, d.templateName)
	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}

	poolBytes, err := demos.Render(e, d.poolManifest, nil, nil)
	if err != nil {
		return err
	}
	if err := e.KoApplyBytes(ctx, poolBytes); err != nil {
		return err
	}

	log.Stepf("Waiting for the %s worker pool rollout...", d.pool)
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, d.atespace, d.pool, steps.DemoTimeout); err != nil {
		return err
	}

	if err := e.EnsureAtespace(ctx, d.atespace); err != nil {
		return err
	}

	tmplBytes, err := demos.Render(e, d.tmplManifest, nil, nil)
	if err != nil {
		return err
	}
	resolvedTmpl, err := e.KoResolveBytes(ctx, tmplBytes)
	if err != nil {
		return err
	}
	if err := e.CreateSubstrateActorTemplate(ctx, resolvedTmpl); err != nil {
		return err
	}

	log.Stepf("Waiting for the %s/%s golden snapshot...", d.atespace, d.templateName)
	return e.WaitSubstrateActorTemplateReady(ctx, d.atespace, d.templateName, d.goldenTimeout)
}

func (d *demoCounterSubstrate) Delete(ctx context.Context, e *steps.Env) error {
	log.Stepf("%s_delete (%s/%s)", d.name, d.atespace, d.templateName)

	if err := e.DeleteDemoActors(ctx, steps.TemplateRef{Namespace: d.atespace, Name: d.templateName}); err != nil {
		return err
	}

	_ = e.DeleteSubstrateActorTemplate(ctx, d.atespace, d.templateName)
	_ = e.DeleteAtespace(ctx, d.atespace)

	poolBytes, err := demos.Render(e, d.poolManifest, nil, nil)
	if err != nil {
		return err
	}
	return e.Kube.DeleteBytes(ctx, poolBytes)
}

func init() {
	demos.Register(&demoCounterSubstrate{
		name:          "demo-counter-substrate",
		description:   "Substrate-resource variant of the counter demo on gVisor",
		poolManifest:  "demos/counter/counter-substrate.yaml.tmpl",
		tmplManifest:  "demos/counter/counter-substrate-template.yaml.tmpl",
		atespace:      "ate-demo-counter-substrate",
		pool:          "counter-substrate",
		templateName:  "counter",
		goldenTimeout: 300 * time.Second,
	})
	demos.Register(&demoCounterSubstrate{
		name:          "demo-counter-substrate-microvm",
		description:   "Substrate-resource variant of the counter demo on micro-VM workers (requires install-microvm-deps.sh)",
		poolManifest:  "demos/counter/counter-substrate-microvm.yaml.tmpl",
		tmplManifest:  "demos/counter/counter-substrate-microvm-template.yaml.tmpl",
		atespace:      "ate-demo-counter-substrate-microvm",
		pool:          "counter-substrate-microvm",
		templateName:  "counter-microvm",
		goldenTimeout: 600 * time.Second,
	})
}
