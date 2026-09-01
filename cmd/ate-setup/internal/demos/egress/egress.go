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

// Package egress installs the egress demo, which exercises egress policy
// enforcement through atenet.
package egress

import (
	"time"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

func init() {
	demos.Register(&demos.Simple{
		DemoName:       "demo-egress",
		Short:          "Egress policy enforcement through atenet",
		Template:       "demos/egress/egress.yaml.tmpl",
		Deployments:    []steps.TemplateRef{{Namespace: "ate-demo-egress", Name: "egress"}},
		ActorTemplates: []steps.TemplateRef{{Namespace: "ate-demo-egress", Name: "egress"}},
	})
	demos.Register(&demos.Simple{
		DemoName:       "demo-egress-microvm",
		Short:          "Egress policy enforcement on micro-VM workers (requires install-microvm-deps.sh)",
		Template:       "demos/egress/egress-microvm.yaml.tmpl",
		Deployments:    []steps.TemplateRef{{Namespace: "ate-demo-egress-microvm", Name: "egress-microvm"}},
		ActorTemplates: []steps.TemplateRef{{Namespace: "ate-demo-egress-microvm", Name: "egress-microvm"}},
		Timeout:        600 * time.Second,
	})
	demos.Register(&demos.Simple{
		DemoName:       "demo-egress-mitm",
		Short:          "MITM egress policy enforcement (requires --experimental-use-sdsmint)",
		Template:       "demos/egress/egress-mitm.yaml.tmpl",
		Deployments:    []steps.TemplateRef{{Namespace: "ate-demo-egress-mitm", Name: "egress-mitm"}},
		ActorTemplates: []steps.TemplateRef{{Namespace: "ate-demo-egress-mitm", Name: "egress-mitm"}},
	})
	demos.Register(&demos.Simple{
		DemoName:       "demo-egress-microvm-mitm",
		Short:          "MITM egress policy enforcement on micro-VM workers (requires sdsmint and microvm)",
		Template:       "demos/egress/egress-microvm-mitm.yaml.tmpl",
		Deployments:    []steps.TemplateRef{{Namespace: "ate-demo-egress-microvm-mitm", Name: "egress-microvm-mitm"}},
		ActorTemplates: []steps.TemplateRef{{Namespace: "ate-demo-egress-microvm-mitm", Name: "egress-microvm-mitm"}},
		Timeout:        600 * time.Second,
	})
}
