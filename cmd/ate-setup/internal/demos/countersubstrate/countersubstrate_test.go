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

package countersubstrate

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

func TestCounterSubstrateManifestsRender(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	env := &steps.Env{Cfg: &config.Config{Root: root, BucketName: "test-bucket"}}

	for _, tc := range []struct {
		name       string
		path       string
		usesBucket bool
	}{
		{"gVisor worker pool", "demos/counter/counter-substrate.yaml.tmpl", false},
		{"gVisor template", "demos/counter/counter-substrate-template.yaml.tmpl", true},
		{"micro-VM worker pool", "demos/counter/counter-substrate-microvm.yaml.tmpl", false},
		{"micro-VM template", "demos/counter/counter-substrate-microvm-template.yaml.tmpl", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := demos.Render(env, tc.path, nil, nil)
			if err != nil {
				t.Fatalf("render %s: %v", tc.path, err)
			}
			if strings.Contains(string(out), "${BUCKET_NAME}") {
				t.Errorf("rendered %s still contains ${BUCKET_NAME}", tc.path)
			}
			if tc.usesBucket && !strings.Contains(string(out), "test-bucket") {
				t.Errorf("rendered %s does not contain test-bucket", tc.path)
			}
		})
	}
}
