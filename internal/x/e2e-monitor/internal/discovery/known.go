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

package discovery

// knownInfo enriches a discovered workload. Discovery does not depend on this
// table: a workload missing from it still gets a component, sorted after the
// known ones.
type knownInfo struct {
	order       int
	description string
	statuszPort int
}

const (
	unknownOrder = 100
	otherOrder   = 1000
)

var known = map[string]knownInfo{
	"ate-api-server":            {1, "ateapi: the control-plane gRPC API for actors, templates, atespaces, and workers.", 0},
	"ate-controller":            {2, "Reconciles WorkerPools into worker pods and syncs workers with ateapi.", 0},
	"atelet":                    {3, "Node agent: restores, runs, and checkpoints actors inside worker pods.", 0},
	"atenet-router":             {4, "Ingress proxy (Envoy + ext_proc) that routes to actors and resumes them on demand.", 4040},
	"atenet-egress":             {5, "Egress gateway that enforces actor egress policies.", 0},
	"postgres":                  {6, "Control-plane storage for ateapi.", 0},
	"podcertificate-controller": {7, "Issues pod certificates for substrate's workload identities.", 0},
	"k8s-credential-provider":   {8, "Credential provider plugin serving secrets for egress credential injection.", 0},
	"rustfs":                    {9, "S3-compatible object store holding actor snapshots (kind installs).", 0},
}

func lookupKnown(name string) knownInfo {
	if info, ok := known[name]; ok {
		return info
	}
	return knownInfo{order: unknownOrder}
}
