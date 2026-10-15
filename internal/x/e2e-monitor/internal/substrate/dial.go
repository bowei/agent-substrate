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

package substrate

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/portforward"
)

// KubeAPIDialer connects to ateapi the way kubectl-ate does: through a
// port-forward with a bearer token minted for the ate-client ServiceAccount.
func KubeAPIDialer(kubeconfig, kubeContext string) APIDialer {
	return func(ctx context.Context) (Lister, func(), error) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		c, err := ateclient.NewClient(ctx, kubeconfig, kubeContext, "", "", false)
		if err != nil {
			return nil, nil, err
		}
		return c, c.Close, nil
	}
}

// KubeRouterDialer port-forwards to a ready pod behind the router Service.
func KubeRouterDialer(config *rest.Config, client kubernetes.Interface, namespace, service string, port int32) RouterDialer {
	return func(ctx context.Context) (string, func(), error) {
		local, stop, err := portforward.ServicePodPortForward(ctx, config, client, namespace, service, port)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("http://127.0.0.1:%d", local), stop, nil
	}
}
