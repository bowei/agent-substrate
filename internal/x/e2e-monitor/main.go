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

// Command e2e-monitor serves a local, read-only dashboard of a cluster running
// Agent Substrate: one tab per discovered component with its pods, events, and
// live logs, the worker pools, and the progress of an e2e run correlated with
// the component logs it caused. See README.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/discovery"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/e2efeed"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/logs"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/monitor"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/server"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/substrate"
	"github.com/agent-substrate/substrate/internal/x/e2e-monitor/internal/tui"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
)

type options struct {
	kubeconfig      string
	kubeContext     string
	listen          string
	namespace       string
	extraNamespaces []string
	resourcePrefix  string
	followWorkers   string
	e2eJSON         string
	replay          bool
	substrateAPI    bool
	tailLines       int64
	bufferLines     int
	exitWhenDone    bool
	verbose         bool
	tui             bool
}

func main() {
	if err := newRootCommand().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	o := &options{}
	root := &cobra.Command{
		Use:   "e2e-monitor",
		Short: "Dashboard of the Agent Substrate components in a cluster and of e2e runs against it",
		Long: `e2e-monitor serves a local, read-only dashboard of a cluster running Agent
Substrate. It discovers every workload in the system namespace (one tab each),
the WorkerPools and their pods, streams their logs, and follows an e2e run,
tying each test to the component log lines that mention the resources it
created.

With no subcommand it behaves like "serve".`,
		SilenceUsage: true,
		RunE:         func(cmd *cobra.Command, _ []string) error { return run(cmd.Context(), o, nil) },
	}
	fs := root.PersistentFlags()
	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "Path to the kubeconfig (default: the standard loading rules)")
	fs.StringVar(&o.kubeContext, "context", "", "Kubeconfig context (default: the current context)")
	fs.StringVar(&o.listen, "listen", "127.0.0.1:8080", "Address to serve the dashboard on")
	fs.StringVar(&o.namespace, "namespace", envOr(ateclient.NamespaceEnv, installdefaults.SystemNamespace), "Substrate's system namespace (env "+ateclient.NamespaceEnv+")")
	fs.StringSliceVar(&o.extraNamespaces, "extra-namespace", []string{"podcertificate-controller-system"}, "More namespaces whose workloads are shown as components")
	fs.StringVar(&o.resourcePrefix, "resource-prefix", os.Getenv("E2E_RESOURCE_PREFIX"), "Prefix the install puts on substrate's resource names (env E2E_RESOURCE_PREFIX)")
	fs.StringVar(&o.followWorkers, "follow-workers", string(monitor.FollowE2E), `Which worker pods' logs to follow continuously: "e2e" (pods of running tests), "all", or "none" (only while viewed)`)
	fs.BoolVar(&o.substrateAPI, "substrate-api", true, "Poll ateapi for workers and actors, and the router's /statusz")
	fs.Int64Var(&o.tailLines, "tail-lines", 500, "Existing lines to load when starting to follow a container")
	fs.IntVar(&o.bufferLines, "buffer-lines", 5000, "Lines kept in memory per container")
	fs.BoolVarP(&o.verbose, "verbose", "v", false, "Debug logging")
	fs.BoolVar(&o.tui, "tui", false, `Run the terminal UI instead of only serving the web UI (set --listen="" to serve no web UI)`)

	serve := &cobra.Command{
		Use:   "serve",
		Short: "Serve the dashboard, optionally following a test2json file",
		Example: `  # Just the cluster view.
  go run ./internal/x/e2e-monitor serve

  # Follow the JSON stream a CI-style run writes (E2E_JUNIT_FILE=/tmp/e2e.xml
  # makes hack/run-e2e.sh write /tmp/e2e.json).
  go run ./internal/x/e2e-monitor serve --e2e-json=/tmp/e2e.json

  # Inspect a recorded run.
  go run ./internal/x/e2e-monitor serve --e2e-json=artifacts/e2e-gvisor.json --replay`,
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE:         func(cmd *cobra.Command, _ []string) error { return run(cmd.Context(), o, nil) },
	}
	serve.Flags().StringVar(&o.e2eJSON, "e2e-json", "", "test2json file to follow (as it is written) for the E2E tab")
	serve.Flags().BoolVar(&o.replay, "replay", false, "Read --e2e-json once instead of following it")

	runCmd := &cobra.Command{
		Use:   "run -- COMMAND [ARGS...]",
		Short: "Run an e2e command with -json added and serve the dashboard while it runs",
		Long: `run starts COMMAND, a "go test" or hack/run-e2e.sh command line, with -json
added before its -args separator, feeds its events to the E2E tab, and echoes
its output to the terminal. The dashboard stays up after the command exits
until interrupted, unless --exit-when-done is set.`,
		Example: `  go run ./internal/x/e2e-monitor run -- hack/run-e2e.sh -run TestDemo -args --preview=ALL
  go run ./internal/x/e2e-monitor run -- hack/run-e2e-kind.sh ./internal/e2e/suites/networking`,
		Args:         cobra.MinimumNArgs(1),
		SilenceUsage: true,
		RunE:         func(cmd *cobra.Command, args []string) error { return run(cmd.Context(), o, args) },
	}
	runCmd.Flags().BoolVar(&o.exitWhenDone, "exit-when-done", false, "Exit when the command exits instead of serving until interrupted")

	root.AddCommand(serve, runCmd)
	return root
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run(ctx context.Context, o *options, command []string) error {
	level := slog.LevelInfo
	if o.verbose {
		level = slog.LevelDebug
	}
	logOut := io.Writer(os.Stderr)
	if o.tui {
		// The TUI owns the terminal; log to a file instead.
		f, err := os.CreateTemp("", "e2e-monitor-*.log")
		if err != nil {
			return err
		}
		defer func() {
			f.Close()
			fmt.Fprintf(os.Stderr, "e2e-monitor log: %s\n", f.Name())
		}()
		logOut = f
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: level})))

	follow, err := monitor.ParseFollowMode(o.followWorkers)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ateclient reads the system namespace from the environment.
	if err := os.Setenv(ateclient.NamespaceEnv, o.namespace); err != nil {
		return err
	}
	config, err := ateclient.LoadKubeConfig(o.kubeconfig, o.kubeContext)
	if err != nil {
		return fmt.Errorf("loading kubeconfig: %w", err)
	}
	// Every followed container is a long-lived request; keep starting them
	// from being throttled.
	config.QPS, config.Burst = 50, 100
	kube, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	sub, err := versioned.NewForConfig(config)
	if err != nil {
		return err
	}

	disc := discovery.New(discovery.Config{
		Kube:           kube,
		Substrate:      sub,
		Namespaces:     append([]string{o.namespace}, o.extraNamespaces...),
		ResourcePrefix: o.resourcePrefix,
	})
	if o.tui {
		fmt.Fprintf(os.Stderr, "Discovering components in %s (context %s)…\n", o.namespace, contextName(o))
	}
	slog.Info("discovering components", "context", contextName(o), "namespace", o.namespace)
	if err := disc.Start(ctx, time.Minute); err != nil {
		return fmt.Errorf("watching the cluster: %w", err)
	}

	logMgr := logs.NewManager(logs.KubeOpener(kube), logs.Options{TailLines: o.tailLines, Capacity: o.bufferLines})
	feed := e2efeed.New()

	var poller *substrate.Poller
	if o.substrateAPI {
		poller = substrate.NewPoller(
			substrate.KubeAPIDialer(o.kubeconfig, o.kubeContext),
			substrate.KubeRouterDialer(config, kube, o.namespace, o.resourcePrefix+installdefaults.RouterServiceName, 4040),
			5*time.Second)
		go poller.Run(ctx)
	}

	mon := monitor.New(ctx, monitor.Config{
		Discovery:     disc,
		Logs:          logMgr,
		Feed:          feed,
		Substrate:     poller,
		FollowWorkers: follow,
		Info:          monitor.Info{Context: contextName(o), SystemNamespace: o.namespace},
	})
	go mon.RunFollower(ctx)

	serveErr := make(chan error, 1)
	if o.listen != "" {
		lis, err := net.Listen("tcp", o.listen)
		if err != nil {
			return err
		}
		httpSrv := &http.Server{Handler: server.New(mon).Handler(), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = httpSrv.Shutdown(shutdownCtx)
		}()
		go func() {
			err := httpSrv.Serve(lis)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			serveErr <- err
		}()
		slog.Info("web UI ready", "url", "http://"+lis.Addr().String())
	} else {
		go func() {
			<-ctx.Done()
			serveErr <- nil
		}()
	}

	cmdDone := make(chan error, 1)
	switch {
	case len(command) > 0:
		echo, errOut := io.Writer(os.Stdout), io.Writer(os.Stderr)
		if o.tui {
			// The E2E tab shows the output; stderr goes into the feed.
			echo, errOut = io.Discard, feed.Writer()
		}
		go func() {
			err := e2efeed.RunCommand(ctx, feed, command, echo, errOut)
			s := feed.Summary()
			slog.Info("e2e command finished", "err", err, "pass", s.Counts.Pass, "fail", s.Counts.Fail, "skip", s.Counts.Skip)
			cmdDone <- err
			if o.exitWhenDone {
				stop()
			} else if !o.tui && ctx.Err() == nil {
				slog.Info("web UI still serving; interrupt to exit")
			}
		}()
	case o.e2eJSON != "":
		go func() {
			if err := e2efeed.FollowFile(ctx, feed, o.e2eJSON, !o.replay); err != nil {
				slog.Error("reading e2e events", "file", o.e2eJSON, "err", err)
			}
		}()
	}

	if o.tui {
		err := tui.Run(ctx, mon)
		stop()
		return errors.Join(err, <-serveErr, commandErr(cmdDone, command))
	}
	err = <-serveErr
	return errors.Join(err, commandErr(cmdDone, command))
}

// commandErr returns the e2e command's error once it has exited, so a failed
// run exits non-zero.
func commandErr(done <-chan error, command []string) error {
	if len(command) == 0 {
		return nil
	}
	select {
	case err := <-done:
		return err
	case <-time.After(35 * time.Second):
		return errors.New("e2e command did not exit")
	}
}

func contextName(o *options) string {
	if o.kubeContext != "" {
		return o.kubeContext
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = o.kubeconfig
	raw, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).RawConfig()
	if err != nil {
		return ""
	}
	return raw.CurrentContext
}
