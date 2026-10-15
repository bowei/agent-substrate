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

package e2efeed

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"
)

// InjectJSON adds -json to a `go test` or hack/run-e2e.sh command line, just
// before its -args separator (or at the end), unless it is already there.
// Both pass flags before -args to go test.
func InjectJSON(args []string) []string {
	for _, a := range args {
		if a == "-args" {
			break
		}
		if a == "-json" || a == "--json" || a == "-json=true" || a == "--json=true" {
			return args
		}
	}
	i := slices.Index(args, "-args")
	if i < 0 {
		i = len(args)
	}
	out := slices.Clone(args[:i])
	out = append(out, "-json")
	return append(out, args[i:]...)
}

// RunCommand runs args with -json injected, feeding its stdout into f and
// echoing human-readable output to echo. stderr is passed through to errOut.
func RunCommand(ctx context.Context, f *Feed, args []string, echo, errOut io.Writer) error {
	if len(args) == 0 {
		return errors.New("no command to run")
	}
	args = InjectJSON(args)
	f.Begin(strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stderr = errOut
	cmd.Stdin = os.Stdin
	// Let the test binary finish its cleanups when interrupted.
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 30 * time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		f.Finish(err)
		return err
	}
	ingestErr := f.Ingest(stdout, echo)
	err = cmd.Wait()
	if err == nil {
		err = ingestErr
	}
	f.Finish(err)
	return err
}

// FollowFile reads a test2json file into f. With follow set it keeps reading
// as the file grows (and waits for it to appear) until ctx ends, as for the
// --jsonfile gotestsum writes during a run; otherwise it replays the file once.
func FollowFile(ctx context.Context, f *Feed, path string, follow bool) error {
	f.Begin(path)
	var file *os.File
	for {
		var err error
		file, err = os.Open(path)
		if err == nil {
			break
		}
		if !follow || !errors.Is(err, os.ErrNotExist) {
			f.Finish(err)
			return err
		}
		select {
		case <-ctx.Done():
			f.Finish(nil)
			return nil
		case <-time.After(time.Second):
		}
	}
	defer file.Close()
	if !follow {
		err := f.Ingest(file, nil)
		f.Finish(err)
		return err
	}
	err := f.Ingest(&tailReader{ctx: ctx, r: file}, nil)
	f.Finish(nil)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// tailReader turns EOF into a wait for more data until ctx ends.
type tailReader struct {
	ctx context.Context
	r   io.Reader
}

func (t *tailReader) Read(p []byte) (int, error) {
	for {
		n, err := t.r.Read(p)
		if n > 0 || (err != nil && err != io.EOF) {
			return n, err
		}
		select {
		case <-t.ctx.Done():
			return 0, io.EOF
		case <-time.After(250 * time.Millisecond):
		}
	}
}
