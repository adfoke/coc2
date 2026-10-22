package server

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// Executable plugins — the original contract, kept as the migration path and as
// the fallback for anything that genuinely needs to be a program of its own
// (a wrapper around an existing binary, a plugin in another language).
//
// They are deprecated in favour of JavaScript plugins and are now the *only*
// reason this package still touches os/exec. Two things stay exactly as they
// were, because two rounds of tests and a real-machine report are pinned to
// them: the process-group kill with its WaitDelay backstop, and the fact that
// the hook's process tree never outlives the hook.
//
// Output is still discarded on success: an executable plugin cannot return a
// decision, which is precisely one of the limitations the JavaScript contract
// removes (docs/plugin-refactor.md §1.3).
const (
	// pluginWaitDelay bounds the wait for a hook's output after the hook itself
	// is gone. Everything a plugin starts normally dies with its process group,
	// so this is the backstop for the two ways that can fail: a descendant that
	// escaped the group (setsid, a double fork), or a plugin that outlived its
	// budget through a path that skipped the kill. Without it, the wait is
	// unbounded.
	pluginWaitDelay = 2 * time.Second
)

type execPlugin struct {
	name   string
	path   string
	logger *zap.Logger
	// envelope is the shape this executable is fed: 1 = the raw payload (the
	// original contract, and the default), 2 = the schema-2 event envelope.
	envelope int
}

func (p *execPlugin) Info() Plugin {
	return Plugin{Name: p.name, Path: p.path, Kind: PluginKindExec}
}

// Has is true for every hook: an executable is told the hook name in argv[1]
// and decides for itself whether it cares. There is no way to ask it in
// advance, and running it is the existing contract.
func (p *execPlugin) Has(string) bool { return true }

// Decidable is false: this runner's output is discarded, so it has no way to
// return a decision. The dispatch gate skips it rather than spawning a process
// on the request path to learn nothing.
func (p *execPlugin) Decidable() bool { return false }

// Run starts the plugin and waits, bounded by ctx. out is unused: an executable
// plugin has no return channel (see the file comment).
func (p *execPlugin) Run(ctx context.Context, hook string, body []byte, _ any) error {
	cmd := exec.CommandContext(ctx, p.path, hook)
	cmd.Stdin = bytes.NewReader(body)
	if p.envelope == pluginEnvelopeSchema {
		cmd.Env = append(os.Environ(), execEnvelopeV2Env+"=2")
	}

	// Run the hook in a process group of its own, so the timeout can take the
	// whole tree rather than just the process we started. A shell that forks
	// (any plugin that is not a single `exec`) leaves the real work in a child;
	// the default cancel only kills the shell. Same shape as task execution in
	// internal/agent/client.go.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// Negative pid targets the group. ESRCH means it is already empty,
		// which is the outcome we wanted.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		// Then the process itself, so the budget still bounds the hook if the
		// group is not ours to kill (this pid is the group only because of
		// Setpgid above, and the child could have moved itself).
		// ErrProcessDone means it is already gone.
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
		return nil
	}

	// CombinedOutput waits for the hook's stdout/stderr pipes to reach EOF, and
	// a pipe only reaches EOF once *every* process holding it has exited.
	// Without this bound that wait has none: a plugin that leaves a descendant
	// behind (or one that escapes its process group with setsid) would pin this
	// goroutine, the payload and both pipe descriptors until the descendant
	// died, and — because the warning below is only logged once this returns —
	// would do it silently.
	cmd.WaitDelay = pluginWaitDelay

	started := time.Now()
	out, err := cmd.CombinedOutput()
	// The hook's process tree does not outlive the hook. Kill whatever is left
	// in the group: after a timeout that is usually a no-op, but on the normal
	// path it reaps a plugin that backgrounded something and returned, which is
	// what used to hold the pipes open for the descendant's whole lifetime.
	closePluginGroup(cmd)
	elapsed := time.Since(started)

	if err == nil {
		// Debug, not info: the event path fires this once per plugin per event,
		// so at info a busy fleet would write a line per metrics report. It
		// exists because without it a working plugin leaves no trace at all —
		// "my hook never ran" and "my hook ran and did nothing" are
		// indistinguishable from the server's logs.
		p.logger.Debug("plugin hook ran",
			zap.String("plugin", p.name),
			zap.String("hook", hook),
			zap.Duration("duration", elapsed),
		)
		return nil
	}

	message := "plugin hook failed"
	if errors.Is(err, exec.ErrWaitDelay) {
		// The hook exited but something it started still held its output. Worth
		// its own message: it is the signature of a plugin that leaves
		// processes behind, not of a plugin that failed.
		message = "plugin hook left processes behind"
	}
	p.logger.Warn(message,
		zap.String("plugin", p.name),
		zap.String("hook", hook),
		zap.ByteString("output", out),
		zap.Duration("duration", elapsed),
		zap.Error(err),
	)
	return err
}

// closePluginGroup kills anything still alive in the hook's process group. The
// group id equals the pid of the process the hook started, and killing a group
// whose leader has already exited still reaches its surviving members — which
// is the whole point, since they are the ones holding the output pipes.
// ESRCH (the group is already empty) is the common case and not an error.
func closePluginGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}

// execEnvelopeV2Env is set for executable plugins when they are given the
// schema-2 envelope, so a script can tell the two shapes apart without sniffing
// the JSON (docs/plugin-refactor.md §3.4).
const execEnvelopeV2Env = "COC2_PLUGIN_ENVELOPE"
