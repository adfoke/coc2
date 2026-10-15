package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"go.uber.org/zap"

	"coc2/internal/common"
)

const (
	// pluginWaitDelay bounds the wait for a hook's output after the hook itself
	// is gone. Everything a plugin starts normally dies with its process group,
	// so this is the backstop for the two ways that can fail: a descendant that
	// escaped the group (setsid, a double fork), or a plugin that outlived its
	// budget through a path that skipped the kill. Without it, the wait is
	// unbounded.
	pluginWaitDelay = 2 * time.Second
)

type Plugin struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// agentConnectedEvent is the plugin-facing view of an agent hello. It is
// deliberately a separate type rather than the wire struct: the wire struct
// carries Token, which authenticates the entire fleet, and plugins are local
// executables that run without a sandbox (see plugins/README.md). Serialising
// the wire struct handed that fleet-wide credential to anyone who could read
// or replace a plugin. Everything a hook legitimately needs is here, so add
// fields here explicitly — never pass AgentHello straight through.
type agentConnectedEvent struct {
	AgentID     string    `json:"agent_id"`
	Hostname    string    `json:"hostname"`
	OS          string    `json:"os"`
	Arch        string    `json:"arch"`
	IPAddrs     []string  `json:"ip_addrs,omitempty"`
	Tags        []string  `json:"tags,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	Version     string    `json:"version"`
	ConnectedAt time.Time `json:"connected_at"`
}

type PluginManager struct {
	logger  *zap.Logger
	plugins []Plugin
}

func NewPluginManager(dir string, logger *zap.Logger) (*PluginManager, error) {
	pm := &PluginManager{logger: logger}
	if dir == "" {
		return pm, nil
	}

	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return pm, nil
		}
		return nil, err
	}
	if !info.IsDir() {
		return pm, nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		fileInfo, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if fileInfo.Mode()&0o111 == 0 {
			continue
		}
		pm.plugins = append(pm.plugins, Plugin{Name: entry.Name(), Path: path})
	}
	return pm, nil
}

func (pm *PluginManager) List() []Plugin {
	return append(make([]Plugin, 0, len(pm.plugins)), pm.plugins...)
}

func (pm *PluginManager) Trigger(hook string, payload any) {
	if len(pm.plugins) == 0 {
		return
	}

	body, err := json.Marshal(payload)
	if err != nil {
		pm.logger.Warn("marshal plugin payload", zap.String("hook", hook), zap.Error(err))
		return
	}

	for _, plugin := range pm.plugins {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), common.PluginHookTimeout)
			defer cancel()

			cmd := exec.CommandContext(ctx, plugin.Path, hook)
			cmd.Stdin = bytes.NewReader(body)

			// Run the hook in a process group of its own, so the timeout can
			// take the whole tree rather than just the process we started. A
			// shell that forks (any plugin that is not a single `exec`) leaves
			// the real work in a child; the default cancel only kills the
			// shell. Same shape as task execution in internal/agent/client.go.
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			cmd.Cancel = func() error {
				if cmd.Process == nil {
					return nil
				}
				// Negative pid targets the group. ESRCH means it is already
				// empty, which is the outcome we wanted.
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				// Then the process itself, so the budget still bounds the hook
				// if the group is not ours to kill (this pid is the group only
				// because of Setpgid above, and the child could have moved
				// itself). ErrProcessDone means it is already gone.
				if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					return err
				}
				return nil
			}

			// CombinedOutput waits for the hook's stdout/stderr pipes to reach
			// EOF, and a pipe only reaches EOF once *every* process holding it
			// has exited. Without this bound that wait has none: a plugin that
			// leaves a descendant behind (or one that escapes its process group
			// with setsid) would pin this goroutine, the payload and both pipe
			// descriptors until the descendant died, and — because the warning
			// below is only logged once this returns — would do it silently.
			cmd.WaitDelay = pluginWaitDelay

			started := time.Now()
			out, err := cmd.CombinedOutput()
			// The hook's process tree does not outlive the hook. Kill whatever
			// is left in the group: after a timeout that is usually a no-op,
			// but on the normal path it reaps a plugin that backgrounded
			// something and returned, which is what used to hold the pipes open
			// for the descendant's whole lifetime.
			closePluginGroup(cmd)
			elapsed := time.Since(started)

			if err == nil {
				// Debug, not info: the event path fires this once per plugin
				// per event, so at info a busy fleet would write a line per
				// metrics report. It exists because without it a working
				// plugin leaves no trace at all — "my hook never ran" and "my
				// hook ran and did nothing" are indistinguishable from the
				// server's logs. Run with log_level: debug to see it, and read
				// `duration` as the early warning that a hook is approaching
				// PluginHookTimeout.
				pm.logger.Debug("plugin hook ran",
					zap.String("plugin", plugin.Name),
					zap.String("hook", hook),
					zap.Duration("duration", elapsed),
				)
				return
			}
			message := "plugin hook failed"
			if errors.Is(err, exec.ErrWaitDelay) {
				// The hook exited but something it started still held its
				// output. Worth its own message: it is the signature of a
				// plugin that leaves processes behind, not of a plugin that
				// failed.
				message = "plugin hook left processes behind"
			}
			pm.logger.Warn(message,
				zap.String("plugin", plugin.Name),
				zap.String("hook", hook),
				zap.ByteString("output", out),
				zap.Duration("duration", elapsed),
				zap.Error(err),
			)
		}()
	}
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
