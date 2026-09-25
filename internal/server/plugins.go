package server

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"go.uber.org/zap"
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			cmd := exec.CommandContext(ctx, plugin.Path, hook)
			cmd.Stdin = bytes.NewReader(body)
			if out, err := cmd.CombinedOutput(); err != nil {
				pm.logger.Warn("plugin hook failed",
					zap.String("plugin", plugin.Name),
					zap.String("hook", hook),
					zap.ByteString("output", out),
					zap.Error(err),
				)
			}
		}()
	}
}
