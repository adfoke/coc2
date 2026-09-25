package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestAgentConnectedPluginPayloadOmitsToken is the regression test for the
// agent_connected hook receiving the raw AgentHello, which carries Token — a
// single credential that authenticates the entire fleet. Plugins are local
// executables run without a sandbox, so serialising the wire struct handed
// that credential to anything able to read or replace a plugin file.
//
// The test drives a real hello through the agent plane and captures what the
// hook actually received, so it fails if anyone wires the wire struct back in.
func TestAgentConnectedPluginPayloadOmitsToken(t *testing.T) {
	dir := t.TempDir()
	captured := filepath.Join(dir, "captured.json")
	pluginPath := filepath.Join(dir, "capture.sh")
	// Absolute path: the plugin runs in the test process's cwd, not in dir.
	script := "#!/bin/sh\ncat > " + captured + "\n"
	if err := os.WriteFile(pluginPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write plugin: %v", err)
	}

	const token = "fleet-secret-token"
	svc, err := New(Config{
		ListenAddr:     ":0",
		OperatorListen: ":0",
		AuthToken:      token,
		PluginDir:      dir,
		DBPath:         filepath.Join(t.TempDir(), "test.db"),
		WriteWait:      2 * time.Second,
		PongWait:       2 * time.Second,
		PingPeriod:     time.Second,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer func() { _ = svc.store.Close() }()

	hello := baseHello()
	hello.Token = token
	hello.AgentID = "plugin-probe"
	hello.Tags = []string{"env=prod"}
	dialAgentWS(t, svc.agentEngine, hello)

	// The hook fires from the connection goroutine, so poll for its output.
	deadline := time.Now().Add(5 * time.Second)
	var body []byte
	for {
		body, err = os.ReadFile(captured)
		if err == nil && len(body) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("plugin never captured a payload: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if strings.Contains(string(body), token) {
		t.Fatalf("agent_connected payload leaked the shared token: %s", body)
	}
	// Guard against "fixing" it by handing over an empty payload instead.
	if !strings.Contains(string(body), "plugin-probe") {
		t.Fatalf("agent_connected payload lost the agent identity: %s", body)
	}
	if !strings.Contains(string(body), "env=prod") {
		t.Fatalf("agent_connected payload lost the tags: %s", body)
	}
}
