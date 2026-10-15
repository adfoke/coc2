package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"coc2/internal/protocol"
)

// This file pins the plugin module's contract as documented in
// plugins/README.md: which directory entries load, how a hook is invoked, how
// long a plugin may run, what happens when one fails, and what a payload may
// contain.
//
// The payload tests assert key *sets* rather than individual fields on purpose.
// The failure they exist to catch is somebody adding a field (to a struct, or
// by passing a wire struct straight through) and silently handing plugins data
// nobody decided to expose; a new key must make these tests fail so that change
// becomes a deliberate one.

// writePluginScript writes an executable hook script.
func writePluginScript(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write plugin %s: %v", path, err)
	}
}

// pluginService starts a service whose plugin directory is dir.
func pluginService(t *testing.T, dir string, logger *zap.Logger) *Service {
	t.Helper()
	if logger == nil {
		logger = zap.NewNop()
	}
	svc, err := New(Config{
		ListenAddr:     ":0",
		OperatorListen: ":0",
		AuthToken:      "test-token",
		PluginDir:      dir,
		DBPath:         filepath.Join(t.TempDir(), "test.db"),
		WriteWait:      2 * time.Second,
		PongWait:       2 * time.Second,
		PingPeriod:     time.Second,
	}, logger)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	t.Cleanup(func() { _ = svc.store.Close() })
	return svc
}

// waitForFile polls until path exists and is non-empty.
func waitForFile(t *testing.T, path string, within time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		raw, err := os.ReadFile(path)
		if err == nil && len(raw) > 0 {
			return raw
		}
		if time.Now().After(deadline) {
			t.Fatalf("file %s never appeared: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForCapture polls until a capture file holds one complete line. Hooks
// write the hook name and the payload in two writes, so "file is non-empty" is
// not yet "payload arrived".
func waitForCapture(t *testing.T, path string, within time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		raw, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(raw), "\n") {
			return strings.TrimRight(string(raw), "\n")
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture %s never completed a line: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForHookFailure waits for the warn the module logs when a plugin's run
// ends badly. It matches on the shared message prefix rather than one exact
// message, because the module distinguishes "the hook failed" from "the hook
// left processes behind" and a test should see either.
func waitForHookFailure(t *testing.T, logs *observer.ObservedLogs, within time.Duration) (observer.LoggedEntry, time.Duration) {
	t.Helper()
	start := time.Now()
	deadline := start.Add(within)
	for {
		if entries := logs.FilterMessageSnippet("plugin hook").All(); len(entries) > 0 {
			return entries[0], time.Since(start)
		}
		if time.Now().After(deadline) {
			t.Fatalf("a failing/hanging plugin was still not reported after %s", within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPluginDiscoveryContract pins which directory entries become plugins.
func TestPluginDiscoveryContract(t *testing.T) {
	t.Run("missing directory is not an error", func(t *testing.T) {
		pm, err := NewPluginManager(filepath.Join(t.TempDir(), "nope"), zap.NewNop())
		if err != nil {
			t.Fatalf("missing dir: %v", err)
		}
		if got := len(pm.List()); got != 0 {
			t.Fatalf("missing dir loaded %d plugins, want 0", got)
		}
	})

	t.Run("empty dir string disables plugins", func(t *testing.T) {
		pm, err := NewPluginManager("", zap.NewNop())
		if err != nil {
			t.Fatalf("empty dir: %v", err)
		}
		if got := len(pm.List()); got != 0 {
			t.Fatalf("empty dir loaded %d plugins, want 0", got)
		}
	})

	t.Run("a regular file is not a directory of plugins", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "not-a-dir")
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write file: %v", err)
		}
		pm, err := NewPluginManager(path, zap.NewNop())
		if err != nil {
			t.Fatalf("file as dir: %v", err)
		}
		if got := len(pm.List()); got != 0 {
			t.Fatalf("file as dir loaded %d plugins, want 0", got)
		}
	})

	t.Run("only executable regular files load, and not recursively", func(t *testing.T) {
		dir := t.TempDir()
		writePluginScript(t, filepath.Join(dir, "10-ok.sh"), "#!/bin/sh\nexit 0\n")
		if err := os.WriteFile(filepath.Join(dir, "20-not-exec.sh"), []byte("#!/bin/sh\n"), 0o644); err != nil {
			t.Fatalf("write non-exec: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "30-notes.txt"), []byte("hello"), 0o644); err != nil {
			t.Fatalf("write txt: %v", err)
		}
		sub := filepath.Join(dir, "subdir")
		if err := os.Mkdir(sub, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		writePluginScript(t, filepath.Join(sub, "40-nested.sh"), "#!/bin/sh\nexit 0\n")

		pm, err := NewPluginManager(dir, zap.NewNop())
		if err != nil {
			t.Fatalf("new plugin manager: %v", err)
		}
		list := pm.List()
		if len(list) != 1 || list[0].Name != "10-ok.sh" {
			t.Fatalf("loaded %+v, want exactly 10-ok.sh", list)
		}
	})

	t.Run("List returns a copy", func(t *testing.T) {
		dir := t.TempDir()
		writePluginScript(t, filepath.Join(dir, "a.sh"), "#!/bin/sh\nexit 0\n")
		pm, err := NewPluginManager(dir, zap.NewNop())
		if err != nil {
			t.Fatalf("new plugin manager: %v", err)
		}
		got := pm.List()
		got[0].Name = "tampered"
		if again := pm.List(); again[0].Name != "a.sh" {
			t.Fatalf("List handed out its backing slice: %+v", again)
		}
	})
}

// TestPluginInvocationContract pins how a hook is called: hook name as argv[1],
// event JSON on stdin, and every loaded plugin invoked once per trigger.
func TestPluginInvocationContract(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "capture-first.log")
	second := filepath.Join(dir, "capture-second.log")
	body := "#!/bin/sh\nprintf '%s\\t' \"$1\" >> \"CAPTURE\"\ncat >> \"CAPTURE\"\nprintf '\\n' >> \"CAPTURE\"\n"
	for name, capture := range map[string]string{"10-first.sh": first, "20-second.sh": second} {
		writePluginScript(t, filepath.Join(dir, name), strings.ReplaceAll(body, "CAPTURE", capture))
	}

	pm, err := NewPluginManager(dir, zap.NewNop())
	if err != nil {
		t.Fatalf("new plugin manager: %v", err)
	}
	payload := map[string]any{"task_id": "t-1", "exit_code": 0}
	pm.Trigger("task_result", payload)

	for _, capture := range []string{first, second} {
		got := waitForCapture(t, capture, 3*time.Second)
		hook, jsonBody, ok := strings.Cut(got, "\t")
		if !ok {
			t.Fatalf("%s: captured %q, want hook<TAB>json", capture, got)
		}
		if hook != "task_result" {
			t.Fatalf("%s: argv[1] = %q, want task_result", capture, hook)
		}
		var gotPayload map[string]any
		if err := json.Unmarshal([]byte(jsonBody), &gotPayload); err != nil {
			t.Fatalf("%s: stdin is not JSON: %v (%q)", capture, err, jsonBody)
		}
		if gotPayload["task_id"] != "t-1" {
			t.Fatalf("%s: payload = %v, want task_id t-1", capture, gotPayload)
		}
	}
}

// TestPluginTriggerReturnsBeforePluginFinishes: hooks are fire-and-forget, so a
// slow plugin must not stall the caller (it shares the connection goroutine).
func TestPluginTriggerReturnsBeforePluginFinishes(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "finished")
	writePluginScript(t, filepath.Join(dir, "slow.sh"),
		"#!/bin/sh\nsleep 2\necho done > \""+marker+"\"\n")

	pm, err := NewPluginManager(dir, zap.NewNop())
	if err != nil {
		t.Fatalf("new plugin manager: %v", err)
	}
	start := time.Now()
	pm.Trigger("metrics_report", map[string]any{"agent_id": "a1"})
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("Trigger blocked for %s; hooks must not block the caller", elapsed)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatalf("plugin finished before Trigger returned; it is not running in the background")
	}
	waitForFile(t, marker, 5*time.Second)
}

// TestSuccessfulHookIsVisibleAtDebugLevel: a plugin that works leaves a trace.
//
// The gap this closes: with only the failure warnings, a healthy hook wrote
// nothing at any level, so "the event never reached my plugin" and "my plugin
// ran and did nothing useful" looked identical from the server's logs. The
// trace is debug-only on purpose — it fires once per plugin per event, so at
// info a busy fleet would write a line per metrics report — which means the
// second half of this test (nothing at the default level) is as much of the
// contract as the first.
func TestSuccessfulHookIsVisibleAtDebugLevel(t *testing.T) {
	dir := t.TempDir()
	writePluginScript(t, filepath.Join(dir, "ok.sh"), "#!/bin/sh\nsleep 0.2\nexit 0\n")

	t.Run("logged at debug", func(t *testing.T) {
		core, logs := observer.New(zap.DebugLevel)
		pm, err := NewPluginManager(dir, zap.New(core))
		if err != nil {
			t.Fatalf("new plugin manager: %v", err)
		}
		pm.Trigger("metrics_report", map[string]any{"agent_id": "a1"})

		deadline := time.Now().Add(5 * time.Second)
		for {
			if entries := logs.FilterMessage("plugin hook ran").All(); len(entries) > 0 {
				entry := entries[0]
				ctx := entry.ContextMap()
				if ctx["plugin"] != "ok.sh" {
					t.Fatalf("trace names plugin %v, want ok.sh", ctx["plugin"])
				}
				if ctx["hook"] != "metrics_report" {
					t.Fatalf("trace names hook %v, want metrics_report", ctx["hook"])
				}
				// The duration is the early warning that a hook is drifting
				// towards PluginHookTimeout, so it has to be there and it has
				// to be plausible: the script sleeps 200ms.
				//
				// The observer hands back the field as a time.Duration, while
				// the file encoder renders it as seconds — accept both so this
				// asserts the value rather than the encoder.
				var dur time.Duration
				switch v := ctx["duration"].(type) {
				case time.Duration:
					dur = v
				case float64:
					dur = time.Duration(v * float64(time.Second))
				default:
					t.Fatalf("trace has no usable duration: %v (%T)", ctx["duration"], ctx["duration"])
				}
				if dur < 150*time.Millisecond || dur > 4*time.Second {
					t.Fatalf("duration %s does not match the 200ms hook", dur)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("a successful hook left no trace at debug level")
			}
			time.Sleep(20 * time.Millisecond)
		}
	})

	t.Run("silent at the default level", func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		pm, err := NewPluginManager(dir, zap.New(core))
		if err != nil {
			t.Fatalf("new plugin manager: %v", err)
		}
		pm.Trigger("metrics_report", map[string]any{"agent_id": "a1"})

		// Give it longer than the hook takes, then assert on nothing.
		time.Sleep(1500 * time.Millisecond)
		if entries := logs.All(); len(entries) != 0 {
			t.Fatalf("a successful hook logged at info level: %+v", entries)
		}
	})
}

// TestPluginTimeoutKillsHangingPlugin: a plugin that is still running at the
// end of its budget is killed, the failure is reported, and the work it would
// have done after the budget never happens.
//
// The script uses `exec` so the shell itself is replaced by the sleeper. The
// forking case (a shell that leaves a child behind) is covered by
// TestPluginProcessTreeDoesNotOutliveTheHook below, which is the shape that
// used to outlive the budget.
func TestPluginTimeoutKillsHangingPlugin(t *testing.T) {
	dir := t.TempDir()
	after := filepath.Join(dir, "after-timeout")
	writePluginScript(t, filepath.Join(dir, "hang.sh"),
		"#!/bin/sh\nexec sleep 120\n")
	// A second plugin proves one hung hook does not stop its siblings.
	healthy := filepath.Join(dir, "healthy.log")
	writePluginScript(t, filepath.Join(dir, "fine.sh"),
		"#!/bin/sh\nprintf '%s' \"$1\" > \""+healthy+"\"\n")

	core, logs := observer.New(zap.WarnLevel)
	pm, err := NewPluginManager(dir, zap.New(core))
	if err != nil {
		t.Fatalf("new plugin manager: %v", err)
	}
	pm.Trigger("task_result", map[string]any{"task_id": "t-hang"})

	entry, elapsed := waitForHookFailure(t, logs, 15*time.Second)
	if elapsed < 4*time.Second || elapsed > 12*time.Second {
		t.Fatalf("timeout fired after %s, want ~5s", elapsed)
	}
	if got := entry.ContextMap()["plugin"]; got != "hang.sh" {
		t.Fatalf("timeout reported for %v, want hang.sh", got)
	}
	if errField, _ := entry.ContextMap()["error"].(string); !strings.Contains(errField, "signal: killed") {
		t.Fatalf("timeout error = %v, want it to say the process was killed", entry.ContextMap()["error"])
	}
	if _, err := os.Stat(after); err == nil {
		t.Fatalf("the killed plugin still ran its post-timeout command")
	}
	if got := strings.TrimSpace(string(waitForFile(t, healthy, 3*time.Second))); got != "task_result" {
		t.Fatalf("sibling plugin saw %q, want task_result", got)
	}
}

// TestPluginProcessTreeDoesNotOutliveTheHook: the budget bounds the hook's whole
// process tree, not just the process the server started.
//
// This is the regression test for the measured defect this replaced: the module
// used to kill only the plugin process, while a descendant kept the stdout/stderr
// pipes open, so CombinedOutput blocked — and because the warning is logged only
// once it returns, the hook was reported late (measured ~8.3s against a 5s
// budget, and ~40s on a real host) or not at all.
//
// Both shapes are covered: a shell that forks its sleeper (no exec), and a shell
// that backgrounds a sleeper and then hangs itself. The second is what really
// held the pipes: with exec alone the group kill, not the pipe, does the work.
func TestPluginProcessTreeDoesNotOutliveTheHook(t *testing.T) {
	cases := []struct {
		name     string
		script   string
		wantsPid bool
	}{
		{
			name:   "shell forks the sleeper",
			script: "#!/bin/sh\nsleep 120\n",
		},
		{
			name:     "backgrounded sleeper plus hanging shell",
			script:   "#!/bin/sh\nsleep 120 &\necho $! > PIDFILE\nexec sleep 120\n",
			wantsPid: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "child.pid")
			writePluginScript(t, filepath.Join(dir, "spawner.sh"),
				strings.ReplaceAll(tc.script, "PIDFILE", pidFile))

			core, logs := observer.New(zap.WarnLevel)
			pm, err := NewPluginManager(dir, zap.New(core))
			if err != nil {
				t.Fatalf("new plugin manager: %v", err)
			}
			pm.Trigger("task_result", map[string]any{"task_id": "t-tree"})
			t.Cleanup(func() {
				if pid, ok := readPluginPid(pidFile); ok {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
			})

			entry, elapsed := waitForHookFailure(t, logs, 15*time.Second)
			// Budget plus the wait delay, plus slack for process teardown. The
			// point is that it is the budget that bounds the hook — before the
			// fix this was the descendant's lifetime instead.
			if elapsed > 5*time.Second+2*time.Second+2*time.Second {
				t.Fatalf("hook reported after %s, want the 5s budget to bound it", elapsed)
			}
			if got := entry.ContextMap()["plugin"]; got != "spawner.sh" {
				t.Fatalf("failure attributed to %v, want spawner.sh", got)
			}
			if errField, _ := entry.ContextMap()["error"].(string); !strings.Contains(errField, "signal: killed") {
				t.Fatalf("error = %v, want the process tree to have been killed", entry.ContextMap()["error"])
			}

			if !tc.wantsPid {
				return
			}
			pid, ok := readPluginPid(pidFile)
			if !ok {
				t.Fatalf("the plugin never recorded its descendant's pid")
			}
			// The descendant must be gone: this is what "kill the tree" buys,
			// and what the old code did not do.
			deadline := time.Now().Add(3 * time.Second)
			for syscall.Kill(pid, 0) == nil {
				if time.Now().After(deadline) {
					t.Fatalf("descendant pid %d survived the hook", pid)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

// TestPluginThatBackgroundsAndExitsIsReported: a plugin that succeeds but leaves
// a descendant holding its output is not silent. It used to block for as long as
// the descendant lived and log nothing (measured 12.3s for `sleep 12 &` followed
// by exit 0); now the wait is bounded, the leftover is reaped, and the operator
// gets a message that says what happened rather than nothing.
func TestPluginThatBackgroundsAndExitsIsReported(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	writePluginScript(t, filepath.Join(dir, "backgrounder.sh"),
		"#!/bin/sh\nsleep 120 &\necho $! > \""+pidFile+"\"\nexit 0\n")

	core, logs := observer.New(zap.WarnLevel)
	pm, err := NewPluginManager(dir, zap.New(core))
	if err != nil {
		t.Fatalf("new plugin manager: %v", err)
	}
	pm.Trigger("transfer_done", map[string]any{"transfer_id": "tx-bg"})
	t.Cleanup(func() {
		if pid, ok := readPluginPid(pidFile); ok {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	// The plugin exits immediately; only its leftover can delay the report.
	entry, elapsed := waitForHookFailure(t, logs, 15*time.Second)
	if elapsed > 5*time.Second {
		t.Fatalf("report took %s, want the wait delay to bound it (~2s)", elapsed)
	}
	if got := entry.Message; got != "plugin hook left processes behind" {
		t.Fatalf("message = %q, want the leftover-processes one", got)
	}
	if errField, _ := entry.ContextMap()["error"].(string); !strings.Contains(errField, "WaitDelay") {
		t.Fatalf("error = %v, want it to name the wait delay", entry.ContextMap()["error"])
	}

	pid, ok := readPluginPid(pidFile)
	if !ok {
		t.Fatalf("the plugin never recorded its descendant's pid")
	}
	deadline := time.Now().Add(3 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("descendant pid %d survived the hook", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// readPluginPid reads the pid a plugin script recorded for its descendant.
func readPluginPid(path string) (int, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, false
	}
	return pid, true
}

// TestFailingPluginIsIsolatedAndLogged: a plugin that fails loudly does not stop
// its siblings, and the operator can see why it failed.
func TestFailingPluginIsIsolatedAndLogged(t *testing.T) {
	dir := t.TempDir()
	healthy := filepath.Join(dir, "healthy.log")
	writePluginScript(t, filepath.Join(dir, "10-broken.sh"),
		"#!/bin/sh\necho 'hook exploded' >&2\nexit 7\n")
	writePluginScript(t, filepath.Join(dir, "20-healthy.sh"),
		"#!/bin/sh\nprintf '%s' \"$1\" > \""+healthy+"\"\n")

	core, logs := observer.New(zap.WarnLevel)
	pm, err := NewPluginManager(dir, zap.New(core))
	if err != nil {
		t.Fatalf("new plugin manager: %v", err)
	}
	pm.Trigger("transfer_done", map[string]any{"transfer_id": "tx-1"})

	if got := strings.TrimSpace(string(waitForFile(t, healthy, 3*time.Second))); got != "transfer_done" {
		t.Fatalf("healthy plugin saw hook %q, want transfer_done", got)
	}
	entry, _ := waitForHookFailure(t, logs, 3*time.Second)
	ctx := entry.ContextMap()
	if ctx["plugin"] != "10-broken.sh" {
		t.Fatalf("failure attributed to %v", ctx["plugin"])
	}
	if out, _ := ctx["output"].(string); !strings.Contains(out, "hook exploded") {
		t.Fatalf("plugin stderr not captured in the log: %v", ctx["output"])
	}
}

// TestPluginPayloadsAreWhitelisted drives the three hooks produced by frames on
// the agent plane and asserts each payload carries exactly the documented
// fields — nothing else, and no credential.
func TestPluginPayloadsAreWhitelisted(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "payloads")
	if err := os.Mkdir(out, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// One file per invocation: hooks fire concurrently, so a shared append
	// target would interleave.
	writePluginScript(t, filepath.Join(dir, "capture.sh"),
		"#!/bin/sh\ncat > \""+out+"/$1.$$.json\"\n")

	// The service is built with test-token, which is also what the hello below
	// authenticates with — the payloads must never carry it.
	const token = "test-token"
	svc := pluginService(t, dir, nil)

	hello := baseHello()
	hello.AgentID = "payload-probe"
	hello.Tags = []string{"env=prod"}
	conn, _, _ := dialAgentWS(t, svc.agentEngine, hello)

	sendFrame(t, conn, protocol.TypeTaskResult, protocol.TaskResult{
		TaskID:      "task-probe",
		AgentID:     "spoofed-identity",
		Status:      "success",
		ExitCode:    0,
		Stdout:      "hello stdout",
		Stderr:      "",
		DurationMS:  12,
		CompletedAt: time.Now().UTC(),
	})
	sendFrame(t, conn, protocol.TypeMetricsReport, protocol.MetricsReport{
		AgentID:            "spoofed-identity",
		Timestamp:          time.Now().UTC(),
		UptimeSecs:         42,
		CPUCount:           2,
		Goroutines:         7,
		ProcessMemoryBytes: 1024,
	})

	allowed := map[string]map[string]bool{
		"agent_connected": setOf("agent_id", "hostname", "os", "arch", "ip_addrs", "tags", "fingerprint", "version", "connected_at"),
		"task_result":     setOf("task_id", "agent_id", "status", "exit_code", "stdout", "stderr", "duration_ms", "completed_at"),
		"metrics_report":  setOf("agent_id", "timestamp", "uptime_secs", "cpu_count", "goroutines", "process_memory_bytes", "root_disk_total_bytes", "root_disk_free_bytes"),
	}

	deadline := time.Now().Add(5 * time.Second)
	seen := map[string]bool{}
	for len(seen) < len(allowed) {
		entries, err := os.ReadDir(out)
		if err != nil {
			t.Fatalf("read payloads dir: %v", err)
		}
		for _, e := range entries {
			hook, _, ok := strings.Cut(e.Name(), ".")
			if !ok {
				t.Fatalf("unexpected capture file %q", e.Name())
			}
			if seen[hook] {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(out, e.Name()))
			if err != nil {
				continue
			}
			fields, known := allowed[hook]
			if !known {
				t.Fatalf("unexpected hook %q", hook)
			}
			if strings.Contains(string(raw), token) {
				t.Fatalf("%s payload leaked the fleet token: %s", hook, raw)
			}
			var payload map[string]any
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Fatalf("%s: payload not JSON: %v", hook, err)
			}
			for key := range payload {
				if !fields[key] {
					t.Fatalf("%s payload carries undocumented field %q: %s", hook, key, raw)
				}
			}
			if payload["agent_id"] != "payload-probe" {
				t.Fatalf("%s payload has agent_id %v, want the authenticated identity", hook, payload["agent_id"])
			}
			seen[hook] = true
		}
		if time.Now().After(deadline) {
			t.Fatalf("hooks seen %v within %s, want all of %v", seen, 5*time.Second, allowed)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func setOf(keys ...string) map[string]bool {
	out := make(map[string]bool, len(keys))
	for _, k := range keys {
		out[k] = true
	}
	return out
}
