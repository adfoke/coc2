package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Calcium-Ion/moejs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"coc2/internal/common"
	"coc2/internal/protocol"
)

// This file pins the JavaScript plugin contract (docs/plugin-js.md). Three of
// these tests exist because the property they check is the *reason* the engine
// was replaced, and each one fails if the engine or the host surface is changed
// without re-establishing it:
//
//   - the envelope carries no body (TestTaskResultEnvelopeCarriesNoBody);
//   - the memory limit cannot be caught by the plugin
//     (TestPluginMemoryLimitCannotBeCaught);
//   - plugins cannot see each other's state or events
//     (TestPluginIsolation, TestPluginEventStoreIsPerPlugin).

func writeJSPlugin(t *testing.T, dir, name, source string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		t.Fatalf("write plugin %s: %v", name, err)
	}
	return path
}

func newObservedManager(t *testing.T, dir string) (*PluginManager, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	pm, err := NewPluginManager(dir, zap.New(core))
	if err != nil {
		t.Fatalf("new plugin manager: %v", err)
	}
	return pm, logs
}

// waitConsole polls for one console line from a named plugin. Console output is
// asynchronous (it is written from the hook's goroutine), so the wait is on the
// line appearing rather than on a fixed sleep.
func waitConsole(t *testing.T, logs *observer.ObservedLogs, plugin, prefix string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, entry := range logs.FilterMessage("plugin console").All() {
			fields := entry.ContextMap()
			if fields["plugin"] != plugin {
				continue
			}
			message, _ := fields["message"].(string)
			if strings.HasPrefix(message, prefix) {
				return message
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no console line from %s with prefix %q; log: %s", plugin, prefix,
				dumpLogs(logs))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func dumpLogs(logs *observer.ObservedLogs) string {
	var b strings.Builder
	for _, entry := range logs.All() {
		b.WriteString(entry.Message)
		for k, v := range entry.ContextMap() {
			b.WriteString(" ")
			b.WriteString(k)
			b.WriteString("=")
			b.WriteString(strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(zapAny(v)), "\n", " ")))
		}
		b.WriteString("\n")
	}
	return b.String()
}

func zapAny(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return "<unprintable>"
	}
	return string(raw)
}

// TestPluginDiscoveryRecognisesBothKinds pins what the directory scan accepts
// after the refactor: *.js is compiled, an executable file stays an executable
// plugin, and a broken source is skipped rather than fatal.
func TestPluginDiscoveryRecognisesBothKinds(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "policy.js", `export function task_dispatch(event) { return { decision: "allow" }; }`)
	writeJSPlugin(t, dir, "broken.js", `export function task_result(event) {`)
	if err := os.WriteFile(filepath.Join(dir, "shell.sh"), []byte("#!/bin/sh\ncat >/dev/null\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not a plugin\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	pm, logs := newObservedManager(t, dir)
	list := pm.List()

	byName := map[string]Plugin{}
	for _, p := range list {
		byName[p.Name] = p
	}
	if len(list) != 2 {
		t.Fatalf("loaded %d plugins, want 2 (policy.js + shell.sh): %+v", len(list), list)
	}
	if got := byName["policy.js"]; got.Kind != PluginKindJS {
		t.Fatalf("policy.js kind = %q, want js", got.Kind)
	} else if len(got.Hooks) != 1 || got.Hooks[0] != "task_dispatch" {
		t.Fatalf("policy.js hooks = %v, want [task_dispatch]", got.Hooks)
	}
	if got := byName["shell.sh"]; got.Kind != PluginKindExec {
		t.Fatalf("shell.sh kind = %q, want exec", got.Kind)
	}
	if _, ok := byName["broken.js"]; ok {
		t.Fatal("a plugin with a syntax error was loaded; it must be skipped")
	}
	if logs.FilterMessage("load javascript plugin").Len() == 0 {
		t.Fatal("skipping a broken plugin was silent; it must be logged")
	}
}

// TestDocumentedExamplePluginLoads keeps plugins/example-plugin.js.sample honest.
// An example that does not compile is worse than no example: it is the first
// thing a plugin author copies.
func TestDocumentedExamplePluginLoads(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "plugins", "example-plugin.js.sample"))
	if err != nil {
		t.Fatalf("read the documented example: %v", err)
	}

	dir := t.TempDir()
	writeJSPlugin(t, dir, "example.js", string(source))

	pm, logs := newObservedManager(t, dir)
	list := pm.List()
	if len(list) != 1 {
		t.Fatalf("loaded %d plugins, want 1: %+v", len(list), list)
	}

	// Reported in pluginHookNames order, which is the canonical list.
	want := []string{"task_result", "metrics_report", "task_dispatch"}
	if got := list[0].Hooks; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("example hooks = %v, want %v", got, want)
	}

	// The deny path the example documents has to work as documented.
	decision := pm.Decide(context.Background(), "task_dispatch", protocol.Task{
		ID: "t-1", AgentID: "ag-1", Type: "shell", Command: "rm -rf /var", TimeoutSecs: 5,
	}, common.PluginDecisionTimeout)
	if decision.Allowed || decision.Plugin != "example.js" {
		t.Fatalf("the example did not deny a blocked command: %+v", decision)
	}

	decision = pm.Decide(context.Background(), "task_dispatch", protocol.Task{
		ID: "t-2", AgentID: "ag-1", Type: "shell", Command: "echo hi", TimeoutSecs: 5,
	}, common.PluginDecisionTimeout)
	if !decision.Allowed {
		t.Fatalf("the example denied a harmless command: %+v", decision)
	}

	// The task_result branch decides from the hint alone on success, and only
	// fetches the body on failure. Both halves are asserted through the console
	// line, because that is the part a reader copies: the shipped example said
	// `status === "ok"` while the protocol says "success", so every successful
	// task took the failure path — fetching 1 MiB per event and logging
	// "failed ... exit=0". Found by running the clone from GitHub, not by a test.
	pm.Trigger("task_result", protocol.TaskResult{
		TaskID: "t-ok", AgentID: "ag-1", Status: "success", ExitCode: 0,
		Stdout: strings.Repeat("q", 4096),
	})
	if got := waitConsole(t, logs, "example.js", "ok "); !strings.Contains(got, "t-ok") {
		t.Fatalf("%q: a successful result did not take the hint-only path", got)
	}

	pm.Trigger("task_result", protocol.TaskResult{
		TaskID: "t-bad", AgentID: "ag-1", Status: "failed", ExitCode: 3, Stderr: "boom-detail",
	})
	got := waitConsole(t, logs, "example.js", "failed ")
	if !strings.Contains(got, "t-bad") || !strings.Contains(got, "boom-detail") {
		t.Fatalf("%q: the failure path did not fetch the body", got)
	}
}

// TestEnvelopeCarriesTheHintNotThePayload pins the envelope's exact key sets.
// The failure this catches is someone widening what a plugin sees — either by
// adding a field to the hint, or by handing the payload struct straight through.
func TestEnvelopeCarriesTheHintNotThePayload(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "shape.js", `
export function task_result(event) {
  console.log("envelope=" + Object.keys(event).sort().join(","));
  console.log("hint=" + Object.keys(event.hint).sort().join(","));
}`)

	pm, logs := newObservedManager(t, dir)
	pm.Trigger("task_result", protocol.TaskResult{
		TaskID: "t-1", AgentID: "ag-1", Status: "failed", ExitCode: 2,
		Stdout: "hello", Stderr: "boom", DurationMS: 12, CompletedAt: time.Now().UTC(),
	})

	envelope := waitConsole(t, logs, "shape.js", "envelope=")
	wantEnvelope := "at,event,hint,id,ref,schema"
	if got := strings.TrimPrefix(envelope, "envelope="); got != wantEnvelope {
		t.Fatalf("envelope keys = %q, want %q", got, wantEnvelope)
	}

	hint := waitConsole(t, logs, "shape.js", "hint=")
	wantHint := "agent_id,duration_ms,exit_code,status,stderr_bytes,stdout_bytes,task_id"
	if got := strings.TrimPrefix(hint, "hint="); got != wantHint {
		t.Fatalf("hint keys = %q, want exactly %q", got, wantHint)
	}
}

// TestTaskResultEnvelopeCarriesNoBody is the performance promise of the
// envelope, and the one that stops the engine from turning every task result
// into a heap allocation per plugin: 1 MiB of stdout must not appear in the
// envelope at all.
func TestTaskResultEnvelopeCarriesNoBody(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "size.js", `
export function task_result(event) {
  console.log("bytes=" + JSON.stringify(event).length);
  console.log("stdout_bytes=" + event.hint.stdout_bytes);
  const full = JSON.parse(coc2.event(event.id));
  console.log("fetched=" + full.stdout.length);
}`)

	pm, logs := newObservedManager(t, dir)
	pm.Trigger("task_result", protocol.TaskResult{
		TaskID: "t-big", AgentID: "ag-1", Status: "ok",
		Stdout: strings.Repeat("x", 1<<20),
	})

	line := waitConsole(t, logs, "size.js", "bytes=")
	size, err := strconv.Atoi(strings.TrimPrefix(line, "bytes="))
	if err != nil {
		t.Fatalf("parse envelope size from %q: %v", line, err)
	}
	if size >= 4096 {
		t.Fatalf("envelope is %d bytes for a 1 MiB result; the body is inline", size)
	}

	if got := waitConsole(t, logs, "size.js", "stdout_bytes="); got != "stdout_bytes=1048576" {
		t.Fatalf("%s, want stdout_bytes=1048576", got)
	}
	// …and the bytes are still reachable on demand, which is what makes the
	// small envelope a trade rather than a loss.
	if got := waitConsole(t, logs, "size.js", "fetched="); got != "fetched=1048576" {
		t.Fatalf("%s, want fetched=1048576", got)
	}
}

// TestPluginCannotFetchAnEventItWasNotHanded pins the capability boundary of an
// event id: an id resolves only for the plugins the event was dispatched to, and
// nothing that is not an id resolves at all.
func TestPluginEventStoreIsPerPlugin(t *testing.T) {
	store := newPluginEventStore(time.Minute, 1<<20)
	id := store.add("task_result", []byte(`{"task_id":"t-1","stdout":"secret"}`))
	store.allow(id, "a.js")

	if _, err := store.get(id, "a.js"); err != nil {
		t.Fatalf("the plugin the event was dispatched to cannot read it: %v", err)
	}
	if _, err := store.get(id, "b.js"); !errors.Is(err, errPluginEventNotFound) {
		t.Fatalf("another plugin reading the event returned %v, want not-found", err)
	}
	if _, err := store.get("/etc/passwd", "a.js"); !errors.Is(err, errPluginEventNotFound) {
		t.Fatalf("a path-shaped id returned %v, want not-found", err)
	}

	// Expiry: an id is a capability, so it must not stay one.
	store.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := store.get(id, "a.js"); !errors.Is(err, errPluginEventNotFound) {
		t.Fatalf("an expired id still resolved: %v", err)
	}

	// The memory cap drops the oldest bodies rather than growing without bound.
	tiny := newPluginEventStore(time.Minute, 64)
	first := tiny.add("task_result", bytes.Repeat([]byte("a"), 40))
	tiny.allow(first, "a.js")
	tiny.add("task_result", bytes.Repeat([]byte("b"), 40))
	if _, err := tiny.get(first, "a.js"); !errors.Is(err, errPluginEventNotFound) {
		t.Fatalf("the oldest body survived the cap: %v", err)
	}
	if count, size := tiny.size(); count != 1 || size != 40 {
		t.Fatalf("store holds %d bodies / %d bytes, want 1 / 40", count, size)
	}
}

// TestPluginEventFunctionRejectsForeignIDs runs the same boundary through the
// host function a plugin actually calls.
func TestPluginEventFunctionRejectsForeignIDs(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "fetch.js", `
export function task_result(event) {
  try {
    JSON.parse(coc2.event("evt_deadbeefdeadbeef"));
    console.log("foreign=allowed");
  } catch (err) {
    console.log("foreign=blocked:" + err.message);
  }
  try {
    coc2.event("/etc/passwd");
    console.log("path=allowed");
  } catch (err) {
    console.log("path=blocked:" + err.message);
  }
  console.log("envelope=" + coc2.envelope);
}`)

	pm, logs := newObservedManager(t, dir)
	pm.Trigger("task_result", protocol.TaskResult{TaskID: "t-1", Status: "ok"})

	if got := waitConsole(t, logs, "fetch.js", "foreign="); !strings.HasPrefix(got, "foreign=blocked:") {
		t.Fatalf("%s: an id this server never issued resolved", got)
	}
	if got := waitConsole(t, logs, "fetch.js", "path="); !strings.HasPrefix(got, "path=blocked:") {
		t.Fatalf("%s: a path-shaped argument resolved", got)
	}
	if got := waitConsole(t, logs, "fetch.js", "envelope="); got != "envelope=2" {
		t.Fatalf("%s, want envelope=2", got)
	}
}

// TestPluginMemoryLimitCannotBeCaught is the property the engine swap rests on:
// the limit is enforced as an interrupt, and an interrupt runs no catch block.
// If this test ever fails, the memory limit is advisory and the "each plugin is
// capped" claim is false.
func TestPluginMemoryLimitCannotBeCaught(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "hog.js", `
export function task_result(event) {
  try {
    const held = [];
    for (;;) { held.push(new Array(100000).fill(7)); }
  } catch (err) {
    console.log("caught=" + err.name);
  }
}`)

	pm, logs := newObservedManager(t, dir)
	runner := pm.runners[0]

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := runner.Run(ctx, "task_result", []byte(`{"schema":2,"hint":{}}`), nil)
	if !errors.Is(err, moejs.ErrMemoryLimit) {
		t.Fatalf("run returned %v, want ErrMemoryLimit", err)
	}

	var limitErr *moejs.MemoryLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("error %T does not carry a *MemoryLimitError", err)
	}
	if limitErr.Limit != common.PluginMemoryLimitBytes {
		t.Fatalf("hit a limit of %d bytes, want %d", limitErr.Limit, common.PluginMemoryLimitBytes)
	}
	if lines := logs.FilterMessage("plugin console").All(); len(lines) != 0 {
		t.Fatalf("the plugin's catch block ran: %s", dumpLogs(logs))
	}
}

// TestPluginDynamicCodeIsDisabled pins the "no code that a reader cannot
// review" half of the host surface: eval and new Function are off.
func TestPluginDynamicCodeIsDisabled(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "dyn.js", `
export function task_result(event) {
  try { eval("1 + 1"); console.log("eval=allowed"); }
  catch (err) { console.log("eval=blocked:" + err.name); }
  try { new Function("return 1"); console.log("function=allowed"); }
  catch (err) { console.log("function=blocked:" + err.name); }
}`)

	pm, logs := newObservedManager(t, dir)
	pm.Trigger("task_result", protocol.TaskResult{TaskID: "t-1", Status: "ok"})

	if got := waitConsole(t, logs, "dyn.js", "eval="); got != "eval=blocked:EvalError" {
		t.Fatalf("%s, want eval=blocked:EvalError", got)
	}
	if got := waitConsole(t, logs, "dyn.js", "function="); got != "function=blocked:EvalError" {
		t.Fatalf("%s, want function=blocked:EvalError", got)
	}
}

// TestPluginHookTimesOut pins the interrupt path: a plugin that never returns
// stops when its budget ends, and the caller gets the context's cause.
func TestPluginHookTimesOut(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "spin.js", `export function task_result(event) { for (;;) {} }`)

	pm, _ := newObservedManager(t, dir)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := pm.runners[0].Run(ctx, "task_result", []byte(`{"schema":2,"hint":{}}`), nil)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run returned %v, want DeadlineExceeded", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("the interrupt took %s; the budget was 100ms", elapsed)
	}
}

// TestTimedOutAsyncHookIsLogged pins the observability half of the interrupt: an
// asynchronous hook that runs out of budget must leave a line saying so.
//
// This is not a nicety. A JavaScript hook has no exit code and no stderr, so
// without this line "my hook hits its budget on every event" and "my hook is
// never called" look identical in the server's log — the exact failure the
// plugins/README.md observability table exists to rule out. (Found by running a
// spinning plugin against a real server: the heartbeat kept working, and nothing
// in the log said why the hook never finished.)
func TestTimedOutAsyncHookIsLogged(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "spin.js", `export function metrics_report(event) { for (;;) {} }`)

	pm, logs := newObservedManager(t, dir)
	start := time.Now()
	pm.Trigger("metrics_report", protocol.MetricsReport{AgentID: "ag-1"})

	deadline := start.Add(common.PluginHookTimeout + 5*time.Second)
	for {
		if entries := logs.FilterMessage("plugin hook timed out").All(); len(entries) > 0 {
			fields := entries[0].ContextMap()
			if fields["plugin"] != "spin.js" || fields["hook"] != "metrics_report" {
				t.Fatalf("warn names the wrong hook: %v", fields)
			}
			if _, ok := fields["duration"]; !ok {
				t.Fatalf("warn has no duration: %v", fields)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a hook that ran out of budget was silent; log: %s", dumpLogs(logs))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPluginThrownErrorBecomesAWarnWithStack pins the other failure shape: a
// plugin that throws is reported with its JavaScript stack, not as a silent
// no-op.
func TestPluginThrownErrorBecomesAWarnWithStack(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "boom.js", `
export function task_result(event) {
  throw new Error("deliberate-" + event.hint.task_id);
}`)

	pm, logs := newObservedManager(t, dir)
	pm.Trigger("task_result", protocol.TaskResult{TaskID: "t-1", Status: "ok"})

	deadline := time.Now().Add(5 * time.Second)
	for {
		entries := logs.FilterMessage("plugin hook failed").All()
		if len(entries) > 0 {
			fields := entries[0].ContextMap()
			if msg, _ := fields["error"].(string); !strings.Contains(msg, "deliberate-t-1") {
				t.Fatalf("error field = %v, want the plugin's own message", fields["error"])
			}
			if _, ok := fields["js_stack"]; !ok {
				t.Fatalf("no JavaScript stack in the warn: %v", fields)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a throwing plugin was silent; log: %s", dumpLogs(logs))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestHostileStackGetterCannotStallTheServer pins the budget on reading a
// thrown error's stack. The property is plugin data, so reading it runs plugin
// code — a plugin (or a plugin author's bug) that redefines it as an endless
// getter must not turn "log the failure" into a hung goroutine.
func TestHostileStackGetterCannotStallTheServer(t *testing.T) {
	dir := t.TempDir()
	// A thrown object with its own endless `stack` getter. (Patching
	// Error.prototype does not work — the engine's builtins are frozen and
	// shared, which TestSharedIntrinsicsAreFrozen pins below — but a thrown
	// literal can define anything it likes.)
	writeJSPlugin(t, dir, "evil.js", `
export function task_result(event) {
  throw { get stack() { for (;;) {} } };
}`)

	pm, logs := newObservedManager(t, dir)

	// The bar is deliberately far below PluginHookTimeout. The hook's own
	// budget would also stop the read (the interrupt installed by Run is still
	// armed), so waiting the full 5 s would not tell the two apart: the
	// property under test is that reading the stack costs its own small budget,
	// not the plugin's entire budget — otherwise one hostile getter holds a
	// runtime and an admission slot for 5 s on every event.
	limit := common.PluginHookTimeout / 5
	start := time.Now()
	pm.Trigger("task_result", protocol.TaskResult{TaskID: "t-1", Status: "ok"})

	deadline := start.Add(limit)
	for {
		if entries := logs.FilterMessage("plugin hook failed").All(); len(entries) > 0 {
			if elapsed := time.Since(start); elapsed > limit {
				t.Fatalf("the failure was reported only after %s: the stack read ran on the "+
					"hook's budget, not on its own (%s)", elapsed, jsStackBudget)
			}
			// No stack is the correct answer here: the read was interrupted.
			if _, ok := entries[0].ContextMap()["js_stack"]; ok {
				t.Fatalf("a stack was reported for an interrupted read: %v", entries[0].ContextMap())
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a hostile stack getter stalled the failure path past %s; log: %s",
				limit, dumpLogs(logs))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSharedIntrinsicsAreFrozen pins the capability the engine gives us for
// free: builtins are deeply frozen and shared between runtimes, so a plugin
// cannot patch Array.prototype for everyone, and cannot redefine
// Error.prototype.stack to break the failure path. The second half matters
// because reading that property is how jsErrorStack gets a line number.
func TestSharedIntrinsicsAreFrozen(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "patch.js", `
export function task_result(event) {
  try {
    Object.defineProperty(Error.prototype, "stack", { get() { for (;;) {} }, configurable: true });
    console.log("patch=allowed");
  } catch (err) {
    console.log("patch=blocked:" + err.name);
  }
  try {
    Array.prototype.push = function () { return 0; };
    console.log("array=allowed");
  } catch (err) {
    console.log("array=blocked:" + err.name);
  }
}`)

	pm, logs := newObservedManager(t, dir)
	pm.Trigger("task_result", protocol.TaskResult{TaskID: "t-1", Status: "ok"})

	if got := waitConsole(t, logs, "patch.js", "patch="); !strings.HasPrefix(got, "patch=blocked:") {
		t.Fatalf("%s: Error.prototype was patchable from a plugin", got)
	}
	if got := waitConsole(t, logs, "patch.js", "array="); !strings.HasPrefix(got, "array=blocked:") {
		t.Fatalf("%s: Array.prototype was patchable from a plugin", got)
	}
}

// TestPluginIsolation pins the per-plugin runtime boundary: state one plugin
// keeps (a global) is invisible to another plugin in the same process.
func TestPluginIsolation(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "a.js", `
export function task_result(event) { globalThis.leak = "from-a"; }`)
	writeJSPlugin(t, dir, "b.js", `
export function task_result(event) { console.log("leak=" + typeof globalThis.leak); }`)

	pm, logs := newObservedManager(t, dir)
	pm.Trigger("task_result", protocol.TaskResult{TaskID: "t-1", Status: "ok"})

	if got := waitConsole(t, logs, "b.js", "leak="); got != "leak=undefined" {
		t.Fatalf("%s: one plugin saw another plugin's global", got)
	}
}

// TestTaskDispatchDenyStopsTheTask is the end-to-end decision path: policy
// plugin → 403 → no task row → nothing on the wire → an audit row that says who
// refused it.
func TestTaskDispatchDenyStopsTheTask(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "policy.js", `
export function task_dispatch(event) {
  if (event.hint.command.includes("rm -rf")) {
    return { decision: "deny", reason: "refusing destructive command" };
  }
  return { decision: "allow" };
}`)

	svc := pluginService(t, dir, nil)

	sends := make(chan wsFrame, 8)
	svc.clients["agent-1"] = &agentConn{id: "agent-1", send: sends, done: make(chan struct{}), service: svc}

	code, body := postTask(t, svc, map[string]any{
		"agent_id": "agent-1",
		"command":  "rm -rf /var/lib/important",
	})
	if code != http.StatusForbidden {
		t.Fatalf("status = %d body=%s, want 403", code, body)
	}

	var resp taskDispatchResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.Denied || resp.DeniedBy != "policy.js" {
		t.Fatalf("response = %+v, want denied by policy.js", resp)
	}
	if !strings.Contains(resp.DenyReason, "destructive") {
		t.Fatalf("deny reason = %q", resp.DenyReason)
	}

	if _, ok, err := svc.store.Task(resp.TaskID); err != nil {
		t.Fatalf("look up task: %v", err)
	} else if ok {
		t.Fatal("a denied task was persisted")
	}
	select {
	case frame := <-sends:
		t.Fatalf("a denied task reached the agent: %s", frame.data)
	default:
	}

	logs, err := svc.store.RecentOpLogs(OpLogQuery{Limit: 10})
	if err != nil {
		t.Fatalf("read oplog: %v", err)
	}
	var found *OpLogEntry
	for i := range logs {
		if logs[i].Plane == "plugin" && logs[i].Path == "task_dispatch" {
			found = &logs[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("no audit row for the denial; oplog=%+v", logs)
	}
	if found.OK || found.Actor != "policy.js" || found.Ref != resp.TaskID {
		t.Fatalf("audit row = %+v", found)
	}

	// The same service still dispatches what the policy allows.
	code, body = postTask(t, svc, map[string]any{"agent_id": "agent-1", "command": "echo fine"})
	if code != http.StatusAccepted {
		t.Fatalf("allowed task status = %d body=%s", code, body)
	}
}

// TestExecPluginIsNotAskedToDecide pins that the dispatch gate does not spawn a
// process for a plugin that cannot possibly answer. An executable plugin's
// output is discarded, so a synchronous call would cost a fork on the request
// path and leave a misleading "decision=timeout" row behind.
func TestExecPluginIsNotAskedToDecide(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "ran")
	script := "#!/bin/sh\ntouch " + marker + "\ncat >/dev/null\n"
	if err := os.WriteFile(filepath.Join(dir, "gate.sh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	svc := pluginService(t, dir, nil)
	svc.clients["agent-1"] = &agentConn{id: "agent-1", send: make(chan wsFrame, 8), done: make(chan struct{}), service: svc}

	code, body := postTask(t, svc, map[string]any{"agent_id": "agent-1", "command": "echo hi"})
	if code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", code, body)
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			t.Fatal("an executable plugin was run synchronously on the dispatch path")
		}
		time.Sleep(20 * time.Millisecond)
	}

	logs, err := svc.store.RecentOpLogs(OpLogQuery{Limit: 10})
	if err != nil {
		t.Fatalf("read oplog: %v", err)
	}
	for _, entry := range logs {
		if entry.Plane == "plugin" {
			t.Fatalf("an executable plugin produced a decision audit row: %+v", entry)
		}
	}
}

// TestTaskDispatchTimeoutFailsOpen pins the fail-open rule and its visibility:
// a wedged policy plugin delays the dispatch by at most its budget, the task
// still goes out, and the audit trail says the answer was a timeout.
func TestTaskDispatchTimeoutFailsOpen(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "slow.js", `
export function task_dispatch(event) { for (;;) {} }`)

	svc := pluginService(t, dir, nil)
	sends := make(chan wsFrame, 8)
	svc.clients["agent-1"] = &agentConn{id: "agent-1", send: sends, done: make(chan struct{}), service: svc}

	start := time.Now()
	code, body := postTask(t, svc, map[string]any{"agent_id": "agent-1", "command": "echo hi"})
	elapsed := time.Since(start)

	if code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s, want 202 (fail open)", code, body)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("the decision budget did not bound the request: took %s for a %s budget",
			elapsed, common.PluginDecisionTimeout)
	}
	select {
	case <-sends:
	case <-time.After(2 * time.Second):
		t.Fatal("the task was not dispatched after the plugin timed out")
	}

	logs, err := svc.store.RecentOpLogs(OpLogQuery{Limit: 10})
	if err != nil {
		t.Fatalf("read oplog: %v", err)
	}
	found := false
	for _, entry := range logs {
		if entry.Plane == "plugin" && strings.Contains(entry.ParamsSummary, "decision=timeout") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the fail-open was not audited as a timeout; oplog=%+v", logs)
	}
}

// TestTaskDispatchFailsOpenWhenNoSlotIsFree covers the other half of fail-open:
// the decision hook is never dropped by admission control, it *waits* — and when
// the wait outlives the budget it allows and says so. Without this test that
// branch is unexercised, and "the gate silently starts denying when the server
// is busy" is exactly the failure a busy day would find first.
func TestTaskDispatchFailsOpenWhenNoSlotIsFree(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "policy.js", `export function task_dispatch(event) { return { decision: "deny", reason: "should not run" }; }`)

	svc := pluginService(t, dir, nil)
	sends := make(chan wsFrame, 8)
	svc.clients["agent-1"] = &agentConn{id: "agent-1", send: sends, done: make(chan struct{}), service: svc}

	for i := 0; i < cap(svc.plugins.sem); i++ {
		svc.plugins.sem <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(svc.plugins.sem); i++ {
			<-svc.plugins.sem
		}
	}()

	start := time.Now()
	code, body := postTask(t, svc, map[string]any{"agent_id": "agent-1", "command": "echo hi"})
	elapsed := time.Since(start)

	if code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s, want 202 (fail open when no slot is free)", code, body)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("waiting for a slot was not bounded by the decision budget: %s", elapsed)
	}

	logs, err := svc.store.RecentOpLogs(OpLogQuery{Limit: 10})
	if err != nil {
		t.Fatalf("read oplog: %v", err)
	}
	found := false
	for _, entry := range logs {
		if entry.Plane == "plugin" && strings.Contains(entry.ParamsSummary, "no plugin slot") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the saturated-gate fail-open was not audited; oplog=%+v", logs)
	}
}

// TestPluginEventEndpointServesRef pins the out-of-process half of the ref: the
// envelope's URL resolves to the body the plugin would have fetched.
func TestPluginEventEndpointServesRef(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "gate.js", `export function task_dispatch(event) { return { decision: "deny", reason: "no" }; }`)

	svc := pluginService(t, dir, nil)
	svc.clients["agent-1"] = &agentConn{id: "agent-1", send: make(chan wsFrame, 4), done: make(chan struct{}), service: svc}

	pm := svc.plugins
	payload := []byte(`{"task_id":"t-9","stdout":"the interesting part"}`)
	id := pm.events.add("task_result", payload)

	req := httptest.NewRequest(http.MethodGet, pluginEventRef(id), nil)
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != string(payload) {
		t.Fatalf("body = %s, want %s", rec.Body.String(), payload)
	}

	req = httptest.NewRequest(http.MethodGet, pluginEventRef("evt_nope"), nil)
	setTestAuth(req)
	rec = httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown id status = %d, want 404", rec.Code)
	}
}

// TestAsyncHooksAreDroppedUnderOverload pins admission control. An unbounded
// fan-out was a structural problem before the refactor; the boundary has to be
// observable rather than implied.
func TestAsyncHooksAreDroppedUnderOverload(t *testing.T) {
	dir := t.TempDir()
	writeJSPlugin(t, dir, "noop.js", `export function metrics_report(event) {}`)

	pm, logs := newObservedManager(t, dir)

	// Fill every slot the manager has: this is the state a fleet-wide
	// reconnect produces, without having to fake hundreds of agents.
	for i := 0; i < cap(pm.sem); i++ {
		pm.sem <- struct{}{}
	}
	pm.Trigger("metrics_report", protocol.MetricsReport{AgentID: "ag-1"})
	for i := 0; i < cap(pm.sem); i++ {
		<-pm.sem
	}

	if logs.FilterMessage("plugin hook dropped: too many hooks in flight").Len() == 0 {
		t.Fatalf("an event past the cap was not reported as dropped: %s", dumpLogs(logs))
	}
}

// TestExecPluginEnvelopeV2 pins the migration switch for executable plugins:
// with -plugins-envelope=2 they get the same envelope, plus the env var that
// lets a script tell the shapes apart.
func TestExecPluginEnvelopeV2(t *testing.T) {
	dir := t.TempDir()
	capture := filepath.Join(t.TempDir(), "captured.json")
	script := "#!/bin/sh\nprintf '%s\\n' \"$COC2_PLUGIN_ENVELOPE\" > " + capture + "\ncat >> " + capture + "\n"
	if err := os.WriteFile(filepath.Join(dir, "cap.sh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	pm, err := NewPluginManagerWithOptions(dir, zap.NewNop(), PluginOptions{Envelope: pluginEnvelopeSchema})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	pm.Trigger("task_result", protocol.TaskResult{TaskID: "t-1", Status: "ok"})

	// The script writes the env var first and the stdin copy second, so "the
	// file is non-empty" is not yet "the envelope arrived": wait for the
	// envelope itself.
	var raw []byte
	deadline := time.Now().Add(5 * time.Second)
	for {
		candidate, err := os.ReadFile(capture)
		if err == nil && bytes.Contains(candidate, []byte(`"schema"`)) {
			raw = candidate
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the plugin never received an envelope: %v (%q)", err, candidate)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.HasPrefix(string(raw), "2\n") {
		t.Fatalf("COC2_PLUGIN_ENVELOPE was not 2: %q", raw)
	}
	var envelope map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(bytes.TrimPrefix(raw, []byte("2\n"))), &envelope); err != nil {
		t.Fatalf("stdin was not one envelope: %v (%q)", err, raw)
	}
	if envelope["schema"] != float64(2) || envelope["event"] != "task_result" {
		t.Fatalf("envelope = %v", envelope)
	}
	if _, ok := envelope["hint"]; !ok {
		t.Fatalf("envelope has no hint: %v", envelope)
	}
}

// postTask fires one single-target dispatch through the operator plane.
func postTask(t *testing.T, svc *Service, body map[string]any) (int, string) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}
