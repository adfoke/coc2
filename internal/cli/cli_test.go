package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIsUDS(t *testing.T) {
	cases := map[string]bool{
		"./coc2.sock":           true,
		"/var/run/coc2.sock":    true,
		"~/x.sock":              true,
		"coc2.sock":             true,
		"http://127.0.0.1:8081": false,
		"https://host:443":      false,
		"127.0.0.1:8081":        false, // host:port is not a path
	}
	for target, want := range cases {
		if got := IsUDS(target); got != want {
			t.Errorf("IsUDS(%q) = %v, want %v", target, got, want)
		}
	}
}

func TestHoistGlobals(t *testing.T) {
	rest, globals := hoistGlobals([]string{
		"agents", "list", "--pretty", "-server", "/tmp/x.sock", "--filter", "prod", "-timeout=5s",
	})
	if strings.Join(globals, " ") != "--pretty -server /tmp/x.sock -timeout=5s" {
		t.Fatalf("globals = %v", globals)
	}
	if strings.Join(rest, " ") != "agents list --filter prod" {
		t.Fatalf("rest = %v", rest)
	}
}

func newFakeRegistry(t *testing.T) (*Registry, *httptest.Server) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true,"plane":"operator"}`))
	})
	mux.HandleFunc("GET /api/v1/agents", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
			{"agent_id":"a1","online":true,"tags":["prod","web","env=staging"]},
			{"agent_id":"a2","online":false,"tags":["dev"]}
		]`))
	})
	mux.HandleFunc("GET /api/v1/protected", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok123" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	// A task_dispatch plugin refusing a command: 403 with the denial fields,
	// not a 401. It must not read as an auth failure.
	mux.HandleFunc("POST /api/v1/denied", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"task_id":"t-1","agent_id":"a1","denied":true,` +
			`"denied_by":"policy.js","deny_reason":"destructive command"}`))
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	r := &Registry{}
	RegisterAll(r)
	r.Add(&Command{
		Name: "protected get",
		Run: func(g *Globals, _ *CmdFlags) error {
			var out map[string]any
			if err := g.Client.Get("/api/v1/protected", &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
	r.Add(&Command{
		Name: "denied post",
		Run: func(g *Globals, _ *CmdFlags) error {
			var out map[string]any
			if err := g.Client.Post("/api/v1/denied", map[string]any{}, &out); err != nil {
				return err
			}
			return Emit(g.Stdout, out, g.Pretty)
		},
	})
	return r, ts
}

func run(t *testing.T, r *Registry, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := r.RunWithIO(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestAgentsListJSONOutput(t *testing.T) {
	r, ts := newFakeRegistry(t)
	code, stdout, stderr := run(t, r, "-server", ts.URL, "agents", "list")
	if code != ExitOK || stderr != "" {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	var agents []map[string]any
	if err := json.Unmarshal([]byte(stdout), &agents); err != nil {
		t.Fatalf("stdout not JSON: %v", err)
	}
	if len(agents) != 2 || agents[0]["agent_id"] != "a1" {
		t.Fatalf("unexpected agents: %s", stdout)
	}
}

func TestAgentsListFilterAndOnline(t *testing.T) {
	r, ts := newFakeRegistry(t)

	code, stdout, _ := run(t, r, "-server", ts.URL, "agents", "list", "--filter", "prod")
	if code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	var agents []map[string]any
	_ = json.Unmarshal([]byte(stdout), &agents)
	if len(agents) != 1 || agents[0]["agent_id"] != "a1" {
		t.Fatalf("filter prod: %s", stdout)
	}

	code, stdout, _ = run(t, r, "-server", ts.URL, "agents", "list", "--online")
	if code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	agents = nil
	_ = json.Unmarshal([]byte(stdout), &agents)
	if len(agents) != 1 {
		t.Fatalf("online: %s", stdout)
	}

	// k=v filter must match exactly and must NOT match bare keys or other values.
	agents = nil
	if code, stdout, _ = run(t, r, "-server", ts.URL, "agents", "list", "--filter", "prod=true"); code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	_ = json.Unmarshal([]byte(stdout), &agents)
	if len(agents) != 0 {
		t.Fatalf("prod=true should match nothing: %s", stdout)
	}
	agents = nil
	if code, stdout, _ = run(t, r, "-server", ts.URL, "agents", "list", "--filter", "web"); code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	_ = json.Unmarshal([]byte(stdout), &agents)
	if len(agents) != 1 || agents[0]["agent_id"] != "a1" {
		t.Fatalf("bare tag web: %s", stdout)
	}

	// Positive k=v match: this is the ONLY assertion that exercises the
	// k=v success branch — negative cases pass with a dead branch.
	agents = nil
	if code, stdout, _ = run(t, r, "-server", ts.URL, "agents", "list", "--filter", "env=staging"); code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	_ = json.Unmarshal([]byte(stdout), &agents)
	if len(agents) != 1 || agents[0]["agent_id"] != "a1" {
		t.Fatalf("k=v tag env=staging: %s", stdout)
	}
}

func TestAgentsGetNotFoundExit1(t *testing.T) {
	r, ts := newFakeRegistry(t)
	code, stdout, stderr := run(t, r, "-server", ts.URL, "agents", "get", "nope")
	if code != ExitFailure {
		t.Fatalf("code=%d want 1 (stdout=%s)", code, stdout)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(stderr), &doc); err != nil {
		t.Fatalf("stderr not JSON: %s", stderr)
	}
	errObj, _ := doc["error"].(map[string]any)
	if errObj == nil || errObj["code"] != "not_found" {
		t.Fatalf("unexpected error doc: %s", stderr)
	}
	if stdout != "" {
		t.Fatalf("errors must not touch stdout: %q", stdout)
	}
}

func TestAuthFailureExit3(t *testing.T) {
	r, ts := newFakeRegistry(t)
	// Without token -> 401 -> ExitAuth.
	if code, _, _ := run(t, r, "-server", ts.URL, "protected", "get"); code != ExitAuth {
		t.Fatalf("tokenless code=%d want 3", code)
	}
	// With token -> success.
	if code, _, _ := run(t, r, "-server", ts.URL, "-token", "tok123", "protected", "get"); code != ExitOK {
		t.Fatalf("tokened code=%d want 0", code)
	}
}

// TestPolicyDenialIsNotAnAuthFailure pins the exit-code contract for the new
// 403: a denied command is a decision, not a credential problem. Reporting it
// as ExitAuth sent callers to re-check a token that was fine.
func TestPolicyDenialIsNotAnAuthFailure(t *testing.T) {
	r, ts := newFakeRegistry(t)
	code, stdout, stderr := run(t, r, "-server", ts.URL, "denied", "post")
	if code != ExitFailure {
		t.Fatalf("code=%d want %d (stderr=%s)", code, ExitFailure, stderr)
	}
	if stdout != "" {
		t.Fatalf("a denial must not touch stdout: %q", stdout)
	}
	var doc struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(stderr), &doc); err != nil {
		t.Fatalf("stderr not JSON: %s", stderr)
	}
	if doc.Error.Code != "denied" {
		t.Fatalf("error code = %q, want denied", doc.Error.Code)
	}
	// The plugin's own words are the actionable part.
	if !strings.Contains(doc.Error.Message, "policy.js") || !strings.Contains(doc.Error.Message, "destructive") {
		t.Fatalf("message %q does not name the plugin and its reason", doc.Error.Message)
	}
}

func TestConnectFailureExit2(t *testing.T) {
	r, _ := newFakeRegistry(t)
	// Closed listener: nothing on this port.
	code, _, stderr := run(t, r, "-server", "http://127.0.0.1:1", "health")
	if code != ExitConnect {
		t.Fatalf("code=%d want 2 (stderr=%s)", code, stderr)
	}
}

func TestUnknownCommandExit4(t *testing.T) {
	r, _ := newFakeRegistry(t)
	code, stdout, stderr := run(t, r, "frobnicate")
	if code != ExitUsage {
		t.Fatalf("code=%d want 4", code)
	}
	if stdout != "" {
		t.Fatalf("usage errors must not print to stdout: %q", stdout)
	}
	if !strings.Contains(stderr, "unknown command") || !strings.Contains(stderr, "commands") {
		t.Fatalf("stderr should list valid commands: %s", stderr)
	}
}

func TestSchemaMachineReadable(t *testing.T) {
	r, _ := newFakeRegistry(t)
	code, stdout, _ := run(t, r, "schema")
	if code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	var doc struct {
		Name        string            `json:"name"`
		ExitCodes   map[string]int    `json:"exit_codes"`
		GlobalFlags []json.RawMessage `json:"global_flags"`
		Commands    []struct {
			Name    string `json:"name"`
			Summary string `json:"summary"`
		} `json:"commands"`
	}
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("schema not JSON: %v", err)
	}
	if doc.Name != "coc2" || len(doc.Commands) < 10 {
		t.Fatalf("schema incomplete: %+v", doc)
	}
	for _, want := range []string{"ok", "failure", "connect", "auth", "usage"} {
		if _, ok := doc.ExitCodes[want]; !ok {
			t.Fatalf("schema missing exit code %q", want)
		}
	}
	names := map[string]bool{}
	for _, c := range doc.Commands {
		if names[c.Name] {
			t.Fatalf("duplicate command %q in schema", c.Name)
		}
		names[c.Name] = true
	}
	for _, want := range []string{"health", "agents list", "tasks get", "transfers list", "metrics overview"} {
		if !names[want] {
			t.Fatalf("schema missing command %q", want)
		}
	}
}

func TestGlobalFlagAnywherePosition(t *testing.T) {
	r, ts := newFakeRegistry(t)
	// --pretty after the command name must still work (hoisted).
	code, stdout, _ := run(t, r, "agents", "list", "-server", ts.URL, "--pretty")
	if code != ExitOK {
		t.Fatalf("code=%d", code)
	}
	if !strings.HasPrefix(stdout, "[\n") {
		t.Fatalf("expected indented JSON, got: %s", stdout)
	}
}
