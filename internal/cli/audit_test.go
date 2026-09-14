package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TestAuditListFlagMapping drives `audit list` against a fake server that
// records the query string, so the test asserts the flag -> parameter
// mapping (and escaping) rather than just "it made a GET".
func TestAuditListFlagMapping(t *testing.T) {
	var gotPath string
	var gotQuery url.Values
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":7,"actor":"john","plane":"uds","method":"POST","path":"/api/v1/tasks","status":202,"ok":true,"params_summary":"command=\"uptime\""}]`))
	}))
	defer ts.Close()

	r := &Registry{}
	RegisterAll(r)

	code, stdout, stderr := run(t, r,
		"-server", ts.URL,
		"audit", "list",
		"--actor", "john doe", // space must arrive URL-escaped
		"--agent", "a1",
		"--ref", "task_id=t9",
		"--since", "2026-09-01",
		"--failed",
		"--limit", "10",
	)
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if gotPath != "/api/v1/oplog" {
		t.Fatalf("path=%q", gotPath)
	}
	checks := map[string]string{
		"actor": "john doe",
		"agent": "a1",
		"ref":   "task_id=t9",
		"since": "2026-09-01",
		"limit": "10",
	}
	for k, want := range checks {
		if got := gotQuery.Get(k); got != want {
			t.Errorf("query %s=%q want %q (raw: %s)", k, got, want, gotQuery)
		}
	}
	if gotQuery.Get("failed") != "1" {
		t.Errorf("failed flag = %q, want 1", gotQuery.Get("failed"))
	}
	if gotQuery.Has("until") || gotQuery.Has("path") {
		t.Errorf("unset flags must not emit params: %v", gotQuery)
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(stdout), &rows); err != nil {
		t.Fatalf("stdout not JSON: %v (%s)", err, stdout)
	}
	if len(rows) != 1 || rows[0]["actor"] != "john" {
		t.Fatalf("rows: %s", stdout)
	}
}

func TestAuditListInSchema(t *testing.T) {
	r := &Registry{}
	RegisterAll(r)
	code, stdout, _ := run(t, r, "schema")
	if code != ExitOK {
		t.Fatalf("schema code=%d", code)
	}
	var doc struct {
		Commands []struct {
			Name    string `json:"name"`
			Summary string `json:"summary"`
		} `json:"commands"`
	}
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("schema not JSON: %v", err)
	}
	found := false
	for _, c := range doc.Commands {
		if c.Name == "audit list" {
			found = true
			if !strings.Contains(strings.ToLower(c.Summary), "who") {
				t.Fatalf("audit list summary should name the actor: %q", c.Summary)
			}
		}
	}
	if !found {
		t.Fatal("schema missing `audit list`")
	}
}
