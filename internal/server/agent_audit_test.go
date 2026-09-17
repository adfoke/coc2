package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"coc2/internal/protocol"
)

// TestOplogRecordsAgentConnectAndResult closes the audit asymmetry: a genuine
// agent connect and a genuinely applied task result must both leave rows on
// the agent plane, so they can be told apart from forged ones after the fact.
func TestOplogRecordsAgentConnectAndResult(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	task := protocol.Task{
		ID: "task-audited", AgentID: "agent-e2e", Type: "shell",
		Command: "true", TimeoutSecs: 60, CreatedAt: time.Now().UTC(),
	}
	if err := svc.store.AddTask(task, time.Time{}); err != nil {
		t.Fatalf("add task: %v", err)
	}
	if err := svc.store.MarkDispatched(task.ID); err != nil {
		t.Fatalf("mark dispatched: %v", err)
	}

	conn, _, _ := dialAgentWS(t, svc.agentEngine, baseHello())
	sendFrame(t, conn, protocol.TypeTaskResult, protocol.TaskResult{
		TaskID: "task-audited", AgentID: "agent-e2e", Status: "success",
		Stdout: "ok\n", CompletedAt: time.Now().UTC(),
	})

	connect := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Plane == "agent" && strings.Contains(e.ParamsSummary, "connect")
	})
	if connect.Actor != "agent-e2e" || !connect.OK {
		t.Fatalf("connect row: %+v", connect)
	}
	if connect.Method != "WEBSOCKET" || connect.Path != "/ws/agent" {
		t.Fatalf("connect row method/path: %+v", connect)
	}
	if connect.Source == "" {
		t.Fatalf("connect row must carry the peer address: %+v", connect)
	}

	result := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Plane == "agent" && e.Ref == "task_id=task-audited"
	})
	if result.Actor != "agent-e2e" || !result.OK {
		t.Fatalf("result row: %+v", result)
	}
	if !strings.Contains(result.ParamsSummary, "task_result") {
		t.Fatalf("result row summary: %q", result.ParamsSummary)
	}
	// The row must be findable by agent filter, like operator-plane rows.
	if len(result.Agents) == 0 || result.Agents[0] != "agent-e2e" {
		t.Fatalf("result row agents: %+v", result.Agents)
	}
}

// TestOplogRecordsRefusedResult: a result the store refuses (unknown task)
// must still be recorded, with ok=false — that is the signal that someone
// tried to write something that was not theirs.
func TestOplogRecordsRefusedResult(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	conn, _, _ := dialAgentWS(t, svc.agentEngine, baseHello())
	sendFrame(t, conn, protocol.TypeTaskResult, protocol.TaskResult{
		TaskID: "task-that-does-not-exist", AgentID: "agent-e2e",
		Status: "success", CompletedAt: time.Now().UTC(),
	})

	entry := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Plane == "agent" && e.Ref == "task_id=task-that-does-not-exist"
	})
	if entry.OK {
		t.Fatalf("refused result must be audited as not-ok: %+v", entry)
	}
	if entry.Status < 400 {
		t.Fatalf("refused result status = %d, want >= 400", entry.Status)
	}
}

// TestOplogAgentRowsSurviveAgentFilter makes sure the --agent filter, which
// matches agents_json, works for agent-plane rows too.
func TestOplogAgentRowsSurviveAgentFilter(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	conn, _, _ := dialAgentWS(t, svc.agentEngine, baseHello())
	sendFrame(t, conn, protocol.TypeTaskResult, protocol.TaskResult{
		TaskID: "t-filter", AgentID: "agent-e2e", Status: "success",
		CompletedAt: time.Now().UTC(),
	})
	pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Plane == "agent" && e.Ref == "task_id=t-filter"
	})

	rows, err := svc.store.RecentOpLogs(OpLogQuery{Limit: 50, Agent: "agent-e2e"})
	if err != nil {
		t.Fatalf("query by agent: %v", err)
	}
	if len(rows) == 0 {
		t.Fatalf("agent filter found no agent-plane rows")
	}
}

// TestBatchTaskRejectsInvalidAgentIDs replaces the old behaviour of silently
// queuing work for a blank target.
func TestBatchTaskRejectsInvalidAgentIDs(t *testing.T) {
	cases := []struct {
		name    string
		agentID string
		want    int
	}{
		{"empty entry", "", http.StatusBadRequest},
		{"whitespace only", "   ", http.StatusBadRequest},
		{"control character", "agent\n1", http.StatusBadRequest},
		{"leading dash", "-agent", http.StatusBadRequest},
		{"embedded slash", "agent/1", http.StatusBadRequest},
		{"too long", strings.Repeat("a", 129), http.StatusBadRequest},
		{"valid", "agent-1", http.StatusAccepted},
		{"valid with dots", "web.prod-01", http.StatusAccepted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, cleanup := newTestService(t)
			defer cleanup()

			raw, _ := json.Marshal(map[string]any{
				"agent_ids":    []string{tc.agentID},
				"command":      "true",
				"timeout_secs": 5,
			})
			req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/batch", bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			setTestAuth(req)
			rec := httptest.NewRecorder()
			svc.engine.ServeHTTP(rec, req)

			if rec.Code != tc.want {
				t.Fatalf("agent_ids=[%q] = %d, want %d (body=%s)",
					tc.agentID, rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

// TestAgentIDPatternAcceptsRealisticIDs guards the validation against being
// tightened past ids that agents actually generate (hostnames, UUIDs, FQDNs).
func TestAgentIDPatternAcceptsRealisticIDs(t *testing.T) {
	valid := []string{
		"web1",
		"web-01.prod.example.com",
		"a1b2c3d4-e5f6-7890-abcd-ef1234567890",
		"agent_1",
		"host:8080",
		"node@dc1",
		"build+runner",
		"a",
		strings.Repeat("a", 128),
	}
	for _, id := range valid {
		if !validAgentID(id) {
			t.Errorf("validAgentID(%q) = false, want true", id)
		}
	}

	invalid := []string{
		"", "   ", "-leading", ".leading", "_leading", "with space",
		"with\ttab", "with\nnewline", "slash/inside", strings.Repeat("a", 129),
	}
	for _, id := range invalid {
		if validAgentID(id) {
			t.Errorf("validAgentID(%q) = true, want false", id)
		}
	}
}
