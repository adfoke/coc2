package server

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"coc2/internal/protocol"
)

// dialBare opens a websocket to the agent plane without performing hello.
func dialBare(t *testing.T, svc *Service) *websocket.Conn {
	t.Helper()

	up := httptest.NewServer(svc.agentEngine)
	t.Cleanup(up.Close)

	wsURL := "ws://" + strings.TrimPrefix(up.URL, "http://") + "/ws/agent"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial agent ws: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// sendFrame writes one legacy JSON envelope.
func sendFrame(t *testing.T, conn *websocket.Conn, msgType string, payload any) {
	t.Helper()
	raw, err := protocol.MarshalMessage(msgType, payload)
	if err != nil {
		t.Fatalf("marshal %s: %v", msgType, err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write %s: %v", msgType, err)
	}
}

// TestPreAuthConnectionCannotSendAnythingButHello pins the connection-level
// auth gate: before hello is accepted, every other message type must be
// rejected AND the connection closed, so no unauthenticated peer can write
// server state.
func TestPreAuthConnectionCannotSendAnythingButHello(t *testing.T) {
	cases := []struct {
		name    string
		msgType string
		payload any
	}{
		{"metrics_report", protocol.TypeMetricsReport, protocol.MetricsReport{AgentID: "victim", Timestamp: time.Now().UTC()}},
		{"task_result", protocol.TypeTaskResult, protocol.TaskResult{TaskID: "t", AgentID: "victim", Status: "success", CompletedAt: time.Now().UTC()}},
		{"task_ack", protocol.TypeTaskAck, protocol.TaskAck{TaskID: "t", AgentID: "victim"}},
		{"heartbeat", protocol.TypeHeartbeat, protocol.Heartbeat{AgentID: "victim", Timestamp: time.Now().UTC()}},
		{"transfer_start", protocol.TypeFileTransferStart, protocol.FileTransferStart{TransferID: "tx", AgentID: "victim"}},
		{"transfer_chunk", protocol.TypeFileTransferChunk, protocol.FileTransferChunk{TransferID: "tx", Seq: 0, Data: []byte("x")}},
		{"transfer_resume", protocol.TypeFileTransferResume, protocol.FileTransferResume{TransferID: "tx", Offset: 5}},
		{"transfer_done", protocol.TypeFileTransferDone, protocol.FileTransferDone{TransferID: "tx", AgentID: "victim", Status: "success"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, cleanup := newTestService(t)
			defer cleanup()

			conn := dialBare(t, svc)
			sendFrame(t, conn, tc.msgType, tc.payload)

			// The server must answer with an error frame and then close.
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, reply, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("expected an error frame before close, got %v", err)
			}
			if !strings.Contains(string(reply), "not_registered") {
				t.Fatalf("reply = %s, want not_registered", reply)
			}

			// The connection must then be closed, not left usable.
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, _, err := conn.ReadMessage(); err == nil {
				t.Fatalf("connection stayed open after an unauthenticated %s", tc.name)
			}

			// Nothing may have reached the store. (An unauthenticated peer
			// must never be able to materialize an agent row either.)
			agents, err := svc.store.Agents()
			if err != nil {
				t.Fatalf("agents: %v", err)
			}
			for _, a := range agents {
				if a.AgentID == "victim" {
					t.Fatalf("%s materialized an agent row for an unauthenticated peer", tc.name)
				}
			}
			if _, ok, err := svc.store.AgentMetrics("victim"); err != nil {
				t.Fatalf("metrics: %v", err)
			} else if ok {
				t.Fatalf("%s wrote metrics before authentication", tc.name)
			}
		})
	}
}

// TestPreAuthMetricsAndResultAreNotPersisted is the direct regression for the
// two confirmed audit findings.
func TestPreAuthMetricsAndResultAreNotPersisted(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	if err := svc.store.AddTask(protocol.Task{
		ID: "task-victim", AgentID: "real-agent", Type: "shell",
		Command: "true", TimeoutSecs: 60, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("add task: %v", err)
	}

	conn := dialBare(t, svc)
	sendFrame(t, conn, protocol.TypeTaskResult, protocol.TaskResult{
		TaskID: "task-victim", AgentID: "someone-else", Status: "success",
		Stdout: "FORGED", CompletedAt: time.Now().UTC(),
	})
	time.Sleep(150 * time.Millisecond)

	task, ok, err := svc.store.Task("task-victim")
	if err != nil || !ok {
		t.Fatalf("read task: %v ok=%v", err, ok)
	}
	if task.Result != nil {
		t.Fatalf("unauthenticated result was persisted: %+v", task.Result)
	}
	if task.State != "queued" {
		t.Fatalf("unauthenticated result changed state to %q", task.State)
	}
}

// TestDuplicateHelloIsRejected: a second hello on an authenticated connection
// must not re-run registration (which would displace the live client entry).
func TestDuplicateHelloIsRejected(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	up := httptest.NewServer(svc.agentEngine)
	defer up.Close()
	wsURL := "ws://" + strings.TrimPrefix(up.URL, "http://") + "/ws/agent"

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	hello := baseHello()
	raw, err := protocol.MarshalMessage(protocol.TypeHello, hello)
	if err != nil {
		t.Fatalf("marshal hello: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("read hello_ack: %v", err)
	}

	// Second hello: must be refused and the connection closed.
	if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write second hello: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, reply, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("expected already_registered error, got %v", err)
	}
	if !strings.Contains(string(reply), "already_registered") {
		t.Fatalf("reply = %s, want already_registered", reply)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatalf("connection stayed open after a duplicate hello")
	}
}

// TestAuthenticatedResultForOwnTaskIsApplied guards against over-tightening:
// the legitimate path must keep working.
func TestAuthenticatedResultForOwnTaskIsApplied(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	task := protocol.Task{
		ID: "task-legit", AgentID: "agent-e2e", Type: "shell",
		Command: "true", TimeoutSecs: 60, CreatedAt: time.Now().UTC(),
	}
	if err := svc.store.AddTask(task); err != nil {
		t.Fatalf("add task: %v", err)
	}
	if err := svc.store.MarkDispatched(task.ID); err != nil {
		t.Fatalf("mark dispatched: %v", err)
	}

	conn, _, _ := dialAgentWS(t, svc.agentEngine, baseHello())

	sendFrame(t, conn, protocol.TypeTaskResult, protocol.TaskResult{
		TaskID: "task-legit", AgentID: "spoofed-id", Status: "success",
		Stdout: "real\n", CompletedAt: time.Now().UTC(),
	})

	deadline := time.Now().Add(3 * time.Second)
	for {
		got, ok, err := svc.store.Task("task-legit")
		if err != nil {
			t.Fatalf("read task: %v", err)
		}
		if ok && got.Result != nil {
			if got.Result.Stdout != "real\n" {
				t.Fatalf("unexpected stdout: %q", got.Result.Stdout)
			}
			// The connection's authenticated id wins over the wire value.
			if got.Result.AgentID != "agent-e2e" {
				t.Fatalf("result agent_id = %q, want the authenticated connection id", got.Result.AgentID)
			}
			if got.State != "success" {
				t.Fatalf("state = %q, want success", got.State)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("legitimate result was never applied: %+v", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSaveResultRejectsForeignAndTerminalWrites covers the store-level guard
// independently of the connection gate.
func TestSaveResultRejectsForeignAndTerminalWrites(t *testing.T) {
	store, err := NewStore(t.TempDir() + "/guard.db")
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if err := store.AddTask(protocol.Task{
		ID: "t-own", AgentID: "owner", Type: "shell", Command: "true",
		TimeoutSecs: 5, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("add task: %v", err)
	}

	// Wrong agent: refused, and must not create a result row.
	applied, err := store.SaveResult(protocol.TaskResult{
		TaskID: "t-own", AgentID: "intruder", Status: "success", CompletedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("save foreign result: %v", err)
	}
	if applied {
		t.Fatalf("a result for another agent must not be applied")
	}

	// Unknown task: refused, and must not invent a row.
	applied, err = store.SaveResult(protocol.TaskResult{
		TaskID: "t-ghost", AgentID: "owner", Status: "success", CompletedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("save unknown result: %v", err)
	}
	if applied {
		t.Fatalf("a result for an unknown task must not be applied")
	}
	tasks, err := store.RecentTasks(10)
	if err != nil {
		t.Fatalf("recent tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("unknown result invented %d extra task rows", len(tasks)-1)
	}

	// Correct agent: applied.
	applied, err = store.SaveResult(protocol.TaskResult{
		TaskID: "t-own", AgentID: "owner", Status: "success", CompletedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("save own result: %v", err)
	}
	if !applied {
		t.Fatalf("the owning agent's result must be applied")
	}

	// Now terminal: a later result must not rewrite it.
	applied, err = store.SaveResult(protocol.TaskResult{
		TaskID: "t-own", AgentID: "owner", Status: "failed", Stdout: "OVERWRITE",
		CompletedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("save late result: %v", err)
	}
	if applied {
		t.Fatalf("a result must not overwrite a finalized task")
	}
	got, _, err := store.Task("t-own")
	if err != nil {
		t.Fatalf("read task: %v", err)
	}
	if got.State != "success" || got.Result == nil || got.Result.Stdout == "OVERWRITE" {
		t.Fatalf("finalized task was rewritten: %+v", got)
	}
}
