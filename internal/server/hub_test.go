package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"coc2/internal/protocol"
	"go.uber.org/zap"
)

func TestBatchTaskRouteTargetsByTagsAndAgentIDs(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	if err := svc.store.UpsertAgent(AgentState{
		AgentID:     "agent-a",
		Hostname:    "a",
		OS:          "linux",
		Arch:        "amd64",
		Tags:        []string{"prod", "web"},
		Online:      false,
		LastSeenAt:  time.Now().UTC(),
		ConnectedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert agent-a: %v", err)
	}
	if err := svc.store.UpsertAgent(AgentState{
		AgentID:     "agent-b",
		Hostname:    "b",
		OS:          "linux",
		Arch:        "amd64",
		Tags:        []string{"ops"},
		Online:      false,
		LastSeenAt:  time.Now().UTC(),
		ConnectedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert agent-b: %v", err)
	}

	body := map[string]any{
		"agent_ids":    []string{"agent-b"},
		"tags":         []string{"prod"},
		"command":      "echo batch",
		"timeout_secs": 5,
	}
	raw, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/batch", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Count int `json:"count"`
		Tasks []struct {
			AgentID string `json:"agent_id"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Count != 2 {
		t.Fatalf("unexpected task count: %+v", resp)
	}
	if !(resp.Tasks[0].AgentID == "agent-a" && resp.Tasks[1].AgentID == "agent-b") {
		t.Fatalf("unexpected targets: %+v", resp.Tasks)
	}
}

func TestBatchTaskRouteTargetsByGroupIDs(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	for _, agentID := range []string{"agent-a", "agent-b"} {
		if err := svc.store.UpsertAgent(AgentState{
			AgentID:     agentID,
			Hostname:    agentID,
			OS:          "linux",
			Arch:        "amd64",
			Online:      false,
			LastSeenAt:  time.Now().UTC(),
			ConnectedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("upsert %s: %v", agentID, err)
		}
	}
	if err := svc.store.CreateOrUpdateGroup(Group{
		ID:        "group-1",
		Name:      "prod",
		AgentIDs:  []string{"agent-a", "agent-b"},
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create group: %v", err)
	}

	raw, _ := json.Marshal(map[string]any{
		"group_ids":    []string{"group-1"},
		"command":      "echo from-group",
		"timeout_secs": 5,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/batch", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
}

// postBatch fires one batch dispatch through the operator plane and decodes the
// task list, so spread tests stay focused on behavior instead of HTTP plumbing.
func postBatch(t *testing.T, svc *Service, body map[string]any) (int, []taskDispatchResponse, string) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/batch", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	var resp struct {
		Count int                    `json:"count"`
		Tasks []taskDispatchResponse `json:"tasks"`
	}
	if rec.Code == http.StatusAccepted {
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode batch response: %v", err)
		}
	}
	return rec.Code, resp.Tasks, rec.Body.String()
}

// TestBatchSpreadDefersEveryTarget pins the core contract of spread: with a
// non-zero window, no target is dispatched inline — every task waits in the
// queue for the release tick, even when its agent is already connected.
func TestBatchSpreadDefersEveryTarget(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	// Deterministic draw keeps the assertion exact instead of probabilistic.
	origSpread := spreadOffset
	spreadOffset = func(int64) time.Duration { return 30 * time.Second }
	defer func() { spreadOffset = origSpread }()

	// Both agents are connected: if the spread were ignored, dispatch would be
	// immediate and each connection would see its task right away.
	sends := map[string]chan wsFrame{}
	for _, id := range []string{"agent-1", "agent-2"} {
		ch := make(chan wsFrame, 4)
		sends[id] = ch
		svc.clients[id] = &agentConn{id: id, send: ch, done: make(chan struct{}), service: svc}
	}

	code, tasks, body := postBatch(t, svc, map[string]any{
		"agent_ids":    []string{"agent-1", "agent-2"},
		"command":      "echo spread",
		"timeout_secs": 5,
		"spread_ms":    60000,
	})
	if code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", code, body)
	}
	if len(tasks) != 2 {
		t.Fatalf("tasks = %+v, want 2", tasks)
	}
	for _, tsk := range tasks {
		if tsk.Dispatched || !tsk.QueuedOnly {
			t.Fatalf("spread task %s was dispatched inline: %+v", tsk.TaskID, tsk)
		}
		if len(sends[tsk.AgentID]) != 0 {
			t.Fatalf("agent %s received a task during the spread window", tsk.AgentID)
		}
		item, ok, err := svc.store.Task(tsk.TaskID)
		if err != nil || !ok || item.State != "queued" {
			t.Fatalf("spread task %s state=%q ok=%v err=%v, want queued", tsk.TaskID, item.State, ok, err)
		}
	}

	// Nothing is due before the window opens...
	if ready, err := svc.store.ReadyQueuedTasks(time.Now().UTC()); err != nil {
		t.Fatalf("ready tasks: %v", err)
	} else if len(ready) != 0 {
		t.Fatalf("tasks became due before the window opened: %+v", ready)
	}

	// ...and once the tick runs past it, each agent gets exactly its own task.
	svc.dispatchReadyTasks(time.Now().UTC().Add(time.Minute))
	for id, ch := range sends {
		if len(ch) != 1 {
			t.Fatalf("agent %s got %d frames after release, want 1", id, len(ch))
		}
	}
	for _, tsk := range tasks {
		item, ok, err := svc.store.Task(tsk.TaskID)
		if err != nil || !ok || item.State != "dispatched" {
			t.Fatalf("released task %s state=%q ok=%v err=%v, want dispatched", tsk.TaskID, item.State, ok, err)
		}
	}
}

// TestBatchSpreadZeroKeepsImmediateDispatch guards the compatibility promise:
// omitting spread_ms (or passing 0) must behave exactly as before, dispatching
// inline to a connected agent.
func TestBatchSpreadZeroKeepsImmediateDispatch(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	ch := make(chan wsFrame, 4)
	svc.clients["agent-1"] = &agentConn{id: "agent-1", send: ch, done: make(chan struct{}), service: svc}

	code, tasks, body := postBatch(t, svc, map[string]any{
		"agent_ids": []string{"agent-1"},
		"command":   "echo now",
	})
	if code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", code, body)
	}
	if len(tasks) != 1 || !tasks[0].Dispatched || tasks[0].QueuedOnly {
		t.Fatalf("spread_ms=0 must dispatch inline: %+v", tasks)
	}
	if len(ch) != 1 {
		t.Fatalf("agent got %d frames, want 1", len(ch))
	}
}

// TestBatchSpreadRejectsOutOfRange: a spread window outside [0, 1h] is a client
// error and must not create any task.
func TestBatchSpreadRejectsOutOfRange(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	for _, spread := range []int{-1, maxSpreadMs + 1} {
		code, _, body := postBatch(t, svc, map[string]any{
			"agent_ids": []string{"agent-1"},
			"command":   "echo",
			"spread_ms": spread,
		})
		if code != http.StatusBadRequest {
			t.Fatalf("spread_ms=%d status = %d, want 400", spread, code)
		}
		if !strings.Contains(body, "spread_ms") {
			t.Fatalf("400 body must name the rejected field: %s", body)
		}
	}

	items, err := svc.store.RecentTasks(10)
	if err != nil {
		t.Fatalf("recent tasks: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("a rejected batch queued %d tasks", len(items))
	}
}

// TestDeferredTaskNotInHelloPendingUntilReleased is the reconnect-side contract:
// an agent that comes back during the spread window must not be handed tasks
// that are not yet due — otherwise an offline fleet would receive the whole
// backlog in one burst on reconnect, which is the herd the spread prevents.
func TestDeferredTaskNotInHelloPendingUntilReleased(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	releaseAt := time.Now().UTC().Add(10 * time.Minute)
	task := protocol.Task{
		ID: "task-deferred", AgentID: "agent-e2e", Type: "shell",
		Command: "echo later", TimeoutSecs: 60, CreatedAt: time.Now().UTC(),
	}
	if err := svc.store.AddTask(task, releaseAt); err != nil {
		t.Fatalf("add deferred task: %v", err)
	}

	conn, _, frame := dialAgentWS(t, svc.agentEngine, baseHello())
	in, err := protocol.DecodeFrame(protocol.FrameText, frame)
	if err != nil || in.MsgType != protocol.TypeHelloAck {
		t.Fatalf("decode hello_ack: type=%q err=%v", in.MsgType, err)
	}
	ack, err := protocol.PayloadOf[protocol.HelloAck](in)
	if err != nil {
		t.Fatalf("decode hello_ack payload: %v", err)
	}
	if len(ack.PendingTasks) != 0 {
		t.Fatalf("hello_ack handed out an undeferred task: %+v", ack.PendingTasks)
	}

	// Run the tick past the release window: the live connection gets it then.
	svc.dispatchReadyTasks(releaseAt.Add(time.Second))

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	opcode, frame, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read released task: %v", err)
	}
	in, err = protocol.DecodeFrame(opcode, frame)
	if err != nil || in.MsgType != protocol.TypeTaskDispatch {
		t.Fatalf("released frame: type=%q err=%v", in.MsgType, err)
	}
	sent, err := protocol.PayloadOf[protocol.Task](in)
	if err != nil || sent.ID != task.ID {
		t.Fatalf("released task = %+v err=%v, want %s", sent, err, task.ID)
	}

	item, ok, err := svc.store.Task(task.ID)
	if err != nil || !ok || item.State != "dispatched" {
		t.Fatalf("released task state=%q ok=%v err=%v, want dispatched", item.State, ok, err)
	}
}

func TestCheckOriginRejectsAllBrowserHandshakes(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost:8080/ws/agent", nil)
	req.Host = "localhost:8080"

	if !checkOrigin(req) {
		t.Fatalf("non-browser request without Origin should be allowed")
	}

	for _, origin := range []string{"http://localhost:8080", "null", "https://evil.example.com", "://bad"} {
		req.Header.Set("Origin", origin)
		if checkOrigin(req) {
			t.Fatalf("request carrying Origin %q should be rejected", origin)
		}
	}
}

func TestAgentMetricsHistoryRoute(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	for i := range 3 {
		if err := svc.store.SaveAgentMetrics(protocol.MetricsReport{
			AgentID:    "agent-1",
			Timestamp:  time.Now().UTC().Add(time.Duration(i) * time.Second),
			UptimeSecs: int64(i),
			CPUCount:   8,
		}); err != nil {
			t.Fatalf("save metrics %d: %v", i, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents/agent-1/metrics/history", nil)
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var history []protocol.MetricsReport
	if err := json.Unmarshal(rec.Body.Bytes(), &history); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("expected 3 samples, got %d", len(history))
	}
	if history[0].UptimeSecs != 2 {
		t.Fatalf("unexpected newest sample: %+v", history[0])
	}
}

func TestDashboardRoutesGone(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	for _, path := range []string{"/", "/dashboard"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		setTestAuth(req)
		rec := httptest.NewRecorder()
		svc.engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s should be gone, got %d body=%s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestDispatchMarksTaskDispatchedOnSend(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	svc.clients["agent-1"] = &agentConn{
		id:      "agent-1",
		send:    make(chan wsFrame, 1),
		done:    make(chan struct{}),
		service: svc,
	}

	resp, err := svc.createTask("agent-1", "echo ok", 5, 0, time.Time{})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if !resp.Dispatched {
		t.Fatalf("expected task to be dispatched for live client")
	}

	item, ok, err := svc.store.Task(resp.TaskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if !ok || item.State != "dispatched" {
		t.Fatalf("unexpected task state after send: %+v", item)
	}
}

func TestReapTasks(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	now := time.Now().UTC()
	past := now.Add(-time.Hour).UTC().Format(time.RFC3339Nano)

	timeoutTask := protocol.Task{ID: "t-timeout", AgentID: "a1", Type: "shell", Command: "echo", TimeoutSecs: 5, CreatedAt: now}
	if err := svc.store.AddTask(timeoutTask, time.Time{}); err != nil {
		t.Fatalf("add timeout task: %v", err)
	}
	if err := svc.store.MarkDispatched(timeoutTask.ID); err != nil {
		t.Fatalf("mark dispatched: %v", err)
	}

	cancelTask := protocol.Task{ID: "t-cancel", AgentID: "a1", Type: "shell", Command: "echo", TimeoutSecs: 5, CreatedAt: now}
	if err := svc.store.AddTask(cancelTask, time.Time{}); err != nil {
		t.Fatalf("add cancel task: %v", err)
	}
	if _, err := svc.store.db.Exec(`UPDATE tasks SET state = 'cancel_requested', dispatched_at = ? WHERE id = ?`, past, cancelTask.ID); err != nil {
		t.Fatalf("set cancel_requested: %v", err)
	}

	// Backdate the dispatched task so it is stale.
	if _, err := svc.store.db.Exec(`UPDATE tasks SET dispatched_at = ? WHERE id = ?`, past, timeoutTask.ID); err != nil {
		t.Fatalf("backdate dispatched_at: %v", err)
	}

	n, err := svc.reapTimedOutTasks(now)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 reaped tasks, got %d", n)
	}

	timeoutItem, ok, err := svc.store.Task(timeoutTask.ID)
	if err != nil {
		t.Fatalf("get timeout task: %v", err)
	}
	if !ok || timeoutItem.State != "timeout" || timeoutItem.Result == nil || timeoutItem.Result.Status != "timeout" {
		t.Fatalf("unexpected timed out task: %+v", timeoutItem)
	}

	cancelItem, ok, err := svc.store.Task(cancelTask.ID)
	if err != nil {
		t.Fatalf("get cancel task: %v", err)
	}
	if !ok || cancelItem.State != "canceled" || cancelItem.Result == nil || cancelItem.Result.Status != "canceled" {
		t.Fatalf("unexpected canceled task: %+v", cancelItem)
	}
}

func TestHandleTransferChunkFailurePersistsAndClearsTransfer(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	state := &transferState{
		ID:         "tx-fail",
		AgentID:    "agent-1",
		Direction:  "download",
		LocalPath:  filepath.Join(t.TempDir(), "out.txt"),
		RemotePath: "/tmp/out.txt",
		Status:     "running",
		CreatedAt:  time.Now().UTC(),
	}
	svc.putTransfer(state)
	svc.handleTransferChunk(protocol.FileTransferChunk{
		TransferID: state.ID,
		Data:       []byte("oops"),
	})

	if _, ok := svc.getTransfer(state.ID); ok {
		t.Fatalf("expected transfer to be cleared")
	}

	audit, ok, err := svc.store.TransferAudit(state.ID)
	if err != nil {
		t.Fatalf("get transfer audit: %v", err)
	}
	if !ok || audit.Status != "failed" {
		t.Fatalf("unexpected transfer audit: %+v", audit)
	}
}

func newTestService(t *testing.T) (*Service, func()) {
	t.Helper()

	svc, err := New(Config{
		ListenAddr:     ":0",
		OperatorListen: ":0",
		AuthToken:      "test-token",
		DBPath:         filepath.Join(t.TempDir(), "test.db"),
		WriteWait:      2 * time.Second,
		PongWait:       2 * time.Second,
		PingPeriod:     time.Second,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	return svc, func() {
		_ = svc.store.Close()
	}
}

func setTestAuth(req *http.Request) {
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("admin:test-token")))
}

func TestOperatorUDSNoTokenAndMode(t *testing.T) {
	sock := shortSockPath(t)
	svc, err := New(Config{
		ListenAddr:      ":0",
		OperatorUDSPath: sock,
		AuthToken:       "test-token",
		DBPath:          filepath.Join(t.TempDir(), "test.db"),
		WriteWait:       2 * time.Second,
		PongWait:        2 * time.Second,
		PingPeriod:      time.Second,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	defer svc.store.Close()

	if err := svc.listenOperatorUDS(); err != nil {
		t.Fatalf("bind uds: %v", err)
	}
	defer svc.operatorUDSrv.Close()

	if fi, statErr := os.Stat(sock); statErr != nil {
		t.Fatalf("stat socket: %v", statErr)
	} else if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket perm = %o, want 600", perm)
	}

	go svc.operatorUDSrv.Serve(svc.udsListener)

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
	// No Authorization header: the UDS plane trusts file permissions.
	resp, err := client.Get("http://unix/api/v1/agents")
	if err != nil {
		t.Fatalf("uds get: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("uds status = %d body=%s, want 200", resp.StatusCode, body)
	}
}

func TestOperatorPlaneRequiresOneListener(t *testing.T) {
	if _, err := New(Config{ListenAddr: ":0", AuthToken: "t"}, zap.NewNop()); err == nil {
		t.Fatalf("expected error when both operator listeners are empty")
	}
}

func TestOperatorTCPPlaneEnforcesToken(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("tokenless request on TCP operator plane = %d, want 401", rec.Code)
	}
}

func TestOperatorAuthAcceptsBearerAndBasicOnly(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	setTestAuth(req) // Basic admin:test-token
	svc.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Basic auth = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	svc.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Bearer auth = %d, want 200", rec.Code)
	}

	// The retired X-Auth-Token custom header must no longer authenticate.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	req.Header.Set("X-Auth-Token", "test-token")
	svc.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("X-Auth-Token = %d, want 401 (header support removed)", rec.Code)
	}
}

func TestEmptyListsSerializeAsArrayNotNull(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	for _, path := range []string{"/api/v1/agents", "/api/v1/tasks", "/api/v1/groups", "/api/v1/transfers"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		setTestAuth(req)
		rec := httptest.NewRecorder()
		svc.engine.ServeHTTP(rec, req)

		if body := strings.TrimSpace(rec.Body.String()); body != "[]" {
			t.Fatalf("%s = %q, want []", path, body)
		}
	}
}

func TestGroupCreateResponseCarriesMemberCount(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	raw, _ := json.Marshal(map[string]any{"name": "g-audit", "agent_ids": []string{"a1", "a2"}})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/groups", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	var group Group
	if err := json.Unmarshal(rec.Body.Bytes(), &group); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if group.MemberCount != 2 || len(group.AgentIDs) != 2 {
		t.Fatalf("create ack must report real membership, got %+v", group)
	}
}

func TestSendMessageBackpressuresInsteadOfDropping(t *testing.T) {
	// Regression: the old `default:` branch returned ErrCloseSent the
	// moment the 16-deep queue filled, silently killing any transfer
	// larger than the buffer (live-proven over WAN: >4MB pushes died at
	// non-deterministic offsets). send must block until the writeLoop
	// catches up, and only fail fast once the connection is closing.
	svc, cleanup := newTestService(t)
	defer cleanup()

	a := &agentConn{
		id:      "slow-agent",
		send:    make(chan wsFrame, 2),
		done:    make(chan struct{}),
		service: svc,
	}
	a.send <- wsFrame{opcode: protocol.FrameText, data: []byte("busy-1")}
	a.send <- wsFrame{opcode: protocol.FrameText, data: []byte("busy-2")}

	errCh := make(chan error, 1)
	go func() {
		errCh <- a.sendMessage(protocol.TypeTaskDispatch, protocol.Task{ID: "t-blocked"})
	}()

	select {
	case err := <-errCh:
		t.Fatalf("send returned %v while queue full: frame was dropped, want block", err)
	case <-time.After(100 * time.Millisecond):
		// still blocked: correct backpressure
	}

	<-a.send // drain one slot
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("send after drain: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send never completed after drain")
	}

	// Closing the connection must release a blocked send immediately.
	// Queue currently holds busy-2 + t-blocked; drain both so the next
	// two direct pushes fill it to exactly capacity without blocking.
	<-a.send
	<-a.send
	a.send <- wsFrame{opcode: protocol.FrameText, data: []byte("full-1")}
	a.send <- wsFrame{opcode: protocol.FrameText, data: []byte("full-2")}
	blocked := make(chan error, 1)
	go func() {
		blocked <- a.sendMessage(protocol.TypeTaskDispatch, protocol.Task{ID: "t-closed"})
	}()
	time.Sleep(30 * time.Millisecond)
	close(a.done)
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("send after close(done) should error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked send did not escape via done")
	}
}
