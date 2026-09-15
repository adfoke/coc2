package server

import (
	"bytes"
	"context"
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

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"coc2/internal/protocol"
)

// pollOpLogs waits until an oplog row satisfies match (oplog writes happen
// server-side after the response is flushed, so an immediate read can race).
func pollOpLogs(t *testing.T, svc *Service, match func(OpLogEntry) bool) OpLogEntry {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		entries, err := svc.store.RecentOpLogs(OpLogQuery{Limit: 200})
		if err != nil {
			t.Fatalf("read oplog: %v", err)
		}
		for _, e := range entries {
			if match(e) {
				return e
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no oplog row matched within 3s; have %d entries", len(entries))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestOplogRecordsTCPDispatch(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	raw, _ := json.Marshal(map[string]any{
		"agent_ids":    []string{"agent-a", "agent-b"},
		"command":      "echo hi",
		"timeout_secs": 5,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks/batch", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d want 202 body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Request-Id") == "" {
		t.Fatal("write response must carry X-Request-Id")
	}

	entry := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Path == "/api/v1/tasks/batch"
	})

	if entry.Plane != "tcp" {
		t.Fatalf("plane=%q want tcp", entry.Plane)
	}
	// Basic auth carries a username: the identity is recorded with it.
	if entry.Actor != "token:admin" {
		t.Fatalf("actor=%q want token:admin", entry.Actor)
	}
	if entry.UID != -1 || entry.PID != -1 {
		t.Fatalf("tcp plane must not claim peer creds: %+v", entry)
	}
	if !entry.OK || entry.Status != http.StatusAccepted {
		t.Fatalf("ok/status: %+v", entry)
	}
	if len(entry.Agents) != 2 || entry.Agents[0] != "agent-a" || entry.Agents[1] != "agent-b" {
		t.Fatalf("agents=%v want [agent-a agent-b]", entry.Agents)
	}
	if !strings.Contains(entry.ParamsSummary, `command="echo hi"`) {
		t.Fatalf("summary lost the command: %q", entry.ParamsSummary)
	}
	if !strings.Contains(entry.ParamsSummary, "timeout_secs=5") {
		t.Fatalf("summary lost timeout: %q", entry.ParamsSummary)
	}
	// Refs must name the created tasks so `audit list --ref <task>` works.
	var resp struct {
		Tasks []struct {
			TaskID string `json:"task_id"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	for _, task := range resp.Tasks {
		if !strings.Contains(entry.Ref, task.TaskID) {
			t.Fatalf("ref %q missing task %q", entry.Ref, task.TaskID)
		}
	}
	if entry.RequestID == "" {
		t.Fatal("request id empty")
	}
}

func TestOplogRecordsGroupMutation(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	raw, _ := json.Marshal(map[string]any{
		"id":        "grp-1",
		"name":      "prod",
		"agent_ids": []string{"a1"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/groups", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	entry := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Path == "/api/v1/groups"
	})
	if entry.Ref != "id=grp-1" {
		t.Fatalf("ref=%q want id=grp-1", entry.Ref)
	}
	if len(entry.Agents) != 1 || entry.Agents[0] != "a1" {
		t.Fatalf("agents=%v", entry.Agents)
	}
	if !strings.Contains(entry.ParamsSummary, `name="prod"`) {
		t.Fatalf("summary=%q", entry.ParamsSummary)
	}
}

func TestOplogSkipsReads(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
		setTestAuth(req)
		rec := httptest.NewRecorder()
		svc.engine.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET failed: %d", rec.Code)
		}
	}

	count, err := svc.store.OplogCount()
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Fatalf("GET requests must not be audited, got %d rows", count)
	}

	// A single write must produce exactly one row, proving the count was
	// zero because reads are skipped rather than because the sink is dead.
	raw, _ := json.Marshal(map[string]any{"name": "g2"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/groups", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)
	pollOpLogs(t, svc, func(e OpLogEntry) bool { return e.Path == "/api/v1/groups" })

	count, _ = svc.store.OplogCount()
	if count != 1 {
		t.Fatalf("rows=%d want 1 (3 reads + 1 write)", count)
	}
}

func TestOplogRecordsAuthFailure(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rec.Code)
	}

	entry := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Path == "/api/v1/tasks" && e.Plane == "tcp"
	})
	if entry.Actor != "unauthorized" || entry.OK {
		t.Fatalf("auth failure row wrong: %+v", entry)
	}
	if entry.ParamsSummary != "auth_failed" {
		t.Fatalf("summary=%q", entry.ParamsSummary)
	}
	if entry.Status != http.StatusUnauthorized {
		t.Fatalf("status=%d", entry.Status)
	}
}

func TestOplogAgentPlaneAuthFailure(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	ts := httptest.NewServer(svc.agentEngine)
	defer ts.Close()

	wsURL := "ws://" + strings.TrimPrefix(ts.URL, "http://") + "/ws/agent"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	hello := baseHello()
	hello.AgentID = "squatter-99"
	hello.Token = "totally-wrong"
	raw, err := protocol.MarshalMessage(protocol.TypeHello, hello)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The server answers (best-effort) with an error frame and drops the
	// connection. The reply-vs-close race on that frame is pre-existing
	// behavior; what this test owns is that the rejection was audited.
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, frame, err := conn.ReadMessage(); err == nil {
		in, decErr := protocol.DecodeFrame(websocket.TextMessage, frame)
		if decErr == nil && in.MsgType != protocol.TypeError {
			t.Fatalf("reply type=%q want error", in.MsgType)
		}
	}
	conn.Close()

	entry := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Plane == "agent"
	})
	if entry.Actor != "unauthorized" || entry.OK {
		t.Fatalf("row: %+v", entry)
	}
	if entry.Method != "WEBSOCKET" || entry.Path != "/ws/agent" {
		t.Fatalf("method/path: %+v", entry)
	}
	if !strings.Contains(entry.Ref, "squatter-99") {
		t.Fatalf("ref must keep the claimed id: %q", entry.Ref)
	}
	if entry.Source == "" {
		t.Fatal("source (peer address) missing")
	}
}

func TestOplogUDSPlaneRecordsUnixActor(t *testing.T) {
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
		t.Fatalf("bind: %v", err)
	}
	defer svc.operatorUDSrv.Close()
	go svc.operatorUDSrv.Serve(svc.udsListener)

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}

	body, _ := json.Marshal(map[string]any{"agent_id": "a-uds", "command": "id"})
	req, _ := http.NewRequest(http.MethodPost, "http://unix/api/v1/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("uds post: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", resp.StatusCode, respBody)
	}

	entry := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Plane == "uds"
	})
	if entry.UID != os.Getuid() {
		t.Fatalf("uid=%d want %d (kernel peer creds)", entry.UID, os.Getuid())
	}
	if want := resolveUsername(os.Getuid()); want != "" && entry.Actor != want {
		t.Fatalf("actor=%q want unix name %q", entry.Actor, want)
	}
	// The client lives in this very process, so the kernel-reported peer
	// pid must be our own.
	if entry.PID != os.Getpid() {
		t.Fatalf("pid=%d want %d", entry.PID, os.Getpid())
	}
	// An unbound unix client has no RemoteAddr; the socket path is the
	// recorded source instead.
	if entry.Source != sock {
		t.Fatalf("source=%q want socket path %q", entry.Source, sock)
	}
	if entry.OK != true || entry.Status != http.StatusAccepted {
		t.Fatalf("row: %+v", entry)
	}
	if !strings.Contains(entry.ParamsSummary, `command="id"`) {
		t.Fatalf("summary=%q", entry.ParamsSummary)
	}
}

func TestOplogLargeBodyReachesHandlerIntact(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	big := strings.Repeat("x", 120_000) // > oplogMaxRequestBytes
	raw, _ := json.Marshal(map[string]any{"agent_id": "a1", "command": big})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tasks", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	var resp struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	stored, ok, err := svc.store.Task(resp.TaskID)
	if err != nil || !ok {
		t.Fatalf("task lookup: ok=%v err=%v", ok, err)
	}
	// The summarize cap must not truncate what the handler (and ultimately
	// the agent) receives.
	if stored.Task.Command != big {
		t.Fatalf("handler received %d bytes, want %d", len(stored.Task.Command), len(big))
	}

	entry := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Path == "/api/v1/tasks"
	})
	if len(entry.ParamsSummary) > oplogMaxSummaryChars+8 {
		t.Fatalf("summary not bounded: %d chars", len(entry.ParamsSummary))
	}
	if !strings.HasSuffix(entry.ParamsSummary, "…") {
		t.Fatalf("truncation must be visible in summary: %q", entry.ParamsSummary[len(entry.ParamsSummary)-20:])
	}
}

func TestSummarizeRequestBody(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "known keys keep values, sorted",
			in:   `{"timeout_secs":5,"command":"rm -rf /tmp/x"}`,
			want: `command="rm -rf /tmp/x" timeout_secs=5`,
		},
		{
			name: "unknown keys are name-only",
			in:   `{"secret_token":"abc","command":"ls"}`,
			want: `command="ls" secret_token=*`,
		},
		{
			name: "newlines flattened",
			in:   "{\"command\":\"a\\nb\"}",
			want: `command="a b"`,
		},
		{
			name: "long arrays collapse",
			in:   `{"agent_ids":["a1","a2","a3","a4","a5","a6","a7","a8","a9","a10"]}`,
			want: `agent_ids=[a1,a2,a3,a4,a5,a6,a7,a8,+2]`,
		},
		{
			name: "invalid json bounded",
			in:   `not-json-at-all-but-rather-long-` + strings.Repeat("z", 200),
			want: `body:not-json-at-all-but-rather-long-`,
		},
	}
	for _, tc := range cases {
		got := summarizeRequestBody([]byte(tc.in))
		if !strings.HasPrefix(got, tc.want) && got != tc.want {
			t.Errorf("%s: got %q want prefix %q", tc.name, got, tc.want)
		}
	}
	if summarizeRequestBody(nil) != "" {
		t.Error("empty body must summarize to empty string")
	}
}

func TestExtractRefs(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"single task", `{"task_id":"t1","agent_id":"a1"}`, "task_id=t1"},
		{"batch tasks", `{"count":2,"tasks":[{"task_id":"b"},{"task_id":"a"}]}`, "task_id=a,task_id=b"},
		{"group id", `{"id":"g1","name":"prod"}`, "id=g1"},
		{"transfer id", `{"transfer_id":"tr1","status":"queued"}`, "transfer_id=tr1"},
		{"no refs", `[{"online":true}]`, ""},
		{"not json", `plain text`, ""},
	}
	for _, tc := range cases {
		got := extractRefs([]byte(tc.in))
		if got != tc.want {
			t.Errorf("%s: refs=%q want %q", tc.name, got, tc.want)
		}
	}
}

func TestRecentOpLogsFilters(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	base := time.Now().UTC().Add(-time.Hour)
	rows := []OpLogEntry{
		{RequestID: "r1", TS: base, Plane: "uds", Actor: "john", UID: 501, Method: "POST", Path: "/api/v1/tasks", Status: 202, OK: true, Agents: []string{"a1"}, Ref: "task_id=t1", ParamsSummary: `command="uptime"`},
		{RequestID: "r2", TS: base.Add(time.Minute), Plane: "tcp", Actor: "token", Method: "POST", Path: "/api/v1/tasks", Status: 401, OK: false, ParamsSummary: "auth_failed"},
		{RequestID: "r3", TS: base.Add(2 * time.Minute), Plane: "tcp", Actor: "token", Method: "POST", Path: "/api/v1/groups", Status: 202, OK: true, Agents: []string{"a1", "b2"}, Ref: "id=g9"},
	}
	for _, r := range rows {
		if err := svc.store.AddOpLog(r); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	all, err := svc.store.RecentOpLogs(OpLogQuery{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("rows=%d want 3", len(all))
	}
	if all[0].RequestID != "r3" {
		t.Fatalf("order must be newest first, got %q", all[0].RequestID)
	}

	byAgent, _ := svc.store.RecentOpLogs(OpLogQuery{Agent: "b2"})
	if len(byAgent) != 1 || byAgent[0].RequestID != "r3" {
		t.Fatalf("agent filter: %+v", byAgent)
	}

	byActor, _ := svc.store.RecentOpLogs(OpLogQuery{Actor: "john"})
	if len(byActor) != 1 || byActor[0].RequestID != "r1" {
		t.Fatalf("actor filter: %+v", byActor)
	}

	byRef, _ := svc.store.RecentOpLogs(OpLogQuery{Ref: "t1"})
	if len(byRef) != 1 || byRef[0].RequestID != "r1" {
		t.Fatalf("ref filter: %+v", byRef)
	}

	onlyFailed, _ := svc.store.RecentOpLogs(OpLogQuery{Failed: true})
	if len(onlyFailed) != 1 || onlyFailed[0].RequestID != "r2" {
		t.Fatalf("failed filter: %+v", onlyFailed)
	}

	window, _ := svc.store.RecentOpLogs(OpLogQuery{Since: base.Add(time.Minute), Until: base.Add(2 * time.Minute)})
	if len(window) != 2 {
		t.Fatalf("time window: %+v", window)
	}

	// Round-trip of the structured fields.
	r1 := byActor[0]
	if len(r1.Agents) != 1 || r1.Agents[0] != "a1" || r1.Ref != "task_id=t1" || r1.UID != 501 {
		t.Fatalf("field round-trip broken: %+v", r1)
	}
	if r1.TS.IsZero() {
		t.Fatal("ts not parsed")
	}
}

func TestOplogEndpointFilters(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	if err := svc.store.AddOpLog(OpLogEntry{
		RequestID: "x1", TS: time.Now().UTC(), Plane: "tcp", Actor: "token:root",
		Method: "POST", Path: "/api/v1/groups", Status: 202, OK: true, Agents: []string{"zz-9"},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/oplog?agent=zz-9&actor=token:root", nil)
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var out []OpLogEntry
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 || out[0].RequestID != "x1" {
		t.Fatalf("filtered set: %+v", out)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/oplog?since=garbage", nil)
	setTestAuth(req)
	rec = httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad since must 400, got %d", rec.Code)
	}
}

func TestOplogSurvivesLegacyDB(t *testing.T) {
	// A pre-oplog database must gain the table on open (CREATE IF NOT
	// EXISTS path) and still serve queries.
	dbPath := filepath.Join(t.TempDir(), "legacy.db")

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := store.AddTask(protocol.Task{ID: "t9", AgentID: "a9", Type: "shell", Command: "echo", TimeoutSecs: 1, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("seed task: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	store2, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	if err := store2.AddOpLog(OpLogEntry{RequestID: "x", TS: time.Now().UTC(), Plane: "uds", Actor: "root", Method: "POST", Path: "/p", Status: 200, OK: true}); err != nil {
		t.Fatalf("oplog insert after migration: %v", err)
	}
	rows, err := store2.RecentOpLogs(OpLogQuery{})
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	if _, ok, _ := store2.Task("t9"); !ok {
		t.Fatal("legacy data lost")
	}
}
