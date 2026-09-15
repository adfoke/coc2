package server

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"coc2/internal/protocol"
)

func TestStoreTaskLifecycleAndPersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "coc2.db")

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	task := protocol.Task{
		ID:          "task-1",
		AgentID:     "agent-1",
		Type:        "shell",
		Command:     "echo ok",
		TimeoutSecs: 5,
		CreatedAt:   time.Now().UTC(),
	}

	if err := store.AddTask(task); err != nil {
		t.Fatalf("add task: %v", err)
	}

	agentsBefore, err := store.Agents()
	if err != nil {
		t.Fatalf("agents before: %v", err)
	}
	if len(agentsBefore) != 0 {
		t.Fatalf("unexpected agents before registration: %d", len(agentsBefore))
	}

	if err := store.UpsertAgent(AgentState{
		AgentID:    "agent-1",
		Hostname:   "host-1",
		OS:         "linux",
		Arch:       "amd64",
		Tags:       []string{"prod", "edge"},
		Online:     true,
		LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert agent: %v", err)
	}

	agents, err := store.Agents()
	if err != nil {
		t.Fatalf("agents: %v", err)
	}
	if len(agents) != 1 || agents[0].PendingCount != 1 {
		t.Fatalf("unexpected agents: %+v", agents)
	}

	pending, err := store.PendingTasks("agent-1")
	if err != nil {
		t.Fatalf("pending tasks: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "task-1" {
		t.Fatalf("unexpected pending tasks: %+v", pending)
	}

	item, ok, err := store.Task("task-1")
	if err != nil {
		t.Fatalf("get task after dispatch: %v", err)
	}
	if !ok || item.State != "queued" {
		t.Fatalf("unexpected task state before ack: %+v", item)
	}

	if err := store.MarkDispatched("task-1"); err != nil {
		t.Fatalf("mark dispatched: %v", err)
	}

	item, ok, err = store.Task("task-1")
	if err != nil {
		t.Fatalf("get task after ack: %v", err)
	}
	if !ok || item.State != "dispatched" {
		t.Fatalf("unexpected task state after ack: %+v", item)
	}

	applied, err := store.SaveResult(protocol.TaskResult{
		TaskID:      "task-1",
		AgentID:     "agent-1",
		Status:      "success",
		ExitCode:    0,
		Stdout:      "ok\n",
		CompletedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("save result: %v", err)
	}
	if !applied {
		t.Fatalf("result for a dispatched task must be applied")
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	item, ok, err = reopened.Task("task-1")
	if err != nil {
		t.Fatalf("get task after reopen: %v", err)
	}
	if !ok || item.Result == nil || item.Result.Stdout != "ok\n" || item.State != "success" {
		t.Fatalf("unexpected stored result: %+v", item)
	}

	agents, err = reopened.Agents()
	if err != nil {
		t.Fatalf("agents after reopen: %v", err)
	}
	if len(agents) != 1 || agents[0].Online {
		t.Fatalf("expected agent offline after reopen: %+v", agents)
	}
}

func TestCancelQueuedTask(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "cancel.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	task := protocol.Task{
		ID:          "task-cancel",
		AgentID:     "agent-1",
		Type:        "shell",
		Command:     "sleep 60",
		TimeoutSecs: 60,
		CreatedAt:   time.Now().UTC(),
	}
	if err := store.AddTask(task); err != nil {
		t.Fatalf("add task: %v", err)
	}

	_, state, ok, err := store.CancelTask(task.ID)
	if err != nil {
		t.Fatalf("cancel task: %v", err)
	}
	if !ok || state != "canceled" {
		t.Fatalf("unexpected cancel result ok=%v state=%s", ok, state)
	}

	item, ok, err := store.Task(task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if !ok || item.State != "canceled" || item.Result == nil || item.Result.Status != "canceled" {
		t.Fatalf("unexpected canceled task: %+v", item)
	}
}

func TestMetricsAndTransferAuditPersistence(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if err := store.SaveAgentMetrics(protocol.MetricsReport{
		AgentID:            "agent-1",
		Timestamp:          time.Now().UTC(),
		UptimeSecs:         12,
		CPUCount:           8,
		Goroutines:         4,
		ProcessMemoryBytes: 1234,
		RootDiskTotalBytes: 9999,
		RootDiskFreeBytes:  5555,
	}); err != nil {
		t.Fatalf("save metrics: %v", err)
	}

	metrics, ok, err := store.AgentMetrics("agent-1")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	if !ok || metrics.CPUCount != 8 || metrics.RootDiskFreeBytes != 5555 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}

	if err := store.UpsertTransferAudit(TransferAudit{
		TransferID:       "tx-1",
		AgentID:          "agent-1",
		Direction:        "upload",
		LocalPath:        "/tmp/a",
		RemotePath:       "/tmp/b",
		Status:           "success",
		Size:             20,
		BytesTransferred: 20,
		ChecksumSHA256:   "abc",
		ChecksumVerified: true,
		CreatedAt:        time.Now().UTC(),
		CompletedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("save transfer audit: %v", err)
	}

	audit, ok, err := store.TransferAudit("tx-1")
	if err != nil {
		t.Fatalf("get transfer audit: %v", err)
	}
	if !ok || !audit.ChecksumVerified || audit.ChecksumSHA256 != "abc" {
		t.Fatalf("unexpected transfer audit: %+v", audit)
	}
}

func TestMetricsHistoryAppendsAndReturnsLatest(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	for i := range 3 {
		if err := store.SaveAgentMetrics(protocol.MetricsReport{
			AgentID:            "agent-1",
			Timestamp:          time.Now().UTC().Add(time.Duration(i) * time.Second),
			UptimeSecs:         int64(i),
			CPUCount:           8,
			Goroutines:         4 + i,
			ProcessMemoryBytes: 1000 + uint64(i),
			RootDiskTotalBytes: 9999,
			RootDiskFreeBytes:  5000 + uint64(i),
		}); err != nil {
			t.Fatalf("save metrics %d: %v", i, err)
		}
	}

	latest, ok, err := store.AgentMetrics("agent-1")
	if err != nil {
		t.Fatalf("get latest metrics: %v", err)
	}
	if !ok || latest.UptimeSecs != 2 || latest.Goroutines != 6 {
		t.Fatalf("unexpected latest metrics: %+v", latest)
	}

	history, err := store.AgentMetricsHistory("agent-1", 10)
	if err != nil {
		t.Fatalf("get metrics history: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("expected 3 history samples, got %d", len(history))
	}
	if history[0].UptimeSecs != 2 || history[2].UptimeSecs != 0 {
		t.Fatalf("unexpected history ordering: %+v", history)
	}
}

func TestMetricsHistoryMigratesLegacySchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE agent_metrics (
			agent_id TEXT PRIMARY KEY,
			timestamp TEXT NOT NULL,
			uptime_secs INTEGER NOT NULL,
			cpu_count INTEGER NOT NULL,
			goroutines INTEGER NOT NULL,
			process_memory_bytes INTEGER NOT NULL,
			root_disk_total_bytes INTEGER NOT NULL,
			root_disk_free_bytes INTEGER NOT NULL
		);`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO agent_metrics(agent_id, timestamp, uptime_secs, cpu_count, goroutines, process_memory_bytes, root_disk_total_bytes, root_disk_free_bytes)
		VALUES('agent-1', '2024-01-01T00:00:00Z', 1, 4, 2, 111, 222, 333)`); err != nil {
		t.Fatalf("insert legacy sample: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store (migration): %v", err)
	}
	defer store.Close()

	latest, ok, err := store.AgentMetrics("agent-1")
	if err != nil {
		t.Fatalf("get migrated metrics: %v", err)
	}
	if !ok || latest.UptimeSecs != 1 {
		t.Fatalf("expected migrated sample, got %+v", latest)
	}

	if err := store.SaveAgentMetrics(protocol.MetricsReport{
		AgentID:    "agent-1",
		Timestamp:  time.Now().UTC(),
		UptimeSecs: 2,
		CPUCount:   4,
	}); err != nil {
		t.Fatalf("append after migration: %v", err)
	}
	history, err := store.AgentMetricsHistory("agent-1", 10)
	if err != nil {
		t.Fatalf("history after migration: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 samples after migration, got %d", len(history))
	}
}

func TestTasksMigrationAddsDispatchedAt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tasks-legacy.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE tasks (
			id TEXT PRIMARY KEY,
			agent_id TEXT NOT NULL,
			type TEXT NOT NULL,
			command TEXT NOT NULL,
			timeout_secs INTEGER NOT NULL,
			priority INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			state TEXT NOT NULL
		);`); err != nil {
		t.Fatalf("create legacy tasks: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO tasks(id, agent_id, type, command, timeout_secs, priority, created_at, state)
		VALUES('t1', 'a1', 'shell', 'echo', 5, 0, '2024-01-01T00:00:00Z', 'dispatched')`); err != nil {
		t.Fatalf("insert legacy task: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("new store (migration): %v", err)
	}
	defer store.Close()

	tasks, err := store.DispatchedTasks()
	if err != nil {
		t.Fatalf("dispatched tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Task.ID != "t1" || tasks[0].DispatchedAt.IsZero() {
		t.Fatalf("unexpected dispatched tasks after migration: %+v", tasks)
	}
}
