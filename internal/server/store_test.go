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

	if err := store.AddTask(task, time.Time{}); err != nil {
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
	if err := store.AddTask(task, time.Time{}); err != nil {
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

// TestTaskReleaseAtRoundTrip covers the store half of batch spread: a deferred
// task must stay invisible to both dispatch paths until its release time, then
// become visible to the tick, with the timestamp surviving the round trip.
func TestTaskReleaseAtRoundTrip(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "release.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	release := now.Add(30 * time.Minute)

	// Two deferred tasks due at the same instant (priority decides their
	// order), plus one immediately-available task.
	for _, task := range []protocol.Task{
		{ID: "task-low", AgentID: "agent-1", Type: "shell", Command: "echo low", TimeoutSecs: 5, CreatedAt: now},
		{ID: "task-high", AgentID: "agent-1", Type: "shell", Command: "echo high", TimeoutSecs: 5, Priority: 9, CreatedAt: now},
	} {
		if err := store.AddTask(task, release); err != nil {
			t.Fatalf("add deferred %s: %v", task.ID, err)
		}
	}
	immediate := protocol.Task{ID: "task-now", AgentID: "agent-1", Type: "shell", Command: "echo now", TimeoutSecs: 5, CreatedAt: now}
	if err := store.AddTask(immediate, time.Time{}); err != nil {
		t.Fatalf("add immediate task: %v", err)
	}

	// A reconnect must not flush deferred work early: only the immediate task
	// is deliverable.
	pending, err := store.PendingTasks("agent-1")
	if err != nil {
		t.Fatalf("pending tasks: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "task-now" {
		t.Fatalf("pending = %+v, want only the immediate task", pending)
	}

	// Neither may the tick, until the window opens.
	if ready, err := store.ReadyQueuedTasks(now); err != nil {
		t.Fatalf("ready tasks: %v", err)
	} else if len(ready) != 0 {
		t.Fatalf("ready = %+v, want none before release", ready)
	}

	ready, err := store.ReadyQueuedTasks(release.Add(time.Second))
	if err != nil {
		t.Fatalf("ready tasks after release: %v", err)
	}
	if len(ready) != 2 {
		t.Fatalf("ready after release = %+v, want the two deferred tasks", ready)
	}
	// Same ordering rule as PendingTasks, so a spread batch dispatches in the
	// order an operator would expect.
	if ready[0].Task.ID != "task-high" || ready[1].Task.ID != "task-low" {
		t.Fatalf("ready order = [%s %s], want [task-high task-low]", ready[0].Task.ID, ready[1].Task.ID)
	}
	// The full task and its release time must round-trip intact: the tick
	// dispatches straight from this struct with no second query.
	if got := ready[1]; got.Task.Command != "echo low" || got.Task.AgentID != "agent-1" || got.Task.TimeoutSecs != 5 || !got.ReleaseAt.Equal(release) {
		t.Fatalf("deferred task did not round-trip: %+v", got)
	}
}

// TestNextDeferredRelease pins the exact-wakeup query the release scheduler
// depends on: it must report the soonest genuinely future deferral, drop
// already-due tasks (returning one would arm the timer for the past and make
// the loop spin), ignore NULL release_at, and answer "nothing scheduled" when
// the queue holds no future work.
func TestNextDeferredRelease(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "next-release.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()

	// Empty queue: no scheduled work, and that is not an error.
	if _, ok, err := store.NextDeferredRelease(now); err != nil || ok {
		t.Fatalf("empty queue: ok=%v err=%v, want false nil", ok, err)
	}

	soon := now.Add(5 * time.Minute)
	later := now.Add(20 * time.Minute)
	// Adding whole minutes to the same instant keeps the sub-second part
	// identical, so these timestamps differ only in a way string comparison
	// orders correctly.
	for _, task := range []protocol.Task{
		{ID: "t-later", AgentID: "a1", Type: "shell", Command: "echo later", TimeoutSecs: 5, CreatedAt: now},
		{ID: "t-soon", AgentID: "a1", Type: "shell", Command: "echo soon", TimeoutSecs: 5, CreatedAt: now},
	} {
		release := later
		if task.ID == "t-soon" {
			release = soon
		}
		if err := store.AddTask(task, release); err != nil {
			t.Fatalf("add %s: %v", task.ID, err)
		}
	}
	// An immediately-available task (NULL release_at) is not deferred, so the
	// scheduler has nothing to wake for and must never be handed it.
	if err := store.AddTask(protocol.Task{
		ID: "t-now", AgentID: "a1", Type: "shell", Command: "echo now", TimeoutSecs: 5, CreatedAt: now,
	}, time.Time{}); err != nil {
		t.Fatalf("add immediate: %v", err)
	}

	got, ok, err := store.NextDeferredRelease(now)
	if err != nil || !ok {
		t.Fatalf("next deferred: ok=%v err=%v, want true nil", ok, err)
	}
	if !got.Equal(soon) {
		t.Fatalf("next deferred = %v, want the earliest future deferral %v", got, soon)
	}

	// A task whose release has arrived must fall out of the query: a due task
	// an offline agent has not taken stays queued forever, so reporting it
	// would wake the scheduler for it again and again with no progress.
	got, ok, err = store.NextDeferredRelease(soon)
	if err != nil || !ok {
		t.Fatalf("next deferred at soon: ok=%v err=%v", ok, err)
	}
	if !got.Equal(later) {
		t.Fatalf("next deferred at soon = %v, want %v (the due task must be excluded)", got, later)
	}

	// Past every deferral: nothing is scheduled.
	if _, ok, err := store.NextDeferredRelease(later); err != nil || ok {
		t.Fatalf("next deferred past all: ok=%v err=%v, want false nil", ok, err)
	}
}

// TestTasksMigrationAddsReleaseAt is the legacy-database guard: an old tasks
// table has no release_at column, and after migration that must be a no-op for
// existing rows (NULL = "release now"), not an invented deferral.
func TestTasksMigrationAddsReleaseAt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tasks-legacy-release.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	// Pre-migration shape: neither dispatched_at nor release_at exists.
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
		VALUES('t-legacy', 'a1', 'shell', 'echo', 5, 0, '2024-01-01T00:00:00Z', 'queued')`); err != nil {
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

	// The new column defaults to NULL, so the legacy queued row stays
	// immediately deliverable.
	pending, err := store.PendingTasks("a1")
	if err != nil {
		t.Fatalf("pending tasks: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "t-legacy" {
		t.Fatalf("legacy queued task should still be pending: %+v", pending)
	}
	// And it is deliberately invisible to the tick (release_at IS NULL).
	if ready, err := store.ReadyQueuedTasks(time.Now().UTC()); err != nil {
		t.Fatalf("ready tasks: %v", err)
	} else if len(ready) != 0 {
		t.Fatalf("NULL release_at must not be picked up by the tick: %+v", ready)
	}

	// The migrated column accepts deferred writes too.
	if err := store.AddTask(protocol.Task{
		ID: "t-new", AgentID: "a1", Type: "shell", Command: "echo", TimeoutSecs: 5, CreatedAt: time.Now().UTC(),
	}, time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatalf("add deferred task after migration: %v", err)
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
