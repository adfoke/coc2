package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	"coc2/internal/protocol"
)

func retentionStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "ret.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func seedHistory(t *testing.T, store *Store, old time.Time) {
	t.Helper()

	// Old terminal task + result.
	if err := store.AddTask(protocol.Task{ID: "old-task", AgentID: "a1", Type: "shell", Command: "echo", TimeoutSecs: 5, CreatedAt: old}); err != nil {
		t.Fatalf("seed old task: %v", err)
	}
	if err := store.SaveResult(protocol.TaskResult{TaskID: "old-task", AgentID: "a1", Status: "success", CompletedAt: old}); err != nil {
		t.Fatalf("seed old result: %v", err)
	}

	// Old transfer audit row (completed).
	if err := store.UpsertTransferAudit(TransferAudit{
		TransferID: "old-tx", AgentID: "a1", Direction: "upload",
		Status: "success", CompletedAt: old, CreatedAt: old,
	}); err != nil {
		t.Fatalf("seed old transfer: %v", err)
	}

	// Old oplog row.
	if err := store.AddOpLog(OpLogEntry{RequestID: "old-req", TS: old, Plane: "uds", Actor: "ghost", Method: "POST", Path: "/p", Status: 202, OK: true}); err != nil {
		t.Fatalf("seed old oplog: %v", err)
	}
}

func TestPruneAuditsDeletesOldKeepsNew(t *testing.T) {
	store := retentionStore(t)
	now := time.Now().UTC()
	old := now.AddDate(0, 0, -400)

	seedHistory(t, store, old)

	// Fresh counterparts that must survive.
	if err := store.AddTask(protocol.Task{ID: "new-task", AgentID: "a1", Type: "shell", Command: "echo", TimeoutSecs: 5, CreatedAt: now}); err != nil {
		t.Fatalf("seed new task: %v", err)
	}
	if err := store.SaveResult(protocol.TaskResult{TaskID: "new-task", AgentID: "a1", Status: "success", CompletedAt: now}); err != nil {
		t.Fatalf("seed new result: %v", err)
	}
	if err := store.UpsertTransferAudit(TransferAudit{
		TransferID: "new-tx", AgentID: "a1", Direction: "download",
		Status: "success", CreatedAt: now, CompletedAt: now,
	}); err != nil {
		t.Fatalf("seed new transfer: %v", err)
	}
	if err := store.AddOpLog(OpLogEntry{RequestID: "new-req", TS: now, Plane: "uds", Actor: "alive", Method: "POST", Path: "/p", Status: 202, OK: true}); err != nil {
		t.Fatalf("seed new oplog: %v", err)
	}

	cutoff := now.AddDate(0, 0, -90)
	counts, err := store.PruneAudits(cutoff)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if counts["tasks"] != 1 || counts["task_results"] != 1 || counts["transfers"] != 1 || counts["oplog"] != 1 {
		t.Fatalf("unexpected removal counts: %+v", counts)
	}

	if _, ok, _ := store.Task("old-task"); ok {
		t.Fatal("old task must be pruned")
	}
	st, ok, err := store.Task("new-task")
	if err != nil || !ok || st.Result == nil || st.Result.Status != "success" {
		t.Fatalf("new task must survive with its result: ok=%v err=%v", ok, err)
	}
	if _, ok, _ := store.TransferAudit("old-tx"); ok {
		t.Fatal("old transfer must be pruned")
	}
	if _, ok, _ := store.TransferAudit("new-tx"); !ok {
		t.Fatal("new transfer must survive")
	}
	entries, err := store.RecentOpLogs(OpLogQuery{})
	if err != nil {
		t.Fatalf("read oplog: %v", err)
	}
	if len(entries) != 1 || entries[0].RequestID != "new-req" {
		t.Fatalf("oplog after prune: %+v", entries)
	}
}

func TestPruneAuditsNeverTouchesLiveTasks(t *testing.T) {
	store := retentionStore(t)
	now := time.Now().UTC()
	ancient := now.AddDate(0, 0, -4000)

	// A queued task and a dispatched task, both far past any retention.
	if err := store.AddTask(protocol.Task{ID: "queued-forever", AgentID: "a1", Type: "shell", Command: "echo", TimeoutSecs: 5, CreatedAt: ancient}); err != nil {
		t.Fatalf("seed queued: %v", err)
	}
	if err := store.AddTask(protocol.Task{ID: "dispatched-forever", AgentID: "a1", Type: "shell", Command: "echo", TimeoutSecs: 5, CreatedAt: ancient}); err != nil {
		t.Fatalf("seed dispatched: %v", err)
	}
	if err := store.MarkDispatched("dispatched-forever"); err != nil {
		t.Fatalf("mark dispatched: %v", err)
	}
	// An in-flight (not completed) transfer row too.
	if err := store.UpsertTransferAudit(TransferAudit{
		TransferID: "running-tx", AgentID: "a1", Direction: "upload",
		Status: "running", CreatedAt: ancient, // CompletedAt zero => not eligible
	}); err != nil {
		t.Fatalf("seed running transfer: %v", err)
	}

	if _, err := store.PruneAudits(now.AddDate(0, 0, -90)); err != nil {
		t.Fatalf("prune: %v", err)
	}

	for _, id := range []string{"queued-forever", "dispatched-forever"} {
		if _, ok, _ := store.Task(id); !ok {
			t.Fatalf("live task %q must never be pruned", id)
		}
	}
	if _, ok, _ := store.TransferAudit("running-tx"); !ok {
		t.Fatal("non-terminal transfer must never be pruned")
	}
}

func TestPruneAuditsSweepsOrphanResults(t *testing.T) {
	// If a task row was removed but its result stayed (manual tampering or
	// a crash between the two deletes), the next pass must reclaim it.
	store := retentionStore(t)
	now := time.Now().UTC()
	old := now.AddDate(0, 0, -400)

	if err := store.AddTask(protocol.Task{ID: "orphan-task", AgentID: "a1", Type: "shell", Command: "echo", TimeoutSecs: 5, CreatedAt: old}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := store.SaveResult(protocol.TaskResult{TaskID: "orphan-task", AgentID: "a1", Status: "success", CompletedAt: old}); err != nil {
		t.Fatalf("seed result: %v", err)
	}
	if _, err := store.db.Exec(`DELETE FROM tasks WHERE id = 'orphan-task'`); err != nil {
		t.Fatalf("simulate orphan: %v", err)
	}

	counts, err := store.PruneAudits(now.AddDate(0, 0, -90))
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if counts["task_results"] != 1 {
		t.Fatalf("orphan result must be swept, counts=%+v", counts)
	}
	var n int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM task_results`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("results remain: %d", n)
	}
}

func TestRetentionOffByDefault(t *testing.T) {
	// AuditRetentionDays == 0 must keep ancient rows: deleting evidence by
	// default is the wrong behaviour for an ops tool.
	svc, cleanup := newTestService(t)
	defer cleanup()

	old := time.Now().UTC().AddDate(0, 0, -9999)
	if err := svc.store.AddOpLog(OpLogEntry{RequestID: "ancient", TS: old, Plane: "uds", Actor: "a", Method: "POST", Path: "/p", Status: 202, OK: true}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	svc.pruneAuditsIfNeeded(time.Now().UTC())

	entries, _ := svc.store.RecentOpLogs(OpLogQuery{})
	if len(entries) != 1 {
		t.Fatalf("retention=0 must not delete, rows=%d", len(entries))
	}
}

func TestRetentionRunsWhenConfigured(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()
	svc.cfg.AuditRetentionDays = 30

	old := time.Now().UTC().AddDate(0, 0, -45)
	newer := time.Now().UTC().AddDate(0, 0, -5)
	if err := svc.store.AddOpLog(OpLogEntry{RequestID: "old-9", TS: old, Plane: "uds", Actor: "a", Method: "POST", Path: "/p", Status: 202, OK: true}); err != nil {
		t.Fatalf("insert old: %v", err)
	}
	if err := svc.store.AddOpLog(OpLogEntry{RequestID: "recent", TS: newer, Plane: "uds", Actor: "a", Method: "POST", Path: "/p", Status: 202, OK: true}); err != nil {
		t.Fatalf("insert new: %v", err)
	}

	svc.pruneAuditsIfNeeded(time.Now().UTC())

	entries, _ := svc.store.RecentOpLogs(OpLogQuery{})
	if len(entries) != 1 || entries[0].RequestID != "recent" {
		t.Fatalf("30d retention kept wrong rows: %+v", entries)
	}
}

func TestShutdownStopsExtendedReapLoop(t *testing.T) {
	// reapLoop now also drives the daily prune; the stop/close handshake
	// must still cover the extended loop body so Shutdown returns.
	svc, err := New(Config{
		ListenAddr:      ":0",
		OperatorUDSPath: filepath.Join(t.TempDir(), "s.sock"),
		AuthToken:       "tok",
		DBPath:          filepath.Join(t.TempDir(), "t.db"),
		WriteWait:       time.Second,
		PongWait:        time.Second,
		PingPeriod:      time.Second,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := svc.listenOperatorUDS(); err != nil {
		t.Fatalf("uds: %v", err)
	}
	go svc.reapLoop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = svc.Shutdown(ctx)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("Shutdown did not return (prune-extended reap loop not stopping?)")
	}
}
