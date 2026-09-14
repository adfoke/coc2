package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"coc2/internal/protocol"
)

func newLiveTransfer(t *testing.T, svc *Service, direction, status string) *transferState {
	t.Helper()
	dir := t.TempDir()
	state := &transferState{
		TransferStatus: TransferStatus{
			ID:         "tx-" + direction + "-" + status,
			AgentID:    "agent-1",
			Direction:  direction,
			LocalPath:  filepath.Join(dir, "local.bin"),
			RemotePath: "/tmp/remote.bin",
			Status:     status,
			CreatedAt:  time.Now().UTC(),
		},
		tempPath: filepath.Join(dir, "local.bin.part"),
		cancelCh: make(chan struct{}),
	}
	svc.putTransfer(state)
	svc.persistTransfer(state)
	return state
}

func TestCancelTransferMarksRequestedAndPersists(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	// upload whose pump is not running: cancel must still land the
	// intermediate state in the audit store (offline-agent case, sent=false).
	state := newLiveTransfer(t, svc, "upload", "running")

	snap, st, sent, found := svc.cancelTransfer(state.ID)
	if !found || st != "cancel_requested" || snap.Status != "cancel_requested" {
		t.Fatalf("unexpected cancel result: found=%v state=%q snap=%+v", found, st, snap)
	}
	if sent {
		t.Fatal("no agent connected: cancel_sent must be false")
	}

	audit, ok, err := svc.store.TransferAudit(state.ID)
	if err != nil || !ok {
		t.Fatalf("audit after cancel request: ok=%v err=%v", ok, err)
	}
	if audit.Status != "cancel_requested" || !audit.CompletedAt.IsZero() {
		t.Fatalf("audit row must hold the intermediate state: %+v", audit)
	}

	// The pump release signal fired for the server-owned side:
	select {
	case <-state.cancelCh:
	default:
		t.Fatal("upload pump cancelCh was not signaled")
	}

	// Re-issue keeps the state and reports it (cancel_sent stays false
	// without a client, but the request itself is idempotent).
	if _, st2, _, found2 := svc.cancelTransfer(state.ID); !found2 || st2 != "cancel_requested" {
		t.Fatalf("re-issue changed the state: found=%v state=%q", found2, st2)
	}
}

func TestCancelTransferTerminalIsIdempotent(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	state := newLiveTransfer(t, svc, "download", "success")
	snap, st, sent, found := svc.cancelTransfer(state.ID)
	if !found || st != "success" || snap.Status != "success" || sent {
		t.Fatalf("terminal cancel must be a no-op report: %q sent=%v snap=%+v", st, sent, snap)
	}
	if _, live := svc.getTransfer(state.ID); !live {
		t.Fatal("terminal-live state must stay live until its owner deletes it")
	}
}

func TestCancelTransferUnknownID(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	if _, _, _, found := svc.cancelTransfer("nope"); found {
		t.Fatal("unknown transfer must report found=false")
	}
}

func TestCancelTransferFallsBackToAuditStore(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	// Finalized transfer: gone from the live map, present in the audit log.
	state := newLiveTransfer(t, svc, "upload", "running")
	svc.finalizeCanceled(state, "canceled by operator")
	if _, live := svc.getTransfer(state.ID); live {
		t.Fatal("finalize must drop the live entry")
	}

	snap, st, sent, found := svc.cancelTransfer(state.ID)
	if !found || st != "canceled" || sent || snap.Status != "canceled" {
		t.Fatalf("canceled transfer via audit store: found=%v state=%q sent=%v", found, st, sent)
	}
}

func TestAgentCancelConfirmationFinalizesUpload(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	state := newLiveTransfer(t, svc, "upload", "cancel_requested")
	state.signalCancel()

	svc.handleTransferDone(protocol.FileTransferDone{
		TransferID:  state.ID,
		AgentID:     "agent-1",
		Direction:   "upload",
		Status:      "canceled",
		Message:     "canceled by operator",
		Size:        4096,
		CompletedAt: time.Now().UTC(),
	})

	if _, live := svc.getTransfer(state.ID); live {
		t.Fatal("agent confirmation must finalize the transfer")
	}
	audit, ok, err := svc.store.TransferAudit(state.ID)
	if err != nil || !ok {
		t.Fatalf("audit lookup: %v", err)
	}
	if audit.Status != "canceled" || audit.Message != "canceled by operator" {
		t.Fatalf("audit did not record the canceled confirmation: %+v", audit)
	}
	if audit.CompletedAt.IsZero() {
		t.Fatal("canceled row needs a completion timestamp")
	}
}

func TestLateDoneAfterTerminalDoesNotOverwrite(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	// A canceled transfer that never got dropped from live (simulate the
	// race where the reaper finalized it first) must keep "canceled" even
	// when a success confirmation arrives late.
	state := newLiveTransfer(t, svc, "upload", "canceled")

	svc.handleTransferDone(protocol.FileTransferDone{
		TransferID: state.ID,
		Status:     "success",
		Size:       99,
	})

	state.mu.Lock()
	got := state.Status
	state.mu.Unlock()
	if got != "canceled" {
		t.Fatalf("late success overwrote terminal canceled: %q", got)
	}
}

func TestCanceledDownloadKeepsPartialAndFinalizes(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	state := newLiveTransfer(t, svc, "download", "cancel_requested")
	// Simulate a partially written .part file the pump left behind.
	if err := os.WriteFile(state.tempPath, []byte("half-written"), 0o600); err != nil {
		t.Fatalf("seed partial: %v", err)
	}
	var file *os.File
	f, err := os.Create(state.tempPath)
	if err == nil {
		file = f
		state.mu.Lock()
		state.file = file
		state.mu.Unlock()
	}

	svc.handleTransferDone(protocol.FileTransferDone{
		TransferID:  state.ID,
		Direction:   "download",
		Status:      "canceled",
		Message:     "canceled by agent",
		CompletedAt: time.Now().UTC(),
	})

	audit, ok, _ := svc.store.TransferAudit(state.ID)
	if !ok || audit.Status != "canceled" {
		t.Fatalf("download cancel must finalize as canceled: %+v ok=%v", audit, ok)
	}
	// The partial must survive (resume contract); the final target must not exist.
	if _, err := os.Stat(state.tempPath); err != nil {
		t.Fatalf("partial .part must be kept for resume: %v", err)
	}
	if _, err := os.Stat(state.LocalPath); !os.IsNotExist(err) {
		t.Fatal("canceled download must not produce the final file")
	}
}

func TestReapCanceledTransfersFinalizesAfterGrace(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	stale := newLiveTransfer(t, svc, "upload", "cancel_requested")
	stale.mu.Lock()
	stale.cancelRequestedAt = time.Now().UTC().Add(-transferCancelGrace - time.Second)
	stale.mu.Unlock()

	fresh := newLiveTransfer(t, svc, "download", "cancel_requested")
	fresh.mu.Lock()
	fresh.cancelRequestedAt = time.Now().UTC()
	fresh.mu.Unlock()

	if n := svc.reapCanceledTransfers(time.Now().UTC()); n != 1 {
		t.Fatalf("expected exactly the stale cancel to be finalized, got %d", n)
	}
	audit, ok, _ := svc.store.TransferAudit(stale.ID)
	if !ok || audit.Status != "canceled" {
		t.Fatalf("stale cancel not finalized in audit: %+v", audit)
	}
	if _, live := svc.getTransfer(fresh.ID); !live {
		t.Fatal("fresh cancel must stay in cancel_requested until grace")
	}
}

func TestSignalCancelNilSafeAndIdempotent(t *testing.T) {
	// Hand-built states (tests, legacy paths) have no cancelCh: signaling
	// must not panic. With a channel, a second signal must not double-close.
	var nilState transferState
	nilState.signalCancel()

	s := transferState{cancelCh: make(chan struct{})}
	s.signalCancel()
	s.signalCancel()
	select {
	case <-s.cancelCh:
	default:
		t.Fatal("cancelCh must be closed")
	}
}

// TestDownloadAckDoesNotResurrectCanceledTransfer regression: the server's
// own handleTransferStart (agent ack for a download) used to unconditionally
// set status=running, clobbering a cancel_requested the operator had just
// persisted (found live during 4 GB e2e via the upload prologue; the same
// overwrite existed on the download receiver init path).
func TestDownloadAckDoesNotResurrectCanceledTransfer(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	dir := t.TempDir()
	tempPath := filepath.Join(dir, "out.part")
	state := &transferState{
		TransferStatus: TransferStatus{
			ID: "tx-no-resurrect", AgentID: "agent-1", Direction: "download",
			LocalPath: filepath.Join(dir, "out.txt"), RemotePath: "/tmp/x",
			Status: "cancel_requested", ChunkSize: 4, CreatedAt: time.Now().UTC(),
		},
		tempPath: tempPath,
	}
	svc.putTransfer(state)
	svc.persistTransfer(state)

	svc.handleTransferStart(protocol.FileTransferStart{
		TransferID: state.ID, Direction: "download", Size: 12, ChunkSize: 4,
	})

	state.mu.Lock()
	got := state.Status
	state.mu.Unlock()
	if got != "cancel_requested" {
		t.Fatalf("download ack must not overwrite cancel_requested, got %q", got)
	}
}

// TestUploadPumpNeverStartsCanceledTransfer regression (the real 4 GB e2e
// bug): cancellation landing during the checksum prologue — seconds on big
// files — used to be clobbered back to "running" when the pump resumed.
func TestUploadPumpNeverStartsCanceledTransfer(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	src := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(src, []byte("hello-payload"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	state := &transferState{
		TransferStatus: TransferStatus{
			ID: "tx-race-prologue", AgentID: "agent-1", Direction: "upload",
			LocalPath: src, RemotePath: "/tmp/remote.bin",
			Status: "cancel_requested", Size: 13, ChunkSize: 4096,
			CreatedAt: time.Now().UTC(),
		},
		cancelCh: make(chan struct{}),
	}
	svc.putTransfer(state)
	svc.persistTransfer(state)

	// A nil client is deliberate: the prologue guard must settle the
	// cancel before ANY use of the connection. If the guard regresses,
	// this test crashes (nil deref) instead of silently sending frames to
	// an agent that does not exist — both outcomes are loud, neither is
	// false-green.
	svc.runUpload(nil, state)

	audit, ok, err := svc.store.TransferAudit(state.ID)
	if err != nil || !ok {
		t.Fatalf("audit lookup: ok=%v err=%v", ok, err)
	}
	if audit.Status != "canceled" {
		t.Fatalf("prologue-race cancel must settle as canceled, got %q (%s)", audit.Status, audit.Message)
	}
	if _, live := svc.getTransfer(state.ID); live {
		t.Fatal("pump must drop the live transfer after finalizing")
	}
}

func TestTransferCancelEndpoint(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	state := newLiveTransfer(t, svc, "download", "running")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/transfers/"+state.ID+"/cancel", nil)
	setTestAuth(req)
	rec := httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["status"] != "cancel_requested" || resp["transfer_id"] != state.ID {
		t.Fatalf("response: %+v", resp)
	}
	if resp["cancel_sent"] != false {
		t.Fatalf("no agent connected, cancel_sent must be false: %+v", resp)
	}

	// Unknown id -> 404.
	req = httptest.NewRequest(http.MethodPost, "/api/v1/transfers/nope/cancel", nil)
	setTestAuth(req)
	rec = httptest.NewRecorder()
	svc.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown transfer = %d, want 404", rec.Code)
	}

	// The cancel POST itself is an audited write operation.
	entry := pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Path == "/api/v1/transfers/"+state.ID+"/cancel"
	})
	if !entry.OK || entry.Ref != "transfer_id="+state.ID {
		t.Fatalf("oplog row for cancel: %+v", entry)
	}
	if len(entry.Agents) != 1 || entry.Agents[0] != "agent-1" {
		t.Fatalf("cancel oplog must name the target agent: %+v", entry.Agents)
	}
}
