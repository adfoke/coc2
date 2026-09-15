package server

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"coc2/internal/common"
	"coc2/internal/protocol"
)

const (
	transferAuditMinInterval = time.Second
	transferAuditMinBytes    = 1 << 20
	transferResumeTimeout    = 30 * time.Second
	transferStallTimeout     = 10 * time.Minute
	// transferCancelGrace is how long a cancel_requested transfer waits for
	// the agent's canceled confirmation before the reaper finalizes it.
	transferCancelGrace = 30 * time.Second
)

// isTerminalTransferStatus reports whether a status ends the transfer's
// lifecycle. "cancel_requested" is deliberately NOT terminal: it is an
// intermediate state awaiting confirmation (from the agent or the reaper).
func isTerminalTransferStatus(status string) bool {
	switch status {
	case "success", "failed", "canceled":
		return true
	}
	return false
}

type TransferStatus struct {
	ID               string    `json:"transfer_id"`
	AgentID          string    `json:"agent_id"`
	Direction        string    `json:"direction"`
	LocalPath        string    `json:"local_path,omitempty"`
	RemotePath       string    `json:"remote_path"`
	Status           string    `json:"status"`
	Message          string    `json:"message,omitempty"`
	Size             int64     `json:"size"`
	BytesTransferred int64     `json:"bytes_transferred"`
	ChunkSize        int       `json:"chunk_size"`
	ChecksumSHA256   string    `json:"checksum_sha256,omitempty"`
	ChecksumVerified bool      `json:"checksum_verified"`
	CreatedAt        time.Time `json:"created_at"`
	CompletedAt      time.Time `json:"completed_at"`
}

type transferState struct {
	TransferStatus

	tempPath string
	file     *os.File
	mu       sync.Mutex

	assembler      *common.ChunkAssembler
	expectedChunks int

	resumeCh chan int64

	// cancelRequestedAt stamps the moment cancellation was requested; the
	// reaper uses it to finalize transfers whose agent never acknowledged.
	cancelRequestedAt time.Time
	// cancelCh is closed (idempotently, via cancelOnce) when cancellation
	// is requested; the upload pump selects on it so a server->agent push
	// stops at the next chunk. Nil-safe: a nil channel never fires, and
	// signalCancel skips it, so hand-built test states need no wiring.
	cancelCh   chan struct{}
	cancelOnce sync.Once

	lastPersistedAt    time.Time
	lastPersistedBytes int64
}

func (t *transferState) snapshot() TransferStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotLocked()
}

func (t *transferState) snapshotLocked() TransferStatus {
	return t.TransferStatus
}

func (s *Service) startUpload(agentID, localPath, remotePath string, chunkSize int) (TransferStatus, error) {
	client, err := s.clientForAgent(agentID)
	if err != nil {
		return TransferStatus{}, err
	}
	if chunkSize <= 0 {
		chunkSize = 256 * 1024
	}

	info, err := os.Stat(localPath)
	if err != nil {
		return TransferStatus{}, err
	}
	if info.IsDir() {
		return TransferStatus{}, errors.New("local_path must be a file")
	}

	state := &transferState{
		ID:         common.NewID(),
		AgentID:    agentID,
		Direction:  "upload",
		LocalPath:  localPath,
		RemotePath: remotePath,
		Status:     "queued",
		Size:       info.Size(),
		ChunkSize:  chunkSize,
		CreatedAt:  time.Now().UTC(),
		resumeCh:   make(chan int64, 1),
		cancelCh:   make(chan struct{}),
	}
	s.putTransfer(state)
	s.persistTransfer(state)

	go s.runUpload(client, state)
	return state.snapshot(), nil
}

func (s *Service) startDownload(agentID, remotePath, localPath string, chunkSize int) (TransferStatus, error) {
	client, err := s.clientForAgent(agentID)
	if err != nil {
		return TransferStatus{}, err
	}
	if chunkSize <= 0 {
		chunkSize = 256 * 1024
	}
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return TransferStatus{}, err
	}

	tempPath := localPath + ".part"
	offset, err := common.ResumeOffset(tempPath, chunkSize)
	if err != nil {
		return TransferStatus{}, err
	}

	state := &transferState{
		ID:               common.NewID(),
		AgentID:          agentID,
		Direction:        "download",
		LocalPath:        localPath,
		RemotePath:       remotePath,
		Status:           "requested",
		ChunkSize:        chunkSize,
		BytesTransferred: offset,
		CreatedAt:        time.Now().UTC(),
		tempPath:         tempPath,
		cancelCh:         make(chan struct{}),
	}
	s.putTransfer(state)
	s.persistTransfer(state)

	start := protocol.FileTransferStart{
		TransferID:  state.ID,
		AgentID:     agentID,
		Direction:   "download",
		LocalPath:   localPath,
		RemotePath:  remotePath,
		Offset:      offset,
		ChunkSize:   chunkSize,
		RequestedAt: time.Now().UTC(),
	}
	if err := client.sendMessage(protocol.TypeFileTransferStart, start); err != nil {
		s.finishTransferWithError(state, err)
		return TransferStatus{}, err
	}

	return state.snapshot(), nil
}

func (s *Service) runUpload(client *agentConn, state *transferState) {
	file, err := os.Open(state.LocalPath)
	if err != nil {
		s.finishTransferWithError(state, err)
		return
	}
	defer file.Close()

	checksum, err := common.FileSHA256(state.LocalPath)
	if err != nil {
		s.finishTransferWithError(state, err)
		return
	}

	// Prologue under state.mu: check cancellation and enqueue file_transfer_start
	// atomically. WebSocket writes are ordered per connection, so holding the
	// lock across the send serializes against cancelTransfer — the agent can
	// never observe start-after-cancel (an orphan receiver streaming into a
	// transfer the server already settled).
	state.mu.Lock()
	if state.Status == "cancel_requested" {
		// Canceled while the checksum ran (seconds on large files): never
		// start the agent-side receiver — settle the cancel immediately.
		state.mu.Unlock()
		s.finalizeCanceled(state, "canceled by operator")
		return
	}
	start := protocol.FileTransferStart{
		TransferID:     state.ID,
		AgentID:        state.AgentID,
		Direction:      state.Direction,
		LocalPath:      state.LocalPath,
		RemotePath:     state.RemotePath,
		Size:           state.Size,
		ChunkSize:      state.ChunkSize,
		ChecksumSHA256: checksum,
		RequestedAt:    time.Now().UTC(),
	}
	startErr := client.sendMessage(protocol.TypeFileTransferStart, start)
	if startErr == nil {
		state.Status = "running"
		state.ChecksumSHA256 = checksum
		s.persistTransferLocked(state)
	}
	state.mu.Unlock()
	if startErr != nil {
		s.finishTransferWithError(state, startErr)
		return
	}

	offset, err := s.waitUploadResume(state)
	if err != nil {
		if errors.Is(err, errTransferCanceled) {
			// Canceled while awaiting the agent's resume offset: same
			// two-phase exit as the pump — stop, stay cancel_requested,
			// let confirmation or the reaper finalize it.
			return
		}
		s.finishTransferWithError(state, err)
		return
	}
	if offset < 0 {
		offset = 0
	}
	if offset > state.Size {
		offset = state.Size
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		s.finishTransferWithError(state, err)
		return
	}
	state.mu.Lock()
	state.BytesTransferred = offset
	state.mu.Unlock()

	buf := make([]byte, state.ChunkSize)
	seq := int(offset / int64(state.ChunkSize))
	for {
		select {
		case <-state.cancelCh:
			// Operator canceled: stop pumping and leave the transfer in
			// cancel_requested. The terminal status comes from the agent's
			// FileTransferDone confirmation (a racing completion still
			// records success — the file really did land), or from the
			// reaper once the grace window expires. Same two-phase shape
			// as task cancellation.
			return
		default:
		}
		n, readErr := file.Read(buf)
		if n > 0 {
			chunk := protocol.FileTransferChunk{
				TransferID: state.ID,
				Seq:        seq,
				Data:       buf[:n],
			}
			if err := client.sendMessage(protocol.TypeFileTransferChunk, chunk); err != nil {
				s.finishTransferWithError(state, err)
				return
			}
			state.mu.Lock()
			state.BytesTransferred += int64(n)
			if s.shouldPersistTransferProgressLocked(state, time.Now().UTC()) {
				s.persistTransferLocked(state)
			}
			state.mu.Unlock()
			seq++
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			if errors.Is(readErr, os.ErrClosed) {
				s.finishTransferWithError(state, readErr)
				return
			}
			s.finishTransferWithError(state, readErr)
			return
		}
	}

	state.mu.Lock()
	state.Status = "waiting_remote"
	state.mu.Unlock()
	s.persistTransfer(state)

	if err := client.sendMessage(protocol.TypeFileTransferDone, protocol.FileTransferDone{
		TransferID:     state.ID,
		AgentID:        state.AgentID,
		Direction:      state.Direction,
		Status:         "complete",
		Size:           state.Size,
		ChecksumSHA256: checksum,
		CompletedAt:    time.Now().UTC(),
	}); err != nil {
		s.finishTransferWithError(state, err)
	}
}

// errTransferCanceled sentinels a pump exit caused by cancellation; the
// caller must finalize as canceled, never as failed.
var errTransferCanceled = errors.New("transfer canceled")

func (s *Service) waitUploadResume(state *transferState) (int64, error) {
	timer := time.NewTimer(transferResumeTimeout)
	defer timer.Stop()
	select {
	case offset := <-state.resumeCh:
		return offset, nil
	case <-state.cancelCh:
		return 0, errTransferCanceled
	case <-timer.C:
		return 0, errors.New("timed out waiting for agent resume offset")
	}
}

func (s *Service) handleTransferStart(msg protocol.FileTransferStart) {
	state, ok := s.getTransfer(msg.TransferID)
	if !ok || state.Direction != "download" {
		return
	}

	var fail error
	state.mu.Lock()
	if state.file == nil {
		chunkSize := msg.ChunkSize
		if chunkSize <= 0 {
			chunkSize = state.ChunkSize
		}
		if chunkSize <= 0 {
			chunkSize = 256 * 1024
		}
		offset := state.BytesTransferred
		file, err := common.OpenPartialFile(state.tempPath, offset)
		if err != nil {
			fail = err
		} else {
			state.file = file
			assembler := common.NewChunkAssembler(0)
			assembler.Resume(int(offset / int64(chunkSize)))
			state.assembler = assembler
			state.expectedChunks = common.ChunkCount(msg.Size, chunkSize)
		}
	}
	if fail == nil {
		// Keep cancel_requested if the operator already pulled the plug:
		// this ack only initializes the receiver, it must not resurrect a
		// canceled transfer as running (the agent's confirmation or the
		// reaper owns the terminal state now).
		if state.Status != "cancel_requested" {
			state.Status = "running"
		}
		state.Size = msg.Size
		s.persistTransferLocked(state)
	}
	state.mu.Unlock()
	if fail != nil {
		s.finishTransferWithError(state, fail)
	}
}

func (s *Service) handleTransferResume(msg protocol.FileTransferResume) {
	state, ok := s.getTransfer(msg.TransferID)
	if !ok || state.Direction != "upload" {
		return
	}

	state.mu.Lock()
	if state.resumeCh != nil {
		select {
		case state.resumeCh <- msg.Offset:
		default:
		}
	}
	state.mu.Unlock()
}

func (s *Service) handleTransferChunk(msg protocol.FileTransferChunk) {
	state, ok := s.getTransfer(msg.TransferID)
	if !ok || state.Direction != "download" {
		return
	}

	data := msg.Data

	state.mu.Lock()
	if state.file == nil {
		state.mu.Unlock()
		s.finishTransferWithError(state, errors.New("download file not initialized"))
		return
	}
	if state.assembler == nil {
		state.assembler = common.NewChunkAssembler(0)
	}
	if err := state.assembler.Write(state.file, msg.Seq, data); err != nil {
		state.mu.Unlock()
		s.finishTransferWithError(state, err)
		return
	}
	state.BytesTransferred += int64(len(data))
	if s.shouldPersistTransferProgressLocked(state, time.Now().UTC()) {
		s.persistTransferLocked(state)
	}
	state.mu.Unlock()
}

// handleTransferDone applies an agent's transfer confirmation. It reports
// whether the transfer was still live, so the caller can audit a confirmation
// that matched nothing.
func (s *Service) handleTransferDone(msg protocol.FileTransferDone) bool {
	state, ok := s.getTransfer(msg.TransferID)
	if !ok {
		return false
	}

	if state.Direction == "download" {
		s.finishDownload(state, msg)
		return true
	}

	state.mu.Lock()
	if isTerminalTransferStatus(state.Status) {
		// Late confirmation after a local terminal outcome (e.g. the pump
		// already failed the transfer when the link dropped): keep the
		// first terminal status — it is what the audit trail says happened.
		state.mu.Unlock()
		return true
	}
	state.Status = msg.Status
	state.Message = msg.Message
	state.Size = msg.Size
	state.ChecksumSHA256 = msg.ChecksumSHA256
	state.ChecksumVerified = msg.Status == "success"
	state.CompletedAt = msg.CompletedAt
	if state.CompletedAt.IsZero() {
		state.CompletedAt = time.Now().UTC()
	}
	s.plugins.Trigger("transfer_done", state.snapshotLocked())
	s.persistTransferLocked(state)
	state.mu.Unlock()
	s.deleteTransfer(state.ID)
	return true
}

// finalizeDownloadFailed records a failed download while the state lock is
// held, then cleans up, persists, emits the audit hook and removes the
// transfer.
func (s *Service) finalizeDownloadFailed(state *transferState, status, message string, removeLocal bool) {
	state.Status = status
	state.Message = message
	s.cleanupTransferFilesLocked(state, removeLocal)
	snap := state.snapshotLocked()
	s.persistTransferLocked(state)
	state.mu.Unlock()
	s.plugins.Trigger("transfer_done", snap)
	s.deleteTransfer(state.ID)
}

func (s *Service) finishDownload(state *transferState, msg protocol.FileTransferDone) {
	state.mu.Lock()

	if state.file != nil {
		_ = state.file.Close()
		state.file = nil
	}

	state.Message = msg.Message
	state.CompletedAt = msg.CompletedAt
	if state.CompletedAt.IsZero() {
		state.CompletedAt = time.Now().UTC()
	}
	state.Size = msg.Size
	state.ChecksumSHA256 = msg.ChecksumSHA256

	if msg.Status != "success" {
		s.finalizeDownloadFailed(state, msg.Status, msg.Message, false)
		return
	}
	if state.assembler != nil {
		if err := state.assembler.Finish(state.expectedChunks); err != nil {
			s.finalizeDownloadFailed(state, "failed", err.Error(), false)
			return
		}
	}
	if err := os.Rename(state.tempPath, state.LocalPath); err != nil {
		s.finalizeDownloadFailed(state, "failed", err.Error(), false)
		return
	}
	checksum, err := common.FileSHA256(state.LocalPath)
	if err != nil {
		s.finalizeDownloadFailed(state, "failed", err.Error(), true)
		return
	}
	state.ChecksumVerified = checksum == msg.ChecksumSHA256
	if !state.ChecksumVerified {
		state.Status = "failed"
		state.Message = "checksum mismatch"
		s.cleanupTransferFilesLocked(state, true)
	} else {
		state.Status = "success"
	}
	snap := state.snapshotLocked()
	s.persistTransferLocked(state)
	state.mu.Unlock()
	s.plugins.Trigger("transfer_done", snap)
	s.deleteTransfer(state.ID)
}

func (s *Service) finishTransferWithError(state *transferState, err error) {
	state.mu.Lock()
	if isTerminalTransferStatus(state.Status) {
		state.mu.Unlock()
		return
	}
	state.Status = "failed"
	state.Message = err.Error()
	state.CompletedAt = time.Now().UTC()
	s.cleanupTransferFilesLocked(state, false)
	snap := state.snapshotLocked()
	s.persistTransferLocked(state)
	state.mu.Unlock()
	s.plugins.Trigger("transfer_done", snap)
	s.deleteTransfer(state.ID)
}

// reapStalledTransfer finalizes a transfer that has made no progress within the
// stall window, keeping its partial file so a retry can resume. It returns true
// if the transfer was reaped by this call.
func (s *Service) reapStalledTransfer(state *transferState, now time.Time) bool {
	state.mu.Lock()
	if isTerminalTransferStatus(state.Status) {
		state.mu.Unlock()
		return false
	}
	if state.lastPersistedAt.IsZero() || now.Sub(state.lastPersistedAt) < transferStallTimeout {
		state.mu.Unlock()
		return false
	}
	state.Status = "failed"
	state.Message = "transfer stalled: no progress"
	state.CompletedAt = now
	s.cleanupTransferFilesLocked(state, false)
	snap := state.snapshotLocked()
	s.persistTransferLocked(state)
	state.mu.Unlock()
	s.plugins.Trigger("transfer_done", snap)
	s.deleteTransfer(state.ID)
	return true
}

// reapStalledTransfers finalizes in-flight transfers that have stalled.
func (s *Service) reapStalledTransfers(now time.Time) int {
	s.transferMu.RLock()
	states := make([]*transferState, 0, len(s.transfers))
	for _, state := range s.transfers {
		states = append(states, state)
	}
	s.transferMu.RUnlock()

	reaped := 0
	for _, state := range states {
		if s.reapStalledTransfer(state, now) {
			reaped++
		}
	}
	return reaped
}

func (s *Service) cleanupTransferFilesLocked(state *transferState, removeLocal bool) {
	if state.file != nil {
		_ = state.file.Close()
		state.file = nil
	}
	// The partial file (tempPath) is intentionally kept so a retry can resume
	// from where it stopped.
	if removeLocal && state.LocalPath != "" {
		if err := os.Remove(state.LocalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.logger.Warn("remove failed transfer file", zap.String("transfer_id", state.ID), zap.String("path", state.LocalPath), zap.Error(err))
		}
	}
}

func (s *Service) putTransfer(state *transferState) {
	s.transferMu.Lock()
	defer s.transferMu.Unlock()
	s.transfers[state.ID] = state
}

func (s *Service) getTransfer(id string) (*transferState, bool) {
	s.transferMu.RLock()
	defer s.transferMu.RUnlock()
	state, ok := s.transfers[id]
	return state, ok
}

func (s *Service) deleteTransfer(id string) {
	s.transferMu.Lock()
	defer s.transferMu.Unlock()
	delete(s.transfers, id)
}

func (s *Service) transferSnapshot(id string) (TransferStatus, bool) {
	state, ok := s.getTransfer(id)
	if !ok {
		audit, ok, err := s.store.TransferAudit(id)
		if err != nil || !ok {
			return TransferStatus{}, false
		}
		return TransferStatus{
			ID:               audit.TransferID,
			AgentID:          audit.AgentID,
			Direction:        audit.Direction,
			LocalPath:        audit.LocalPath,
			RemotePath:       audit.RemotePath,
			Status:           audit.Status,
			Message:          audit.Message,
			Size:             audit.Size,
			BytesTransferred: audit.BytesTransferred,
			ChecksumSHA256:   audit.ChecksumSHA256,
			ChecksumVerified: audit.ChecksumVerified,
			CreatedAt:        audit.CreatedAt,
			CompletedAt:      audit.CompletedAt,
		}, true
	}
	return state.snapshot(), true
}

func (s *Service) clientForAgent(agentID string) (*agentConn, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	client := s.clients[agentID]
	if client == nil {
		return nil, errors.New("agent is offline")
	}
	return client, nil
}

func (s *Service) persistTransfer(state *transferState) {
	state.mu.Lock()
	defer state.mu.Unlock()
	s.persistTransferLocked(state)
}

func (s *Service) persistTransferLocked(state *transferState) {
	if err := s.store.UpsertTransferAudit(TransferAudit{
		TransferID:       state.ID,
		AgentID:          state.AgentID,
		Direction:        state.Direction,
		LocalPath:        state.LocalPath,
		RemotePath:       state.RemotePath,
		Status:           state.Status,
		Message:          state.Message,
		Size:             state.Size,
		BytesTransferred: state.BytesTransferred,
		ChecksumSHA256:   state.ChecksumSHA256,
		ChecksumVerified: state.ChecksumVerified,
		CreatedAt:        state.CreatedAt,
		CompletedAt:      state.CompletedAt,
	}); err != nil {
		s.logger.Warn("persist transfer audit", zap.String("transfer_id", state.ID), zap.Error(err))
		return
	}
	state.lastPersistedAt = time.Now().UTC()
	state.lastPersistedBytes = state.BytesTransferred
}

func (s *Service) shouldPersistTransferProgressLocked(state *transferState, now time.Time) bool {
	if state.lastPersistedAt.IsZero() {
		return true
	}
	if state.BytesTransferred-state.lastPersistedBytes >= transferAuditMinBytes {
		return true
	}
	return now.Sub(state.lastPersistedAt) >= transferAuditMinInterval
}

// cancelTransfer requests cancellation of a live transfer (or reports the
// status of an already-finalized one). The request is persisted as
// cancel_requested BEFORE the signal is sent — the operator's intent is
// auditable even if the agent never acknowledges. The transfer reaches
// terminal "canceled" either via the agent's FileTransferDone{canceled}
// confirmation or, for agents that never acknowledge (offline, or old
// agents without cancel support), via the reaper after the grace window.
//
// Returns (snapshot, state, cancelSent, found). Canceling a terminal
// transfer is idempotent: its terminal status is reported with sent=false.
func (s *Service) cancelTransfer(transferID string) (TransferStatus, string, bool, bool) {
	state, ok := s.getTransfer(transferID)
	if !ok {
		// Not live; a finalized audit record still answers the request.
		if audit, found, err := s.store.TransferAudit(transferID); err == nil && found {
			return TransferStatus{
				ID:               audit.TransferID,
				AgentID:          audit.AgentID,
				Direction:        audit.Direction,
				LocalPath:        audit.LocalPath,
				RemotePath:       audit.RemotePath,
				Status:           audit.Status,
				Message:          audit.Message,
				Size:             audit.Size,
				BytesTransferred: audit.BytesTransferred,
				ChecksumSHA256:   audit.ChecksumSHA256,
				ChecksumVerified: audit.ChecksumVerified,
				CreatedAt:        audit.CreatedAt,
				CompletedAt:      audit.CompletedAt,
			}, audit.Status, false, true
		}
		return TransferStatus{}, "", false, false
	}

	state.mu.Lock()
	if isTerminalTransferStatus(state.Status) {
		snap := state.snapshotLocked()
		state.mu.Unlock()
		return snap, snap.Status, false, true
	}
	firstRequest := state.Status != "cancel_requested"
	if firstRequest {
		state.Status = "cancel_requested"
		state.Message = "cancellation requested"
		state.cancelRequestedAt = time.Now().UTC()
		s.persistTransferLocked(state)
	}
	// Release the upload pump (if this is a server->agent push) regardless
	// of whether the agent turn-up was online: the pump owns the local .part
	// on the server side and must stop touching it.
	state.signalCancel()
	snap := state.snapshotLocked()
	agentID := state.AgentID
	state.mu.Unlock()

	if firstRequest {
		s.logger.Info("transfer cancel requested",
			zap.String("transfer_id", transferID), zap.String("agent_id", agentID))
	} else {
		s.logger.Info("transfer cancel re-signal",
			zap.String("transfer_id", transferID), zap.String("agent_id", agentID))
	}

	// Re-issued requests re-send the signal (same contract as task cancel:
	// asking again means "try telling the agent again"), so a repeat POST
	// after a lost frame can still land the terminal confirmation early
	// instead of waiting out the reaper grace.
	sent := false
	if client, err := s.clientForAgent(agentID); err == nil {
		if err := client.sendMessage(protocol.TypeFileTransferCancel, protocol.FileTransferCancel{
			TransferID:  transferID,
			AgentID:     agentID,
			RequestedAt: time.Now().UTC(),
		}); err == nil {
			sent = true
		}
	}
	return snap, snap.Status, sent, true
}

// signalCancel releases any watcher blocked/selecting on cancelCh.
// Idempotent and lock-free: safe to call while holding state.mu (close on
// an uncontended channel never blocks). A nil cancelCh (hand-built test
// state) is a no-op.
func (t *transferState) signalCancel() {
	if t.cancelCh == nil {
		return
	}
	t.cancelOnce.Do(func() { close(t.cancelCh) })
}

// finalizeCanceled moves a live transfer to terminal "canceled", persists the
// audit row, fires the plugin hook and drops it from the live map. The
// partial (.part) file stays on both ends so a retry can resume. Safe to
// call concurrently: the terminal guard under the state lock makes all but
// one racing caller a no-op.
func (s *Service) finalizeCanceled(state *transferState, message string) {
	state.mu.Lock()
	if isTerminalTransferStatus(state.Status) {
		state.mu.Unlock()
		return
	}
	state.Status = "canceled"
	state.Message = message
	state.CompletedAt = time.Now().UTC()
	s.cleanupTransferFilesLocked(state, false)
	snap := state.snapshotLocked()
	s.persistTransferLocked(state)
	state.mu.Unlock()
	s.plugins.Trigger("transfer_done", snap)
	s.deleteTransfer(state.ID)
}

// reapCanceledTransfers finalizes cancel_requested transfers whose agent has
// not confirmed within the grace window, so a cancel against a dead
// connection can never dangle in a non-terminal state forever.
func (s *Service) reapCanceledTransfers(now time.Time) int {
	s.transferMu.RLock()
	states := make([]*transferState, 0, len(s.transfers))
	for _, state := range s.transfers {
		states = append(states, state)
	}
	s.transferMu.RUnlock()

	reaped := 0
	for _, state := range states {
		state.mu.Lock()
		due := state.Status == "cancel_requested" &&
			!state.cancelRequestedAt.IsZero() &&
			now.Sub(state.cancelRequestedAt) >= transferCancelGrace
		state.mu.Unlock()
		if due {
			s.finalizeCanceled(state, "canceled: agent did not confirm within grace")
			reaped++
		}
	}
	return reaped
}
