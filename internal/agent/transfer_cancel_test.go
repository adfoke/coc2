package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"coc2/internal/common"
	"coc2/internal/protocol"
)

// dialAgentPair spins up a fake ws server that records every inbound frame,
// and returns an upgraded client conn wired to a zero-value-usable Client.
func dialAgentPair(t *testing.T) (*Client, *websocket.Conn, chan protocol.Inbound) {
	t.Helper()

	inbound := make(chan protocol.Inbound, 256)
	up := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		go func() {
			defer conn.Close()
			for {
				opcode, raw, err := conn.ReadMessage()
				if err != nil {
					return
				}
				in, err := protocol.DecodeFrame(opcode, raw)
				if err != nil {
					t.Errorf("decode frame: %v", err)
					continue
				}
				inbound <- in
			}
		}()
	}))
	t.Cleanup(server.Close)

	wsURL := "ws://" + strings.TrimPrefix(server.URL, "http://")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	c, err := New(Config{
		ServerURL:         wsURL,
		Token:             "tok",
		AgentID:           "cancel-agent",
		HeartbeatInterval: time.Hour,
		MaxBackoff:        time.Hour,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return c, conn, inbound
}

func waitForDone(t *testing.T, inbound chan protocol.Inbound, transferID string, timeout time.Duration) protocol.FileTransferDone {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case in := <-inbound:
			if in.MsgType != protocol.TypeFileTransferDone {
				continue
			}
			done, err := protocol.PayloadOf[protocol.FileTransferDone](in)
			if err != nil {
				t.Fatalf("decode done: %v", err)
			}
			if done.TransferID == transferID {
				return done
			}
		case <-deadline:
			t.Fatalf("no FileTransferDone for %s within %s", transferID, timeout)
		}
	}
}

// TestDownloadPumpStopsOnCancel: a download sendFile pump whose context is
// already canceled must halt at the first chunk boundary and confirm with
// status=canceled — never silent success, never failed.
func TestDownloadPumpStopsOnCancel(t *testing.T) {
	c, conn, inbound := dialAgentPair(t)

	dir := t.TempDir()
	src := filepath.Join(dir, "payload.bin")
	if err := os.WriteFile(src, []byte(strings.Repeat("z", 64*1024)), 0o600); err != nil {
		t.Fatalf("seed payload: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // operator cancel arrives before the pump's first iteration

	start := protocol.FileTransferStart{
		TransferID: "tx-cancel-dl",
		Direction:  "download", // agent -> server push
		RemotePath: src,
		Size:       64 * 1024,
		ChunkSize:  1024,
	}
	c.sendFile(ctx, conn, start)

	done := waitForDone(t, inbound, start.TransferID, 5*time.Second)
	if done.Status != "canceled" {
		t.Fatalf("status=%q want canceled (done=%+v)", done.Status, done)
	}
	if done.TransferID != start.TransferID || done.AgentID != "cancel-agent" {
		t.Fatalf("wrong envelope: %+v", done)
	}
	// Zero bytes moved, but the checksum of the intended payload is reported
	// so the server audit row can identify what was aborted.
	if done.Size != 0 {
		t.Fatalf("a canceled-before-first-chunk pump must report size 0, got %d", done.Size)
	}
	if len(done.ChecksumSHA256) != 64 {
		t.Fatalf("canceled download should still carry the sha256, got %q", done.ChecksumSHA256)
	}
}

// TestDownloadPumpUnregistersItself: even when the pump finishes normally
// (no cancel), its downloads map slot must be gone — a leaked entry would
// let a stale cancel touch a recycled id.
func TestDownloadPumpUnregistersItself(t *testing.T) {
	c, conn, inbound := dialAgentPair(t)

	src := filepath.Join(t.TempDir(), "tiny.bin")
	if err := os.WriteFile(src, []byte("abc"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	start := protocol.FileTransferStart{
		TransferID: "tx-clean-exit",
		Direction:  "download",
		RemotePath: src,
		Size:       3,
		ChunkSize:  1024,
	}
	c.downloadMu.Lock()
	c.downloads[start.TransferID] = func() {}
	c.downloadMu.Unlock()
	c.sendFile(context.Background(), conn, start)

	done := waitForDone(t, inbound, start.TransferID, 5*time.Second)
	if done.Status != "success" {
		t.Fatalf("uncanceled small download must succeed: %+v", done)
	}
	c.downloadMu.Lock()
	_, leaked := c.downloads[start.TransferID]
	c.downloadMu.Unlock()
	if leaked {
		t.Fatal("pump exited without deregistering its cancel func")
	}
}

// TestUploadCancelAbortsAndKeepsPartial: canceling a server->agent upload
// must close the receiving file, remove the upload state, keep the .part
// for a future resume, and answer with status=canceled + bytes received.
func TestUploadCancelAbortsAndKeepsPartial(t *testing.T) {
	c, conn, inbound := dialAgentPair(t)

	dir := t.TempDir()
	remotePath := filepath.Join(dir, "incoming.bin")
	tempPath := remotePath + ".part"
	if err := os.WriteFile(tempPath, []byte(strings.Repeat("p", 2048)), 0o600); err != nil {
		t.Fatalf("seed partial: %v", err)
	}
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open partial: %v", err)
	}

	c.uploadMu.Lock()
	c.uploads["tx-cancel-up"] = &uploadState{
		remotePath: remotePath,
		tempPath:   tempPath,
		file:       f,
		size:       4096,
		received:   2048,
	}
	c.uploadMu.Unlock()

	c.handleTransferCancel(conn, protocol.FileTransferCancel{
		TransferID:  "tx-cancel-up",
		RequestedAt: time.Now().UTC(),
	})

	done := waitForDone(t, inbound, "tx-cancel-up", 5*time.Second)
	if done.Status != "canceled" {
		t.Fatalf("upload cancel status=%q want canceled: %+v", done.Status, done)
	}
	if done.Direction != "upload" || done.Size != 2048 {
		t.Fatalf("upload cancel must report received bytes: %+v", done)
	}

	c.uploadMu.Lock()
	_, live := c.uploads["tx-cancel-up"]
	c.uploadMu.Unlock()
	if live {
		t.Fatal("canceled upload must be deregistered")
	}
	// .part kept (resume contract), final path never created.
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("partial must survive for resume: %v", err)
	}
	if _, err := os.Stat(remotePath); !os.IsNotExist(err) {
		t.Fatal("canceled upload must not finalize the target file")
	}
	// Writing to the closed handle proves the file was really closed.
	if _, err := f.Write([]byte("x")); err == nil {
		t.Fatal("upload file handle must be closed after cancel")
	}
}

func TestCancelUnknownTransferIsIgnored(t *testing.T) {
	c, conn, inbound := dialAgentPair(t)

	c.handleTransferCancel(conn, protocol.FileTransferCancel{TransferID: "ghost"})

	// No confirmation may be invented for an unknown id. Give any wrong-path
	// send ample time to surface, then assert the channel is quiet.
	select {
	case in := <-inbound:
		t.Fatalf("unknown cancel must not emit frames, got %s", in.MsgType)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestUploadResumePathUsesChecksumForCanceledReport(t *testing.T) {
	// Guards the pump contract end-to-end at unit level: a canceled pump
	// still carries sha256 so the server audit row can identify the payload.
	// (Covered inside TestDownloadPumpStopsOnCancel; here we pin that
	// common.FileSHA256 output format matches what done.ChecksumSHA256 must be.)
	src := filepath.Join(t.TempDir(), "f.bin")
	if err := os.WriteFile(src, []byte("data"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sum, err := common.FileSHA256(src)
	if err != nil || len(sum) != 64 {
		t.Fatalf("FileSHA256: %v (%q)", err, sum)
	}
}
