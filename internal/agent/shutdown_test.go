package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// fakeAgentPeer accepts one agent session, answers hello_ack, optionally
// dispatches a task, and then holds the connection open without sending
// anything further. Holding a *healthy* connection open is the whole point:
// that is the state in which the read loop blocks forever and used to swallow
// context cancellation, so a SIGTERM left the agent running and heartbeating
// while the server still counted it online.
type fakeAgentPeer struct {
	srv       *httptest.Server
	connected chan struct{} // closed once hello_ack has been written
	taskSent  chan struct{} // closed once the task dispatch has been written
}

func newFakeAgentPeer(t *testing.T, taskCommand string) *fakeAgentPeer {
	t.Helper()

	peer := &fakeAgentPeer{
		connected: make(chan struct{}),
		taskSent:  make(chan struct{}),
	}

	up := websocket.Upgrader{}
	peer.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		opcode, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		in, err := protocol.DecodeFrame(opcode, raw)
		if err != nil {
			return
		}
		hello, err := protocol.PayloadOf[protocol.AgentHello](in)
		if err != nil {
			return
		}

		ackPayload, _ := json.Marshal(protocol.HelloAck{ServerTime: time.Now().UTC(), AgentID: hello.AgentID})
		ackFrame, _ := json.Marshal(protocol.Envelope{Type: protocol.TypeHelloAck, Payload: ackPayload})
		if err := conn.WriteMessage(websocket.TextMessage, ackFrame); err != nil {
			return
		}
		close(peer.connected)

		if taskCommand != "" {
			taskPayload, _ := json.Marshal(protocol.Task{
				ID:          "t-shutdown",
				AgentID:     hello.AgentID,
				Type:        "shell",
				Command:     taskCommand,
				TimeoutSecs: 60,
				CreatedAt:   time.Now().UTC(),
			})
			taskFrame, _ := json.Marshal(protocol.Envelope{Type: protocol.TypeTaskDispatch, Payload: taskPayload})
			if err := conn.WriteMessage(websocket.TextMessage, taskFrame); err != nil {
				return
			}
			close(peer.taskSent)
		}

		// Stay up and keep reading so the link never breaks on its own.
		for {
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(peer.srv.Close)
	return peer
}

func newShutdownClient(t *testing.T, peer *fakeAgentPeer) *Client {
	t.Helper()
	client, err := New(Config{
		ServerURL: "ws://" + strings.TrimPrefix(peer.srv.URL, "http://") + "/ws/agent",
		Token:     "tok",
		AgentID:   "agent-shutdown",
		// Silence the ticker and reconnect backoff so the test measures the
		// shutdown path and nothing else.
		HeartbeatInterval: time.Hour,
		MaxBackoff:        time.Hour,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client
}

// TestRunExitsOnContextCancellation is the regression test for a SIGTERM
// (systemd stop / docker stop / `kill`) that left the agent running: the read
// loop blocked on ReadMessage and never observed ctx being cancelled.
func TestRunExitsOnContextCancellation(t *testing.T) {
	for _, tc := range []struct {
		name string
		task string
	}{
		{name: "idle session"},
		{name: "mid-task", task: "sleep 30"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := newFakeAgentPeer(t, tc.task)
			client := newShutdownClient(t, peer)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			runDone := make(chan error, 1)
			go func() { runDone <- client.Run(ctx) }()

			select {
			case <-peer.connected:
			case <-time.After(5 * time.Second):
				t.Fatalf("agent never established a session")
			}
			if tc.task != "" {
				select {
				case <-peer.taskSent:
				case <-time.After(5 * time.Second):
					t.Fatalf("task never dispatched")
				}
			}

			cancel()

			select {
			case err := <-runDone:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Run returned %v, want context.Canceled", err)
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("Run did not return after cancellation: a SIGTERM would leave the agent running")
			}
		})
	}
}

// TestShutdownKillsRunningTaskTree guards the documented "cancellation kills
// the whole process tree, no orphans" property across shutdown. The fix that
// makes the agent exit on cancellation could otherwise have introduced a
// regression where the process disappears before delivering the kill.
func TestShutdownKillsRunningTaskTree(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "orphan-marker")
	// The shell writes the marker once the sleep returns. If the process
	// group survives shutdown it keeps running on its own and writes it,
	// which is exactly the orphan this asserts against.
	peer := newFakeAgentPeer(t, fmt.Sprintf("sleep 2; echo orphaned > %s", marker))
	client := newShutdownClient(t, peer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(ctx) }()

	select {
	case <-peer.taskSent:
	case <-time.After(5 * time.Second):
		t.Fatalf("task never dispatched")
	}
	// Give the task a moment to actually start before shutting down.
	time.Sleep(200 * time.Millisecond)

	cancel()
	select {
	case <-runDone:
	case <-time.After(15 * time.Second):
		t.Fatalf("Run did not return after cancellation")
	}

	// Wait past the point the marker would have been written had the task
	// survived, then confirm it did not.
	time.Sleep(3 * time.Second)
	if _, err := os.Stat(marker); err == nil {
		raw, _ := os.ReadFile(marker)
		t.Fatalf("task process group survived shutdown and wrote %s (%q)", marker, string(raw))
	}
}
