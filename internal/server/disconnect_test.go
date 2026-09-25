package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// waitAgentOnline polls until the agent's stored online flag equals want,
// reporting whether it converged before the deadline.
func waitAgentOnline(t *testing.T, svc *Service, agentID string, want bool) bool {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got, found := agentOnline(t, svc, agentID)
		if found && got == want {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func agentOnline(t *testing.T, svc *Service, agentID string) (online, found bool) {
	t.Helper()
	agents, err := svc.store.Agents()
	if err != nil {
		t.Fatalf("read agents: %v", err)
	}
	for _, a := range agents {
		if a.AgentID == agentID {
			return a.Online, true
		}
	}
	return false, false
}

// isDisconnectRow matches the agent-plane session-end row.
func isDisconnectRow(e OpLogEntry) bool {
	return e.Plane == "agent" && strings.HasPrefix(e.ParamsSummary, "disconnect")
}

// newQuietTestService is newTestService with the liveness ping disabled.
//
// The ping loop matters here: the server's pong handler calls touch(), which
// re-marks the agent online. That repair is exactly what hides the reconnect
// race in production (it self-heals within one ping period), so a test that
// wants to observe the wrong flag has to remove the thing that fixes it.
func newQuietTestService(t *testing.T) (*Service, func()) {
	t.Helper()

	svc, err := New(Config{
		ListenAddr:     ":0",
		OperatorListen: ":0",
		AuthToken:      "test-token",
		DBPath:         filepath.Join(t.TempDir(), "test.db"),
		WriteWait:      2 * time.Second,
		PongWait:       time.Hour,
		PingPeriod:     time.Hour,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return svc, func() { _ = svc.store.Close() }
}

// dummyWSConn returns a real, closeable websocket connection for building
// agentConn values that never speak to the hub.
func dummyWSConn(t *testing.T) *websocket.Conn {
	t.Helper()

	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	conn, _, err := websocket.DefaultDialer.Dial("ws://"+strings.TrimPrefix(srv.URL, "http://"), nil)
	if err != nil {
		t.Fatalf("dial dummy ws: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestSupersededTeardownLeavesAgentOnline pins the invariant that a retired
// session's teardown must not clear the online flag of the session that
// replaced it.
//
// The teardown used to clear the flag unconditionally. Whether that is
// observable depends purely on goroutine ordering: register() retires the old
// connection *before* it writes the new one, so in practice the replacement's
// write usually lands last and hides it (a live reconnect therefore rarely
// showed as offline, and a heartbeat repairs it within one ping period anyway).
// Relying on that ordering is the bug, so this test drives the ordering
// directly instead of racing for it: the replacement is registered first, then
// the retired session is torn down.
func TestSupersededTeardownLeavesAgentOnline(t *testing.T) {
	svc, cleanup := newQuietTestService(t)
	defer cleanup()

	hello := baseHello()
	hello.AgentID = "agent-supersede-online"

	// The replacement session, live and registered.
	dialAgentWS(t, svc.agentEngine, hello)
	if online, found := agentOnline(t, svc, hello.AgentID); !found || !online {
		t.Fatalf("replacement session did not register as online")
	}

	// The session it replaced, torn down now that the replacement holds the id.
	stale := &agentConn{
		id:           hello.AgentID,
		service:      svc,
		conn:         dummyWSConn(t),
		send:         make(chan wsFrame, 1),
		done:         make(chan struct{}),
		drained:      make(chan struct{}),
		registeredAt: time.Now().Add(-30 * time.Second),
	}
	close(stale.drained) // nothing queued: let drainThenClose return at once
	svc.unregister(stale)

	if online, _ := agentOnline(t, svc, hello.AgentID); !online {
		t.Fatalf("the superseded session's teardown cleared the live session's online flag")
	}
}

// TestUnregisterLogsDisconnect covers the audit side of a session ending. The
// server knew the moment an agent went away — it flips the online flag right
// there — but never recorded it, so the agent-plane trail showed connects and
// nothing else.
func TestUnregisterLogsDisconnect(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	hello := baseHello()
	hello.AgentID = "agent-audit-down"
	conn, _, _ := dialAgentWS(t, svc.agentEngine, hello)

	// Wait for the connect row first, so the disconnect row cannot be confused
	// with it.
	pollOpLogs(t, svc, func(e OpLogEntry) bool {
		return e.Plane == "agent" && strings.HasPrefix(e.ParamsSummary, "connect")
	})

	_ = conn.Close()

	row := pollOpLogs(t, svc, isDisconnectRow)
	if !row.OK {
		t.Fatalf("disconnect row marked not-ok: %+v", row)
	}
	if row.Actor != hello.AgentID {
		t.Fatalf("disconnect attributed to %q, want %q", row.Actor, hello.AgentID)
	}
	if !strings.Contains(row.ParamsSummary, "session ended") {
		t.Fatalf("disconnect summary missing the session-ended wording: %q", row.ParamsSummary)
	}
	if !waitAgentOnline(t, svc, hello.AgentID, false) {
		t.Fatalf("agent still online after its only session closed")
	}
}

// TestSupersededSessionIsMarkedNotOffline: when a reconnect retires a session,
// its disconnect row must say so rather than pretend the agent went away.
func TestSupersededSessionIsMarkedNotOffline(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	hello := baseHello()
	hello.AgentID = "agent-audit-supersede"
	dialAgentWS(t, svc.agentEngine, hello)
	dialAgentWS(t, svc.agentEngine, hello)

	row := pollOpLogs(t, svc, isDisconnectRow)
	if !strings.Contains(row.ParamsSummary, "superseded") {
		t.Fatalf("superseded session not marked as such: %q", row.ParamsSummary)
	}
	// The replacement session is live, so the agent must stay online.
	if !waitAgentOnline(t, svc, hello.AgentID, true) {
		t.Fatalf("agent went offline despite a live replacement session")
	}
}
