package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"coc2/internal/protocol"
)

// dialAgentWS connects a raw websocket client to the agent plane engine and
// performs the hello exchange, returning the connection plus the first reply
// frame (hello_ack or error) so tests can assert on its opcode.
func dialAgentWS(t *testing.T, engine http.Handler, hello protocol.AgentHello) (*websocket.Conn, int, []byte) {
	t.Helper()

	up := httptest.NewServer(engine)
	t.Cleanup(up.Close)

	wsURL := "ws://" + strings.TrimPrefix(up.URL, "http://") + "/ws/agent"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial agent ws: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	raw, err := protocol.MarshalMessage(protocol.TypeHello, hello)
	if err != nil {
		t.Fatalf("marshal hello: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	opcode, frame, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read hello reply: %v", err)
	}
	return conn, opcode, frame
}

func baseHello() protocol.AgentHello {
	return protocol.AgentHello{
		AgentID:     "agent-e2e",
		Token:       "test-token",
		Hostname:    "test",
		OS:          "linux",
		Arch:        "arm64",
		Version:     "v0.4.0",
		ConnectedAt: time.Now().UTC(),
	}
}

// TestNegotiationUpgradesToProtobuf: a hello advertising ProtoVersion makes
// the server answer hello_ack with a binary frame and keep the whole session
// on protobuf framing.
func TestNegotiationUpgradesToProtobuf(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	hello := baseHello()
	hello.ProtoVersion = protocol.BinaryWireVersion
	conn, opcode, frame := dialAgentWS(t, svc.agentEngine, hello)

	if opcode != websocket.BinaryMessage {
		t.Fatalf("hello_ack opcode = %d, want binary after proto_version", opcode)
	}
	typ, decoded, err := protocol.UnmarshalBinaryEnvelope(frame)
	if err != nil {
		t.Fatalf("decode binary hello_ack: %v", err)
	}
	if typ != protocol.TypeHelloAck {
		t.Fatalf("reply type = %q, want hello_ack", typ)
	}
	ack, ok := decoded.(protocol.HelloAck)
	if !ok || ack.AgentID != "agent-e2e" {
		t.Fatalf("unexpected decoded ack: %+v", decoded)
	}

	// A dispatched task must now travel as a binary frame too.
	if _, err := svc.createTask("agent-e2e", "echo hi", 5, 0, time.Time{}); err != nil {
		t.Fatalf("create task: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	opcode, frame, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("read task frame: %v", err)
	}
	if opcode != websocket.BinaryMessage {
		t.Fatalf("task_dispatch opcode = %d, want binary mid-session", opcode)
	}
	typ, decoded, err = protocol.UnmarshalBinaryEnvelope(frame)
	if err != nil || typ != protocol.TypeTaskDispatch {
		t.Fatalf("decode task frame: type=%q err=%v", typ, err)
	}
	task := decoded.(protocol.Task)
	if task.Command != "echo hi" {
		t.Fatalf("task command = %q", task.Command)
	}
}

// TestNegotiationLegacyStaysJSON: a hello without ProtoVersion (old agent)
// keeps the session on legacy JSON text frames.
func TestNegotiationLegacyStaysJSON(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	hello := baseHello() // ProtoVersion zero: legacy agent
	conn, opcode, frame := dialAgentWS(t, svc.agentEngine, hello)

	if opcode != websocket.TextMessage {
		t.Fatalf("hello_ack opcode = %d, want text for legacy agent", opcode)
	}
	var env protocol.Envelope
	if err := json.Unmarshal(frame, &env); err != nil {
		t.Fatalf("decode json hello_ack: %v", err)
	}
	if env.Type != protocol.TypeHelloAck {
		t.Fatalf("reply type = %q", env.Type)
	}
	ack, err := protocol.UnmarshalPayload[protocol.HelloAck](env)
	if err != nil || ack.AgentID != "agent-e2e" {
		t.Fatalf("bad legacy ack: %+v err=%v", ack, err)
	}

	// The legacy agent must still receive its task as JSON text.
	if _, err := svc.createTask("agent-e2e", "uptime", 5, 0, time.Time{}); err != nil {
		t.Fatalf("create task: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	opcode, frame, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("read task frame: %v", err)
	}
	if opcode != websocket.TextMessage {
		t.Fatalf("legacy session leaked binary framing (opcode %d)", opcode)
	}
	if err := json.Unmarshal(frame, &env); err != nil || env.Type != protocol.TypeTaskDispatch {
		t.Fatalf("legacy task frame bad: %s err=%v", frame, err)
	}
}

// TestBinaryFrameAcceptedFromAgent proves the dual-stack read path: a hello
// sent as a protobuf binary frame must register the agent and get a binary
// ack (agent-initiated upgrade shape).
func TestBinaryFrameAcceptedFromAgent(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	up := httptest.NewServer(svc.agentEngine)
	defer up.Close()

	wsURL := "ws://" + strings.TrimPrefix(up.URL, "http://") + "/ws/agent"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	hello := baseHello()
	hello.ProtoVersion = protocol.BinaryWireVersion
	raw, err := protocol.MarshalBinaryEnvelope(protocol.TypeHello, hello)
	if err != nil {
		t.Fatalf("marshal binary hello: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, raw); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	opcode, frame, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if opcode != websocket.BinaryMessage {
		t.Fatalf("ack opcode = %d, want binary", opcode)
	}
	if _, _, err := protocol.UnmarshalBinaryEnvelope(frame); err != nil {
		t.Fatalf("ack not valid protobuf: %v", err)
	}
}

// TestBadFrameDoesNotKillSession: an undecodable frame is logged and skipped,
// the connection stays alive for the next valid message.
func TestBadFrameDoesNotKillSession(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	hello := baseHello()
	hello.ProtoVersion = protocol.BinaryWireVersion
	conn, _, _ := dialAgentWS(t, svc.agentEngine, hello)

	// Garbage binary frame: not a valid WireEnvelope.
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xff, 0xff}); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	// Then a legit heartbeat on the same connection.
	hb, err := protocol.MarshalBinaryEnvelope(protocol.TypeHeartbeat, protocol.Heartbeat{
		AgentID: "agent-e2e", Timestamp: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, hb); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}

	// If the session survived, the next dispatch arrives as a binary frame.
	if _, err := svc.createTask("agent-e2e", "echo alive", 5, 0, time.Time{}); err != nil {
		t.Fatalf("create task after garbage frame: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	opcode, frame, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("connection died on bad frame: %v", err)
	}
	if opcode != websocket.BinaryMessage {
		t.Fatalf("opcode = %d, want binary", opcode)
	}
	if typ, _, err := protocol.UnmarshalBinaryEnvelope(frame); err != nil || typ != protocol.TypeTaskDispatch {
		t.Fatalf("unexpected post-garbage frame: type=%q err=%v", typ, err)
	}
}
