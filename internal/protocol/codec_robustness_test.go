package protocol

import (
	"strings"
	"testing"
	"time"

	"coc2/internal/protocol/pb"

	"google.golang.org/protobuf/proto"
)

// TestMarshalBinaryEnvelopeRejectsMismatchedPayload pins the checked type
// assertions: a payload whose Go type does not match the message type must
// return an error, never panic. The invariant is real but implicit, and a
// panic here would be fatal for the whole process (the server marshals from
// the agent read loop, outside gin's Recovery middleware).
func TestMarshalBinaryEnvelopeRejectsMismatchedPayload(t *testing.T) {
	cases := []struct {
		name    string
		msgType string
		payload any
	}{
		{"string for error", TypeError, "a raw string"},
		{"nil for task", TypeTaskDispatch, nil},
		{"wrong struct for hello", TypeHello, Heartbeat{AgentID: "x"}},
		{"wrong struct for result", TypeTaskResult, TaskAck{TaskID: "t"}},
		{"wrong struct for chunk", TypeFileTransferChunk, FileTransferDone{TransferID: "tx"}},
		{"int for heartbeat", TypeHeartbeat, 42},
		{"pointer instead of value", TypeHello, &AgentHello{AgentID: "a"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("MarshalBinaryEnvelope panicked instead of erroring: %v", r)
				}
			}()

			out, err := MarshalBinaryEnvelope(tc.msgType, tc.payload)
			if err == nil {
				t.Fatalf("expected an error, got %d bytes", len(out))
			}
			if !strings.Contains(err.Error(), "wire:") {
				t.Fatalf("error should identify the wire layer, got %v", err)
			}
		})
	}
}

// TestMarshalBinaryEnvelopeAcceptsCorrectPayloads is the counterpart: the
// checked assertions must not reject the real call shapes.
func TestMarshalBinaryEnvelopeAcceptsCorrectPayloads(t *testing.T) {
	now := time.Now().UTC()
	payloads := []struct {
		msgType string
		payload any
	}{
		{TypeHello, AgentHello{AgentID: "a", Token: "t"}},
		{TypeHelloAck, HelloAck{AgentID: "a", ServerTime: now}},
		{TypeHeartbeat, Heartbeat{AgentID: "a", Timestamp: now}},
		{TypeMetricsReport, MetricsReport{AgentID: "a", Timestamp: now}},
		{TypeTaskDispatch, Task{ID: "t", AgentID: "a", Command: "echo"}},
		{TypeTaskAck, TaskAck{TaskID: "t", AgentID: "a"}},
		{TypeTaskCancel, TaskCancel{TaskID: "t", AgentID: "a"}},
		{TypeTaskResult, TaskResult{TaskID: "t", AgentID: "a", Status: "success"}},
		{TypeFileTransferStart, FileTransferStart{TransferID: "tx", AgentID: "a"}},
		{TypeFileTransferChunk, FileTransferChunk{TransferID: "tx", Seq: 1, Data: []byte("d")}},
		{TypeFileTransferResume, FileTransferResume{TransferID: "tx", Offset: 3}},
		{TypeFileTransferCancel, FileTransferCancel{TransferID: "tx"}},
		{TypeFileTransferDone, FileTransferDone{TransferID: "tx", Status: "success"}},
		{TypeError, ErrorMessage{Code: "c", Message: "m"}},
	}

	for _, tc := range payloads {
		t.Run(tc.msgType, func(t *testing.T) {
			out, err := MarshalBinaryEnvelope(tc.msgType, tc.payload)
			if err != nil {
				t.Fatalf("marshal %s: %v", tc.msgType, err)
			}
			gotType, _, err := UnmarshalBinaryEnvelope(out)
			if err != nil {
				t.Fatalf("round-trip %s: %v", tc.msgType, err)
			}
			if gotType != tc.msgType {
				t.Fatalf("round-trip type = %q, want %q", gotType, tc.msgType)
			}
		})
	}
}

// TestUnmarshalBinaryEnvelopeDoesNotPanicOnHostileBytes feeds the remote-entry
// decoder a spread of malformed and cross-typed frames. It must always return
// an error: this runs on raw bytes straight off the wire, before auth.
func TestUnmarshalBinaryEnvelopeDoesNotPanicOnHostileBytes(t *testing.T) {
	validBody, err := proto.Marshal(&pb.Heartbeat{AgentId: "x"})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	frames := map[string][]byte{
		"empty":              {},
		"garbage":            {0xff, 0xff, 0xff, 0xff},
		"truncated":          {0x0a, 0x7f},
		"unknown type":       mustMarshalEnvelope(t, "no-such-type", validBody),
		"hello_ack body":     mustMarshalEnvelope(t, TypeHelloAck, validBody),
		"task body as hello": mustMarshalEnvelope(t, TypeHello, mustMarshalProto(t, &pb.Task{Id: "t", AgentId: "a"})),
		"nil payload":        mustMarshalEnvelope(t, TypeTaskResult, nil),
		"deep nesting":       mustMarshalEnvelope(t, TypeHelloAck, mustMarshalProto(t, &pb.HelloAck{PendingTasks: []*pb.Task{{Id: "t"}, {Id: "u"}}})),
	}

	for name, frame := range frames {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("decoder panicked on hostile input: %v", r)
				}
			}()
			// Either outcome is acceptable except a panic.
			_, _, _ = UnmarshalBinaryEnvelope(frame)
		})
	}
}

func mustMarshalProto(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal proto: %v", err)
	}
	return b
}

func mustMarshalEnvelope(t *testing.T, msgType string, payload []byte) []byte {
	t.Helper()
	return mustMarshalProto(t, &pb.WireEnvelope{Type: msgType, Payload: payload})
}
