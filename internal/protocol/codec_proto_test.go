package protocol

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"coc2/internal/protocol/pb"
)

// roundTripSamples covers every wire message with edge values: sub-second
// UTC timestamps and empty/zero fields. All payloads here are valid UTF-8 —
// parity across the two encodings must be exact. Invalid-UTF-8 bytes are
// covered separately by TestProtoPreservesArbitraryBytes: the legacy JSON
// transport corrupts them (encoding/json replaces invalid sequences with
// U+FFFD) while protobuf keeps them raw.
func roundTripSamples() []struct {
	msgType string
	payload any
} {
	ts := time.Date(2026, 9, 8, 7, 30, 15, 123456789, time.UTC)
	cmd := "echo 你好 && ls" // valid UTF-8 in both transports
	return []struct {
		msgType string
		payload any
	}{
		{TypeHello, AgentHello{
			AgentID: "a1", Token: "t", Hostname: "h", OS: "linux", Arch: "arm64",
			IPAddrs: []string{"10.0.0.1", "::1"}, Tags: []string{"prod", "web"},
			Fingerprint: "fp", Version: "v0.4.0", ConnectedAt: ts, ProtoVersion: BinaryWireVersion,
		}},
		{TypeHello, AgentHello{AgentID: "minimal"}},
		{TypeHelloAck, HelloAck{
			ServerTime: ts, AgentID: "a1",
			PendingTasks: []Task{{ID: "t1", AgentID: "a1", Type: "shell", Command: cmd, TimeoutSecs: 30, Priority: 2, CreatedAt: ts}},
		}},
		{TypeHelloAck, HelloAck{ServerTime: ts, AgentID: "a1"}},
		{TypeHeartbeat, Heartbeat{AgentID: "a1", Timestamp: ts}},
		{TypeMetricsReport, MetricsReport{
			AgentID: "a1", Timestamp: ts, UptimeSecs: 999999, CPUCount: 16,
			Goroutines: 42, ProcessMemoryBytes: 1 << 40, RootDiskTotalBytes: 500, RootDiskFreeBytes: 499,
		}},
		{TypeTaskDispatch, Task{ID: "t1", AgentID: "a1", Type: "shell", Command: cmd, TimeoutSecs: 60, Priority: 5, CreatedAt: ts}},
		{TypeTaskAck, TaskAck{TaskID: "t1", AgentID: "a1", ReceivedAt: ts}},
		{TypeTaskCancel, TaskCancel{TaskID: "t1", AgentID: "a1", RequestedAt: ts}},
		{TypeTaskResult, TaskResult{
			TaskID: "t1", AgentID: "a1", Status: "success", ExitCode: 0,
			Stdout: cmd, Stderr: "warning: line\u0000end", DurationMS: 12, CompletedAt: ts,
		}},
		{TypeTaskResult, TaskResult{TaskID: "t2", Status: "failed", ExitCode: -1}},
		{TypeFileTransferStart, FileTransferStart{
			TransferID: "x1", AgentID: "a1", Direction: "upload", LocalPath: "/l", RemotePath: "/r",
			Size: 123, Offset: 45, ChunkSize: 64 << 10, ChecksumSHA256: "abc", RequestedAt: ts,
		}},
		{TypeFileTransferChunk, FileTransferChunk{TransferID: "x1", Seq: 7, Data: []byte{0, 1, 2, 0xff, 0xfe}}},
		{TypeFileTransferChunk, FileTransferChunk{TransferID: "x1", Seq: 0, Data: []byte{}}},
		{TypeFileTransferResume, FileTransferResume{TransferID: "x1", AgentID: "a1", Offset: 1 << 33}},
		{TypeFileTransferCancel, FileTransferCancel{TransferID: "x1", AgentID: "a1", RequestedAt: ts}},
		{TypeFileTransferDone, FileTransferDone{
			TransferID: "x1", AgentID: "a1", Direction: "download", Status: "complete",
			Message: "ok", Size: 999, ChecksumSHA256: "def", CompletedAt: ts,
		}},
		{TypeError, ErrorMessage{Code: "auth_failed", Message: "token mismatch"}},
	}
}

func decodeJSONRoundTrip(t *testing.T, msgType string, payload any) any {
	t.Helper()
	raw, err := MarshalMessage(msgType, payload)
	if err != nil {
		t.Fatalf("json marshal %s: %v", msgType, err)
	}
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("json envelope decode %s: %v", msgType, err)
	}
	switch payload.(type) {
	case AgentHello:
		v, err := UnmarshalPayload[AgentHello](env)
		must(t, err)
		return v
	case HelloAck:
		v, err := UnmarshalPayload[HelloAck](env)
		must(t, err)
		return v
	case Heartbeat:
		v, err := UnmarshalPayload[Heartbeat](env)
		must(t, err)
		return v
	case MetricsReport:
		v, err := UnmarshalPayload[MetricsReport](env)
		must(t, err)
		return v
	case Task:
		v, err := UnmarshalPayload[Task](env)
		must(t, err)
		return v
	case TaskAck:
		v, err := UnmarshalPayload[TaskAck](env)
		must(t, err)
		return v
	case TaskCancel:
		v, err := UnmarshalPayload[TaskCancel](env)
		must(t, err)
		return v
	case TaskResult:
		v, err := UnmarshalPayload[TaskResult](env)
		must(t, err)
		return v
	case FileTransferStart:
		v, err := UnmarshalPayload[FileTransferStart](env)
		must(t, err)
		return v
	case FileTransferChunk:
		v, err := UnmarshalPayload[FileTransferChunk](env)
		must(t, err)
		return v
	case FileTransferResume:
		v, err := UnmarshalPayload[FileTransferResume](env)
		must(t, err)
		return v
	case FileTransferCancel:
		v, err := UnmarshalPayload[FileTransferCancel](env)
		must(t, err)
		return v
	case FileTransferDone:
		v, err := UnmarshalPayload[FileTransferDone](env)
		must(t, err)
		return v
	case ErrorMessage:
		v, err := UnmarshalPayload[ErrorMessage](env)
		must(t, err)
		return v
	}
	t.Fatalf("no json decoder branch for %T", payload)
	return nil
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func decodeProtoRoundTrip(t *testing.T, msgType string, payload any) any {
	t.Helper()
	raw, err := MarshalBinaryEnvelope(msgType, payload)
	if err != nil {
		t.Fatalf("proto marshal %s: %v", msgType, err)
	}
	typ, decoded, err := UnmarshalBinaryEnvelope(raw)
	if err != nil {
		t.Fatalf("proto unmarshal %s: %v", msgType, err)
	}
	if typ != msgType {
		t.Fatalf("proto envelope type = %q, want %q", typ, msgType)
	}
	return decoded
}

func timePtrEqual(a, b time.Time) bool {
	if a.IsZero() != b.IsZero() {
		return false
	}
	return a.IsZero() || a.UTC().Equal(b.UTC())
}

func tasksEqual(a, b Task) bool {
	return a.ID == b.ID && a.AgentID == b.AgentID && a.Type == b.Type &&
		a.Command == b.Command && a.TimeoutSecs == b.TimeoutSecs &&
		a.Priority == b.Priority && timePtrEqual(a.CreatedAt, b.CreatedAt)
}

func assertEqualDecoded(t *testing.T, msgType string, orig, viaJSON, viaProto any) {
	t.Helper()
	switch want := viaJSON.(type) {
	case AgentHello:
		got := viaProto.(AgentHello)
		if got.AgentID != want.AgentID || got.Token != want.Token || got.Hostname != want.Hostname ||
			got.OS != want.OS || got.Arch != want.Arch || got.Fingerprint != want.Fingerprint ||
			got.Version != want.Version || got.ProtoVersion != want.ProtoVersion ||
			!timePtrEqual(got.ConnectedAt, want.ConnectedAt) ||
			join(got.IPAddrs) != join(want.IPAddrs) ||
			join(got.Tags) != join(want.Tags) {
			t.Fatalf("%s mismatch:\njson =%+v\nproto=%+v", msgType, want, got)
		}
	case HelloAck:
		got := viaProto.(HelloAck)
		if got.AgentID != want.AgentID || !timePtrEqual(got.ServerTime, want.ServerTime) || len(got.PendingTasks) != len(want.PendingTasks) {
			t.Fatalf("%s mismatch:\njson =%+v\nproto=%+v", msgType, want, got)
		}
		for i := range want.PendingTasks {
			if !tasksEqual(got.PendingTasks[i], want.PendingTasks[i]) {
				t.Fatalf("%s pending[%d] mismatch:\njson =%+v\nproto=%+v", msgType, i, want.PendingTasks[i], got.PendingTasks[i])
			}
		}
	case Heartbeat:
		got := viaProto.(Heartbeat)
		if got.AgentID != want.AgentID || !timePtrEqual(got.Timestamp, want.Timestamp) {
			t.Fatalf("%s mismatch: json=%+v proto=%+v", msgType, want, got)
		}
	case MetricsReport:
		got := viaProto.(MetricsReport)
		if got.AgentID != want.AgentID || got.UptimeSecs != want.UptimeSecs || got.CPUCount != want.CPUCount ||
			got.Goroutines != want.Goroutines || got.ProcessMemoryBytes != want.ProcessMemoryBytes ||
			got.RootDiskTotalBytes != want.RootDiskTotalBytes || got.RootDiskFreeBytes != want.RootDiskFreeBytes ||
			!timePtrEqual(got.Timestamp, want.Timestamp) {
			t.Fatalf("%s mismatch:\njson =%+v\nproto=%+v", msgType, want, got)
		}
	case Task:
		if !tasksEqual(viaProto.(Task), want) {
			t.Fatalf("%s mismatch:\njson =%+v\nproto=%+v", msgType, want, viaProto)
		}
	case TaskAck:
		got := viaProto.(TaskAck)
		if got.TaskID != want.TaskID || got.AgentID != want.AgentID || !timePtrEqual(got.ReceivedAt, want.ReceivedAt) {
			t.Fatalf("%s mismatch: json=%+v proto=%+v", msgType, want, got)
		}
	case TaskCancel:
		got := viaProto.(TaskCancel)
		if got.TaskID != want.TaskID || got.AgentID != want.AgentID || !timePtrEqual(got.RequestedAt, want.RequestedAt) {
			t.Fatalf("%s mismatch: json=%+v proto=%+v", msgType, want, got)
		}
	case TaskResult:
		got := viaProto.(TaskResult)
		if got.TaskID != want.TaskID || got.AgentID != want.AgentID || got.Status != want.Status ||
			got.ExitCode != want.ExitCode || got.Stdout != want.Stdout || got.Stderr != want.Stderr ||
			got.DurationMS != want.DurationMS || !timePtrEqual(got.CompletedAt, want.CompletedAt) {
			t.Fatalf("%s mismatch:\njson =%+v\nproto=%+v", msgType, want, got)
		}
	case FileTransferStart:
		got := viaProto.(FileTransferStart)
		if got.TransferID != want.TransferID || got.AgentID != want.AgentID || got.Direction != want.Direction ||
			got.LocalPath != want.LocalPath || got.RemotePath != want.RemotePath || got.Size != want.Size ||
			got.Offset != want.Offset || got.ChunkSize != want.ChunkSize || got.ChecksumSHA256 != want.ChecksumSHA256 ||
			!timePtrEqual(got.RequestedAt, want.RequestedAt) {
			t.Fatalf("%s mismatch:\njson =%+v\nproto=%+v", msgType, want, got)
		}
	case FileTransferChunk:
		got := viaProto.(FileTransferChunk)
		if got.TransferID != want.TransferID || got.Seq != want.Seq || !bytes.Equal(got.Data, want.Data) {
			t.Fatalf("%s mismatch: json=%+v proto=%+v", msgType, want, got)
		}
	case FileTransferResume:
		got := viaProto.(FileTransferResume)
		if got != want {
			t.Fatalf("%s mismatch: json=%+v proto=%+v", msgType, want, got)
		}
	case FileTransferCancel:
		got := viaProto.(FileTransferCancel)
		if got.TransferID != want.TransferID || got.AgentID != want.AgentID || !timePtrEqual(got.RequestedAt, want.RequestedAt) {
			t.Fatalf("%s mismatch: json=%+v proto=%+v", msgType, want, got)
		}
	case FileTransferDone:
		got := viaProto.(FileTransferDone)
		if got.TransferID != want.TransferID || got.AgentID != want.AgentID || got.Direction != want.Direction ||
			got.Status != want.Status || got.Message != want.Message || got.Size != want.Size ||
			got.ChecksumSHA256 != want.ChecksumSHA256 || !timePtrEqual(got.CompletedAt, want.CompletedAt) {
			t.Fatalf("%s mismatch:\njson =%+v\nproto=%+v", msgType, want, got)
		}
	case ErrorMessage:
		if viaProto.(ErrorMessage) != want {
			t.Fatalf("%s mismatch: json=%+v proto=%+v", msgType, want, viaProto)
		}
	default:
		t.Fatalf("%s: unexpected decoded type %T", msgType, want)
	}
	_ = orig
}

func join(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += "\x00"
		}
		out += s
	}
	return out
}

// TestJSONProtoParity is the S2 acceptance hammer: for every wire message,
// JSON-round-tripped and proto-round-tripped copies of the same domain value
// must agree field by field. It catches drift between the hand-written codec
// table in codec.go and the generated types.
func TestJSONProtoParity(t *testing.T) {
	for _, sample := range roundTripSamples() {
		sample := sample
		t.Run(sample.msgType, func(t *testing.T) {
			viaJSON := decodeJSONRoundTrip(t, sample.msgType, sample.payload)
			viaProto := decodeProtoRoundTrip(t, sample.msgType, sample.payload)
			assertEqualDecoded(t, sample.msgType, sample.payload, viaJSON, viaProto)
		})
	}
}

// TestProtoPreservesArbitraryBytes pins the second half of the A1 upgrade:
// command/output/chunk bytes are arbitrary binary. The proto path must keep
// them exactly, including sequences encoding/json would silently destroy.
func TestProtoPreservesArbitraryBytes(t *testing.T) {
	raw := []byte{0xff, 0xfe, 0x00, 0x80, 'x'}

	chunk := decodeProtoRoundTrip(t, TypeFileTransferChunk, FileTransferChunk{TransferID: "x", Seq: 1, Data: raw}).(FileTransferChunk)
	if !bytes.Equal(chunk.Data, raw) {
		t.Fatalf("proto chunk data corrupted: %v", chunk.Data)
	}

	res := decodeProtoRoundTrip(t, TypeTaskResult, TaskResult{TaskID: "t", Stdout: string(raw), Stderr: string(raw)}).(TaskResult)
	if res.Stdout != string(raw) || res.Stderr != string(raw) {
		t.Fatalf("proto result streams corrupted: %q / %q", res.Stdout, res.Stderr)
	}

	tk := decodeProtoRoundTrip(t, TypeTaskDispatch, Task{ID: "t", Command: string(raw)}).(Task)
	if tk.Command != string(raw) {
		t.Fatalf("proto command corrupted: %q", tk.Command)
	}

	// Characterise the legacy JSON behaviour honestly: streams get mangled
	// (U+FFFD replacement) while chunk bytes survive untouched via base64.
	jsonRes := decodeJSONRoundTrip(t, TypeTaskResult, TaskResult{TaskID: "t", Stdout: string(raw)}).(TaskResult)
	if jsonRes.Stdout == string(raw) {
		t.Fatalf("expected legacy JSON to corrupt invalid UTF-8; if json fixed this, update the parity doc")
	}
	jsonChunk := decodeJSONRoundTrip(t, TypeFileTransferChunk, FileTransferChunk{TransferID: "x", Seq: 1, Data: raw}).(FileTransferChunk)
	if !bytes.Equal(jsonChunk.Data, raw) {
		t.Fatalf("json chunk data must stay intact via base64: %v", jsonChunk.Data)
	}
}

// TestBinaryEnvelopeTypeRouting pins the envelope contract the transport
// relies on: the outer type string selects the payload decoder, and corrupt
// frames fail loudly.
func TestBinaryEnvelopeTypeRouting(t *testing.T) {
	raw, err := MarshalBinaryEnvelope(TypeHeartbeat, Heartbeat{AgentID: "x", Timestamp: time.Now().UTC().Truncate(time.Second)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var env pb.WireEnvelope
	if err := proto.Unmarshal(raw, &env); err != nil {
		t.Fatalf("outer unmarshal: %v", err)
	}
	if env.Type != TypeHeartbeat {
		t.Fatalf("envelope type = %q", env.Type)
	}
	if _, _, err := UnmarshalBinaryEnvelope(append([]byte{0xff, 0xff}, raw...)); err == nil {
		t.Fatalf("corrupt envelope must fail to decode")
	}
	if _, _, err := UnmarshalBinaryEnvelope(mustMarshal(t, &pb.WireEnvelope{Type: "no_such_type", Payload: []byte{}})); err == nil {
		t.Fatalf("unknown type must fail to decode")
	}
	if _, err := MarshalBinaryEnvelope("no_such_type", struct{}{}); err == nil {
		t.Fatalf("unknown type must fail to encode")
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// TestChunkDataNoBase64Inflation documents the A1 payoff: on the binary path
// chunk bytes go over the wire raw, while the legacy JSON path still carries
// the base64 form (wire-compatible with pre-S2 agents).
func TestChunkDataNoBase64Inflation(t *testing.T) {
	payload := bytes.Repeat([]byte{0xde, 0xad, 0xbe, 0xef}, 256)
	chunk := FileTransferChunk{TransferID: "x", Seq: 1, Data: payload}

	jsonRaw, err := MarshalMessage(TypeFileTransferChunk, chunk)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	protoRaw, err := MarshalBinaryEnvelope(TypeFileTransferChunk, chunk)
	if err != nil {
		t.Fatalf("proto marshal: %v", err)
	}
	if float64(len(jsonRaw)-len(protoRaw)) < float64(len(payload))*0.3 {
		t.Fatalf("expected meaningful base64 saving, got proto=%d json=%d", len(protoRaw), len(jsonRaw))
	}
}
