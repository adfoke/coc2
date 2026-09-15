package protocol

import (
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"coc2/internal/protocol/pb"
)

// BinaryWireVersion is the protobuf framing version advertised by AgentHello
// and understood by this build. A peer advertising 0 (or predating the field)
// stays on the legacy JSON text protocol.
const BinaryWireVersion = uint32(1)

// JSON encoding (legacy) ----------------------------------------------------

func MarshalMessage(msgType string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return json.Marshal(Envelope{
		Type:    msgType,
		Payload: raw,
	})
}

func UnmarshalPayload[T any](env Envelope) (T, error) {
	var out T
	err := json.Unmarshal(env.Payload, &out)
	return out, err
}

// Protobuf <-> domain conversion -------------------------------------------
//
// The pb package is a wire-only representation (decision A1). Domain structs
// remain the in-memory currency everywhere else, so this file is the single
// translate boundary. Timestamps are normalised to UTC on the way out and
// truncated to nanosecond precision by the Timestamp round-trip.

func toProtoTime(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}

func fromProtoTime(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}

func taskToProto(t Task) *pb.Task {
	return &pb.Task{
		Id:          t.ID,
		AgentId:     t.AgentID,
		Type:        t.Type,
		Command:     []byte(t.Command),
		TimeoutSecs: int32(t.TimeoutSecs),
		Priority:    int32(t.Priority),
		CreatedAt:   toProtoTime(t.CreatedAt),
	}
}

func taskFromProto(p *pb.Task) Task {
	return Task{
		ID:          p.Id,
		AgentID:     p.AgentId,
		Type:        p.Type,
		Command:     string(p.Command),
		TimeoutSecs: int(p.TimeoutSecs),
		Priority:    int(p.Priority),
		CreatedAt:   fromProtoTime(p.CreatedAt),
	}
}

func tasksToProto(tasks []Task) []*pb.Task {
	out := make([]*pb.Task, len(tasks))
	for i, t := range tasks {
		out[i] = taskToProto(t)
	}
	return out
}

func tasksFromProto(tasks []*pb.Task) []Task {
	out := make([]Task, len(tasks))
	for i, t := range tasks {
		out[i] = taskFromProto(t)
	}
	return out
}

func codecFor(msgType string) (toProto func(any) (proto.Message, error), fromProto func(proto.Message) (any, error), ok bool) {
	switch msgType {
	case TypeHello:
		return func(v any) (proto.Message, error) {
				m, ok := v.(AgentHello)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.AgentHello{
					AgentId: m.AgentID, Token: m.Token, Hostname: m.Hostname,
					Os: m.OS, Arch: m.Arch, IpAddrs: m.IPAddrs, Tags: m.Tags,
					Fingerprint: m.Fingerprint, Version: m.Version,
					ConnectedAt: toProtoTime(m.ConnectedAt), ProtoVersion: m.ProtoVersion,
				}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.AgentHello)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return AgentHello{
					AgentID: m.AgentId, Token: m.Token, Hostname: m.Hostname,
					OS: m.Os, Arch: m.Arch, IPAddrs: m.IpAddrs, Tags: m.Tags,
					Fingerprint: m.Fingerprint, Version: m.Version,
					ConnectedAt: fromProtoTime(m.ConnectedAt), ProtoVersion: m.ProtoVersion,
				}, nil
			}, true
	case TypeHelloAck:
		return func(v any) (proto.Message, error) {
				m, ok := v.(HelloAck)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.HelloAck{
					ServerTime: toProtoTime(m.ServerTime), AgentId: m.AgentID,
					PendingTasks: tasksToProto(m.PendingTasks),
				}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.HelloAck)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return HelloAck{
					ServerTime: fromProtoTime(m.ServerTime), AgentID: m.AgentId,
					PendingTasks: tasksFromProto(m.PendingTasks),
				}, nil
			}, true
	case TypeHeartbeat:
		return func(v any) (proto.Message, error) {
				m, ok := v.(Heartbeat)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.Heartbeat{AgentId: m.AgentID, Timestamp: toProtoTime(m.Timestamp)}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.Heartbeat)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return Heartbeat{AgentID: m.AgentId, Timestamp: fromProtoTime(m.Timestamp)}, nil
			}, true
	case TypeMetricsReport:
		return func(v any) (proto.Message, error) {
				m, ok := v.(MetricsReport)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.MetricsReport{
					AgentId: m.AgentID, Timestamp: toProtoTime(m.Timestamp),
					UptimeSecs: m.UptimeSecs, CpuCount: int32(m.CPUCount),
					Goroutines: int32(m.Goroutines), ProcessMemoryBytes: m.ProcessMemoryBytes,
					RootDiskTotalBytes: m.RootDiskTotalBytes, RootDiskFreeBytes: m.RootDiskFreeBytes,
				}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.MetricsReport)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return MetricsReport{
					AgentID: m.AgentId, Timestamp: fromProtoTime(m.Timestamp),
					UptimeSecs: m.UptimeSecs, CPUCount: int(m.CpuCount),
					Goroutines: int(m.Goroutines), ProcessMemoryBytes: m.ProcessMemoryBytes,
					RootDiskTotalBytes: m.RootDiskTotalBytes, RootDiskFreeBytes: m.RootDiskFreeBytes,
				}, nil
			}, true
	case TypeTaskDispatch:
		return func(v any) (proto.Message, error) {
				m, ok := v.(Task)
				if !ok {
					return nil, errType(msgType, v)
				}
				return taskToProto(m), nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.Task)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return taskFromProto(m), nil
			}, true
	case TypeTaskAck:
		return func(v any) (proto.Message, error) {
				m, ok := v.(TaskAck)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.TaskAck{TaskId: m.TaskID, AgentId: m.AgentID, ReceivedAt: toProtoTime(m.ReceivedAt)}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.TaskAck)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return TaskAck{TaskID: m.TaskId, AgentID: m.AgentId, ReceivedAt: fromProtoTime(m.ReceivedAt)}, nil
			}, true
	case TypeTaskCancel:
		return func(v any) (proto.Message, error) {
				m, ok := v.(TaskCancel)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.TaskCancel{TaskId: m.TaskID, AgentId: m.AgentID, RequestedAt: toProtoTime(m.RequestedAt)}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.TaskCancel)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return TaskCancel{TaskID: m.TaskId, AgentID: m.AgentId, RequestedAt: fromProtoTime(m.RequestedAt)}, nil
			}, true
	case TypeTaskResult:
		return func(v any) (proto.Message, error) {
				m, ok := v.(TaskResult)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.TaskResult{
					TaskId: m.TaskID, AgentId: m.AgentID, Status: m.Status,
					ExitCode: int32(m.ExitCode), Stdout: []byte(m.Stdout), Stderr: []byte(m.Stderr),
					DurationMs: m.DurationMS, CompletedAt: toProtoTime(m.CompletedAt),
				}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.TaskResult)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return TaskResult{
					TaskID: m.TaskId, AgentID: m.AgentId, Status: m.Status,
					ExitCode: int(m.ExitCode), Stdout: string(m.Stdout), Stderr: string(m.Stderr),
					DurationMS: m.DurationMs, CompletedAt: fromProtoTime(m.CompletedAt),
				}, nil
			}, true
	case TypeFileTransferStart:
		return func(v any) (proto.Message, error) {
				m, ok := v.(FileTransferStart)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.FileTransferStart{
					TransferId: m.TransferID, AgentId: m.AgentID, Direction: m.Direction,
					LocalPath: m.LocalPath, RemotePath: m.RemotePath, Size: m.Size,
					Offset: m.Offset, ChunkSize: int32(m.ChunkSize), ChecksumSha256: m.ChecksumSHA256,
					RequestedAt: toProtoTime(m.RequestedAt),
				}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.FileTransferStart)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return FileTransferStart{
					TransferID: m.TransferId, AgentID: m.AgentId, Direction: m.Direction,
					LocalPath: m.LocalPath, RemotePath: m.RemotePath, Size: m.Size,
					Offset: m.Offset, ChunkSize: int(m.ChunkSize), ChecksumSHA256: m.ChecksumSha256,
					RequestedAt: fromProtoTime(m.RequestedAt),
				}, nil
			}, true
	case TypeFileTransferChunk:
		return func(v any) (proto.Message, error) {
				m, ok := v.(FileTransferChunk)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.FileTransferChunk{TransferId: m.TransferID, Seq: int32(m.Seq), Data: m.Data}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.FileTransferChunk)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return FileTransferChunk{TransferID: m.TransferId, Seq: int(m.Seq), Data: m.Data}, nil
			}, true
	case TypeFileTransferResume:
		return func(v any) (proto.Message, error) {
				m, ok := v.(FileTransferResume)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.FileTransferResume{TransferId: m.TransferID, AgentId: m.AgentID, Offset: m.Offset}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.FileTransferResume)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return FileTransferResume{TransferID: m.TransferId, AgentID: m.AgentId, Offset: m.Offset}, nil
			}, true
	case TypeFileTransferCancel:
		return func(v any) (proto.Message, error) {
				m, ok := v.(FileTransferCancel)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.FileTransferCancel{TransferId: m.TransferID, AgentId: m.AgentID, RequestedAt: toProtoTime(m.RequestedAt)}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.FileTransferCancel)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return FileTransferCancel{TransferID: m.TransferId, AgentID: m.AgentId, RequestedAt: fromProtoTime(m.RequestedAt)}, nil
			}, true
	case TypeFileTransferDone:
		return func(v any) (proto.Message, error) {
				m, ok := v.(FileTransferDone)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.FileTransferDone{
					TransferId: m.TransferID, AgentId: m.AgentID, Direction: m.Direction,
					Status: m.Status, Message: m.Message, Size: m.Size,
					ChecksumSha256: m.ChecksumSHA256, CompletedAt: toProtoTime(m.CompletedAt),
				}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.FileTransferDone)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return FileTransferDone{
					TransferID: m.TransferId, AgentID: m.AgentId, Direction: m.Direction,
					Status: m.Status, Message: m.Message, Size: m.Size,
					ChecksumSHA256: m.ChecksumSha256, CompletedAt: fromProtoTime(m.CompletedAt),
				}, nil
			}, true
	case TypeError:
		return func(v any) (proto.Message, error) {
				m, ok := v.(ErrorMessage)
				if !ok {
					return nil, errType(msgType, v)
				}
				return &pb.ErrorMessage{Code: m.Code, Message: m.Message}, nil
			}, func(pm proto.Message) (any, error) {
				m, ok := pm.(*pb.ErrorMessage)
				if !ok {
					return nil, errType(msgType, pm)
				}
				return ErrorMessage{Code: m.Code, Message: m.Message}, nil
			}, true
	default:
		return nil, nil, false
	}
}

// errType reports a payload whose Go type does not match the envelope's
// message type. It is the checked replacement for the unchecked type
// assertions this codec used to rely on: the invariant "the caller passes the
// payload type matching msgType" is real but implicit, and a failed assertion
// would take down the whole process instead of failing one message.
func errType(msgType string, got any) error {
	return fmt.Errorf("wire: payload for %q is %T", msgType, got)
}

func newProtoMessage(msgType string) (proto.Message, bool) {
	switch msgType {
	case TypeHello:
		return &pb.AgentHello{}, true
	case TypeHelloAck:
		return &pb.HelloAck{}, true
	case TypeHeartbeat:
		return &pb.Heartbeat{}, true
	case TypeMetricsReport:
		return &pb.MetricsReport{}, true
	case TypeTaskDispatch:
		return &pb.Task{}, true
	case TypeTaskAck:
		return &pb.TaskAck{}, true
	case TypeTaskCancel:
		return &pb.TaskCancel{}, true
	case TypeTaskResult:
		return &pb.TaskResult{}, true
	case TypeFileTransferStart:
		return &pb.FileTransferStart{}, true
	case TypeFileTransferChunk:
		return &pb.FileTransferChunk{}, true
	case TypeFileTransferResume:
		return &pb.FileTransferResume{}, true
	case TypeFileTransferCancel:
		return &pb.FileTransferCancel{}, true
	case TypeFileTransferDone:
		return &pb.FileTransferDone{}, true
	case TypeError:
		return &pb.ErrorMessage{}, true
	default:
		return nil, false
	}
}

// MarshalBinaryEnvelope encodes a domain payload into a protobuf WireEnvelope.
func MarshalBinaryEnvelope(msgType string, payload any) ([]byte, error) {
	to, _, ok := codecFor(msgType)
	if !ok {
		return nil, fmt.Errorf("wire: no proto codec for %q", msgType)
	}
	pm, err := to(payload)
	if err != nil {
		return nil, err
	}
	body, err := proto.Marshal(pm)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pb.WireEnvelope{Type: msgType, Payload: body})
}

// UnmarshalBinaryEnvelope decodes a protobuf WireEnvelope into the domain
// payload for its type.
func UnmarshalBinaryEnvelope(raw []byte) (string, any, error) {
	var env pb.WireEnvelope
	if err := proto.Unmarshal(raw, &env); err != nil {
		return "", nil, err
	}
	pm, ok := newProtoMessage(env.Type)
	if !ok {
		return env.Type, nil, fmt.Errorf("wire: unknown type %q", env.Type)
	}
	if err := proto.Unmarshal(env.Payload, pm); err != nil {
		return env.Type, nil, err
	}
	_, from, ok := codecFor(env.Type)
	if !ok {
		return env.Type, nil, fmt.Errorf("wire: no proto decoder for %q", env.Type)
	}
	decoded, err := from(pm)
	if err != nil {
		return env.Type, nil, err
	}
	return env.Type, decoded, nil
}
