package server

import (
	"crypto/subtle"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"coc2/internal/protocol"
)

func (a *agentConn) readLoop() {
	defer a.service.unregister(a)

	a.conn.SetReadLimit(16 << 20)
	a.conn.SetReadDeadline(time.Now().Add(a.service.cfg.PongWait))
	a.conn.SetPongHandler(func(string) error {
		a.conn.SetReadDeadline(time.Now().Add(a.service.cfg.PongWait))
		if a.id != "" {
			a.service.touch(a.id)
		}
		return nil
	})

	for {
		opcode, raw, err := a.conn.ReadMessage()
		if err != nil {
			a.service.logger.Info("agent disconnected", zap.String("agent_id", a.id), zap.Error(err))
			return
		}
		in, err := protocol.DecodeFrame(opcode, raw)
		if err != nil {
			a.service.logger.Warn("bad frame", zap.String("agent_id", a.id), zap.Int("opcode", opcode), zap.Error(err))
			continue
		}

		// Authentication is a property of the CONNECTION, not of one message
		// type. Until hello has been accepted (a.id is set by register), the
		// only frame this peer may send is hello; anything else would let an
		// unauthenticated TCP client write task results, metrics, or transfer
		// data for arbitrary agent ids. Heartbeat used to be the only guarded
		// case, and it merely `continue`d — leaving the connection alive.
		switch {
		case in.MsgType == protocol.TypeHello:
			if a.id != "" {
				// Already authenticated on this connection: a second hello
				// would re-run register and displace the live entry.
				a.sendProtocolError("already_registered", "hello already accepted on this connection")
				return
			}
		case a.id == "":
			a.sendProtocolError("not_registered", "hello is required first")
			return // close: an unauthenticated peer gets no second chance
		}

		switch in.MsgType {
		case protocol.TypeHello:
			hello, err := protocol.PayloadOf[protocol.AgentHello](in)
			if err != nil {
				a.sendProtocolError("bad_hello", err.Error())
				continue
			}
			if subtle.ConstantTimeCompare([]byte(hello.Token), []byte(a.service.cfg.AuthToken)) != 1 {
				a.service.logAgentAuthFailure(a.remoteAddr, hello.AgentID)
				a.sendProtocolError("auth_failed", "token mismatch")
				return
			}

			// Honour the peer's advertised wire version before composing
			// hello_ack, so the ack itself already uses the new framing.
			a.binaryOut.Store(hello.ProtoVersion >= protocol.BinaryWireVersion)

			pending, err := a.service.register(a, hello)
			if err != nil {
				a.sendProtocolError("register_failed", err.Error())
				return
			}

			ack := protocol.HelloAck{
				ServerTime:   time.Now().UTC(),
				AgentID:      hello.AgentID,
				PendingTasks: pending,
			}
			if err := a.sendMessage(protocol.TypeHelloAck, ack); err != nil {
				a.requeueTasks(pending)
				return
			}
			for i, task := range pending {
				if err := a.sendTask(task); err != nil {
					a.requeueTasks(pending[i:])
					return
				}
				if err := a.service.store.MarkDispatched(task.ID); err != nil {
					a.service.logger.Warn("mark dispatched", zap.String("task_id", task.ID), zap.Error(err))
				}
			}
			a.service.plugins.Trigger("agent_connected", agentConnectedEvent{
				AgentID:     hello.AgentID,
				Hostname:    hello.Hostname,
				OS:          hello.OS,
				Arch:        hello.Arch,
				IPAddrs:     hello.IPAddrs,
				Tags:        hello.Tags,
				Fingerprint: hello.Fingerprint,
				Version:     hello.Version,
				ConnectedAt: hello.ConnectedAt,
			})
			a.service.logger.Info("agent connected", zap.String("agent_id", hello.AgentID), zap.String("hostname", hello.Hostname))
			a.service.auditAgentEvent(a, agentEvent{
				Kind:    "connect",
				AgentID: hello.AgentID,
				OK:      true,
				Summary: fmt.Sprintf("host=%s os=%s arch=%s version=%s pending=%d",
					truncateForLog(hello.Hostname, 64), hello.OS, hello.Arch,
					truncateForLog(hello.Version, 32), len(pending)),
			})
		case protocol.TypeHeartbeat:
			// a.id is non-empty here: the gate above closes the connection
			// for any non-hello frame on an unauthenticated connection.
			if _, err := protocol.PayloadOf[protocol.Heartbeat](in); err != nil {
				a.sendProtocolError("bad_heartbeat", err.Error())
				continue
			}
			a.service.touch(a.id)
		case protocol.TypeTaskResult:
			result, err := protocol.PayloadOf[protocol.TaskResult](in)
			if err != nil {
				a.sendProtocolError("bad_result", err.Error())
				continue
			}
			// The wire carries an agent_id, but the only trustworthy identity
			// is the one this connection authenticated with. Overwrite it so
			// a peer cannot attribute its result to another agent.
			result.AgentID = a.id
			applied, err := a.service.store.SaveResult(result)
			if err != nil {
				a.service.logger.Warn("save result", zap.String("task_id", result.TaskID), zap.Error(err))
				continue
			}
			if !applied {
				// Already final, owned by another agent, or unknown. The
				// store refused it, so nothing was changed — but the attempt
				// is exactly what an investigator needs to see.
				a.service.logger.Warn("discarded result for non-live task",
					zap.String("task_id", result.TaskID),
					zap.String("agent_id", a.id))
			}
			a.service.plugins.Trigger("task_result", result)
			a.service.touch(a.id)
			a.service.auditAgentEvent(a, agentEvent{
				Kind:    "task_result",
				AgentID: a.id,
				OK:      applied,
				Ref:     "task_id=" + truncateForLog(result.TaskID, 64),
				Summary: fmt.Sprintf("status=%s exit=%d applied=%t", result.Status, result.ExitCode, applied),
			})
		case protocol.TypeTaskAck:
			if _, err := protocol.PayloadOf[protocol.TaskAck](in); err != nil {
				a.sendProtocolError("bad_task_ack", err.Error())
				continue
			}
			a.service.touch(a.id)
		case protocol.TypeMetricsReport:
			report, err := protocol.PayloadOf[protocol.MetricsReport](in)
			if err != nil {
				a.sendProtocolError("bad_metrics_report", err.Error())
				continue
			}
			report.AgentID = a.id
			if err := a.service.handleMetricsReport(report); err != nil {
				a.sendProtocolError("metrics_rejected", err.Error())
			}
		case protocol.TypeFileTransferStart:
			start, err := protocol.PayloadOf[protocol.FileTransferStart](in)
			if err != nil {
				a.sendProtocolError("bad_transfer_start", err.Error())
				continue
			}
			a.service.handleTransferStart(start)
		case protocol.TypeFileTransferChunk:
			chunk, err := protocol.PayloadOf[protocol.FileTransferChunk](in)
			if err != nil {
				a.sendProtocolError("bad_transfer_chunk", err.Error())
				continue
			}
			a.service.handleTransferChunk(chunk)
		case protocol.TypeFileTransferResume:
			resume, err := protocol.PayloadOf[protocol.FileTransferResume](in)
			if err != nil {
				a.sendProtocolError("bad_transfer_resume", err.Error())
				continue
			}
			a.service.handleTransferResume(resume)
		case protocol.TypeFileTransferDone:
			done, err := protocol.PayloadOf[protocol.FileTransferDone](in)
			if err != nil {
				a.sendProtocolError("bad_transfer_done", err.Error())
				continue
			}
			live := a.service.handleTransferDone(done)
			// A done for an id the server is not tracking is either a stale
			// frame or someone guessing ids; either way it is audit-worthy.
			a.service.auditAgentEvent(a, agentEvent{
				Kind:    "transfer_done",
				AgentID: a.id,
				OK:      live,
				Ref:     "transfer_id=" + truncateForLog(done.TransferID, 64),
				Summary: fmt.Sprintf("direction=%s status=%s applied=%t", done.Direction, done.Status, live),
			})
		default:
			a.sendProtocolError("unsupported_type", in.MsgType)
		}
	}
}

func (a *agentConn) writeLoop() {
	ticker := time.NewTicker(a.service.cfg.PingPeriod)
	defer func() {
		ticker.Stop()
		// Signal that the queue is drained (or the writer is gone) so a
		// graceful close can proceed without cutting off queued frames.
		a.closeRun.Do(func() { close(a.drained) })
		a.close()
	}()

	for {
		select {
		case msg, ok := <-a.send:
			if !ok {
				return
			}
			a.conn.SetWriteDeadline(time.Now().Add(a.service.cfg.WriteWait))
			if err := a.conn.WriteMessage(msg.opcode, msg.data); err != nil {
				return
			}
		case <-a.done:
			return
		case <-ticker.C:
			a.conn.SetWriteDeadline(time.Now().Add(a.service.cfg.WriteWait))
			if err := a.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (a *agentConn) sendTask(task protocol.Task) error {
	return a.sendMessage(protocol.TypeTaskDispatch, task)
}

func (a *agentConn) sendProtocolError(code, message string) {
	_ = a.sendMessage(protocol.TypeError, protocol.ErrorMessage{
		Code:    code,
		Message: message,
	})
}

func (a *agentConn) sendMessage(msgType string, payload any) error {
	var frame wsFrame
	if a.binaryOut.Load() {
		data, err := protocol.MarshalBinaryEnvelope(msgType, payload)
		if err != nil {
			return err
		}
		frame = wsFrame{opcode: websocket.BinaryMessage, data: data}
	} else {
		data, err := protocol.MarshalMessage(msgType, payload)
		if err != nil {
			return err
		}
		frame = wsFrame{opcode: websocket.TextMessage, data: data}
	}

	// sendMu serializes against closeSend, which closes the channel: a send
	// on a closed channel panics, so the two must not interleave.
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	if a.sendClosed {
		return websocket.ErrCloseSent
	}

	select {
	case a.send <- frame:
		return nil
	case <-a.done:
		// Connection is going away: fail instead of queueing frames that
		// nobody will ever write. (The old `default:` branch dropped
		// frames whenever the 16-deep queue was full, which silently
		// killed every file transfer over ~4 MB — the writeLoop lags the
		// TCP sender on real links, and upload pumps outran it.)
		return websocket.ErrCloseSent
	}
}

func (a *agentConn) close() {
	a.closeOnce.Do(func() {
		close(a.done)
		_ = a.conn.Close()
	})
}

// closeSend closes the outbound queue exactly once. The writeLoop drains what
// is already queued before exiting, so a frame handed to send() before this
// call is still written. Sends after it fail fast instead of panicking.
func (a *agentConn) closeSend() {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	if a.sendClosed {
		return
	}
	a.sendClosed = true
	close(a.send)
}

// drainThenClose stops the writer and waits (bounded by the write timeout) for
// the queue to be flushed before closing the socket. Without this, a
// rejection frame queued just before the connection is torn down is lost, and
// the peer sees a bare abnormal closure with no explanation.
func (a *agentConn) drainThenClose() {
	a.closeSend()
	select {
	case <-a.drained:
	case <-time.After(a.service.cfg.WriteWait):
	}
	a.close()
}

func (a *agentConn) requeueTasks(tasks []protocol.Task) {
	for _, task := range tasks {
		// Requeue as immediately available. These tasks came from PendingTasks,
		// which already filtered out anything with a future release_at, so any
		// original spread window has elapsed and must not be reinstated.
		if err := a.service.store.AddTask(task, time.Time{}); err != nil {
			a.service.logger.Warn("requeue task", zap.String("task_id", task.ID), zap.Error(err))
		}
	}
}
