package server

import (
	"crypto/subtle"
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
			a.service.plugins.Trigger("agent_connected", hello)
			a.service.logger.Info("agent connected", zap.String("agent_id", hello.AgentID), zap.String("hostname", hello.Hostname))
		case protocol.TypeHeartbeat:
			if a.id == "" {
				a.sendProtocolError("not_registered", "hello is required first")
				continue
			}
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
			if err := a.service.store.SaveResult(result); err != nil {
				a.service.logger.Warn("save result", zap.String("task_id", result.TaskID), zap.Error(err))
				continue
			}
			a.service.plugins.Trigger("task_result", result)
			a.service.touch(result.AgentID)
		case protocol.TypeTaskAck:
			ack, err := protocol.PayloadOf[protocol.TaskAck](in)
			if err != nil {
				a.sendProtocolError("bad_task_ack", err.Error())
				continue
			}
			a.service.touch(ack.AgentID)
		case protocol.TypeMetricsReport:
			report, err := protocol.PayloadOf[protocol.MetricsReport](in)
			if err != nil {
				a.sendProtocolError("bad_metrics_report", err.Error())
				continue
			}
			a.service.handleMetricsReport(report)
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
			a.service.handleTransferDone(done)
		default:
			a.sendProtocolError("unsupported_type", in.MsgType)
		}
	}
}

func (a *agentConn) writeLoop() {
	ticker := time.NewTicker(a.service.cfg.PingPeriod)
	defer func() {
		ticker.Stop()
		a.close()
	}()

	for {
		select {
		case msg := <-a.send:
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

func (a *agentConn) requeueTasks(tasks []protocol.Task) {
	for _, task := range tasks {
		if err := a.service.store.AddTask(task); err != nil {
			a.service.logger.Warn("requeue task", zap.String("task_id", task.ID), zap.Error(err))
		}
	}
}
