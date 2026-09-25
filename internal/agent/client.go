// Package agent implements the CoC2 agent: a long-lived client that connects
// back to the server, executes shell tasks, reports heartbeat and metrics, and
// handles file transfers.
package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"coc2/internal/common"
	"coc2/internal/protocol"
)

const (
	// maxTaskOutputBytes and maxConcurrentTasks are the operator-visible task
	// limits; they live in internal/common because `coc2 schema` publishes
	// them, and the published number must be the enforced one.
	maxTaskOutputBytes = common.MaxTaskOutputBytes
	maxConcurrentTasks = common.MaxConcurrentTasks

	maxCachedResults = 256
	cachedResultTTL  = 10 * time.Minute

	// maxInboundFrameBytes bounds a single frame from the server. The server
	// already caps what it sends; this is the agent's own defence so a
	// hostile or buggy peer cannot make it allocate without limit. It matches
	// the server's own SetReadLimit.
	maxInboundFrameBytes = 16 << 20

	// agentWriteTimeout bounds one websocket write. Generous enough for a
	// 256 KiB chunk over a slow link, short enough that a dead peer cannot
	// hold writeMu indefinitely.
	agentWriteTimeout = 30 * time.Second

	// shutdownGrace bounds how long Run waits for in-flight tasks to tear
	// their process groups down once the context is cancelled. Those tasks
	// derive from the same context, so cancelling has already asked them to
	// kill; the wait only covers delivering the signal. Exiting the process
	// first would orphan their children, which is exactly what the
	// process-group kill exists to prevent.
	shutdownGrace = 5 * time.Second
)

// errTooManyTasks is reported back for a dispatch that arrived while
// maxConcurrentTasks were already running.
var errTooManyTasks = errors.New("agent is already running the maximum number of tasks")

type Client struct {
	cfg       Config
	logger    *zap.Logger
	host      common.HostInfo
	agentID   string
	startedAt time.Time
	dialer    *websocket.Dialer
	writeMu   sync.Mutex
	// binaryOut flips once hello_ack arrives as a binary frame, proving the
	// server completed the protobuf negotiation; every message after it is
	// sent protobuf-encoded. Reads always accept both framings by opcode.
	binaryOut atomic.Bool
	taskMu    sync.Mutex
	running   map[string]context.CancelFunc
	// taskWG tracks the task goroutines so shutdown can wait for them to
	// finish killing their process groups instead of exiting on top of them.
	taskWG     sync.WaitGroup
	resultMu   sync.Mutex
	results    map[string]cachedTaskResult
	uploadMu   sync.Mutex
	uploads    map[string]*uploadState
	downloadMu sync.Mutex
	// downloads holds the cancel func of every in-flight sendFile pump so a
	// server-initiated file_transfer_cancel can stop an agent->server push.
	downloads map[string]context.CancelFunc
}

type cachedTaskResult struct {
	result   protocol.TaskResult
	cachedAt time.Time
}

type uploadState struct {
	remotePath string
	tempPath   string
	file       *os.File
	size       int64
	received   int64

	assembler      *common.ChunkAssembler
	expectedChunks int
}

type cappedBuffer struct {
	buf       strings.Builder
	limit     int
	truncated bool
}

func New(cfg Config, logger *zap.Logger) (*Client, error) {
	if cfg.ServerURL == "" {
		return nil, errors.New("server url is required")
	}
	if cfg.Token == "" {
		return nil, errors.New("token is required")
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}

	if !strings.HasPrefix(cfg.ServerURL, "wss://") {
		logger.Warn("agent is connecting over an unencrypted websocket; the token and all traffic are sent in plaintext. Use a wss:// server URL with TLS for production.")
	}

	host := common.CollectHostInfo()
	agentID := cfg.AgentID
	if agentID == "" {
		agentID = host.Hostname
		if agentID == "" {
			agentID = common.NewID()
		}
	}

	tlsCfg, err := buildClientTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:       cfg,
		logger:    logger,
		host:      host,
		agentID:   agentID,
		startedAt: time.Now(),
		dialer: &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 10 * time.Second,
			TLSClientConfig:  tlsCfg,
		},
		running:   make(map[string]context.CancelFunc),
		results:   make(map[string]cachedTaskResult),
		uploads:   make(map[string]*uploadState),
		downloads: make(map[string]context.CancelFunc),
	}, nil
}

func (c *Client) Run(ctx context.Context) error {
	backoff := time.Second

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := c.runOnce(ctx)
		if ctx.Err() != nil {
			// Shutting down: let the task goroutines finish killing their
			// process groups before this process disappears from under them.
			c.waitRunningTasks(shutdownGrace)
			return ctx.Err()
		}
		if err != nil {
			c.logger.Warn("agent session ended", zap.Error(err))
		}

		sleep := backoff + time.Duration(rand.IntN(500))*time.Millisecond
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > c.cfg.MaxBackoff {
			backoff = c.cfg.MaxBackoff
		}
	}
}

func (c *Client) runOnce(ctx context.Context) error {
	conn, _, err := c.dialer.DialContext(ctx, c.cfg.ServerURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Mirror the server's read limit: a single frame larger than this is a
	// protocol violation, not something to buffer. (No read deadline here:
	// the client does not answer the server's pings, and liveness is already
	// covered by the heartbeat write failing on a dead link.)
	conn.SetReadLimit(maxInboundFrameBytes)

	// ...which means the read loop below blocks indefinitely on a healthy
	// connection and never observes ctx being cancelled. Without this watcher
	// a SIGTERM (systemd stop, docker stop, `kill`) would leave the agent
	// running, still heartbeating and still counted online by the server,
	// while every task dispatched to it died as "canceled". Closing the
	// socket makes ReadMessage return, unwinding runOnce so Run can exit.
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopWatch:
		}
	}()

	// Fresh connection: fall back to legacy framing until the peer proves
	// protobuf support, then upgrade mid-session at hello_ack.
	c.binaryOut.Store(false)

	if err := c.send(conn, protocol.TypeHello, protocol.AgentHello{
		AgentID:      c.agentID,
		Token:        c.cfg.Token,
		Hostname:     c.host.Hostname,
		OS:           c.host.OS,
		Arch:         c.host.Arch,
		IPAddrs:      c.host.IPAddrs,
		Tags:         c.cfg.Tags,
		Fingerprint:  c.host.Fingerprint,
		Version:      "v0.4.0",
		ConnectedAt:  time.Now().UTC(),
		ProtoVersion: protocol.BinaryWireVersion,
	}); err != nil {
		return err
	}

	heartbeatDone := make(chan struct{})
	go c.heartbeatLoop(ctx, conn, heartbeatDone)
	defer close(heartbeatDone)

	for {
		opcode, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		in, err := protocol.DecodeFrame(opcode, raw)
		if err != nil {
			c.logger.Warn("bad frame", zap.Int("opcode", opcode), zap.Error(err))
			continue
		}

		switch in.MsgType {
		case protocol.TypeHelloAck:
			ack, err := protocol.PayloadOf[protocol.HelloAck](in)
			if err != nil {
				return err
			}
			// The server switches its own outbound at hello regardless of
			// what the agent advertised; mirror that decision from the
			// frame we just received so a mismatch is self-healing.
			c.binaryOut.Store(in.Opcode == protocol.FrameBinary)
			c.logger.Info("connected",
				zap.String("agent_id", ack.AgentID),
				zap.Int("pending", len(ack.PendingTasks)),
				zap.String("wire", wireName(in.Opcode)))
		case protocol.TypeTaskDispatch:
			task, err := protocol.PayloadOf[protocol.Task](in)
			if err != nil {
				return err
			}
			if err := c.ackTask(conn, task.ID); err != nil {
				return err
			}
			if result, ok := c.cachedResult(task.ID); ok {
				if err := c.send(conn, protocol.TypeTaskResult, result); err != nil {
					return err
				}
				continue
			}
			if c.taskRunning(task.ID) {
				continue
			}
			if !c.startTask(ctx, conn, task) {
				// Refusing silently would leave the server waiting for a
				// result that never comes; report why instead.
				result := protocol.TaskResult{
					TaskID:      task.ID,
					AgentID:     c.agentID,
					Status:      "failed",
					ExitCode:    -1,
					Stderr:      errTooManyTasks.Error(),
					CompletedAt: time.Now().UTC(),
				}
				c.cacheResult(result)
				if err := c.send(conn, protocol.TypeTaskResult, result); err != nil {
					return err
				}
			}
		case protocol.TypeTaskCancel:
			cancelMsg, err := protocol.PayloadOf[protocol.TaskCancel](in)
			if err != nil {
				return err
			}
			c.cancelTask(cancelMsg.TaskID)
		case protocol.TypeFileTransferStart:
			start, err := protocol.PayloadOf[protocol.FileTransferStart](in)
			if err != nil {
				return err
			}
			c.handleTransferStart(ctx, conn, start)
		case protocol.TypeFileTransferChunk:
			chunk, err := protocol.PayloadOf[protocol.FileTransferChunk](in)
			if err != nil {
				return err
			}
			c.handleTransferChunk(conn, chunk)
		case protocol.TypeFileTransferCancel:
			cancelMsg, err := protocol.PayloadOf[protocol.FileTransferCancel](in)
			if err != nil {
				return err
			}
			c.handleTransferCancel(conn, cancelMsg)
		case protocol.TypeFileTransferDone:
			done, err := protocol.PayloadOf[protocol.FileTransferDone](in)
			if err != nil {
				return err
			}
			c.handleTransferDone(conn, done)
		case protocol.TypeError:
			msg, err := protocol.PayloadOf[protocol.ErrorMessage](in)
			if err != nil {
				return err
			}
			return fmt.Errorf("%s: %s", msg.Code, msg.Message)
		}
	}
}

func (c *Client) heartbeatLoop(ctx context.Context, conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			_ = c.send(conn, protocol.TypeHeartbeat, protocol.Heartbeat{
				AgentID:   c.agentID,
				Timestamp: time.Now().UTC(),
			})
			if metrics, err := c.collectMetrics(); err == nil {
				_ = c.send(conn, protocol.TypeMetricsReport, metrics)
			}
		}
	}
}

// taskAdmission is the outcome of trying to reserve a slot for a dispatch.
type taskAdmission int

const (
	// taskReserved: the caller owns a slot and must run the task.
	taskReserved taskAdmission = iota
	// taskDuplicate: this task id is already running; the dispatch is a
	// retransmission and must be ignored, not answered.
	taskDuplicate
	// taskAtCapacity: maxConcurrentTasks are already running.
	taskAtCapacity
)

// tryReserveTask claims a concurrency slot for task under taskMu. It reports
// why admission was refused so the caller can distinguish a retransmission
// from genuine overload. On success the returned cancel func is already
// registered, so unregisterTask is the only cleanup needed.
func (c *Client) tryReserveTask(task protocol.Task, cancel context.CancelFunc) taskAdmission {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()

	if _, running := c.running[task.ID]; running {
		return taskDuplicate
	}
	if len(c.running) >= maxConcurrentTasks {
		return taskAtCapacity
	}
	c.running[task.ID] = cancel
	return taskReserved
}

// startTask launches a task unless the concurrency cap is already reached. It
// reports whether the task was started; a false return means the caller must
// answer the dispatch itself.
func (c *Client) startTask(ctx context.Context, conn *websocket.Conn, task protocol.Task) bool {
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(task.TimeoutSecs)*time.Second)

	if admission := c.tryReserveTask(task, cancel); admission != taskReserved {
		cancel()
		if admission == taskAtCapacity {
			c.logger.Warn("task refused: concurrency limit reached",
				zap.String("task_id", task.ID), zap.Int("limit", maxConcurrentTasks))
			return false
		}
		// Duplicate: the running copy already owns the slot. Nothing to do.
		return true
	}

	// Registered before the goroutine starts, and Done runs last (defers are
	// LIFO), so waitRunningTasks sees the slot released only once the process
	// group has been reaped. The read loop is the only Add site and it has
	// already exited by the time anyone waits, so there is no Add/Wait race.
	c.taskWG.Add(1)
	go func() {
		defer c.taskWG.Done()
		defer cancel()
		defer c.unregisterTask(task.ID)

		result := c.runCommand(runCtx, task)
		c.cacheResult(result)
		if err := c.send(conn, protocol.TypeTaskResult, result); err != nil {
			c.logger.Warn("send result", zap.String("task_id", task.ID), zap.Error(err))
		}
	}()
	return true
}

// waitRunningTasks blocks until every in-flight task goroutine has exited, or
// until timeout elapses. Only Run calls it, on the way out, after the read
// loop is done — so no new task can be admitted while it waits.
func (c *Client) waitRunningTasks(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		c.taskWG.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		c.logger.Warn("shutdown grace elapsed with tasks still running",
			zap.Duration("grace", timeout))
	}
}

// runCommand executes a single shell task and returns its result. The shell is
// started in its own process group so that timeout or cancel kills the whole
// process tree, not just the shell.
func (c *Client) runCommand(ctx context.Context, task protocol.Task) protocol.TaskResult {
	start := time.Now()
	result := protocol.TaskResult{
		TaskID:  task.ID,
		AgentID: c.agentID,
		Status:  "success",
	}

	cmd := exec.Command("/bin/sh", "-c", task.Command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutBuffer := &cappedBuffer{limit: maxTaskOutputBytes}
	stderrBuffer := &cappedBuffer{limit: maxTaskOutputBytes}
	cmd.Stdout = stdoutBuffer
	cmd.Stderr = stderrBuffer

	if err := cmd.Start(); err != nil {
		result.Status = "failed"
		result.ExitCode = -1
		result.Stderr = err.Error()
		result.CompletedAt = time.Now().UTC()
		result.DurationMS = time.Since(start).Milliseconds()
		return result
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	var runErr error
	select {
	case <-ctx.Done():
		c.killProcessGroup(cmd)
		runErr = <-waitCh
	case runErr = <-waitCh:
	}

	result.Stdout = stdoutBuffer.String()
	result.Stderr = stderrBuffer.String()
	result.CompletedAt = time.Now().UTC()
	result.DurationMS = time.Since(start).Milliseconds()

	if runErr != nil {
		result.Status = "failed"
		switch ctx.Err() {
		case context.DeadlineExceeded:
			result.Status = "timeout"
		case context.Canceled:
			result.Status = "canceled"
		}
		if exitErr := new(exec.ExitError); errors.As(runErr, &exitErr) {
			result.ExitCode = exitErr.ExitCode()
		} else if result.Status == "failed" {
			result.Stderr = strings.TrimSpace(result.Stderr + "\n" + runErr.Error())
		}
	}

	return result
}

func (c *Client) killProcessGroup(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	// A negative pid targets the whole process group created via Setpgid.
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		_ = cmd.Process.Kill()
	}
}

func (c *Client) cancelTask(taskID string) {
	c.taskMu.Lock()
	cancel := c.running[taskID]
	c.taskMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Client) unregisterTask(taskID string) {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()
	delete(c.running, taskID)
}

func (c *Client) taskRunning(taskID string) bool {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()
	_, ok := c.running[taskID]
	return ok
}

func (c *Client) cacheResult(result protocol.TaskResult) {
	c.resultMu.Lock()
	defer c.resultMu.Unlock()
	now := time.Now()
	c.pruneResultsLocked(now)
	c.results[result.TaskID] = cachedTaskResult{
		result:   result,
		cachedAt: now,
	}
	for len(c.results) > maxCachedResults {
		oldestID := ""
		var oldestAt time.Time
		for taskID, cached := range c.results {
			if oldestID == "" || cached.cachedAt.Before(oldestAt) {
				oldestID = taskID
				oldestAt = cached.cachedAt
			}
		}
		if oldestID == "" {
			break
		}
		delete(c.results, oldestID)
	}
}

func (c *Client) cachedResult(taskID string) (protocol.TaskResult, bool) {
	c.resultMu.Lock()
	defer c.resultMu.Unlock()
	c.pruneResultsLocked(time.Now())
	cached, ok := c.results[taskID]
	if !ok {
		return protocol.TaskResult{}, false
	}
	return cached.result, true
}

func (c *Client) ackTask(conn *websocket.Conn, taskID string) error {
	return c.send(conn, protocol.TypeTaskAck, protocol.TaskAck{
		TaskID:     taskID,
		AgentID:    c.agentID,
		ReceivedAt: time.Now().UTC(),
	})
}

func (c *Client) handleTransferStart(ctx context.Context, conn *websocket.Conn, start protocol.FileTransferStart) {
	switch start.Direction {
	case "upload":
		c.beginUpload(conn, start)
	case "download":
		// The pump runs on its own goroutine (so the readLoop stays free
		// to process a file_transfer_cancel); register a cancel func for
		// it under the session context.
		dctx, cancel := context.WithCancel(ctx)
		c.downloadMu.Lock()
		c.downloads[start.TransferID] = cancel
		c.downloadMu.Unlock()
		go c.sendFile(dctx, conn, start)
	}
}

// handleTransferCancel stops an in-flight transfer at the operator's
// request and confirms with FileTransferDone{canceled} — the server keeps
// the authoritative audit row.
//
// Both directions converge here: an agent->server push is running in
// sendFile's goroutine (cancelled via its registered cancel func; the pump
// sends the confirmation itself); a server->agent upload is buffered in
// uploads and is aborted synchronously, keeping the .part so a retry can
// resume. Unknown ids are ignored: the transfer already reached a terminal
// state and the server has the outcome.
func (c *Client) handleTransferCancel(conn *websocket.Conn, msg protocol.FileTransferCancel) {
	c.uploadMu.Lock()
	up := c.uploads[msg.TransferID]
	if up != nil {
		delete(c.uploads, msg.TransferID)
	}
	c.uploadMu.Unlock()

	if up != nil {
		if up.file != nil {
			_ = up.file.Close()
		}
		c.sendTransferDone(conn, protocol.FileTransferDone{
			TransferID:  msg.TransferID,
			AgentID:     c.agentID,
			Direction:   "upload",
			Status:      "canceled",
			Message:     "canceled by operator",
			Size:        up.received,
			CompletedAt: time.Now().UTC(),
		})
		return
	}

	c.downloadMu.Lock()
	cancel := c.downloads[msg.TransferID]
	c.downloadMu.Unlock()
	if cancel != nil {
		// The sendFile pump observes the closed ctx at its next chunk
		// boundary, sends the canceled done, and deregisters itself.
		cancel()
	}
}

func (c *Client) beginUpload(conn *websocket.Conn, start protocol.FileTransferStart) {
	if err := os.MkdirAll(filepath.Dir(start.RemotePath), 0o755); err != nil {
		c.sendTransferDone(conn, protocol.FileTransferDone{
			TransferID:  start.TransferID,
			AgentID:     c.agentID,
			Direction:   start.Direction,
			Status:      "failed",
			Message:     err.Error(),
			CompletedAt: time.Now().UTC(),
		})
		return
	}

	chunkSize := start.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 256 * 1024
	}
	tempPath := start.RemotePath + ".part"

	// Close any stale upload targeting the same destination so it cannot write
	// to the shared partial file concurrently.
	c.abortUploadsForPath(start.RemotePath)

	offset, err := common.ResumeOffset(tempPath, chunkSize)
	if err != nil {
		c.sendTransferDone(conn, protocol.FileTransferDone{
			TransferID:  start.TransferID,
			AgentID:     c.agentID,
			Direction:   start.Direction,
			Status:      "failed",
			Message:     err.Error(),
			CompletedAt: time.Now().UTC(),
		})
		return
	}
	// A resumed upload is the one transfer fact the audit trail cannot show by
	// itself: the transfer record looks the same whether the bytes came from
	// the start or from a retained .part, so say so when it happens. Only the
	// failure to *announce* a resume was logged before, which left a successful
	// resume indistinguishable from a full re-send.
	if offset > 0 {
		c.logger.Info("resuming partial upload",
			zap.String("transfer_id", start.TransferID),
			zap.String("remote_path", start.RemotePath),
			zap.Int64("offset", offset),
			zap.Int64("size", start.Size))
	}

	file, err := common.OpenPartialFile(tempPath, offset)
	if err != nil {
		c.sendTransferDone(conn, protocol.FileTransferDone{
			TransferID:  start.TransferID,
			AgentID:     c.agentID,
			Direction:   start.Direction,
			Status:      "failed",
			Message:     err.Error(),
			CompletedAt: time.Now().UTC(),
		})
		return
	}

	assembler := common.NewChunkAssembler(0)
	assembler.Resume(int(offset / int64(chunkSize)))

	c.uploadMu.Lock()
	c.uploads[start.TransferID] = &uploadState{
		remotePath:     start.RemotePath,
		tempPath:       tempPath,
		file:           file,
		size:           start.Size,
		received:       offset,
		assembler:      assembler,
		expectedChunks: common.ChunkCount(start.Size, chunkSize),
	}
	c.uploadMu.Unlock()

	if err := c.send(conn, protocol.TypeFileTransferResume, protocol.FileTransferResume{
		TransferID: start.TransferID,
		AgentID:    c.agentID,
		Offset:     offset,
	}); err != nil {
		c.logger.Warn("send transfer resume", zap.String("transfer_id", start.TransferID), zap.Error(err))
	}
}

// abortUploadsForPath closes and drops any in-flight upload state targeting the
// given remote path.
func (c *Client) abortUploadsForPath(remotePath string) {
	c.uploadMu.Lock()
	defer c.uploadMu.Unlock()
	for id, state := range c.uploads {
		if state.remotePath == remotePath {
			if state.file != nil {
				_ = state.file.Close()
			}
			delete(c.uploads, id)
		}
	}
}

func (c *Client) handleTransferChunk(conn *websocket.Conn, chunk protocol.FileTransferChunk) {
	c.uploadMu.Lock()
	state := c.uploads[chunk.TransferID]
	c.uploadMu.Unlock()
	if state == nil {
		return
	}

	data := chunk.Data
	if state.assembler == nil {
		state.assembler = common.NewChunkAssembler(0)
	}
	if err := state.assembler.Write(state.file, chunk.Seq, data); err != nil {
		c.failUpload(conn, chunk.TransferID, state, err)
		return
	}
	state.received += int64(len(data))
}

func (c *Client) handleTransferDone(conn *websocket.Conn, done protocol.FileTransferDone) {
	if done.Direction != "upload" {
		return
	}

	c.uploadMu.Lock()
	state := c.uploads[done.TransferID]
	delete(c.uploads, done.TransferID)
	c.uploadMu.Unlock()
	if state == nil {
		return
	}

	_ = state.file.Close()
	c.sendTransferDone(conn, c.finalizeUpload(state, done))
}

func (c *Client) sendFile(ctx context.Context, conn *websocket.Conn, start protocol.FileTransferStart) {
	defer func() {
		c.downloadMu.Lock()
		if c.downloads[start.TransferID] != nil {
			delete(c.downloads, start.TransferID)
		}
		c.downloadMu.Unlock()
	}()

	file, err := os.Open(start.RemotePath)
	if err != nil {
		c.sendTransferDone(conn, protocol.FileTransferDone{
			TransferID:  start.TransferID,
			AgentID:     c.agentID,
			Direction:   start.Direction,
			Status:      "failed",
			Message:     err.Error(),
			CompletedAt: time.Now().UTC(),
		})
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		c.sendTransferDone(conn, protocol.FileTransferDone{
			TransferID:  start.TransferID,
			AgentID:     c.agentID,
			Direction:   start.Direction,
			Status:      "failed",
			Message:     err.Error(),
			CompletedAt: time.Now().UTC(),
		})
		return
	}

	chunkSize := start.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 256 * 1024
	}

	offset := max(start.Offset, 0)
	if offset > info.Size() {
		offset = info.Size()
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		c.sendTransferDone(conn, protocol.FileTransferDone{
			TransferID:  start.TransferID,
			AgentID:     c.agentID,
			Direction:   start.Direction,
			Status:      "failed",
			Message:     err.Error(),
			CompletedAt: time.Now().UTC(),
		})
		return
	}

	checksum, err := common.FileSHA256(start.RemotePath)
	if err != nil {
		c.sendTransferDone(conn, protocol.FileTransferDone{
			TransferID:  start.TransferID,
			AgentID:     c.agentID,
			Direction:   start.Direction,
			Status:      "failed",
			Message:     err.Error(),
			CompletedAt: time.Now().UTC(),
		})
		return
	}

	if err := c.send(conn, protocol.TypeFileTransferStart, protocol.FileTransferStart{
		TransferID:     start.TransferID,
		AgentID:        c.agentID,
		Direction:      start.Direction,
		LocalPath:      start.LocalPath,
		RemotePath:     start.RemotePath,
		Size:           info.Size(),
		Offset:         offset,
		ChunkSize:      chunkSize,
		ChecksumSHA256: checksum,
		RequestedAt:    time.Now().UTC(),
	}); err != nil {
		c.logger.Warn("send download start", zap.String("transfer_id", start.TransferID), zap.Error(err))
		return
	}

	buf := make([]byte, chunkSize)
	seq := int(offset / int64(chunkSize))
	transferred := offset
	for {
		select {
		case <-ctx.Done():
			// Operator cancel (handleTransferCancel closed our ctx): stop
			// sending and confirm — the server flips the audit row to a
			// terminal "canceled". The partial on the server side stays, so
			// a retry resumes from its offset.
			c.sendTransferDone(conn, protocol.FileTransferDone{
				TransferID:     start.TransferID,
				AgentID:        c.agentID,
				Direction:      start.Direction,
				Status:         "canceled",
				Message:        "canceled by operator",
				Size:           transferred,
				ChecksumSHA256: checksum,
				CompletedAt:    time.Now().UTC(),
			})
			return
		default:
		}
		n, readErr := file.Read(buf)
		if n > 0 {
			if err := c.send(conn, protocol.TypeFileTransferChunk, protocol.FileTransferChunk{
				TransferID: start.TransferID,
				Seq:        seq,
				Data:       buf[:n],
			}); err != nil {
				c.logger.Warn("send download chunk", zap.String("transfer_id", start.TransferID), zap.Error(err))
				return
			}
			transferred += int64(n)
			seq++
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			c.sendTransferDone(conn, protocol.FileTransferDone{
				TransferID:  start.TransferID,
				AgentID:     c.agentID,
				Direction:   start.Direction,
				Status:      "failed",
				Message:     readErr.Error(),
				CompletedAt: time.Now().UTC(),
			})
			return
		}
	}

	c.sendTransferDone(conn, protocol.FileTransferDone{
		TransferID:     start.TransferID,
		AgentID:        c.agentID,
		Direction:      start.Direction,
		Status:         "success",
		Size:           transferred,
		ChecksumSHA256: checksum,
		CompletedAt:    time.Now().UTC(),
	})
}

func (c *Client) sendTransferDone(conn *websocket.Conn, done protocol.FileTransferDone) {
	if err := c.send(conn, protocol.TypeFileTransferDone, done); err != nil {
		c.logger.Warn("send transfer done", zap.String("transfer_id", done.TransferID), zap.Error(err))
	}
}

func (c *Client) failUpload(conn *websocket.Conn, transferID string, state *uploadState, err error) {
	c.uploadMu.Lock()
	if current := c.uploads[transferID]; current == state {
		delete(c.uploads, transferID)
	}
	c.uploadMu.Unlock()
	if state.file != nil {
		_ = state.file.Close()
	}
	// Keep the partial file so a later attempt can resume from it.
	c.sendTransferDone(conn, protocol.FileTransferDone{
		TransferID:  transferID,
		AgentID:     c.agentID,
		Direction:   "upload",
		Status:      "failed",
		Message:     err.Error(),
		Size:        state.received,
		CompletedAt: time.Now().UTC(),
	})
}

func (c *Client) send(conn *websocket.Conn, msgType string, payload any) error {
	opcode := websocket.TextMessage
	var msg []byte
	var err error
	if c.binaryOut.Load() {
		opcode = websocket.BinaryMessage
		msg, err = protocol.MarshalBinaryEnvelope(msgType, payload)
	} else {
		msg, err = protocol.MarshalMessage(msgType, payload)
	}
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// Without a write deadline a peer that stops reading blocks this call
	// forever while holding writeMu — which stalls heartbeats and every task
	// result queued behind them, turning a slow reader into a silent hang.
	_ = conn.SetWriteDeadline(time.Now().Add(agentWriteTimeout))
	return conn.WriteMessage(opcode, msg)
}

func wireName(opcode int) string {
	if opcode == protocol.FrameBinary {
		return "protobuf"
	}
	return "json"
}

func buildClientTLSConfig(cfg Config) (*tls.Config, error) {
	if !strings.HasPrefix(cfg.ServerURL, "wss://") {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: cfg.ServerName,
	}

	if cfg.CACertFile != "" {
		caPEM, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("read ca cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("append ca cert failed")
		}
		tlsCfg.RootCAs = pool
	}

	if cfg.ClientCertFile != "" || cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

func (c *Client) finalizeUpload(state *uploadState, done protocol.FileTransferDone) protocol.FileTransferDone {
	status := protocol.FileTransferDone{
		TransferID:  done.TransferID,
		AgentID:     c.agentID,
		Direction:   done.Direction,
		Status:      "success",
		Size:        state.received,
		CompletedAt: time.Now().UTC(),
	}

	removeTemp := true
	defer func() {
		if removeTemp && state.tempPath != "" {
			_ = os.Remove(state.tempPath)
		}
	}()

	if done.Status != "complete" {
		status.Status = "failed"
		status.Message = done.Message
		return status
	}

	if state.assembler != nil {
		if err := state.assembler.Finish(state.expectedChunks); err != nil {
			status.Status = "failed"
			status.Message = err.Error()
			return status
		}
	}

	checksum, err := common.FileSHA256(state.tempPath)
	if err != nil {
		status.Status = "failed"
		status.Message = err.Error()
		return status
	}
	status.ChecksumSHA256 = checksum
	if done.ChecksumSHA256 != "" && checksum != done.ChecksumSHA256 {
		status.Status = "failed"
		status.Message = "checksum mismatch"
		return status
	}
	if err := os.Rename(state.tempPath, state.remotePath); err != nil {
		status.Status = "failed"
		status.Message = err.Error()
		return status
	}
	removeTemp = false
	return status
}

func (c *Client) pruneResultsLocked(now time.Time) {
	for taskID, cached := range c.results {
		if now.Sub(cached.cachedAt) > cachedResultTTL {
			delete(c.results, taskID)
		}
	}
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		b.truncated = b.truncated || len(p) > 0
		return len(p), nil
	}

	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		if len(p) > remaining {
			_, _ = b.buf.Write(p[:remaining])
			b.truncated = true
			return len(p), nil
		}
		_, _ = b.buf.Write(p)
		return len(p), nil
	}

	b.truncated = true
	return len(p), nil
}

func (b *cappedBuffer) String() string {
	out := b.buf.String()
	if !b.truncated {
		return out
	}
	if out == "" {
		return "[output truncated]"
	}
	return out + "\n[output truncated]"
}
