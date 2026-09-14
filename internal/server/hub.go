// Package server implements the CoC2 server: the HTTP/WebSocket control
// plane, task dispatch, file transfer management, plugin hooks, and SQLite
// persistence.
package server

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"coc2/internal/common"
	"coc2/internal/protocol"
)

type Service struct {
	cfg     Config
	logger  *zap.Logger
	store   *Store
	plugins *PluginManager

	agentEngine   *gin.Engine // /ws/agent + /healthz, token auth via hello
	engine        *gin.Engine // operator plane: CLI/API
	httpSrv       *http.Server
	operatorSrv   *http.Server // optional TCP escape hatch
	operatorUDSrv *http.Server // Unix socket, no token (file mode 0600)
	udsListener   net.Listener

	mu         sync.RWMutex
	clients    map[string]*agentConn
	transferMu sync.RWMutex
	transfers  map[string]*transferState

	reaperStop chan struct{}
	reaperDone chan struct{}
	reaperOnce sync.Once
}

// wsFrame is a transport-frame with the WebSocket opcode it must go out on:
// TextMessage selects legacy JSON, BinaryMessage protobuf.
type wsFrame struct {
	opcode int
	data   []byte
}

type agentConn struct {
	id        string
	meta      protocol.AgentHello
	conn      *websocket.Conn
	send      chan wsFrame
	done      chan struct{}
	service   *Service
	closeOnce sync.Once

	// remoteAddr is the peer address of the underlying TCP connection,
	// captured at upgrade time. The oplog uses it to attribute failed
	// authentications before any agent identity exists.
	remoteAddr string

	// binaryOut flips to true once the hello exchange proves the peer
	// speaks protobuf; reads always accept both framings by opcode.
	binaryOut atomic.Bool
}

type taskRequest struct {
	AgentID     string `json:"agent_id" binding:"required"`
	Command     string `json:"command" binding:"required"`
	TimeoutSecs int    `json:"timeout_secs"`
	Priority    int    `json:"priority"`
}

type batchTaskRequest struct {
	AgentIDs    []string `json:"agent_ids"`
	GroupIDs    []string `json:"group_ids"`
	Tags        []string `json:"tags"`
	Command     string   `json:"command" binding:"required"`
	TimeoutSecs int      `json:"timeout_secs"`
	Priority    int      `json:"priority"`
}

type groupRequest struct {
	ID          string   `json:"id"`
	Name        string   `json:"name" binding:"required"`
	Description string   `json:"description"`
	AgentIDs    []string `json:"agent_ids"`
}

type fileTransferRequest struct {
	AgentID    string `json:"agent_id" binding:"required"`
	LocalPath  string `json:"local_path" binding:"required"`
	RemotePath string `json:"remote_path" binding:"required"`
	ChunkSize  int    `json:"chunk_size"`
}

type taskDispatchResponse struct {
	TaskID     string `json:"task_id"`
	AgentID    string `json:"agent_id"`
	Dispatched bool   `json:"dispatched"`
	QueuedOnly bool   `json:"queued_only"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: checkOrigin,
}

// checkOrigin rejects every WebSocket handshake that carries an Origin
// header. There is no browser UI anymore: legitimate agents send no Origin,
// while any browser-originated request (including fetch with "Origin: null")
// is hostile by definition.
func checkOrigin(r *http.Request) bool {
	return r.Header.Get("Origin") == ""
}

func New(cfg Config, logger *zap.Logger) (*Service, error) {
	// Silence gin's debug banner and per-route startup dump (22 lines to
	// stdout). The operator plane contract is JSON-only. Honors GIN_MODE
	// when the operator explicitly set it.
	if os.Getenv("GIN_MODE") == "" {
		gin.SetMode(gin.ReleaseMode)
	}

	if cfg.ListenAddr == "" {
		return nil, errors.New("listen addr is required")
	}
	if cfg.AuthToken == "" {
		return nil, errors.New("auth token is required")
	}
	if cfg.OperatorUDSPath == "" && cfg.OperatorListen == "" {
		return nil, errors.New("operator plane requires -operator-uds and/or -operator-listen")
	}
	if cfg.APIToken == "" {
		cfg.APIToken = cfg.AuthToken
	}
	if cfg.WriteWait == 0 {
		cfg.WriteWait = 10 * time.Second
	}
	if cfg.PongWait == 0 {
		cfg.PongWait = 70 * time.Second
	}
	if cfg.PingPeriod == 0 {
		cfg.PingPeriod = 25 * time.Second
	}

	svc := &Service{
		cfg:        cfg,
		logger:     logger,
		clients:    make(map[string]*agentConn),
		transfers:  make(map[string]*transferState),
		reaperStop: make(chan struct{}),
		reaperDone: make(chan struct{}),
	}

	store, err := NewStore(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	svc.store = store
	plugins, err := NewPluginManager(cfg.PluginDir, logger)
	if err != nil {
		return nil, err
	}
	svc.plugins = plugins
	svc.agentEngine = svc.agentRoutes()
	svc.engine = svc.operatorRoutes()
	svc.httpSrv = &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: svc.agentEngine,
	}
	if cfg.OperatorListen != "" {
		svc.operatorSrv = &http.Server{
			Addr:    cfg.OperatorListen,
			Handler: svc.engine,
		}
	}

	tlsCfg, err := buildServerTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		svc.httpSrv.TLSConfig = tlsCfg
		if svc.operatorSrv != nil {
			svc.operatorSrv.TLSConfig = tlsCfg
		}
	}
	if cfg.RequireTLS && tlsCfg == nil {
		return nil, errors.New("require_tls is set but no TLS certificate configured (tls_cert/tls_key)")
	}

	return svc, nil
}

func (s *Service) Run() error {
	if err := s.listenOperatorUDS(); err != nil {
		return err
	}

	s.logger.Info("agent plane listening", zap.String("addr", s.cfg.ListenAddr))
	if s.cfg.OperatorUDSPath != "" {
		s.logger.Info("operator plane listening", zap.String("uds", s.cfg.OperatorUDSPath))
	}
	if s.operatorSrv != nil {
		s.logger.Warn("operator plane exposed on TCP; token auth is enforced", zap.String("addr", s.cfg.OperatorListen))
	}

	go s.reapLoop()

	errCh := make(chan error, 3)
	go func() { errCh <- serveAgent(s) }()
	if s.udsListener != nil {
		go func() { errCh <- s.operatorUDSrv.Serve(s.udsListener) }()
	}
	if s.operatorSrv != nil {
		go func() {
			var err error
			if s.operatorSrv.TLSConfig != nil {
				err = s.operatorSrv.ListenAndServeTLS(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
			} else {
				err = s.operatorSrv.ListenAndServe()
			}
			errCh <- err
		}()
	}
	return <-errCh
}

func serveAgent(s *Service) error {
	if s.httpSrv.TLSConfig != nil {
		return s.httpSrv.ListenAndServeTLS(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
	}
	s.logger.Warn("server is running without TLS; agent traffic and tokens are unencrypted. Configure tls_cert/tls_key (and optionally client_ca) for production.")
	return s.httpSrv.ListenAndServe()
}

// listenOperatorUDS prepares the Unix socket for the operator plane. A
// leftover socket file from a previous crashed run is removed first; the
// live bind below then recreates it with 0600.
func (s *Service) listenOperatorUDS() error {
	path := s.cfg.OperatorUDSPath
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err == nil {
		if probe, dialErr := net.DialTimeout("unix", path, 500*time.Millisecond); dialErr == nil {
			probe.Close()
			return fmt.Errorf("operator socket %s is already in use by a live server", path)
		}
		if rmErr := os.Remove(path); rmErr != nil {
			return fmt.Errorf("remove stale operator socket: %w", rmErr)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create operator socket dir: %w", err)
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen operator uds: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return fmt.Errorf("chmod operator socket: %w", err)
	}
	s.udsListener = ln
	s.operatorUDSrv = &http.Server{
		Handler: s.engine,
		// Attach kernel peer credentials (uid, pid on Linux) to every
		// request context so the oplog can name the operator behind a
		// tokenless UDS call. This is the only actor identity available
		// on that plane, and it cannot be spoofed by the client.
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return withPeerCred(ctx, c)
		},
	}
	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.reaperOnce.Do(func() { close(s.reaperStop) })
	select {
	case <-s.reaperDone:
	case <-ctx.Done():
	}
	if s.operatorUDSrv != nil {
		_ = s.operatorUDSrv.Shutdown(ctx)
	}
	if s.operatorSrv != nil {
		_ = s.operatorSrv.Shutdown(ctx)
	}
	err := s.httpSrv.Shutdown(ctx)
	if s.cfg.OperatorUDSPath != "" {
		_ = os.Remove(s.cfg.OperatorUDSPath)
	}
	s.store.Close()
	return err
}

// agentRoutes serves the agent-facing plane: only the WebSocket uplink and
// a liveness probe. Agents authenticate inside the protocol (hello token).
func (s *Service) agentRoutes() *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Recovery())

	engine.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true, "plane": "agent"})
	})
	engine.GET("/ws/agent", s.handleAgentWS)
	return engine
}

// operatorRoutes serves the CLI/API plane. Whether token auth applies is
// decided once per server (see requireOperatorAuth): on the Unix socket the
// filesystem is the boundary, on the TCP escape hatch the token is not
// optional.
func (s *Service) operatorRoutes() *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(s.requireOperatorAuth())
	engine.Use(s.oplogMiddleware())

	engine.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"ok": true, "plane": "operator"})
	})

	engine.GET("/api/v1/agents", func(c *gin.Context) {
		agents, err := s.store.Agents()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, agents)
	})

	engine.GET("/api/v1/agents/:id/metrics", func(c *gin.Context) {
		metrics, ok, err := s.store.AgentMetrics(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "metrics not found"})
			return
		}
		c.JSON(http.StatusOK, metrics)
	})

	engine.GET("/api/v1/agents/:id/metrics/history", func(c *gin.Context) {
		history, err := s.store.AgentMetricsHistory(c.Param("id"), queryLimit(c, 50, 500))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if len(history) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": "no metrics history"})
			return
		}
		c.JSON(http.StatusOK, history)
	})

	engine.GET("/api/v1/metrics/overview", func(c *gin.Context) {
		agents, err := s.store.Agents()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		online := 0
		pending := 0
		for _, agent := range agents {
			if agent.Online {
				online++
			}
			pending += agent.PendingCount
		}
		oplogEntries, err := s.store.OplogCount()
		if err != nil {
			oplogEntries = -1
		}
		c.JSON(http.StatusOK, gin.H{
			"total_agents":     len(agents),
			"online_agents":    online,
			"pending_tasks":    pending,
			"active_transfers": s.activeTransfersCount(),
			"plugins":          len(s.plugins.List()),
			"oplog_entries":    oplogEntries,
		})
	})

	engine.GET("/api/v1/plugins", func(c *gin.Context) {
		c.JSON(http.StatusOK, s.plugins.List())
	})

	engine.GET("/api/v1/groups", func(c *gin.Context) {
		groups, err := s.store.Groups()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, groups)
	})

	engine.GET("/api/v1/groups/:id", func(c *gin.Context) {
		group, ok, err := s.store.Group(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "group not found"})
			return
		}
		c.JSON(http.StatusOK, group)
	})

	engine.GET("/api/v1/tasks", func(c *gin.Context) {
		tasks, err := s.store.RecentTasks(queryLimit(c, 20, 100))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, tasks)
	})

	engine.POST("/api/v1/groups", func(c *gin.Context) {
		var req groupRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		group := Group{
			ID:          req.ID,
			Name:        req.Name,
			Description: req.Description,
			AgentIDs:    req.AgentIDs,
			MemberCount: len(req.AgentIDs),
			CreatedAt:   time.Now().UTC(),
		}
		if group.ID == "" {
			group.ID = common.NewID()
		}
		if err := s.store.CreateOrUpdateGroup(group); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, group)
	})

	engine.GET("/api/v1/tasks/:id", func(c *gin.Context) {
		task, ok, err := s.store.Task(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
			return
		}
		c.JSON(http.StatusOK, task)
	})

	engine.POST("/api/v1/tasks", func(c *gin.Context) {
		var req taskRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if req.TimeoutSecs <= 0 {
			req.TimeoutSecs = 60
		}

		resp, err := s.createTask(req.AgentID, req.Command, req.TimeoutSecs, req.Priority)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, resp)
	})

	engine.POST("/api/v1/tasks/batch", func(c *gin.Context) {
		var req batchTaskRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if req.TimeoutSecs <= 0 {
			req.TimeoutSecs = 60
		}

		targets, err := s.resolveTargetAgentIDs(req.AgentIDs, req.GroupIDs, req.Tags)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		results := make([]taskDispatchResponse, 0, len(targets))
		for _, agentID := range targets {
			resp, err := s.createTask(agentID, req.Command, req.TimeoutSecs, req.Priority)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			results = append(results, resp)
		}

		c.JSON(http.StatusAccepted, gin.H{"count": len(results), "tasks": results})
	})

	engine.POST("/api/v1/tasks/:id/cancel", func(c *gin.Context) {
		task, state, ok, err := s.store.CancelTask(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
			return
		}

		cancelSent := false
		if state == "cancel_requested" {
			cancelSent = s.sendTaskCancel(task) == nil
		}

		c.JSON(http.StatusAccepted, gin.H{
			"task_id":     task.ID,
			"agent_id":    task.AgentID,
			"state":       state,
			"cancel_sent": cancelSent,
		})
	})

	engine.POST("/api/v1/files/upload", func(c *gin.Context) {
		var req fileTransferRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		transfer, err := s.startUpload(req.AgentID, req.LocalPath, req.RemotePath, req.ChunkSize)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, transfer)
	})

	engine.POST("/api/v1/files/download", func(c *gin.Context) {
		var req fileTransferRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		transfer, err := s.startDownload(req.AgentID, req.RemotePath, req.LocalPath, req.ChunkSize)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, transfer)
	})

	engine.GET("/api/v1/transfers/:id", func(c *gin.Context) {
		transfer, ok := s.transferSnapshot(c.Param("id"))
		if !ok {
			audit, ok, err := s.store.TransferAudit(c.Param("id"))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			if !ok {
				c.JSON(http.StatusNotFound, gin.H{"error": "transfer not found"})
				return
			}
			c.JSON(http.StatusOK, audit)
			return
		}
		c.JSON(http.StatusOK, transfer)
	})

	engine.GET("/api/v1/transfers", func(c *gin.Context) {
		transfers, err := s.store.RecentTransferAudits(queryLimit(c, 20, 100))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, transfers)
	})

	engine.POST("/api/v1/transfers/:id/cancel", func(c *gin.Context) {
		snap, state, cancelSent, found := s.cancelTransfer(c.Param("id"))
		if !found {
			c.JSON(http.StatusNotFound, gin.H{"error": "transfer not found"})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{
			"transfer_id": snap.ID,
			"agent_id":    snap.AgentID,
			"direction":   snap.Direction,
			"status":      state,
			"cancel_sent": cancelSent,
		})
	})

	engine.GET("/api/v1/oplog", func(c *gin.Context) {
		since, until, err := parseTimeRange(c)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		entries, err := s.store.RecentOpLogs(OpLogQuery{
			Limit:  queryLimit(c, 50, 500),
			Actor:  c.Query("actor"),
			Agent:  c.Query("agent"),
			Path:   c.Query("path"),
			Ref:    c.Query("ref"),
			Since:  since,
			Until:  until,
			Failed: c.Query("failed") == "1" || c.Query("failed") == "true",
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, entries)
	})

	return engine
}

// parseTimeRange reads optional ?since=&until= RFC3339 (or RFC3339 without
// time part tolerated via time.Parse fallbacks) query parameters.
func parseTimeRange(c *gin.Context) (time.Time, time.Time, error) {
	p := func(name string) (time.Time, error) {
		raw := c.Query(name)
		if raw == "" {
			return time.Time{}, nil
		}
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			// allow date-only form, interpreted as UTC midnight
			t, err = time.Parse("2006-01-02", raw)
			if err != nil {
				return time.Time{}, fmt.Errorf("%s must be RFC3339 or YYYY-MM-DD", name)
			}
			t = t.UTC()
		}
		return t, nil
	}
	since, err := p("since")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	until, err := p("until")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return since, until, nil
}

func (s *Service) handleAgentWS(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		s.logger.Warn("upgrade websocket", zap.Error(err))
		return
	}

	agent := &agentConn{
		conn:       conn,
		remoteAddr: c.Request.RemoteAddr,
		send:       make(chan wsFrame, 16),
		done:       make(chan struct{}),
		service:    s,
	}
	go agent.writeLoop()
	agent.readLoop()
}

func (s *Service) dispatchOrQueue(task protocol.Task) (bool, error) {
	if err := s.store.AddTask(task); err != nil {
		return false, err
	}

	s.mu.RLock()
	client, ok := s.clients[task.AgentID]
	s.mu.RUnlock()

	if ok {
		if err := client.sendTask(task); err == nil {
			if err := s.store.MarkDispatched(task.ID); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) register(client *agentConn, hello protocol.AgentHello) ([]protocol.Task, error) {
	client.id = hello.AgentID
	client.meta = hello

	s.mu.Lock()
	if old := s.clients[hello.AgentID]; old != nil && old != client {
		old.close()
	}
	s.clients[hello.AgentID] = client
	s.mu.Unlock()

	if err := s.store.UpsertAgent(AgentState{
		AgentID:     hello.AgentID,
		Hostname:    hello.Hostname,
		OS:          hello.OS,
		Arch:        hello.Arch,
		Tags:        hello.Tags,
		Fingerprint: hello.Fingerprint,
		Online:      true,
		LastSeenAt:  time.Now().UTC(),
		ConnectedAt: hello.ConnectedAt,
	}); err != nil {
		return nil, err
	}

	return s.store.PendingTasks(hello.AgentID)
}

func (s *Service) touch(agentID string) {
	if err := s.store.SetAgentOnline(agentID, true, time.Now().UTC()); err != nil {
		s.logger.Warn("touch agent", zap.String("agent_id", agentID), zap.Error(err))
	}
}

func (s *Service) unregister(client *agentConn) {
	if client.id == "" {
		client.close()
		return
	}

	s.mu.Lock()
	if current := s.clients[client.id]; current == client {
		delete(s.clients, client.id)
	}
	s.mu.Unlock()

	if err := s.store.SetAgentOnline(client.id, false, time.Now().UTC()); err != nil {
		s.logger.Warn("set agent offline", zap.String("agent_id", client.id), zap.Error(err))
	}
	client.close()
}

func (s *Service) createTask(agentID, command string, timeoutSecs, priority int) (taskDispatchResponse, error) {
	task := protocol.Task{
		ID:          common.NewID(),
		AgentID:     agentID,
		Type:        "shell",
		Command:     command,
		TimeoutSecs: timeoutSecs,
		Priority:    priority,
		CreatedAt:   time.Now().UTC(),
	}

	dispatched, err := s.dispatchOrQueue(task)
	if err != nil {
		return taskDispatchResponse{}, err
	}
	return taskDispatchResponse{
		TaskID:     task.ID,
		AgentID:    task.AgentID,
		Dispatched: dispatched,
		QueuedOnly: !dispatched,
	}, nil
}

func (s *Service) resolveTargetAgentIDs(agentIDs, groupIDs, tags []string) ([]string, error) {
	set := make(map[string]struct{})
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID != "" {
			set[agentID] = struct{}{}
		}
	}

	if len(groupIDs) > 0 {
		groupAgentIDs, err := s.store.ResolveGroupAgentIDs(groupIDs)
		if err != nil {
			return nil, err
		}
		for _, agentID := range groupAgentIDs {
			set[agentID] = struct{}{}
		}
	}

	if len(tags) > 0 {
		agents, err := s.store.Agents()
		if err != nil {
			return nil, err
		}
		for _, agent := range agents {
			if matchAgentTags(agent.Tags, tags) {
				set[agent.AgentID] = struct{}{}
			}
		}
	}

	if len(set) == 0 {
		return nil, errors.New("no target agents")
	}

	targets := make([]string, 0, len(set))
	for agentID := range set {
		targets = append(targets, agentID)
	}
	sort.Strings(targets)
	return targets, nil
}

func (s *Service) sendTaskCancel(task protocol.Task) error {
	client, err := s.clientForAgent(task.AgentID)
	if err != nil {
		return err
	}
	return client.sendMessage(protocol.TypeTaskCancel, protocol.TaskCancel{
		TaskID:      task.ID,
		AgentID:     task.AgentID,
		RequestedAt: time.Now().UTC(),
	})
}

func (s *Service) requireOperatorAuth() gin.HandlerFunc {
	// The Unix socket carries no token: file permission 0600 is the access
	// boundary. Once the TCP escape hatch is enabled, every request on the
	// operator plane must prove the token.
	authRequired := s.cfg.OperatorListen != "" || s.cfg.OperatorUDSPath == ""
	return func(c *gin.Context) {
		if !authRequired || s.authorizedOperator(c.Request) {
			c.Next()
			return
		}
		// Persist the rejection before answering: a rejected request that
		// only lives in stdout is invisible to whoever asks "was someone
		// trying our token?" after the log rotated.
		s.logOperatorAuthFailure(c)
		// No WWW-Authenticate: it exists to trigger browser login prompts,
		// and there is no browser on this plane anymore.
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
	}
}

func (s *Service) authorizedOperator(r *http.Request) bool {
	token := s.cfg.APIToken
	if token == "" {
		return false
	}
	// Two accepted forms: Bearer (what the CLI sends) and Basic (curl -u
	// ergonomics). The old X-Auth-Token custom header had no caller left
	// after the dashboard was removed.
	if auth := strings.TrimSpace(r.Header.Get("Authorization")); auth != "" {
		if user, pass, ok := r.BasicAuth(); ok && user != "" {
			return subtle.ConstantTimeCompare([]byte(pass), []byte(token)) == 1
		}
		if bearer, ok := strings.CutPrefix(auth, "Bearer "); ok {
			return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(bearer)), []byte(token)) == 1
		}
	}
	return false
}

func buildServerTLSConfig(cfg Config) (*tls.Config, error) {
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}
	if cfg.ClientCAFile == "" {
		return tlsCfg, nil
	}

	caPEM, err := os.ReadFile(cfg.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read client ca: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("append client ca failed")
	}

	tlsCfg.ClientCAs = pool
	tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	return tlsCfg, nil
}

func queryLimit(c *gin.Context, fallback, max int) int {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(fallback)))
	if err != nil || limit <= 0 {
		return fallback
	}
	if limit > max {
		return max
	}
	return limit
}
