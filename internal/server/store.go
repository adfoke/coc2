package server

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	_ "modernc.org/sqlite"

	"coc2/internal/protocol"
)

type AgentState struct {
	AgentID      string    `json:"agent_id"`
	Hostname     string    `json:"hostname"`
	OS           string    `json:"os"`
	Arch         string    `json:"arch"`
	Tags         []string  `json:"tags,omitempty"`
	Fingerprint  string    `json:"fingerprint,omitempty"`
	Online       bool      `json:"online"`
	LastSeenAt   time.Time `json:"last_seen_at"`
	ConnectedAt  time.Time `json:"connected_at"`
	PendingCount int       `json:"pending_count"`
}

type Group struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	AgentIDs    []string  `json:"agent_ids,omitempty"`
	MemberCount int       `json:"member_count"`
	CreatedAt   time.Time `json:"created_at"`
}

type TransferAudit struct {
	TransferID       string    `json:"transfer_id"`
	AgentID          string    `json:"agent_id"`
	Direction        string    `json:"direction"`
	LocalPath        string    `json:"local_path,omitempty"`
	RemotePath       string    `json:"remote_path"`
	Status           string    `json:"status"`
	Message          string    `json:"message,omitempty"`
	Size             int64     `json:"size"`
	BytesTransferred int64     `json:"bytes_transferred"`
	ChecksumSHA256   string    `json:"checksum_sha256,omitempty"`
	ChecksumVerified bool      `json:"checksum_verified"`
	CreatedAt        time.Time `json:"created_at"`
	CompletedAt      time.Time `json:"completed_at"`
}

type taskStatus struct {
	Task   protocol.Task        `json:"task"`
	Result *protocol.TaskResult `json:"result,omitempty"`
	State  string               `json:"state"`
}

// taskStatusColumns is the shared SELECT column list for reading a task joined
// with its optional result.
const taskStatusColumns = `
	t.id,
	t.agent_id,
	t.type,
	t.command,
	t.timeout_secs,
	t.priority,
	t.created_at,
	t.state,
	COALESCE(r.task_id, ''),
	COALESCE(r.agent_id, ''),
	COALESCE(r.status, ''),
	COALESCE(r.exit_code, 0),
	COALESCE(r.stdout, ''),
	COALESCE(r.stderr, ''),
	COALESCE(r.duration_ms, 0),
	COALESCE(r.completed_at, '')`

type Store struct {
	db *sql.DB
}

// maxAgentMetricsHistory bounds per-agent metric history so the table cannot
// grow without limit under a periodic heartbeat.
const maxAgentMetricsHistory = 1000

func NewStore(path string) (*Store, error) {
	if path == "" {
		path = "coc2.db"
	}
	if err := ensureParentDir(path); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &Store{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.ResetOnlineAgents(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) init() error {
	stmts := []string{
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA busy_timeout = 5000;`,
		`PRAGMA foreign_keys = ON;`,
		`CREATE TABLE IF NOT EXISTS agents (
			agent_id TEXT PRIMARY KEY,
			hostname TEXT NOT NULL DEFAULT '',
			os TEXT NOT NULL DEFAULT '',
			arch TEXT NOT NULL DEFAULT '',
			tags_json TEXT NOT NULL DEFAULT '[]',
			fingerprint TEXT NOT NULL DEFAULT '',
			online INTEGER NOT NULL DEFAULT 0,
			last_seen_at TEXT,
			connected_at TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS tasks (
			id TEXT PRIMARY KEY,
			agent_id TEXT NOT NULL,
			type TEXT NOT NULL,
			command TEXT NOT NULL,
			timeout_secs INTEGER NOT NULL,
			priority INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			state TEXT NOT NULL,
			dispatched_at TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS task_results (
			task_id TEXT PRIMARY KEY,
			agent_id TEXT NOT NULL,
			status TEXT NOT NULL,
			exit_code INTEGER NOT NULL,
			stdout TEXT NOT NULL DEFAULT '',
			stderr TEXT NOT NULL DEFAULT '',
			duration_ms INTEGER NOT NULL DEFAULT 0,
			completed_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS groups (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			description TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS group_members (
			group_id TEXT NOT NULL,
			agent_id TEXT NOT NULL,
			PRIMARY KEY(group_id, agent_id),
			FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS agent_metrics (
			id INTEGER PRIMARY KEY,
			agent_id TEXT NOT NULL,
			timestamp TEXT NOT NULL,
			uptime_secs INTEGER NOT NULL,
			cpu_count INTEGER NOT NULL,
			goroutines INTEGER NOT NULL,
			process_memory_bytes INTEGER NOT NULL,
			root_disk_total_bytes INTEGER NOT NULL,
			root_disk_free_bytes INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS transfer_audit (
			transfer_id TEXT PRIMARY KEY,
			agent_id TEXT NOT NULL,
			direction TEXT NOT NULL,
			local_path TEXT NOT NULL DEFAULT '',
			remote_path TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			message TEXT NOT NULL DEFAULT '',
			size INTEGER NOT NULL DEFAULT 0,
			bytes_transferred INTEGER NOT NULL DEFAULT 0,
			checksum_sha256 TEXT NOT NULL DEFAULT '',
			checksum_verified INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			completed_at TEXT
		);`,
		// oplog is the operator-plane audit trail: one row per write request
		// (task dispatch, cancel, group mutation, transfer start) plus auth
		// failures. It answers "who did what, from where, and did it work".
		// params_summary is redacted at write time (see oplog.go); actor is
		// resolved from the connection (UDS peer uid / TCP client IP).
		`CREATE TABLE IF NOT EXISTS oplog (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			request_id TEXT NOT NULL,
			ts TEXT NOT NULL,
			plane TEXT NOT NULL,
			actor TEXT NOT NULL,
			uid INTEGER NOT NULL DEFAULT -1,
			pid INTEGER NOT NULL DEFAULT -1,
			source TEXT NOT NULL DEFAULT '',
			method TEXT NOT NULL,
			path TEXT NOT NULL,
			status INTEGER NOT NULL,
			ok INTEGER NOT NULL DEFAULT 0,
			ref TEXT NOT NULL DEFAULT '',
			agents_json TEXT NOT NULL DEFAULT '[]',
			params_summary TEXT NOT NULL DEFAULT ''
		);`,
		`CREATE INDEX IF NOT EXISTS idx_oplog_ts ON oplog(ts DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_agent_state_created ON tasks(agent_id, state, created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_group_members_agent ON group_members(agent_id);`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_audit_agent_created ON transfer_audit(agent_id, created_at);`,
	}

	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("init sqlite: %w", err)
		}
	}
	if err := s.migrateAgentMetrics(); err != nil {
		return fmt.Errorf("migrate agent metrics: %w", err)
	}
	if err := s.migrateTasks(); err != nil {
		return fmt.Errorf("migrate tasks: %w", err)
	}
	return nil
}

// migrateAgentMetrics upgrades the legacy agent_metrics table (one row per
// agent, agent_id as primary key) to an append-only time series, preserving
// any existing samples.
func (s *Store) migrateAgentMetrics() error {
	hasID, err := s.tableHasColumn("agent_metrics", "id")
	if err != nil {
		return err
	}

	if !hasID {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		stmts := []string{
			`ALTER TABLE agent_metrics RENAME TO agent_metrics_legacy;`,
			`CREATE TABLE agent_metrics (
				id INTEGER PRIMARY KEY,
				agent_id TEXT NOT NULL,
				timestamp TEXT NOT NULL,
				uptime_secs INTEGER NOT NULL,
				cpu_count INTEGER NOT NULL,
				goroutines INTEGER NOT NULL,
				process_memory_bytes INTEGER NOT NULL,
				root_disk_total_bytes INTEGER NOT NULL,
				root_disk_free_bytes INTEGER NOT NULL
			);`,
			`INSERT INTO agent_metrics(agent_id, timestamp, uptime_secs, cpu_count, goroutines, process_memory_bytes, root_disk_total_bytes, root_disk_free_bytes)
				SELECT agent_id, timestamp, uptime_secs, cpu_count, goroutines, process_memory_bytes, root_disk_total_bytes, root_disk_free_bytes
				FROM agent_metrics_legacy;`,
			`DROP TABLE agent_metrics_legacy;`,
		}
		for _, stmt := range stmts {
			if _, err := tx.Exec(stmt); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_agent_metrics_agent_time ON agent_metrics(agent_id, timestamp);`)
	return err
}

// migrateTasks adds the dispatched_at column to legacy databases and backfills
// it for any tasks that were already dispatched under the old schema, so the
// task reaper has a timestamp to work from.
func (s *Store) migrateTasks() error {
	has, err := s.tableHasColumn("tasks", "dispatched_at")
	if err != nil {
		return err
	}
	if !has {
		if _, err := s.db.Exec(`ALTER TABLE tasks ADD COLUMN dispatched_at TEXT`); err != nil {
			return err
		}
	}

	_, err = s.db.Exec(`
		UPDATE tasks SET dispatched_at = created_at
		WHERE state IN ('dispatched', 'cancel_requested') AND dispatched_at IS NULL
	`)
	return err
}

func (s *Store) tableHasColumn(table, column string) (bool, error) {
	rows, err := s.db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (s *Store) ResetOnlineAgents() error {
	_, err := s.db.Exec(`UPDATE agents SET online = 0`)
	return err
}

func (s *Store) UpsertAgent(state AgentState) error {
	tagsJSON, err := json.Marshal(state.Tags)
	if err != nil {
		return fmt.Errorf("marshal tags: %w", err)
	}

	_, err = s.db.Exec(`
		INSERT INTO agents(agent_id, hostname, os, arch, tags_json, fingerprint, online, last_seen_at, connected_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET
			hostname = excluded.hostname,
			os = excluded.os,
			arch = excluded.arch,
			tags_json = excluded.tags_json,
			fingerprint = excluded.fingerprint,
			online = excluded.online,
			last_seen_at = excluded.last_seen_at,
			connected_at = excluded.connected_at
	`,
		state.AgentID,
		state.Hostname,
		state.OS,
		state.Arch,
		string(tagsJSON),
		state.Fingerprint,
		boolToInt(state.Online),
		formatNullTime(state.LastSeenAt),
		formatNullTime(state.ConnectedAt),
	)
	return err
}

func (s *Store) SetAgentOnline(agentID string, online bool, seenAt time.Time) error {
	_, err := s.db.Exec(`
		INSERT INTO agents(agent_id, online, last_seen_at)
		VALUES(?, ?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET
			online = excluded.online,
			last_seen_at = excluded.last_seen_at
	`, agentID, boolToInt(online), formatNullTime(seenAt))
	return err
}

func (s *Store) Agents() ([]AgentState, error) {
	rows, err := s.db.Query(`
		SELECT
			a.agent_id,
			a.hostname,
			a.os,
			a.arch,
			a.tags_json,
			a.fingerprint,
			a.online,
			COALESCE(a.last_seen_at, ''),
			COALESCE(a.connected_at, ''),
			COALESCE(SUM(CASE WHEN t.state = 'queued' THEN 1 ELSE 0 END), 0) AS pending_count
		FROM agents a
		LEFT JOIN tasks t ON t.agent_id = a.agent_id
		GROUP BY a.agent_id, a.hostname, a.os, a.arch, a.tags_json, a.fingerprint, a.online, a.last_seen_at, a.connected_at
		ORDER BY a.agent_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]AgentState, 0, 8)
	for rows.Next() {
		var (
			item        AgentState
			tagsJSON    string
			onlineInt   int
			lastSeenRaw string
			connRaw     string
		)
		if err := rows.Scan(
			&item.AgentID,
			&item.Hostname,
			&item.OS,
			&item.Arch,
			&tagsJSON,
			&item.Fingerprint,
			&onlineInt,
			&lastSeenRaw,
			&connRaw,
			&item.PendingCount,
		); err != nil {
			return nil, err
		}
		item.Online = onlineInt == 1
		item.Tags = decodeTags(tagsJSON)
		item.LastSeenAt = parseNullTime(lastSeenRaw)
		item.ConnectedAt = parseNullTime(connRaw)
		items = append(items, item)
	}

	return items, rows.Err()
}

func (s *Store) AddTask(task protocol.Task) error {
	_, err := s.db.Exec(`
		INSERT INTO tasks(id, agent_id, type, command, timeout_secs, priority, created_at, state)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			agent_id = excluded.agent_id,
			type = excluded.type,
			command = excluded.command,
			timeout_secs = excluded.timeout_secs,
			priority = excluded.priority,
			created_at = excluded.created_at,
			state = excluded.state,
			dispatched_at = NULL
	`, task.ID, task.AgentID, task.Type, task.Command, task.TimeoutSecs, task.Priority, task.CreatedAt.UTC().Format(time.RFC3339Nano), "queued")
	return err
}

func (s *Store) PendingTasks(agentID string) ([]protocol.Task, error) {
	rows, err := s.db.Query(`
			SELECT id, agent_id, type, command, timeout_secs, priority, created_at
			FROM tasks
			WHERE agent_id = ? AND state = 'queued'
			ORDER BY priority DESC, created_at ASC
		`, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []protocol.Task
	for rows.Next() {
		var (
			item       protocol.Task
			createdRaw string
		)
		if err := rows.Scan(&item.ID, &item.AgentID, &item.Type, &item.Command, &item.TimeoutSecs, &item.Priority, &createdRaw); err != nil {
			return nil, err
		}
		item.CreatedAt = parseNullTime(createdRaw)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) MarkDispatched(taskID string) error {
	_, err := s.db.Exec(
		`UPDATE tasks SET state = 'dispatched', dispatched_at = ? WHERE id = ? AND state = 'queued'`,
		time.Now().UTC().Format(time.RFC3339Nano), taskID,
	)
	return err
}

// dispatchedTask is a task that has been sent to an agent but not yet
// finalized, along with when it was dispatched.
type dispatchedTask struct {
	Task         protocol.Task
	State        string
	DispatchedAt time.Time
}

// DispatchedTasks returns tasks in 'dispatched' or 'cancel_requested' state,
// which the reaper may need to finalize.
func (s *Store) DispatchedTasks() ([]dispatchedTask, error) {
	rows, err := s.db.Query(`
		SELECT id, agent_id, type, command, timeout_secs, priority, created_at, state, COALESCE(dispatched_at, '')
		FROM tasks
		WHERE state IN ('dispatched', 'cancel_requested')
		ORDER BY dispatched_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []dispatchedTask
	for rows.Next() {
		var (
			item          dispatchedTask
			createdRaw    string
			dispatchedRaw string
		)
		if err := rows.Scan(
			&item.Task.ID,
			&item.Task.AgentID,
			&item.Task.Type,
			&item.Task.Command,
			&item.Task.TimeoutSecs,
			&item.Task.Priority,
			&createdRaw,
			&item.State,
			&dispatchedRaw,
		); err != nil {
			return nil, err
		}
		item.Task.CreatedAt = parseNullTime(createdRaw)
		item.DispatchedAt = parseNullTime(dispatchedRaw)
		items = append(items, item)
	}
	return items, rows.Err()
}

// MarkTaskTimedOut marks a still-dispatched task as timed out and records a
// synthetic result, returning true if the task was finalized by this call.
func (s *Store) MarkTaskTimedOut(taskID, agentID string, at time.Time) (bool, error) {
	return s.markTaskFinal(taskID, agentID, "timeout", "no result received before timeout", at, "dispatched")
}

// MarkTaskCanceledAfterReap marks a still-cancel_requested task as canceled and
// records a synthetic result, returning true if the task was finalized.
func (s *Store) MarkTaskCanceledAfterReap(taskID, agentID string, at time.Time) (bool, error) {
	return s.markTaskFinal(taskID, agentID, "canceled", "canceled before a result was received", at, "cancel_requested")
}

func (s *Store) markTaskFinal(taskID, agentID, status, stderr string, at time.Time, fromState string) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	res, err := tx.Exec(`UPDATE tasks SET state = ? WHERE id = ? AND state = ?`, status, taskID, fromState)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}

	if _, err := tx.Exec(`
		INSERT INTO task_results(task_id, agent_id, status, exit_code, stdout, stderr, duration_ms, completed_at)
		VALUES(?, ?, ?, 0, '', ?, 0, ?)
		ON CONFLICT(task_id) DO UPDATE SET
			status = excluded.status,
			stderr = excluded.stderr,
			completed_at = excluded.completed_at
	`, taskID, agentID, status, stderr, at.UTC().Format(time.RFC3339Nano)); err != nil {
		return false, err
	}

	return true, tx.Commit()
}

// terminalTaskStates lists the states a task can never leave, as a bare SQL
// list (callers wrap it in parentheses). Used both by SaveResult (a late
// result must not rewrite a finalized task) and by the retention pruner.
const terminalTaskStates = `'success','failed','timeout','canceled'`

// nonTerminalTaskStates lists the states from which a result may be applied.
// 'cancel_requested' is included deliberately: a cancel is only an intent
// until the agent confirms, so a racing completion still records what really
// happened (same contract as transfers; see cancelTransfer in transfer.go).
// 'queued' is included because a dispatch mark and the agent's result are not
// ordered by the database — the result must not be lost to that race.
const nonTerminalTaskStates = `('queued','dispatched','cancel_requested')`

// SaveResult records a task result reported by an agent. The write is
// conditional on the task still being non-terminal AND belonging to the
// reporting agent, so a forged or late result can neither invent a task nor
// overwrite a finalized outcome. It returns applied=false when the task was
// already terminal, belonged to a different agent, or does not exist.
func (s *Store) SaveResult(result protocol.TaskResult) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	res, err := tx.Exec(`
		UPDATE tasks SET state = ?
		WHERE id = ? AND agent_id = ? AND state IN `+nonTerminalTaskStates,
		result.Status, result.TaskID, result.AgentID,
	)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 0 {
		return false, nil
	}

	if _, err := tx.Exec(`
		INSERT INTO task_results(task_id, agent_id, status, exit_code, stdout, stderr, duration_ms, completed_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(task_id) DO UPDATE SET
			agent_id = excluded.agent_id,
			status = excluded.status,
			exit_code = excluded.exit_code,
			stdout = excluded.stdout,
			stderr = excluded.stderr,
			duration_ms = excluded.duration_ms,
			completed_at = excluded.completed_at
	`, result.TaskID, result.AgentID, result.Status, result.ExitCode, result.Stdout, result.Stderr, result.DurationMS, result.CompletedAt.UTC().Format(time.RFC3339Nano)); err != nil {
		return false, err
	}

	return true, tx.Commit()
}

func (s *Store) Task(taskID string) (taskStatus, bool, error) {
	row := s.db.QueryRow(`
		SELECT `+taskStatusColumns+`
		FROM tasks t
		LEFT JOIN task_results r ON r.task_id = t.id
		WHERE t.id = ?
	`, taskID)

	item, err := scanTaskStatus(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return taskStatus{}, false, nil
		}
		return taskStatus{}, false, err
	}
	return item, true, nil
}

func (s *Store) RecentTasks(limit int) ([]taskStatus, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.db.Query(`
		SELECT `+taskStatusColumns+`
		FROM tasks t
		LEFT JOIN task_results r ON r.task_id = t.id
		ORDER BY t.created_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]taskStatus, 0, 8)
	for rows.Next() {
		item, err := scanTaskStatus(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) SaveAgentMetrics(metrics protocol.MetricsReport) error {
	if _, err := s.db.Exec(`
		INSERT INTO agent_metrics(agent_id, timestamp, uptime_secs, cpu_count, goroutines, process_memory_bytes, root_disk_total_bytes, root_disk_free_bytes)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
	`, metrics.AgentID, metrics.Timestamp.UTC().Format(time.RFC3339Nano), metrics.UptimeSecs, metrics.CPUCount, metrics.Goroutines, metrics.ProcessMemoryBytes, metrics.RootDiskTotalBytes, metrics.RootDiskFreeBytes); err != nil {
		return err
	}
	return s.pruneAgentMetrics(metrics.AgentID)
}

func (s *Store) pruneAgentMetrics(agentID string) error {
	_, err := s.db.Exec(`
		DELETE FROM agent_metrics
		WHERE agent_id = ?
		AND id NOT IN (
			SELECT id FROM agent_metrics WHERE agent_id = ?
			ORDER BY timestamp DESC, id DESC
			LIMIT ?
		)
	`, agentID, agentID, maxAgentMetricsHistory)
	return err
}

func (s *Store) AgentMetrics(agentID string) (protocol.MetricsReport, bool, error) {
	row := s.db.QueryRow(`
		SELECT agent_id, timestamp, uptime_secs, cpu_count, goroutines, process_memory_bytes, root_disk_total_bytes, root_disk_free_bytes
		FROM agent_metrics
		WHERE agent_id = ?
		ORDER BY timestamp DESC, id DESC
		LIMIT 1
	`, agentID)

	var metrics protocol.MetricsReport
	var ts string
	if err := row.Scan(&metrics.AgentID, &ts, &metrics.UptimeSecs, &metrics.CPUCount, &metrics.Goroutines, &metrics.ProcessMemoryBytes, &metrics.RootDiskTotalBytes, &metrics.RootDiskFreeBytes); err != nil {
		if err == sql.ErrNoRows {
			return protocol.MetricsReport{}, false, nil
		}
		return protocol.MetricsReport{}, false, err
	}
	metrics.Timestamp = parseNullTime(ts)
	return metrics, true, nil
}

// AgentMetricsHistory returns up to limit recent metric samples for an agent,
// newest first.
func (s *Store) AgentMetricsHistory(agentID string, limit int) ([]protocol.MetricsReport, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	rows, err := s.db.Query(`
		SELECT agent_id, timestamp, uptime_secs, cpu_count, goroutines, process_memory_bytes, root_disk_total_bytes, root_disk_free_bytes
		FROM agent_metrics
		WHERE agent_id = ?
		ORDER BY timestamp DESC, id DESC
		LIMIT ?
	`, agentID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]protocol.MetricsReport, 0, 8)
	for rows.Next() {
		var m protocol.MetricsReport
		var ts string
		if err := rows.Scan(&m.AgentID, &ts, &m.UptimeSecs, &m.CPUCount, &m.Goroutines, &m.ProcessMemoryBytes, &m.RootDiskTotalBytes, &m.RootDiskFreeBytes); err != nil {
			return nil, err
		}
		m.Timestamp = parseNullTime(ts)
		items = append(items, m)
	}
	return items, rows.Err()
}

func (s *Store) UpsertTransferAudit(audit TransferAudit) error {
	_, err := s.db.Exec(`
		INSERT INTO transfer_audit(transfer_id, agent_id, direction, local_path, remote_path, status, message, size, bytes_transferred, checksum_sha256, checksum_verified, created_at, completed_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(transfer_id) DO UPDATE SET
			agent_id = excluded.agent_id,
			direction = excluded.direction,
			local_path = excluded.local_path,
			remote_path = excluded.remote_path,
			status = excluded.status,
			message = excluded.message,
			size = excluded.size,
			bytes_transferred = excluded.bytes_transferred,
			checksum_sha256 = excluded.checksum_sha256,
			checksum_verified = excluded.checksum_verified,
			created_at = excluded.created_at,
			completed_at = excluded.completed_at
	`, audit.TransferID, audit.AgentID, audit.Direction, audit.LocalPath, audit.RemotePath, audit.Status, audit.Message, audit.Size, audit.BytesTransferred, audit.ChecksumSHA256, boolToInt(audit.ChecksumVerified), audit.CreatedAt.UTC().Format(time.RFC3339Nano), formatNullTime(audit.CompletedAt))
	return err
}

func (s *Store) TransferAudit(transferID string) (TransferAudit, bool, error) {
	row := s.db.QueryRow(`
		SELECT transfer_id, agent_id, direction, local_path, remote_path, status, message, size, bytes_transferred, checksum_sha256, checksum_verified, created_at, COALESCE(completed_at, '')
		FROM transfer_audit
		WHERE transfer_id = ?
	`, transferID)

	var (
		audit      TransferAudit
		verified   int
		createdRaw string
		doneRaw    string
	)
	if err := row.Scan(&audit.TransferID, &audit.AgentID, &audit.Direction, &audit.LocalPath, &audit.RemotePath, &audit.Status, &audit.Message, &audit.Size, &audit.BytesTransferred, &audit.ChecksumSHA256, &verified, &createdRaw, &doneRaw); err != nil {
		if err == sql.ErrNoRows {
			return TransferAudit{}, false, nil
		}
		return TransferAudit{}, false, err
	}
	audit.ChecksumVerified = verified == 1
	audit.CreatedAt = parseNullTime(createdRaw)
	audit.CompletedAt = parseNullTime(doneRaw)
	return audit, true, nil
}

func (s *Store) RecentTransferAudits(limit int) ([]TransferAudit, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.db.Query(`
		SELECT transfer_id, agent_id, direction, local_path, remote_path, status, message, size, bytes_transferred, checksum_sha256, checksum_verified, created_at, COALESCE(completed_at, '')
		FROM transfer_audit
		ORDER BY created_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]TransferAudit, 0, 8)
	for rows.Next() {
		var (
			audit      TransferAudit
			verified   int
			createdRaw string
			doneRaw    string
		)
		if err := rows.Scan(&audit.TransferID, &audit.AgentID, &audit.Direction, &audit.LocalPath, &audit.RemotePath, &audit.Status, &audit.Message, &audit.Size, &audit.BytesTransferred, &audit.ChecksumSHA256, &verified, &createdRaw, &doneRaw); err != nil {
			return nil, err
		}
		audit.ChecksumVerified = verified == 1
		audit.CreatedAt = parseNullTime(createdRaw)
		audit.CompletedAt = parseNullTime(doneRaw)
		items = append(items, audit)
	}
	return items, rows.Err()
}

func (s *Store) CreateOrUpdateGroup(group Group) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`
		INSERT INTO groups(id, name, description, created_at)
		VALUES(?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			description = excluded.description
	`, group.ID, group.Name, group.Description, group.CreatedAt.UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM group_members WHERE group_id = ?`, group.ID); err != nil {
		return err
	}
	for _, agentID := range group.AgentIDs {
		if _, err := tx.Exec(`INSERT INTO group_members(group_id, agent_id) VALUES(?, ?)`, group.ID, agentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Groups() ([]Group, error) {
	rows, err := s.db.Query(`
		SELECT g.id, g.name, g.description, g.created_at, COUNT(m.agent_id)
		FROM groups g
		LEFT JOIN group_members m ON m.group_id = g.id
		GROUP BY g.id, g.name, g.description, g.created_at
		ORDER BY g.name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]Group, 0, 8)
	for rows.Next() {
		var item Group
		var createdRaw string
		if err := rows.Scan(&item.ID, &item.Name, &item.Description, &createdRaw, &item.MemberCount); err != nil {
			return nil, err
		}
		item.CreatedAt = parseNullTime(createdRaw)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) Group(groupID string) (Group, bool, error) {
	row := s.db.QueryRow(`SELECT id, name, description, created_at FROM groups WHERE id = ?`, groupID)

	var group Group
	var createdRaw string
	if err := row.Scan(&group.ID, &group.Name, &group.Description, &createdRaw); err != nil {
		if err == sql.ErrNoRows {
			return Group{}, false, nil
		}
		return Group{}, false, err
	}
	group.CreatedAt = parseNullTime(createdRaw)

	rows, err := s.db.Query(`SELECT agent_id FROM group_members WHERE group_id = ? ORDER BY agent_id`, groupID)
	if err != nil {
		return Group{}, false, err
	}
	defer rows.Close()

	for rows.Next() {
		var agentID string
		if err := rows.Scan(&agentID); err != nil {
			return Group{}, false, err
		}
		group.AgentIDs = append(group.AgentIDs, agentID)
	}
	group.MemberCount = len(group.AgentIDs)
	return group, true, rows.Err()
}

func (s *Store) ResolveGroupAgentIDs(groupIDs []string) ([]string, error) {
	if len(groupIDs) == 0 {
		return nil, nil
	}

	set := make(map[string]struct{})
	for _, groupID := range groupIDs {
		rows, err := s.db.Query(`SELECT agent_id FROM group_members WHERE group_id = ?`, groupID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var agentID string
			if err := rows.Scan(&agentID); err != nil {
				rows.Close()
				return nil, err
			}
			set[agentID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	agentIDs := make([]string, 0, len(set))
	for agentID := range set {
		agentIDs = append(agentIDs, agentID)
	}
	return agentIDs, nil
}

func (s *Store) CancelTask(taskID string) (protocol.Task, string, bool, error) {
	task, state, ok, err := s.taskRow(taskID)
	if err != nil || !ok {
		return protocol.Task{}, "", ok, err
	}

	switch state {
	case "success", "failed", "timeout", "canceled":
		return task, state, true, nil
	case "queued":
		now := time.Now().UTC()
		tx, err := s.db.Begin()
		if err != nil {
			return protocol.Task{}, "", false, err
		}
		defer tx.Rollback()

		if _, err := tx.Exec(`UPDATE tasks SET state = 'canceled' WHERE id = ?`, taskID); err != nil {
			return protocol.Task{}, "", false, err
		}
		if _, err := tx.Exec(`
			INSERT INTO task_results(task_id, agent_id, status, exit_code, stdout, stderr, duration_ms, completed_at)
			VALUES(?, ?, 'canceled', 0, '', 'canceled before dispatch', 0, ?)
			ON CONFLICT(task_id) DO UPDATE SET
				status = excluded.status,
				stderr = excluded.stderr,
				completed_at = excluded.completed_at
		`, taskID, task.AgentID, now.Format(time.RFC3339Nano)); err != nil {
			return protocol.Task{}, "", false, err
		}
		if err := tx.Commit(); err != nil {
			return protocol.Task{}, "", false, err
		}
		return task, "canceled", true, nil
	default:
		_, err := s.db.Exec(`UPDATE tasks SET state = 'cancel_requested' WHERE id = ?`, taskID)
		return task, "cancel_requested", true, err
	}
}

func (s *Store) taskRow(taskID string) (protocol.Task, string, bool, error) {
	row := s.db.QueryRow(`
		SELECT id, agent_id, type, command, timeout_secs, priority, created_at, state
		FROM tasks
		WHERE id = ?
	`, taskID)

	var task protocol.Task
	var createdRaw string
	var state string
	if err := row.Scan(&task.ID, &task.AgentID, &task.Type, &task.Command, &task.TimeoutSecs, &task.Priority, &createdRaw, &state); err != nil {
		if err == sql.ErrNoRows {
			return protocol.Task{}, "", false, nil
		}
		return protocol.Task{}, "", false, err
	}
	task.CreatedAt = parseNullTime(createdRaw)
	return task, state, true, nil
}

func scanTaskStatus(scanner interface {
	Scan(dest ...any) error
}) (taskStatus, error) {
	var (
		item          taskStatus
		createdRaw    string
		resultTaskID  string
		resultAgentID string
		resultStatus  string
		resultExit    int
		resultStdout  string
		resultStderr  string
		resultDurMS   int64
		completedRaw  string
	)
	if err := scanner.Scan(
		&item.Task.ID,
		&item.Task.AgentID,
		&item.Task.Type,
		&item.Task.Command,
		&item.Task.TimeoutSecs,
		&item.Task.Priority,
		&createdRaw,
		&item.State,
		&resultTaskID,
		&resultAgentID,
		&resultStatus,
		&resultExit,
		&resultStdout,
		&resultStderr,
		&resultDurMS,
		&completedRaw,
	); err != nil {
		return taskStatus{}, err
	}

	item.Task.CreatedAt = parseNullTime(createdRaw)
	if resultTaskID != "" {
		item.Result = &protocol.TaskResult{
			TaskID:      resultTaskID,
			AgentID:     resultAgentID,
			Status:      resultStatus,
			ExitCode:    resultExit,
			Stdout:      resultStdout,
			Stderr:      resultStderr,
			DurationMS:  resultDurMS,
			CompletedAt: parseNullTime(completedRaw),
		}
	}
	return item, nil
}

func matchAgentTags(agentTags, filterTags []string) bool {
	if len(filterTags) == 0 {
		return true
	}
	for _, tag := range agentTags {
		if slices.Contains(filterTags, tag) {
			return true
		}
	}
	return false
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" || path == ":memory:" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func decodeTags(raw string) []string {
	if raw == "" {
		return nil
	}
	var tags []string
	if err := json.Unmarshal([]byte(raw), &tags); err != nil {
		return nil
	}
	return tags
}

func parseNullTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return t
}

func formatNullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
