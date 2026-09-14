package server

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"coc2/internal/common"
)

// OpLogEntry is one row of the operator-plane audit trail: who performed a
// write operation, from which connection, against which endpoint, and what
// the server decided. Only operations that can change fleet state are
// recorded (dispatch, cancel, group mutation, transfers), plus authentication
// failures on both planes. Plain reads (GET/HEAD) are not operations.
type OpLogEntry struct {
	ID            int64     `json:"id"`
	RequestID     string    `json:"request_id"`
	TS            time.Time `json:"ts"`
	Plane         string    `json:"plane"` // "uds" | "tcp" | "agent"
	Actor         string    `json:"actor"` // unix username (uds) / auth identity (tcp) / claimed agent id (agent)
	UID           int       `json:"uid"`   // -1 when unknown/not applicable
	PID           int       `json:"pid"`   // peer process id, -1 when unknown
	Source        string    `json:"source"`
	Method        string    `json:"method"`
	Path          string    `json:"path"`
	Status        int       `json:"status"`
	OK            bool      `json:"ok"`
	Ref           string    `json:"ref,omitempty"` // ids created/ mutated by the op: task/transfer/group
	Agents        []string  `json:"agents,omitempty"`
	ParamsSummary string    `json:"params_summary,omitempty"`
}

const (
	oplogMaxRequestBytes = 64 << 10 // read-cap for request bodies to summarize
	oplogMaxSummaryChars = 512
	oplogMaxRefChars     = 256
	oplogMaxAgents       = 64
	oplogMaxBodyCapture  = 8 << 10 // response bytes parsed for refs
)

// oplogBodyKeys are the JSON keys whose values are safe and useful to keep in
// params_summary. Anything else is recorded as a bare key name. "command" is
// operator-entered input and the primary thing an investigator looks for, so
// it is kept (truncated at the summary cap). No credential-bearing key lives
// here: tokens travel in headers, which are never summarized.
var oplogBodyKeys = map[string]bool{
	"command":      true,
	"agent_id":     true,
	"agent_ids":    true,
	"group_ids":    true,
	"tags":         true,
	"name":         true,
	"description":  true,
	"local_path":   true,
	"remote_path":  true,
	"timeout_secs": true,
	"priority":     true,
	"chunk_size":   true,
}

// oplogRefKeys are response JSON keys that name a resource an operation
// created or mutated.
var oplogRefKeys = map[string]bool{
	"task_id":     true,
	"transfer_id": true,
	"id":          true,
}

type oplogActor struct {
	plane    string
	identity string
	uid      int
	pid      int
	source   string
}

// usernameCache resolves uid -> login name once; the mapping cannot change
// meaningfully within one server process.
var (
	usernameMu    sync.Mutex
	usernameCache = map[int]string{}
)

func resolveUsername(uid int) string {
	if uid < 0 {
		return ""
	}
	usernameMu.Lock()
	defer usernameMu.Unlock()
	if name, ok := usernameCache[uid]; ok {
		return name
	}
	name := ""
	if u, err := user.LookupId(strconv.Itoa(uid)); err == nil {
		name = u.Username
	}
	usernameCache[uid] = name
	return name
}

func actorDisplay(a oplogActor) string {
	if a.plane == "uds" {
		if name := resolveUsername(a.uid); name != "" {
			return name
		}
		return "uid=" + strconv.Itoa(a.uid)
	}
	return a.identity
}

// identifyOperator builds the actor record for an operator-plane request. On
// the UDS the peer uid/pid come from the connection credentials attached by
// ConnContext; on TCP the identity is the shared token (plus the basic-auth
// username when the client sent one) and the source is the peer address.
func identifyOperator(c *gin.Context, peer *peerID, udsPath string) oplogActor {
	if peer != nil {
		// An unbound unix client has an empty RemoteAddr, so the socket
		// path is the meaningful "where" on this plane — the uid/pid on
		// the row already identify who.
		source := udsPath
		if source == "" {
			source = c.Request.RemoteAddr
		}
		return oplogActor{
			plane:  "uds",
			uid:    peer.uid,
			pid:    peer.pid,
			source: source,
		}
	}
	a := oplogActor{plane: "tcp", uid: -1, pid: -1, source: c.Request.RemoteAddr, identity: "token"}
	// BasicAuth returns (username, password, ok): take ONLY the username.
	// The password IS the shared API token and must never reach the audit
	// log — recording it would turn the oplog into a credential store.
	if name, _, ok := c.Request.BasicAuth(); ok && name != "" {
		a.identity = "token:" + name
	}
	return a
}

// statusRecorder captures a bounded copy of the response body so the audit
// row can name the resources an operation created. Status is read back from
// gin's own tracking via the embedded ResponseWriter.Status(), never
// re-derived here, so a 202 dispatch stays 202 instead of collapsing to a
// default 200.
type statusRecorder struct {
	gin.ResponseWriter
	body bytes.Buffer
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.body.Len() < oplogMaxBodyCapture {
		if remaining := oplogMaxBodyCapture - r.body.Len(); len(b) > remaining {
			r.body.Write(b[:remaining])
		} else {
			r.body.Write(b)
		}
	}
	return r.ResponseWriter.Write(b)
}

// oplogMiddleware records every authenticated write request (POST/PUT/PATCH/
// DELETE) into the oplog table. GET/HEAD requests are reads and are not
// logged. It runs after requireOperatorAuth, so 401 rejections never reach it
// (they are logged by logOperatorAuthFailure instead).
func (s *Service) oplogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == http.MethodGet || c.Request.Method == http.MethodHead ||
			c.Request.URL.Path == "/healthz" {
			c.Next()
			return
		}

		reqID := common.NewID()
		c.Header("X-Request-Id", reqID)

		// Read the body to summarize, then hand it back to the handler
		// intact. The summarize cap must never shrink what the handler
		// sees, so anything past the cap is still buffered in full.
		var bodyBytes []byte
		if c.Request.Body != nil {
			head, _ := io.ReadAll(io.LimitReader(c.Request.Body, oplogMaxRequestBytes))
			tail, _ := io.ReadAll(c.Request.Body)
			bodyBytes = append(head, tail...)
			c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}

		peer := peerFromContext(c.Request.Context())
		rec := &statusRecorder{ResponseWriter: c.Writer}
		c.Writer = rec

		c.Next()

		status := rec.Status()
		a := identifyOperator(c, peer, s.cfg.OperatorUDSPath)
		entry := OpLogEntry{
			RequestID:     reqID,
			TS:            time.Now().UTC(),
			Plane:         a.plane,
			Actor:         actorDisplay(a),
			UID:           a.uid,
			PID:           a.pid,
			Source:        a.source,
			Method:        c.Request.Method,
			Path:          c.Request.URL.Path,
			Status:        status,
			OK:            status < http.StatusBadRequest,
			ParamsSummary: summarizeRequestBody(bodyBytes),
		}
		entry.Agents = extractAgents(bodyBytes, rec.body.Bytes())
		entry.Ref = extractRefs(rec.body.Bytes())

		s.appendOpLog(entry)
	}
}

// logOperatorAuthFailure persists a rejected request on the operator plane
// before the 401 is written: failed auth is exactly the event an operator
// needs to be able to find after the fact.
func (s *Service) logOperatorAuthFailure(c *gin.Context) {
	peer := peerFromContext(c.Request.Context())
	a := identifyOperator(c, peer, s.cfg.OperatorUDSPath)
	s.appendOpLog(OpLogEntry{
		RequestID:     common.NewID(),
		TS:            time.Now().UTC(),
		Plane:         a.plane,
		Actor:         "unauthorized",
		UID:           a.uid,
		PID:           a.pid,
		Source:        a.source,
		Method:        c.Request.Method,
		Path:          c.Request.URL.Path,
		Status:        http.StatusUnauthorized,
		OK:            false,
		ParamsSummary: "auth_failed",
	})
}

// logAgentAuthFailure persists a hello with a bad token on the agent plane.
// The connection is unauthenticated, so agent_id is whatever the caller
// claimed — the row records it as such.
func (s *Service) logAgentAuthFailure(remoteAddr, claimedAgentID string) {
	s.appendOpLog(OpLogEntry{
		RequestID:     common.NewID(),
		TS:            time.Now().UTC(),
		Plane:         "agent",
		Actor:         "unauthorized",
		UID:           -1,
		PID:           -1,
		Source:        remoteAddr,
		Method:        "WEBSOCKET",
		Path:          "/ws/agent",
		Status:        http.StatusUnauthorized,
		OK:            false,
		Ref:           "agent_id=" + truncateForLog(claimedAgentID, 64),
		ParamsSummary: "auth_failed",
	})
}

func (s *Service) appendOpLog(entry OpLogEntry) {
	if err := s.store.AddOpLog(entry); err != nil {
		s.logger.Warn("persist oplog",
			zap.String("request_id", entry.RequestID),
			zap.String("path", entry.Path),
			zap.Error(err))
	}
}

// summarizeRequestBody renders a bounded, single-line description of a write
// request body. Values for known operational keys are kept (they are the
// evidence: which command, against which path, which members); unknown keys
// appear by name only so nothing silently bloats the log.
func summarizeRequestBody(raw []byte) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	// Only the first oplogMaxRequestBytes are parsed; mark it so a reader
	// knows the summary is a prefix view, not the whole request.
	truncated := false
	if len(raw) > oplogMaxRequestBytes {
		raw = raw[:oplogMaxRequestBytes]
		truncated = true
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		out := "body:" + truncateForLog(string(raw), 120)
		if truncated {
			out += " …"
		}
		return out
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		if !oplogBodyKeys[k] {
			b.WriteString(k)
			b.WriteString("=*")
			continue
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(jsonValueBrief(obj[k]))
	}
	if truncated {
		b.WriteString(" …")
	}
	return truncateForLog(b.String(), oplogMaxSummaryChars)
}

func jsonValueBrief(v any) string {
	switch t := v.(type) {
	case string:
		return strconv.Quote(truncateForLog(t, 200))
	case []any:
		parts := make([]string, 0, len(t))
		for i, item := range t {
			if i >= 8 {
				parts = append(parts, fmt.Sprintf("+%d", len(t)-i))
				break
			}
			parts = append(parts, strings.Trim(jsonValueBrief(item), `"`))
		}
		return "[" + strings.Join(parts, ",") + "]"
	default:
		return fmt.Sprintf("%v", t)
	}
}

func truncateForLog(s string, max int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// extractAgents collects target agent ids from the request (agent_id,
// agent_ids) and the response (per-task agent_id), deduplicated and capped.
func extractAgents(reqBody, respBody []byte) []string {
	set := map[string]struct{}{}
	collectStringFields(reqBody, set, "agent_id")
	collectStringArrayFields(reqBody, set, "agent_ids")
	collectStringFields(respBody, set, "agent_id")

	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	if len(out) > oplogMaxAgents {
		out = out[:oplogMaxAgents]
	}
	return out
}

func collectStringFields(raw []byte, set map[string]struct{}, key string) {
	var obj any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return
	}
	walkJSON(obj, func(m map[string]any) {
		if v, ok := m[key].(string); ok && v != "" {
			set[v] = struct{}{}
		}
	})
}

func collectStringArrayFields(raw []byte, set map[string]struct{}, key string) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return
	}
	arr, ok := obj[key].([]any)
	if !ok {
		return
	}
	for _, v := range arr {
		if s, ok := v.(string); ok && s != "" {
			set[s] = struct{}{}
		}
	}
}

// extractRefs names the resources the operation produced: task ids, transfer
// id, group id — pulled from any nesting depth of the response JSON.
func extractRefs(respBody []byte) string {
	var obj any
	if err := json.Unmarshal(respBody, &obj); err != nil {
		return ""
	}
	seen := map[string]struct{}{}
	var ordered []string
	walkJSON(obj, func(m map[string]any) {
		for _, key := range []string{"task_id", "transfer_id", "id"} {
			if !oplogRefKeys[key] {
				continue
			}
			v, ok := m[key].(string)
			if !ok || v == "" {
				continue
			}
			if _, dup := seen[v]; dup {
				continue
			}
			seen[v] = struct{}{}
			ordered = append(ordered, key+"="+v)
		}
	})
	if len(ordered) == 0 {
		return ""
	}
	sort.Strings(ordered) // deterministic across map iteration
	return truncateForLog(strings.Join(ordered, ","), oplogMaxRefChars)
}

func walkJSON(v any, visit func(map[string]any)) {
	switch t := v.(type) {
	case map[string]any:
		visit(t)
		for _, child := range t {
			walkJSON(child, visit)
		}
	case []any:
		for _, child := range t {
			walkJSON(child, visit)
		}
	}
}

// OplogCount reports how many audit rows exist (overview endpoint).
func (s *Store) OplogCount() (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM oplog`).Scan(&n)
	return n, err
}

// ---- store ----

func (s *Store) AddOpLog(entry OpLogEntry) error {
	agentsJSON, err := json.Marshal(entry.Agents)
	if err != nil || string(agentsJSON) == "null" {
		agentsJSON = []byte("[]")
	}
	_, err = s.db.Exec(`
		INSERT INTO oplog(request_id, ts, plane, actor, uid, pid, source, method, path, status, ok, ref, agents_json, params_summary)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		entry.RequestID,
		entry.TS.UTC().Format(time.RFC3339Nano),
		entry.Plane,
		entry.Actor,
		entry.UID,
		entry.PID,
		entry.Source,
		entry.Method,
		entry.Path,
		entry.Status,
		boolToInt(entry.OK),
		entry.Ref,
		string(agentsJSON),
		entry.ParamsSummary,
	)
	return err
}

// OpLogQuery filters the audit trail. Empty/zero fields are unconstrained.
type OpLogQuery struct {
	Limit  int
	Actor  string
	Agent  string
	Path   string
	Ref    string // substring match against the ref column (task/transfer ids)
	Since  time.Time
	Until  time.Time
	Failed bool // restrict to ok = 0 rows
}

func (s *Store) RecentOpLogs(q OpLogQuery) ([]OpLogEntry, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	where := make([]string, 0, 6)
	args := make([]any, 0, 7)
	if q.Actor != "" {
		where = append(where, "actor = ?")
		args = append(args, q.Actor)
	}
	if q.Agent != "" {
		where = append(where, "agents_json LIKE ?")
		args = append(args, "%\""+q.Agent+"\"%")
	}
	if q.Path != "" {
		where = append(where, "path = ?")
		args = append(args, q.Path)
	}
	if q.Ref != "" {
		where = append(where, "ref LIKE ?")
		args = append(args, "%"+q.Ref+"%")
	}
	if !q.Since.IsZero() {
		where = append(where, "ts >= ?")
		args = append(args, q.Since.UTC().Format(time.RFC3339Nano))
	}
	if !q.Until.IsZero() {
		where = append(where, "ts <= ?")
		args = append(args, q.Until.UTC().Format(time.RFC3339Nano))
	}
	if q.Failed {
		where = append(where, "ok = 0")
	}

	clause := ""
	if len(where) > 0 {
		clause = " WHERE " + strings.Join(where, " AND ")
	}

	rows, err := s.db.Query(`
		SELECT id, request_id, ts, plane, actor, uid, pid, source, method, path, status, ok, ref, agents_json, params_summary
		FROM oplog`+clause+`
		ORDER BY id DESC
		LIMIT ?
	`, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]OpLogEntry, 0, 8)
	for rows.Next() {
		var (
			item       OpLogEntry
			tsRaw      string
			okInt      int
			agentsJSON string
		)
		if err := rows.Scan(&item.ID, &item.RequestID, &tsRaw, &item.Plane, &item.Actor,
			&item.UID, &item.PID, &item.Source, &item.Method, &item.Path, &item.Status, &okInt, &item.Ref, &agentsJSON, &item.ParamsSummary); err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, err
		}
		item.OK = okInt == 1
		item.TS = parseNullTime(tsRaw)
		item.Agents = decodeTags(agentsJSON)
		items = append(items, item)
	}
	return items, rows.Err()
}
