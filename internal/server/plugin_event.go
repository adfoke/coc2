package server

import (
	"errors"
	"sync"
	"time"

	"coc2/internal/common"
	"coc2/internal/protocol"
)

// The plugin event envelope.
//
// A hook used to receive the payload struct itself: every field of the wire
// type, inline, whether or not the hook could do anything with it. That was
// fine while a plugin was a subprocess — the bytes lived in another address
// space and died with it — and it stops being fine the moment plugins run in
// this process (see docs/plugin-refactor.md §0/§2.1): a 2 MiB task result
// materialised per plugin per event lands in the server's own heap.
//
// So the envelope carries a *description* of the event, not the event:
//
//	{"schema":2,"event":"task_result","id":"evt_…","at":"…",
//	 "ref":"/api/v1/events/evt_…","hint":{ … the decision-sized summary … }}
//
// `hint` is chosen so the common question ("what happened?") is answerable
// without fetching anything; `ref` (or the `coc2.event(id)` host function, for
// in-process plugins) resolves the full payload. The bytes cross the boundary
// only when a plugin asks for them — and then they are charged to that plugin's
// memory limit, not to the server's steady state.
const pluginEnvelopeSchema = 2

// pluginHookNames is the closed set of hooks the server emits. A JavaScript
// plugin exports the ones it cares about and is skipped for the rest, so this
// list is also how a plugin's capabilities are discovered at load time.
var pluginHookNames = []string{
	"agent_connected",
	"task_result",
	"transfer_done",
	"metrics_report",
	"task_dispatch",
}

// pluginDecidableHooks are the hooks whose return value the server reads. Only
// a hook on a path that can wait may be here: task_dispatch runs before a task
// is handed to an agent, so 200 ms of policy is affordable. Everything else —
// task_result, transfer_done, metrics_report — has already happened (or is on
// the heart-path), so denying it would either lose data or stall a heartbeat.
var pluginDecidableHooks = map[string]bool{
	"task_dispatch": true,
}

type pluginEnvelope struct {
	Schema int       `json:"schema"`
	Event  string    `json:"event"`
	ID     string    `json:"id"`
	At     time.Time `json:"at"`
	Ref    string    `json:"ref"`
	Hint   any       `json:"hint"`
}

// PluginEventRefPrefix is the URL path an event id resolves under. It is also
// what the envelope's `ref` contains, so an out-of-process consumer (a script
// with a token, a debugging session) reads the same thing an in-process plugin
// gets from coc2.event().
const PluginEventRefPrefix = "/api/v1/events/"

func pluginEventRef(id string) string { return PluginEventRefPrefix + id }

// pluginHint projects a payload onto the fields a hook is allowed to see
// without fetching the body.
//
// The rule for what belongs here (docs/plugin-refactor.md §3.2): a plugin that
// only wants to know *what happened* must not have to fetch anything. A plugin
// that wants the bytes (task stdout, transfer paths) fetches.
//
// Unknown payload types are passed through whole. That is deliberate — it keeps
// this function honest for test doubles and future hooks — but every real hook
// is listed explicitly, and the contract tests assert the exact key set, so
// adding a field to a wire struct cannot silently widen what plugins see.
func pluginHint(hook string, payload any) any {
	switch v := payload.(type) {
	case protocol.TaskResult:
		return taskResultHint(v)
	case *protocol.TaskResult:
		if v == nil {
			return nil
		}
		return taskResultHint(*v)
	case TransferStatus:
		return transferHint(v)
	case *TransferStatus:
		if v == nil {
			return nil
		}
		return transferHint(*v)
	case protocol.Task:
		return taskDispatchHint(v)
	case *protocol.Task:
		if v == nil {
			return nil
		}
		return taskDispatchHint(*v)
	default:
		// agent_connected and metrics_report are already small and are handed
		// over whole; anything unknown is too.
		return payload
	}
}

// taskResultHint is task_result's hint: the fields that answer "did it work",
// and byte counts instead of the output itself. The 2 MiB of stdout/stderr are
// reachable only through the event body — this is the single largest reason the
// envelope exists.
func taskResultHint(r protocol.TaskResult) any {
	// Exactly the fields docs/plugin-refactor.md §3.2 lists, no more: the
	// contract test asserts the key set, so a field added here has to be a
	// decision rather than a struct riding along.
	return struct {
		TaskID      string `json:"task_id"`
		AgentID     string `json:"agent_id"`
		Status      string `json:"status"`
		ExitCode    int    `json:"exit_code"`
		DurationMS  int64  `json:"duration_ms"`
		StdoutBytes int    `json:"stdout_bytes"`
		StderrBytes int    `json:"stderr_bytes"`
	}{
		TaskID:      r.TaskID,
		AgentID:     r.AgentID,
		Status:      r.Status,
		ExitCode:    r.ExitCode,
		DurationMS:  r.DurationMS,
		StdoutBytes: len(r.Stdout),
		StderrBytes: len(r.Stderr),
	}
}

// transferHint drops the local/remote paths and the chunk geometry: a plugin
// deciding "did this transfer succeed" does not need to know where the file
// lives, and paths are the most sensitive thing a transfer carries.
func transferHint(t TransferStatus) any {
	return struct {
		TransferID     string `json:"transfer_id"`
		AgentID        string `json:"agent_id"`
		Status         string `json:"status"`
		Size           int64  `json:"size"`
		ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	}{
		TransferID:     t.ID,
		AgentID:        t.AgentID,
		Status:         t.Status,
		Size:           t.Size,
		ChecksumSHA256: t.ChecksumSHA256,
	}
}

// taskDispatchHint is task_dispatch's hint. It carries the command, because a
// policy plugin that cannot see the command can only deny blindly — the command
// *is* the thing being decided about. It is truncated (with a flag saying so)
// so a pathological command cannot turn the envelope into the payload it was
// supposed to avoid.
func taskDispatchHint(t protocol.Task) any {
	command, truncated := truncateBytes(t.Command, pluginDispatchCommandHintBytes)
	return struct {
		TaskID           string `json:"task_id"`
		AgentID          string `json:"agent_id"`
		Type             string `json:"type"`
		Command          string `json:"command"`
		CommandBytes     int    `json:"command_bytes"`
		CommandTruncated bool   `json:"command_truncated,omitempty"`
		TimeoutSecs      int    `json:"timeout_secs"`
		Priority         int    `json:"priority"`
	}{
		TaskID:           t.ID,
		AgentID:          t.AgentID,
		Type:             t.Type,
		Command:          command,
		CommandBytes:     len(t.Command),
		CommandTruncated: truncated,
		TimeoutSecs:      t.TimeoutSecs,
		Priority:         t.Priority,
	}
}

// pluginDispatchCommandHintBytes bounds the command a task_dispatch plugin sees
// inline. 8 KiB is far past any real command line (the kernel's own ARG_MAX
// limit is 2 MiB but a dispatched command is a single shell invocation) and
// small enough that 64 concurrent envelopes cannot matter.
const pluginDispatchCommandHintBytes = 8 << 10

// truncateBytes cuts s to at most n bytes without splitting a UTF-8 sequence,
// reporting whether it cut anything.
func truncateBytes(s string, n int) (string, bool) {
	if len(s) <= n {
		return s, false
	}
	cut := n
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut], true
}

// errPluginEventNotFound covers three cases that a plugin must not be able to
// tell apart: an id that was never issued, one that expired, and one that was
// evicted to stay inside the memory cap.
var errPluginEventNotFound = errors.New("event not found or expired")

// pluginEventStore keeps recent event bodies so that an id handed to a plugin
// still resolves for a while.
//
// Two boundaries make it a capability rather than a hole:
//   - an id must have been issued by this server, and it is unguessable
//     (evt_ + 16 random bytes), never a path or a URL — so a plugin cannot ask
//     the server to read a file, and cannot construct an id to probe for one;
//   - an id resolves only for the plugins the event was actually dispatched to.
//     "Someone else's event id" is rejected, so one plugin cannot read another
//     plugin's stream by guessing.
//
// The operator plane sees every id: it is already trusted with the underlying
// payload (it can read task results directly).
type pluginEventStore struct {
	mu       sync.Mutex
	ttl      time.Duration
	maxBytes int
	bytes    int
	order    []string
	byID     map[string]*pluginEvent
	now      func() time.Time
}

type pluginEvent struct {
	id      string
	hook    string
	at      time.Time
	payload []byte
	// allowed is the set of plugin names this event was dispatched to. Empty
	// means "created but not yet delivered" — a plugin can never be in that
	// state while calling, because allow() runs before the hook starts.
	allowed map[string]struct{}
}

func newPluginEventStore(ttl time.Duration, maxBytes int) *pluginEventStore {
	return &pluginEventStore{
		ttl:      ttl,
		maxBytes: maxBytes,
		byID:     make(map[string]*pluginEvent),
		now:      time.Now,
	}
}

// add stores a payload and returns the id handed out for it. The payload is
// treated as immutable: callers pass the freshly marshalled JSON of one event,
// which nothing else writes to afterwards.
func (st *pluginEventStore) add(hook string, payload []byte) string {
	id := "evt_" + common.NewID()
	now := st.now()

	st.mu.Lock()
	defer st.mu.Unlock()

	st.evictExpiredLocked(now)
	st.byID[id] = &pluginEvent{
		id:      id,
		hook:    hook,
		at:      now,
		payload: payload,
		allowed: make(map[string]struct{}, 4),
	}
	st.order = append(st.order, id)
	st.bytes += len(payload)
	st.evictOverCapLocked()
	return id
}

// allow records that plugin was handed this event. Delivery is per plugin and
// happens before the hook runs, so a plugin calling coc2.event(id) with an id it
// received always finds itself in the set.
func (st *pluginEventStore) allow(id, plugin string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if ev, ok := st.byID[id]; ok {
		ev.allowed[plugin] = struct{}{}
	}
}

// get resolves an id for one plugin.
func (st *pluginEventStore) get(id, plugin string) ([]byte, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	ev, ok := st.byID[id]
	if !ok {
		return nil, errPluginEventNotFound
	}
	if st.now().Sub(ev.at) > st.ttl {
		st.dropLocked(id)
		return nil, errPluginEventNotFound
	}
	if _, ok := ev.allowed[plugin]; !ok {
		return nil, errPluginEventNotFound
	}
	return ev.payload, nil
}

// getForOperator resolves an id without the per-plugin restriction. It backs
// GET /api/v1/events/:id, which is what the envelope's ref points at.
func (st *pluginEventStore) getForOperator(id string) ([]byte, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()

	ev, ok := st.byID[id]
	if !ok {
		return nil, false
	}
	if st.now().Sub(ev.at) > st.ttl {
		st.dropLocked(id)
		return nil, false
	}
	return ev.payload, true
}

func (st *pluginEventStore) evictExpiredLocked(now time.Time) {
	for len(st.order) > 0 {
		id := st.order[0]
		ev, ok := st.byID[id]
		if ok && now.Sub(ev.at) <= st.ttl {
			return
		}
		st.order = st.order[1:]
		if ok {
			st.bytes -= len(ev.payload)
			delete(st.byID, id)
		}
	}
}

func (st *pluginEventStore) evictOverCapLocked() {
	for st.bytes > st.maxBytes && len(st.order) > 0 {
		id := st.order[0]
		st.order = st.order[1:]
		st.dropLocked(id)
	}
}

func (st *pluginEventStore) dropLocked(id string) {
	ev, ok := st.byID[id]
	if !ok {
		return
	}
	st.bytes -= len(ev.payload)
	delete(st.byID, id)
}

// size reports how many bodies are retained and how many bytes they take. Used
// by tests to pin the memory boundary.
func (st *pluginEventStore) size() (int, int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.byID), st.bytes
}
