package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"coc2/internal/common"
)

// The plugin module.
//
// A plugin is a file in the plugin directory:
//
//   - *.js — a JavaScript module run in-process by moejs. It exports the hooks
//     it handles (export function task_result(event) {…}), gets the schema-2
//     envelope described in plugin_event.go, and may return a decision from
//     task_dispatch.
//   - any other executable file — the original contract: started per hook with
//     the hook name in argv[1] and the payload on stdin. Deprecated, kept as a
//     migration path (see plugin_legacy.go).
//
// The manager owns three boundaries the hooks depend on:
//
//   - admission control (sem): a fleet reconnect must not turn into an
//     unbounded fan-out of goroutines and runtimes. Async hooks past the cap are
//     dropped with a warning; the synchronous decision hook waits inside its own
//     budget instead.
//   - the event store: bodies retained so `ref`/coc2.event(id) resolves,
//     bounded in bytes and lifetime.
//   - isolation between plugins: the JS runners never share a runtime.

// PluginKind distinguishes the two file types a plugin directory can hold.
type PluginKind string

const (
	PluginKindJS   PluginKind = "js"
	PluginKindExec PluginKind = "exec"
)

type Plugin struct {
	Name  string     `json:"name"`
	Path  string     `json:"path"`
	Kind  PluginKind `json:"kind"`
	Hooks []string   `json:"hooks,omitempty"`
}

// agentConnectedEvent is the plugin-facing view of an agent hello. It is
// deliberately a separate type rather than the wire struct: the wire struct
// carries Token, which authenticates the entire fleet, and plugins run beside
// the server. Serialising the wire struct handed that fleet-wide credential to
// anyone who could read or replace a plugin. Everything a hook legitimately
// needs is here, so add fields here explicitly — never pass AgentHello straight
// through.
type agentConnectedEvent struct {
	AgentID     string    `json:"agent_id"`
	Hostname    string    `json:"hostname"`
	OS          string    `json:"os"`
	Arch        string    `json:"arch"`
	IPAddrs     []string  `json:"ip_addrs,omitempty"`
	Tags        []string  `json:"tags,omitempty"`
	Fingerprint string    `json:"fingerprint,omitempty"`
	Version     string    `json:"version"`
	ConnectedAt time.Time `json:"connected_at"`
}

// pluginRunner is one loaded plugin, whatever its kind.
type pluginRunner interface {
	Info() Plugin
	Has(hook string) bool
	// Decidable reports whether this runner can return a decision at all. It is
	// a property of the *kind*, not of the hook: an executable plugin's output
	// is discarded, so it can never veto anything.
	Decidable() bool
	Run(ctx context.Context, hook string, body []byte, out any) error
}

// PluginOptions are the knobs the manager needs from configuration.
type PluginOptions struct {
	// Envelope is the envelope shape handed to *executable* plugins: 1 (the
	// raw payload, the original contract) or 2 (the schema-2 envelope).
	// JavaScript plugins always get schema 2 — they are new, and the whole
	// point of the envelope is that new plugins are written against it.
	Envelope int
}

type PluginManager struct {
	logger  *zap.Logger
	runners []pluginRunner
	events  *pluginEventStore
	sem     chan struct{}
	// envelope is the exec-plugin shape; see PluginOptions.
	envelope int
}

// NewPluginManager keeps its original signature for callers that do not care
// about the envelope (tests, and the common case).
func NewPluginManager(dir string, logger *zap.Logger) (*PluginManager, error) {
	return NewPluginManagerWithOptions(dir, logger, PluginOptions{Envelope: 1})
}

func NewPluginManagerWithOptions(dir string, logger *zap.Logger, opts PluginOptions) (*PluginManager, error) {
	if opts.Envelope != 1 && opts.Envelope != pluginEnvelopeSchema {
		return nil, fmt.Errorf("plugin envelope must be 1 or %d, got %d", pluginEnvelopeSchema, opts.Envelope)
	}

	pm := &PluginManager{
		logger:   logger,
		events:   newPluginEventStore(common.PluginEventTTL, common.PluginEventStoreBytes),
		sem:      make(chan struct{}, common.PluginMaxConcurrentHooks),
		envelope: opts.Envelope,
	}
	if dir == "" {
		return pm, nil
	}

	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return pm, nil
		}
		return nil, err
	}
	if !info.IsDir() {
		return pm, nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		fileInfo, err := entry.Info()
		if err != nil {
			return nil, err
		}

		if isJavaScriptPlugin(entry.Name()) {
			// A broken plugin is skipped, not fatal: the server still has a
			// fleet to serve, and one syntax error must not stop it from
			// starting. The error is loud because the alternative — a plugin
			// that silently never runs — is indistinguishable from a hook
			// nobody cares about.
			source, err := os.ReadFile(path)
			if err != nil {
				logger.Error("read plugin", zap.String("plugin", entry.Name()), zap.Error(err))
				continue
			}
			plugin, err := newJSPlugin(entry.Name(), path, source, pm.events, logger)
			if err != nil {
				logger.Error("load javascript plugin", zap.String("plugin", entry.Name()), zap.Error(err))
				continue
			}
			if len(plugin.hooks) == 0 {
				logger.Warn("javascript plugin exports no known hook",
					zap.String("plugin", entry.Name()),
					zap.Strings("known_hooks", pluginHookNames))
				continue
			}
			pm.runners = append(pm.runners, plugin)
			continue
		}

		if fileInfo.Mode()&0o111 == 0 {
			continue
		}
		pm.runners = append(pm.runners, &execPlugin{
			name:     entry.Name(),
			path:     path,
			logger:   logger,
			envelope: opts.Envelope,
		})
	}
	return pm, nil
}

// isJavaScriptPlugin recognises the sources this package can compile. The
// extension decides — mode bits do not — because a .js file is data for the
// engine, not something the kernel executes.
func isJavaScriptPlugin(name string) bool {
	base := strings.ToLower(name)
	return strings.HasSuffix(base, ".js") || strings.HasSuffix(base, ".mjs")
}

func (pm *PluginManager) List() []Plugin {
	out := make([]Plugin, 0, len(pm.runners))
	for _, r := range pm.runners {
		out = append(out, r.Info())
	}
	return out
}

// Trigger runs an asynchronous hook on every plugin that handles it. It returns
// immediately; the caller is on an event path and must never wait for a
// notification (docs/plugin-refactor.md §4.3).
func (pm *PluginManager) Trigger(hook string, payload any) {
	if pm == nil || len(pm.runners) == 0 {
		return
	}

	targets := make([]pluginRunner, 0, len(pm.runners))
	for _, r := range pm.runners {
		if r.Has(hook) {
			targets = append(targets, r)
		}
	}
	if len(targets) == 0 {
		return
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		pm.logger.Warn("marshal plugin payload", zap.String("hook", hook), zap.Error(err))
		return
	}

	// The envelope is built once per event, not once per plugin: a task result
	// with 2 MiB of output would otherwise be copied per plugin, which is the
	// cost this design exists to avoid.
	needEnvelope := pm.envelope == pluginEnvelopeSchema
	if !needEnvelope {
		for _, r := range targets {
			if r.Info().Kind == PluginKindJS {
				needEnvelope = true
				break
			}
		}
	}

	var envelopeJSON []byte
	var eventID string
	if needEnvelope {
		eventID = pm.events.add(hook, payloadJSON)
		envelopeJSON, err = json.Marshal(pluginEnvelope{
			Schema: pluginEnvelopeSchema,
			Event:  hook,
			ID:     eventID,
			At:     time.Now().UTC(),
			Ref:    pluginEventRef(eventID),
			Hint:   pluginHint(hook, payload),
		})
		if err != nil {
			pm.logger.Warn("marshal plugin envelope", zap.String("hook", hook), zap.Error(err))
			return
		}
	}

	for _, r := range targets {
		body := payloadJSON
		if r.Info().Kind == PluginKindJS || pm.envelope == pluginEnvelopeSchema {
			pm.events.allow(eventID, r.Info().Name)
			body = envelopeJSON
		}

		// Admission control. Dropping an async event under overload is the
		// explicit choice: the alternative is an unbounded backlog of hooks
		// (and runtimes) growing behind a fleet-wide reconnect, which would
		// take the control plane's memory down with it.
		select {
		case pm.sem <- struct{}{}:
		default:
			pm.logger.Warn("plugin hook dropped: too many hooks in flight",
				zap.String("hook", hook),
				zap.Int("limit", cap(pm.sem)),
			)
			continue
		}

		target := r
		go func() {
			defer func() { <-pm.sem }()
			ctx, cancel := context.WithTimeout(context.Background(), common.PluginHookTimeout)
			defer cancel()
			_ = target.Run(ctx, hook, body, nil)
		}()
	}
}

// PluginDecision is what a synchronous hook decided.
type PluginDecision struct {
	// Allowed is the only field the dispatch path acts on. Everything else is
	// evidence: which plugin spoke, what it said, and whether the answer is a
	// real "allow" or a fail-open after a timeout.
	Allowed bool
	// Decision is "allow", "deny", "timeout" or "malformed".
	Decision string
	Plugin   string
	Reason   string
}

const (
	decisionAllow     = "allow"
	decisionDeny      = "deny"
	decisionTimeout   = "timeout"
	decisionMalformed = "malformed"
)

// Decide runs a synchronous hook and reports whether the action may proceed.
//
// Fail open, always: a plugin that is slow, wedged, broken, or returns garbage
// must not stop the fleet from being operated. The failure is returned in
// Decision so the caller can put it in the audit trail — silently allowing is
// the one outcome that is not allowed.
//
// Only task_dispatch is wired to this. The other hooks fire after the fact and
// have nothing to intercept.
func (pm *PluginManager) Decide(ctx context.Context, hook string, payload any, budget time.Duration) PluginDecision {
	allow := PluginDecision{Allowed: true, Decision: decisionAllow}
	if pm == nil || len(pm.runners) == 0 || !pluginDecidableHooks[hook] {
		return allow
	}

	targets := make([]pluginRunner, 0, len(pm.runners))
	for _, r := range pm.runners {
		// Only a runner with a return channel can decide. An executable plugin
		// has none (plugin_legacy.go), so asking it synchronously would cost a
		// process spawn on the dispatch path to learn nothing — and would leave
		// a "decision=timeout" audit row implying the gate had a say when it
		// could not.
		if !r.Has(hook) || !r.Decidable() {
			continue
		}
		targets = append(targets, r)
	}
	if len(targets) == 0 {
		return allow
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		pm.logger.Warn("marshal plugin payload", zap.String("hook", hook), zap.Error(err))
		return allow
	}

	eventID := pm.events.add(hook, payloadJSON)
	envelopeJSON, err := json.Marshal(pluginEnvelope{
		Schema: pluginEnvelopeSchema,
		Event:  hook,
		ID:     eventID,
		At:     time.Now().UTC(),
		Ref:    pluginEventRef(eventID),
		Hint:   pluginHint(hook, payload),
	})
	if err != nil {
		pm.logger.Warn("marshal plugin envelope", zap.String("hook", hook), zap.Error(err))
		return allow
	}

	// One deadline covers the whole chain: ten plugins do not get ten budgets,
	// because the operator is waiting on the request, not on a plugin count.
	deadline := time.Now().Add(budget)
	// One timer for the whole chain, stopped on the way out: a per-iteration
	// time.After would leave an unstopped timer behind for every plugin on
	// every dispatch.
	budgetTimer := time.NewTimer(budget)
	defer budgetTimer.Stop()

	last := allow
	for _, r := range targets {
		pm.events.allow(eventID, r.Info().Name)

		// A synchronous hook must not be dropped by admission control: the
		// caller needs an answer. Wait for a slot, but no longer than what is
		// left of the budget.
		select {
		case pm.sem <- struct{}{}:
		case <-ctx.Done():
			return PluginDecision{Allowed: true, Decision: decisionTimeout, Plugin: r.Info().Name, Reason: ctx.Err().Error()}
		case <-budgetTimer.C:
			return PluginDecision{Allowed: true, Decision: decisionTimeout, Plugin: r.Info().Name, Reason: "no plugin slot within the decision budget"}
		}

		out := struct {
			Decision string `json:"decision"`
			Reason   string `json:"reason"`
		}{}

		callCtx, cancel := context.WithDeadline(ctx, deadline)
		err := r.Run(callCtx, hook, envelopeJSON, &out)
		cancel()
		<-pm.sem

		if err != nil {
			// Every failure here is a fail-open, and each one is named
			// differently so the audit row can say which happened: a plugin
			// that ran out of time is a different operational problem from one
			// that threw.
			//
			// No warn here: the runner already logged the hook's failure with
			// its stack. Two lines for one event is how a log stops being read.
			// What only this branch knows — "and the task went through anyway"
			// — is what the audit row records.
			last = PluginDecision{Allowed: true, Plugin: r.Info().Name, Reason: err.Error()}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				last.Decision = decisionTimeout
			} else {
				last.Decision = decisionMalformed
			}
			return last
		}

		switch strings.ToLower(strings.TrimSpace(out.Decision)) {
		case "", decisionAllow:
			last = PluginDecision{Allowed: true, Decision: decisionAllow, Plugin: r.Info().Name}
		case decisionDeny:
			return PluginDecision{Allowed: false, Decision: decisionDeny, Plugin: r.Info().Name, Reason: out.Reason}
		default:
			// An unknown decision is a bug in the plugin, not a veto. Treating
			// it as deny would turn a typo into an outage; treating it as allow
			// silently would hide the typo, so it is reported.
			pm.logger.Warn("plugin returned an unknown decision, allowing",
				zap.String("plugin", r.Info().Name),
				zap.String("hook", hook),
				zap.String("decision", out.Decision),
			)
			last = PluginDecision{Allowed: true, Decision: decisionMalformed, Plugin: r.Info().Name, Reason: "unknown decision " + out.Decision}
		}
	}
	return last
}

// EventPayload resolves an event id for the operator plane, backing
// GET /api/v1/events/:id — the URL an envelope's ref points at.
func (pm *PluginManager) EventPayload(id string) ([]byte, bool) {
	if pm == nil {
		return nil, false
	}
	return pm.events.getForOperator(id)
}
