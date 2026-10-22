package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Calcium-Ion/moejs"
	"go.uber.org/zap"

	"coc2/internal/common"
)

// JavaScript plugins, run in-process by moejs.
//
// The module contract (plugins/README.md):
//
//	// hooks are optional: export the ones you care about
//	export function task_result(event) {
//	  if (event.hint.exit_code !== 0) {
//	    const full = JSON.parse(coc2.event(event.id));  // opt in to the bytes
//	    console.log(full.stdout);
//	  }
//	}
//
//	export function task_dispatch(event) {
//	  if (event.hint.command.startsWith("rm -rf")) {
//	    return { decision: "deny", reason: "destructive command" };
//	  }
//	}
//
// Three properties are structural rather than promised, and each one is
// covered by a test that would fail if the engine were swapped or the host
// surface widened:
//
//   - Isolation. Every plugin owns its runtimes, and a runtime is never handed
//     to another plugin. One plugin's globals, prototype patches and retained
//     state are therefore invisible to every other plugin.
//   - Memory. Each runtime runs with Options.MemoryLimit, enforced by the
//     engine as an interrupt — a script cannot catch it, which is the only
//     thing that makes an allocation limit a limit.
//   - No host capability beyond the event body. eval and new Function are
//     disabled, import() rejects (no Importer is installed), and the globals
//     installed below are the complete list: console and coc2.
//
// Crash isolation is the property this trades away, and it is not hand-waved:
// see docs/plugin-refactor.md §5.1/§5.3. A moejs panic comes back as an error
// (the runtime stays usable), and the call is bounded by an interrupt, so the
// remaining exposure is an engine bug rather than plugin code. That is the
// deliberate trade for dropping the ~3 ms per-event process cost and the
// deployment step of shipping an executable per plugin.

// maxPluginConsoleBytes bounds one console.log line. A plugin that prints a
// payload to the log should not be able to fill the log with it — the bytes are
// available through coc2.event(), which is the sanctioned way to look at them.
const maxPluginConsoleBytes = 2 << 10

type jsPlugin struct {
	name string
	path string
	mod  *moejs.Module
	// hooks is the set of hook exports the module declares. Resolved once at
	// compile time; a hook the plugin does not export is simply not called.
	hooks  map[string]moejs.Hook
	idle   chan *moejs.Runtime
	slots  chan struct{}
	logger *zap.Logger
	events *pluginEventStore
	// broken is set once a runtime could not be created or the module could
	// not be loaded. The plugin is then skipped instead of retried on every
	// event: a module whose top level throws would otherwise log per hook,
	// for every agent, forever.
	broken error
}

// newJSPlugin compiles one plugin source. A compile error is the caller's cue
// to skip the file: one broken plugin must not keep the server from serving
// the fleet, and it must be visible in the log rather than silent.
func newJSPlugin(name, path string, source []byte, events *pluginEventStore, logger *zap.Logger) (*jsPlugin, error) {
	mod, err := moejs.Compile(name, string(source))
	if err != nil {
		return nil, err
	}

	hooks := make(map[string]moejs.Hook, len(pluginHookNames))
	for _, hook := range pluginHookNames {
		h, err := mod.Hook(hook)
		if err != nil {
			if errors.Is(err, moejs.ErrHookNotFound) {
				continue
			}
			return nil, err
		}
		hooks[hook] = h
	}

	p := &jsPlugin{
		name:   name,
		path:   path,
		mod:    mod,
		hooks:  hooks,
		idle:   make(chan *moejs.Runtime, common.PluginMaxRuntimesPerPlugin),
		slots:  make(chan struct{}, common.PluginMaxRuntimesPerPlugin),
		logger: logger,
		events: events,
	}
	// Load eagerly into the first runtime, so a module whose top level throws
	// is reported at startup rather than on the first event of the first
	// agent. The runtime then goes straight into the pool.
	rt, err := p.newRuntime()
	if err != nil {
		return nil, err
	}
	// The slot has to be taken for this runtime too: slots counts live
	// runtimes, and release() frees one when a runtime is dropped. A runtime
	// that was never counted would block the first failure forever.
	p.slots <- struct{}{}
	p.idle <- rt
	return p, nil
}

func (p *jsPlugin) Info() Plugin {
	hooks := make([]string, 0, len(p.hooks))
	for _, name := range pluginHookNames {
		if _, ok := p.hooks[name]; ok {
			hooks = append(hooks, name)
		}
	}
	return Plugin{Name: p.name, Path: p.path, Kind: PluginKindJS, Hooks: hooks}
}

func (p *jsPlugin) Has(hook string) bool {
	if p.broken != nil {
		return false
	}
	_, ok := p.hooks[hook]
	return ok
}

// Decidable: a JavaScript hook returns a value, so it can veto.
func (p *jsPlugin) Decidable() bool { return true }

// Run executes one hook, reporting a failure as a warn before returning it.
//
// The log is here rather than at the call sites for one reason: a JavaScript
// hook that is interrupted has no other trace. There is no process, no exit
// code, no stderr — without this line a plugin that hits its budget every single
// time is indistinguishable from a plugin that was never called, which is the
// exact confusion plugins/README.md's observability section exists to prevent.
func (p *jsPlugin) Run(ctx context.Context, hook string, body []byte, out any) error {
	started := time.Now()
	err := p.run(ctx, hook, body, out)
	if err == nil {
		// Debug, not info: once per plugin per event, so a busy fleet would
		// write a line per metrics report.
		p.logger.Debug("plugin hook ran",
			zap.String("plugin", p.name),
			zap.String("hook", hook),
			zap.Duration("duration", time.Since(started)),
		)
		return nil
	}

	message := "plugin hook failed"
	if errors.Is(err, moejs.ErrMemoryLimit) {
		// Its own message: "the plugin allocated too much" is a different
		// problem from "the plugin threw", and the operator's next step
		// differs (raise the limit or fix the plugin).
		message = "plugin hook exceeded its memory limit"
	} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		message = "plugin hook timed out"
	}
	fields := []zap.Field{
		zap.String("plugin", p.name),
		zap.String("hook", hook),
		zap.Duration("duration", time.Since(started)),
		zap.Error(err),
	}
	// A separate field (not folded into the message) so a log viewer can
	// collapse it: the stack is several lines and most readers only want the
	// first one.
	var hookErr *jsHookError
	if errors.As(err, &hookErr) && hookErr.stack != "" {
		fields = append(fields, zap.String("js_stack", hookErr.stack))
	}
	var limitErr *moejs.MemoryLimitError
	if errors.As(err, &limitErr) && limitErr.Stack != "" {
		fields = append(fields, zap.String("js_stack", limitErr.Stack))
	}
	p.logger.Warn(message, fields...)
	return err
}

// run is Run without the logging.
//
// A runtime is returned to the pool only when the call ended cleanly. A runtime
// that was interrupted — timeout, or the memory limit — may have left the
// module's state half-updated, and the engine's own guidance is to drop it
// rather than trust it. Creating a fresh one costs microseconds; carrying a
// poisoned one across events costs correctness.
func (p *jsPlugin) run(ctx context.Context, hook string, body []byte, out any) error {
	if p.broken != nil {
		return p.broken
	}
	h, ok := p.hooks[hook]
	if !ok {
		return fmt.Errorf("plugin %s does not export %s", p.name, hook)
	}

	rt, err := p.acquire(ctx)
	if err != nil {
		return err
	}

	// Interrupt is the only call into a runtime that is safe from another
	// goroutine, and it is what bounds the call: a plugin spinning in a loop
	// never returns on its own. The wait on `interrupted` is the engine's
	// contract — Interrupt must have returned before the runtime is reused or
	// dropped.
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		rt.Interrupt(context.Cause(ctx))
		close(interrupted)
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
	}()

	arg, err := rt.ParseJSONString(string(body))
	if err != nil {
		p.release(rt, false)
		return err
	}

	res, err := rt.Call(h, arg)
	if err != nil {
		// The engine puts a stack on the two errors it raises itself
		// (the memory limit and the interrupt); a thrown JavaScript Error
		// carries its stack as a property of the *JavaScript* object, which
		// never reaches the Go error. Read it while the runtime is still here,
		// then drop the runtime as usual.
		stack := jsErrorStack(rt, err)
		p.release(rt, false)
		if stack != "" {
			return &jsHookError{err: err, stack: stack}
		}
		return err
	}

	if out != nil {
		if err := rt.Unmarshal(res, out); err != nil {
			p.release(rt, false)
			return err
		}
	}

	p.release(rt, true)
	return nil
}

// jsHookError is a hook failure with the plugin's own stack attached.
//
// It exists because the stack is the only thing that says *where* the plugin
// broke, and the engine only hands it over on the JavaScript object: without
// this wrapper the log would name the error and stop there, leaving the author
// to bisect their own file.
type jsHookError struct {
	err   error
	stack string
}

func (e *jsHookError) Error() string { return e.err.Error() }
func (e *jsHookError) Unwrap() error { return e.err }

// jsStackBudget bounds reading the `stack` property. It is short because this
// only runs on the failure path, and because the property is *plugin-defined*
// data: a plugin can redefine Error.prototype.stack as a getter (accidentally or
// otherwise), and reading it runs that code. The read is therefore a JavaScript
// call like any other and gets a budget and an interrupt of its own.
const jsStackBudget = 50 * time.Millisecond

// jsErrorStack extracts a thrown JavaScript error's stack, or "" when there is
// none — which is the normal answer for a string throw, for an interrupt, and
// for a memory-limit hit (the engine puts its stack in the Go error instead).
func jsErrorStack(rt *moejs.Runtime, err error) (stack string) {
	var exc *moejs.Exception
	if !errors.As(err, &exc) {
		return ""
	}
	if exc.Stack != "" {
		return exc.Stack
	}
	if exc.Value.IsUndefined() || exc.Value.IsNull() {
		return ""
	}

	// The interrupt the failing call left behind (if any) would stop this read
	// before it started.
	rt.ClearInterrupt()

	ctx, cancel := context.WithTimeout(context.Background(), jsStackBudget)
	defer cancel()
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		rt.Interrupt(context.Cause(ctx))
		close(interrupted)
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
		rt.ClearInterrupt()
		// The property is plugin-defined data and reading it can run plugin
		// code: never let a panic there escape into the logging path.
		if recover() != nil {
			stack = ""
		}
	}()

	v, err := rt.Get(exc.Value, "stack")
	if err != nil {
		return ""
	}
	converted, err := rt.ToGo(v)
	if err != nil {
		return ""
	}
	text, ok := converted.(string)
	if !ok {
		return ""
	}
	return text
}

// acquire takes an idle runtime, or makes one when the pool has room, or waits
// for one to come back. Waiting is what bounds a plugin's memory: the pool size
// is the concurrency it can have, so a burst queues here instead of allocating.
func (p *jsPlugin) acquire(ctx context.Context) (*moejs.Runtime, error) {
	select {
	case rt := <-p.idle:
		return rt, nil
	default:
	}

	select {
	case p.slots <- struct{}{}:
		rt, err := p.newRuntime()
		if err != nil {
			<-p.slots
			p.markBroken(err)
			return nil, err
		}
		return rt, nil
	case rt := <-p.idle:
		return rt, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// release returns a runtime to the pool, or drops it (reusable=false) and frees
// its slot. The send cannot block: idle and slots have the same capacity and
// this caller holds one runtime, so there is always room.
func (p *jsPlugin) release(rt *moejs.Runtime, reusable bool) {
	if !reusable {
		<-p.slots
		return
	}
	rt.ClearInterrupt()
	rt.ReleaseCallData()
	select {
	case p.idle <- rt:
	default:
		<-p.slots
	}
}

func (p *jsPlugin) markBroken(err error) {
	if p.broken != nil {
		return
	}
	p.broken = err
	p.logger.Error("plugin disabled",
		zap.String("plugin", p.name),
		zap.Error(err),
	)
}

// newRuntime builds one isolated JavaScript global environment for this
// plugin.
func (p *jsPlugin) newRuntime() (*moejs.Runtime, error) {
	rt := moejs.NewRuntime(moejs.Options{
		MemoryLimit: common.PluginMemoryLimitBytes,
		// eval and new Function compile source at run time; a plugin host has
		// no reason to allow it, and allowing it would put dynamically built
		// code beyond what a reader of the plugin file can review.
		DisableDynamicCode: true,
	})

	if err := rt.SetGlobal("console", map[string]any{
		"log":   moejs.NativeFunc(p.consoleFunc("log")),
		"warn":  moejs.NativeFunc(p.consoleFunc("warn")),
		"error": moejs.NativeFunc(p.consoleFunc("error")),
	}); err != nil {
		return nil, err
	}

	// coc2 is the entire host surface: the envelope version, and a way to turn
	// an event id into its body. There is no filesystem, no network, no clock
	// beyond Date, and no way to reach another plugin.
	if err := rt.SetGlobal("coc2", map[string]any{
		"envelope": int64(pluginEnvelopeSchema),
		"event":    moejs.NativeFunc(p.eventFunc),
	}); err != nil {
		return nil, err
	}

	if err := rt.Load(p.mod); err != nil {
		return nil, err
	}
	return rt, nil
}

// consoleFunc implements console.log/warn/error. Output goes to the server log
// at debug, never to the hook's return value: the in-process engine has no
// output channel to pollute, which is what makes the "a log line broke my
// decision" failure impossible here.
func (p *jsPlugin) consoleFunc(level string) func(*moejs.Realm, moejs.Value, []moejs.Value) (moejs.Value, error) {
	return func(r *moejs.Realm, _ moejs.Value, args []moejs.Value) (moejs.Value, error) {
		parts := make([]string, 0, len(args))
		for _, arg := range args {
			s, err := r.ToString(arg)
			if err != nil {
				return moejs.Undefined(), err
			}
			parts = append(parts, s.GoString())
		}
		message := strings.Join(parts, " ")
		if len(message) > maxPluginConsoleBytes {
			message = message[:maxPluginConsoleBytes] + " [console output truncated]"
		}
		p.logger.Debug("plugin console",
			zap.String("plugin", p.name),
			zap.String("level", level),
			zap.String("message", message),
		)
		return moejs.Undefined(), nil
	}
}

// eventFunc implements coc2.event(id).
//
// It accepts only ids this server issued for this plugin, and returns the body
// as JSON text — the plugin parses it with JSON.parse. Returning text rather
// than a converted object keeps one conversion boundary (the plugin decides how
// much of it to walk), and keeps the bytes charged to the plugin's memory limit.
func (p *jsPlugin) eventFunc(r *moejs.Realm, _ moejs.Value, args []moejs.Value) (moejs.Value, error) {
	if len(args) == 0 {
		return moejs.Undefined(), errors.New("coc2.event(id) needs an event id")
	}
	id, err := r.ToString(args[0])
	if err != nil {
		return moejs.Undefined(), err
	}
	payload, err := p.events.get(id.GoString(), p.name)
	if err != nil {
		// Returned as a JavaScript Error: a plugin that fetches an id it was
		// not handed (or one that expired) can catch it and carry on, but it
		// cannot read the bytes.
		return moejs.Undefined(), err
	}
	return moejs.String(string(payload)), nil
}
