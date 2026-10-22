package common

import "time"

// Operator-facing limits.
//
// These are enforced by the agent or the server, but they are also part of the
// operator-facing contract: they decide what a result is allowed to contain and
// why a dispatch or a plugin hook can come back failed. The CLI publishes them
// in `coc2 schema`, so they live here rather than inside one binary — a number
// copied into the schema output would drift from the code that enforces it, and
// a limit nobody can look up is how "my output ends with [output truncated]"
// becomes a support question.
const (
	// MaxTaskOutputBytes caps each of a task's captured stdout and stderr.
	// Output past the cap is discarded and a "[output truncated]" line is
	// appended, so an operator can distinguish a short result from a cut-off
	// one instead of silently losing the tail.
	MaxTaskOutputBytes = 1 << 20

	// PluginHookTimeout is how long one plugin hook may run before the server
	// kills it. A hook runs on the event path (see internal/server/plugins.go),
	// so a plugin that never exits would otherwise pin one goroutine, the
	// hook's payload and its two pipe descriptors per event, forever.
	//
	// It is a server-side limit, but it is published with the task limits
	// because plugins are written by operators: the first question after "my
	// hook was killed" is how long it was allowed to take.
	PluginHookTimeout = 5 * time.Second

	// PluginDecisionTimeout is the budget of a *synchronous* hook — today only
	// task_dispatch, the gate a policy plugin uses to refuse a task before it
	// reaches an agent. Every other hook is fire-and-forget and never waits.
	//
	// Two rules follow from what it gates:
	//   - It must stay well under the operator's own patience for a dispatch
	//     request, because the request waits for it.
	//   - Running out of it means *allow* (fail open) with an audit row. A
	//     plugin that is slow, wedged or being restarted must not stop the
	//     fleet from receiving work; the failure has to be visible instead of
	//     silently blocking.
	PluginDecisionTimeout = 200 * time.Millisecond

	// PluginMaxConcurrentHooks bounds how many hooks run at once across all
	// plugins. Without it a fleet reconnect (hundreds of agents at once) times
	// the plugin count becomes an unbounded fan-out: every hook was one
	// goroutine and, for executable plugins, one process. Events past the cap
	// are dropped with a warning — an async hook is a notification, so losing
	// one under overload is better than letting the backlog own the control
	// plane. The synchronous decision hook never drops: it waits inside its
	// own budget and fails open.
	PluginMaxConcurrentHooks = 64

	// PluginMaxRuntimesPerPlugin caps the runtime pool of one JavaScript
	// plugin. Each runtime is a separate JavaScript global environment with its
	// own memory limit, so this is also the per-plugin memory ceiling: a plugin
	// can never be handed more than this many concurrent calls, and the rest
	// queue for a runtime instead of allocating another.
	PluginMaxRuntimesPerPlugin = 8

	// PluginMemoryLimitBytes caps what one call's JavaScript may allocate
	// inside a plugin runtime. The engine enforces it by interrupting the
	// script, which a script cannot catch — so a plugin that tries to allocate
	// its way through the limit stops instead of growing the server's heap.
	//
	// It is deliberately several times the largest payload (a task result is at
	// most 2 MiB of captured output) because the limit counts allocation, not
	// live memory: a plugin that copies a payload a few times still fits, and
	// the gate that really keeps payloads out of the server's heap is the
	// envelope's hint/ref split, not this number.
	PluginMemoryLimitBytes int64 = 32 << 20

	// PluginEventTTL is how long a plugin can resolve the `ref`/id of an event
	// it was handed. The event body lives in server memory for this long, and
	// then the id stops resolving — an id is a capability, so it must not stay
	// one forever.
	PluginEventTTL = 5 * time.Minute

	// PluginEventStoreBytes caps the memory all retained event bodies may take.
	// Task output is the only large payload, so this is what stops a burst of
	// 2 MiB results from turning "keep the body so a plugin can fetch it" into
	// an unbounded buffer. Past the cap the oldest bodies are dropped; their
	// ids stop resolving early, which a plugin sees as a fetch error rather
	// than as a silent truncation.
	PluginEventStoreBytes = 16 << 20

	// MaxConcurrentTasks bounds how many shell tasks one agent runs at once.
	// Every task is a process group plus up to 2 MiB of captured output, so
	// without a cap a burst of dispatches (or a compromised server) could
	// exhaust the agent host. Excess dispatches are answered with a failed
	// result rather than silently dropped, so the operator sees why nothing
	// ran.
	MaxConcurrentTasks = 16
)
