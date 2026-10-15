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

	// MaxConcurrentTasks bounds how many shell tasks one agent runs at once.
	// Every task is a process group plus up to 2 MiB of captured output, so
	// without a cap a burst of dispatches (or a compromised server) could
	// exhaust the agent host. Excess dispatches are answered with a failed
	// result rather than silently dropped, so the operator sees why nothing
	// ran.
	MaxConcurrentTasks = 16
)
