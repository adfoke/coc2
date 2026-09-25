package common

// Per-task limits.
//
// These are properties of the agent implementation, but they are also part of
// the operator-facing contract: they decide what a result is allowed to
// contain and why a dispatch can come back failed. The CLI publishes them in
// `coc2 schema`, so they live here rather than inside one binary — a number
// copied into the schema output would drift from the agent that enforces it,
// and a limit nobody can look up is how "my output ends with [output
// truncated]" becomes a support question.
const (
	// MaxTaskOutputBytes caps each of a task's captured stdout and stderr.
	// Output past the cap is discarded and a "[output truncated]" line is
	// appended, so an operator can distinguish a short result from a cut-off
	// one instead of silently losing the tail.
	MaxTaskOutputBytes = 1 << 20

	// MaxConcurrentTasks bounds how many shell tasks one agent runs at once.
	// Every task is a process group plus up to 2 MiB of captured output, so
	// without a cap a burst of dispatches (or a compromised server) could
	// exhaust the agent host. Excess dispatches are answered with a failed
	// result rather than silently dropped, so the operator sees why nothing
	// ran.
	MaxConcurrentTasks = 16
)
