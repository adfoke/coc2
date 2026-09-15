package agent

import (
	"context"
	"fmt"
	"testing"

	"coc2/internal/protocol"
)

func TestTryReserveTaskAdmission(t *testing.T) {
	c := &Client{running: make(map[string]context.CancelFunc)}

	// Fresh ids fill the pool up to the cap.
	for i := range maxConcurrentTasks {
		task := protocol.Task{ID: fmt.Sprintf("task-%02d", i)}
		if got := c.tryReserveTask(task, func() {}); got != taskReserved {
			t.Fatalf("reserving task %d = %v, want taskReserved", i, got)
		}
	}
	if len(c.running) != maxConcurrentTasks {
		t.Fatalf("running = %d, want %d", len(c.running), maxConcurrentTasks)
	}

	// One past the cap must be refused, and must not be recorded.
	overflow := protocol.Task{ID: "task-overflow"}
	if got := c.tryReserveTask(overflow, func() {}); got != taskAtCapacity {
		t.Fatalf("reserving past the cap = %v, want taskAtCapacity", got)
	}
	if _, ok := c.running[overflow.ID]; ok {
		t.Fatalf("a refused task must not occupy a slot")
	}

	// A retransmission of a running task is a duplicate, not a new slot.
	dup := protocol.Task{ID: "task-00"}
	if got := c.tryReserveTask(dup, func() {}); got != taskDuplicate {
		t.Fatalf("re-reserving a running task = %v, want taskDuplicate", got)
	}
	if len(c.running) != maxConcurrentTasks {
		t.Fatalf("duplicate changed pool size: %d", len(c.running))
	}

	// Freeing a slot admits the previously refused task.
	c.unregisterTask("task-07")
	if got := c.tryReserveTask(overflow, func() {}); got != taskReserved {
		t.Fatalf("after freeing a slot = %v, want taskReserved", got)
	}
}

// TestConcurrencyCapIsBounded guards the configured value itself: the point of
// the cap is that it exists and is small enough to matter.
func TestConcurrencyCapIsBounded(t *testing.T) {
	if maxConcurrentTasks <= 0 {
		t.Fatalf("maxConcurrentTasks = %d, effectively unbounded", maxConcurrentTasks)
	}
	if maxConcurrentTasks > 256 {
		t.Fatalf("maxConcurrentTasks = %d is too high to protect the agent host", maxConcurrentTasks)
	}
}

func TestMaxInboundFrameBytesMatchesServer(t *testing.T) {
	// The agent must not accept frames the server would never send.
	if maxInboundFrameBytes != 16<<20 {
		t.Fatalf("maxInboundFrameBytes = %d, want 16 MiB to match the server", maxInboundFrameBytes)
	}
}

func TestAgentWriteTimeoutIsFinite(t *testing.T) {
	if agentWriteTimeout <= 0 {
		t.Fatalf("agentWriteTimeout = %v, a blocking write would hold writeMu forever", agentWriteTimeout)
	}
}
