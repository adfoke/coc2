package server

import (
	"testing"
	"time"

	"coc2/internal/protocol"
)

// TestReleaseSleepIsExact guards the property that motivated exact wakeups: the
// scheduler must sleep to a sub-second release, not round it up to a tick. A
// 300ms delay that became 1s would land on the very tick the spread exists to
// avoid. Pure arithmetic, so the assertion can be tight without timing flake.
func TestReleaseSleepIsExact(t *testing.T) {
	now := time.Now().UTC()

	if d := releaseSleep(now, now.Add(300*time.Millisecond)); d != 300*time.Millisecond {
		t.Fatalf("releaseSleep(now, now+300ms) = %v, want exactly 300ms", d)
	}
	if d := releaseSleep(now, now.Add(1500*time.Millisecond)); d != 1500*time.Millisecond {
		t.Fatalf("releaseSleep(now, now+1500ms) = %v, want 1500ms", d)
	}

	// A due (or already past) release clamps to zero so the timer fires at once
	// rather than being reset to a negative duration.
	if d := releaseSleep(now, now); d != 0 {
		t.Fatalf("releaseSleep(now, now) = %v, want 0", d)
	}
	if d := releaseSleep(now, now.Add(-time.Second)); d != 0 {
		t.Fatalf("releaseSleep past = %v, want 0", d)
	}
}

// TestNudgeReleaseSchedulerNeverBlocks pins the contract that keeps a request
// handler off the scheduler's critical path: with the single-slot buffer full,
// further nudges must return immediately (the default branch) instead of
// blocking. A panic here would mean the buffered channel was not wired up.
func TestNudgeReleaseSchedulerNeverBlocks(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			svc.nudgeReleaseScheduler()
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("nudgeReleaseScheduler blocked")
	}
	// The extras were dropped, not queued: one pending wakeup is enough.
	if got := len(svc.releaseWake); got != 1 {
		t.Fatalf("releaseWake depth = %d, want 1", got)
	}
}

// startReapLoop runs the real scheduler goroutine and tears it down with the
// test, so no test leaks a goroutine.
func startReapLoop(t *testing.T, svc *Service) {
	t.Helper()
	go svc.reapLoop()
	t.Cleanup(func() {
		svc.reaperOnce.Do(func() { close(svc.reaperStop) })
		select {
		case <-svc.reaperDone:
		case <-time.After(5 * time.Second):
			t.Error("reapLoop did not stop")
		}
	})
}

// TestReleaseTimerFiresAtExactDueTime is the end-to-end guard for the change.
// Two deferrals in the same wall-clock second must go out at their own moments
// (≈300ms and ≈600ms after start), not batched together onto the 1s backstop
// sweep. Before exact wakeups, both would arrive on the same tick — the
// quantization this change removes. Tolerances are deliberately loose.
func TestReleaseTimerFiresAtExactDueTime(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	chans := map[string]chan wsFrame{}
	for _, id := range []string{"agent-1", "agent-2"} {
		ch := make(chan wsFrame, 8)
		chans[id] = ch
		svc.clients[id] = &agentConn{id: id, send: ch, done: make(chan struct{}), service: svc}
	}

	start := time.Now().UTC()
	due := []struct {
		id      string
		agent   string
		release time.Time
	}{
		{"task-early", "agent-1", start.Add(300 * time.Millisecond)},
		{"task-late", "agent-2", start.Add(600 * time.Millisecond)},
	}
	// Both deferrals are queued before the loop starts, so the initial arm
	// already sees them and no wakeup is needed.
	for _, d := range due {
		if err := svc.store.AddTask(protocol.Task{
			ID: d.id, AgentID: d.agent, Type: "shell", Command: "echo", TimeoutSecs: 5, CreatedAt: start,
		}, d.release); err != nil {
			t.Fatalf("add %s: %v", d.id, err)
		}
	}

	startReapLoop(t, svc)

	arrivals := map[string]time.Duration{}
	for _, d := range due {
		select {
		case <-chans[d.agent]:
			arrivals[d.id] = time.Since(start)
		case <-time.After(3 * time.Second):
			t.Fatalf("%s was never dispatched (release timer did not fire)", d.id)
		}
	}

	// Each went out near its own due time, not on a shared 1s tick.
	if got := arrivals["task-early"]; got < 200*time.Millisecond || got > 900*time.Millisecond {
		t.Fatalf("early task dispatched at %v, want ≈300ms (a 1s tick would land ≈1s)", got)
	}
	if got := arrivals["task-late"]; got < 500*time.Millisecond || got > 1100*time.Millisecond {
		t.Fatalf("late task dispatched at %v, want ≈600ms", got)
	}
	// The decisive check: the two did not collapse onto one shared wakeup.
	if gap := arrivals["task-late"] - arrivals["task-early"]; gap < 150*time.Millisecond {
		t.Fatalf("deferrals were batched: gap = %v, want the ≈300ms between their due times", gap)
	}
}

// TestNudgeRearmsRunningScheduler exercises the wakeup path a live batch uses:
// with the timer already aimed at a far-off deferral, submitting a nearer one
// must re-aim it at once. The near task is due well inside a second, so landing
// it faster than the 1s backstop sweep proves the nudge did the work, not the
// poll. Tolerances stay loose.
func TestNudgeRearmsRunningScheduler(t *testing.T) {
	svc, cleanup := newTestService(t)
	defer cleanup()

	ch := make(chan wsFrame, 8)
	svc.clients["agent-1"] = &agentConn{id: "agent-1", send: ch, done: make(chan struct{}), service: svc}

	// A far deferral parks the timer on something no assertion waits for.
	if _, err := svc.createTask("agent-1", "echo far", 5, 0, time.Now().UTC().Add(10*time.Second)); err != nil {
		t.Fatalf("queue far task: %v", err)
	}

	startReapLoop(t, svc)
	// Let the loop arm to the far deferral before the nudge, so what follows
	// tests re-arming rather than the initial arm.
	time.Sleep(100 * time.Millisecond)

	// A nearer deferral via the normal path: dispatchOrQueue nudges the loop.
	if _, err := svc.createTask("agent-1", "echo near", 5, 0, time.Now().UTC().Add(200*time.Millisecond)); err != nil {
		t.Fatalf("queue near task: %v", err)
	}
	submitted := time.Now()

	// The near task must arrive promptly. Its release is ≈200ms out, while the
	// earliest a backstop sweep could deliver it is a full second after the
	// loop started — so landing well inside that window is the nudge's doing.
	select {
	case frame := <-ch:
		msg, err := protocol.DecodeFrame(frame.opcode, frame.data)
		if err != nil {
			t.Fatalf("decode dispatch: %v", err)
		}
		sent, err := protocol.PayloadOf[protocol.Task](msg)
		if err != nil || sent.Command != "echo near" {
			t.Fatalf("near task not dispatched first: %+v err=%v", sent, err)
		}
		if d := time.Since(submitted); d > 700*time.Millisecond {
			t.Fatalf("near task took %v after submit, want the nudge to fire it before the 1s sweep can", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("near task was never dispatched")
	}
}
