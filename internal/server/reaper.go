package server

import (
	"time"

	"go.uber.org/zap"
)

const (
	// taskReapInterval is how often the reaper scans for stuck tasks.
	taskReapInterval = 30 * time.Second
	// taskReapGrace is extra time beyond a task's own timeout before the server
	// finalizes it, accounting for network latency and in-flight results.
	taskReapGrace = 30 * time.Second
	// taskDispatchInterval is the backstop sweep for deferred tasks. It is no
	// longer the release mechanism — releaseTimer fires at each task's exact
	// due moment (see reapLoop) — but a coarse 1s pass still picks up a task
	// whose send failed or whose wakeup was lost, so recovery does not wait for
	// the agent to reconnect. Polling alone cannot stagger a short window: a 3s
	// window of 50 targets averages one task per 60ms, and quantizing that to a
	// 1s tick stacks ~17 tasks into each bucket, which is the herd spread
	// exists to prevent.
	taskDispatchInterval = time.Second
	// releaseIdleSleep is how long the release timer sleeps when nothing is
	// deferred. It is not a poll: a newly queued deferral re-arms the timer via
	// nudgeReleaseScheduler, so this only backstops a lost wakeup.
	releaseIdleSleep = time.Hour
)

// releaseSleep is how long the scheduler waits before the next deferred task is
// due. It deliberately does not round: a task due in 300ms sleeps 300ms, not a
// whole tick, which is the entire point of exact wakeup. A task already past
// its moment (or one due right now) clamps to zero so the timer fires at once
// rather than in the past.
func releaseSleep(now, next time.Time) time.Duration {
	if d := next.Sub(now); d > 0 {
		return d
	}
	return 0
}

func (s *Service) reapLoop() {
	defer close(s.reaperDone)
	ticker := time.NewTicker(taskReapInterval)
	defer ticker.Stop()

	// The backstop sweep shares this loop's lifecycle (reaperStop/reaperDone)
	// rather than owning a goroutine of its own.
	dispatchTicker := time.NewTicker(taskDispatchInterval)
	defer dispatchTicker.Stop()

	// releaseTimer is the primary release mechanism: it fires at the exact
	// release_at of the next deferred task. Everything here stays on this one
	// goroutine, so the timer needs no lock of its own and Shutdown keeps
	// waiting on a single reaperDone.
	releaseTimer := time.NewTimer(releaseIdleSleep)
	defer releaseTimer.Stop()

	// One retention pass at startup, then once a day on the same ticker.
	lastPrune := time.Now().UTC()
	s.pruneAuditsIfNeeded(lastPrune)

	// arm recomputes the next wakeup and (re)points the timer at it. It first
	// flushes everything already due, so the timer only ever sleeps toward
	// genuinely future work — arming for an already-due task would fire
	// immediately and spin. Every path that could change "what is next"
	// (expiry, new deferral, backstop sweep) calls this, which is what keeps
	// the single timer honest.
	arm := func() {
		now := time.Now().UTC()
		s.dispatchReadyTasks(now)

		// Canonical safe re-arm: Stop reports false when the timer already
		// fired, and the non-blocking drain clears that pending value so the
		// following Reset cannot be mistaken for it.
		if !releaseTimer.Stop() {
			select {
			case <-releaseTimer.C:
			default:
			}
		}

		next, ok, err := s.store.NextDeferredRelease(now)
		switch {
		case err != nil:
			s.logger.Warn("scan deferred release", zap.Error(err))
			releaseTimer.Reset(releaseIdleSleep)
		case ok:
			releaseTimer.Reset(releaseSleep(now, next))
		default:
			releaseTimer.Reset(releaseIdleSleep)
		}
	}
	arm()

	for {
		select {
		case <-s.reaperStop:
			return
		case <-releaseTimer.C:
			// A deferred task came due: flush it and aim at the next one.
			arm()
		case <-s.releaseWake:
			// A new deferral may be earlier than the one the timer is aimed
			// at, so re-derive the wakeup from scratch.
			arm()
		case <-dispatchTicker.C:
			// Backstop only; the timer above is the normal path.
			s.dispatchReadyTasks(time.Now().UTC())
		case <-ticker.C:
			now := time.Now().UTC()
			if n, err := s.reapTimedOutTasks(now); err != nil {
				s.logger.Warn("reap tasks", zap.Error(err))
			} else if n > 0 {
				s.logger.Info("reaped tasks", zap.Int("count", n))
			}
			if n := s.reapStalledTransfers(now); n > 0 {
				s.logger.Info("reaped stalled transfers", zap.Int("count", n))
			}
			if n := s.reapCanceledTransfers(now); n > 0 {
				s.logger.Info("finalized unconfirmed transfer cancels", zap.Int("count", n))
			}
			if lastPrune.IsZero() || now.Sub(lastPrune) >= pruneInterval {
				lastPrune = now
				s.pruneAuditsIfNeeded(now)
			}
		}
	}
}

// reapTimedOutTasks finalizes dispatched or cancel-requested tasks that have
// exceeded their timeout (plus grace) without producing a result. It returns
// the number of tasks finalized.
func (s *Service) reapTimedOutTasks(now time.Time) (int, error) {
	tasks, err := s.store.DispatchedTasks()
	if err != nil {
		return 0, err
	}

	reaped := 0
	for _, dt := range tasks {
		if dt.DispatchedAt.IsZero() {
			continue
		}
		deadline := dt.DispatchedAt.Add(time.Duration(dt.Task.TimeoutSecs)*time.Second + taskReapGrace)
		if now.Before(deadline) {
			continue
		}

		var (
			ok  bool
			err error
		)
		switch dt.State {
		case "cancel_requested":
			ok, err = s.store.MarkTaskCanceledAfterReap(dt.Task.ID, dt.Task.AgentID, now)
		default:
			ok, err = s.store.MarkTaskTimedOut(dt.Task.ID, dt.Task.AgentID, now)
		}
		if err != nil {
			s.logger.Warn("reap task", zap.String("task_id", dt.Task.ID), zap.String("state", dt.State), zap.Error(err))
			continue
		}
		if ok {
			reaped++
		}
	}
	return reaped, nil
}
