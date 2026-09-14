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
)

func (s *Service) reapLoop() {
	defer close(s.reaperDone)
	ticker := time.NewTicker(taskReapInterval)
	defer ticker.Stop()

	// One retention pass at startup, then once a day on the same ticker.
	lastPrune := time.Now().UTC()
	s.pruneAuditsIfNeeded(lastPrune)

	for {
		select {
		case <-s.reaperStop:
			return
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
