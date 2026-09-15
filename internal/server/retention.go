package server

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Audit retention. On by default with a two-week window; setting
// audit_retention_days to 0 keeps everything forever, which is an explicit
// operator choice. A prune runs at startup and once per day afterwards.

const pruneInterval = 24 * time.Hour

// terminalTaskStates (defined in store.go) lists finalized task states:
// history, eligible for retention. Anything else (queued/dispatched/
// cancel_requested) is live work and is never pruned regardless of age.

// PruneAudits deletes audit/history rows strictly older than the cutoff and
// returns per-table removal counts. Ordering matters: terminal tasks go
// first, then their results are picked up as orphans in one pass.
func (s *Store) PruneAudits(olderThan time.Time) (map[string]int64, error) {
	cutoff := olderThan.UTC().Format(time.RFC3339Nano)

	prunes := []struct {
		key  string
		stmt string
	}{
		{"tasks", `DELETE FROM tasks WHERE state IN (` + terminalTaskStates + `) AND created_at < ?`},
		{"task_results", `DELETE FROM task_results WHERE task_id NOT IN (SELECT id FROM tasks)`},
		{"transfers", `DELETE FROM transfer_audit WHERE completed_at IS NOT NULL AND completed_at < ?`},
		{"oplog", `DELETE FROM oplog WHERE ts < ?`},
	}

	out := map[string]int64{}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for _, p := range prunes {
		args := []any{cutoff}
		if p.key == "task_results" {
			args = nil // orphan sweep, age-independent
		}
		res, err := tx.Exec(p.stmt, args...)
		if err != nil {
			return nil, fmt.Errorf("prune %s: %w", p.key, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		out[p.key] = n
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// pruneAuditsIfNeeded runs one retention pass when retention is configured.
func (s *Service) pruneAuditsIfNeeded(now time.Time) {
	if s.cfg.AuditRetentionDays <= 0 {
		return
	}
	cutoff := now.AddDate(0, 0, -s.cfg.AuditRetentionDays)
	counts, err := s.store.PruneAudits(cutoff)
	if err != nil {
		s.logger.Warn("prune audits", zap.Error(err))
		return
	}
	total := int64(0)
	for _, n := range counts {
		total += n
	}
	if total > 0 {
		s.logger.Info("pruned audits",
			zap.Int("retention_days", s.cfg.AuditRetentionDays),
			zap.Int64("tasks", counts["tasks"]),
			zap.Int64("task_results", counts["task_results"]),
			zap.Int64("transfers", counts["transfers"]),
			zap.Int64("oplog", counts["oplog"]))
	}
}
