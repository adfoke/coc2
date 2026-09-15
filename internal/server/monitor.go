package server

import (
	"go.uber.org/zap"

	"coc2/internal/protocol"
)

// handleMetricsReport persists a metrics sample. The agent id is the one the
// connection authenticated with (the read loop overwrites the wire value), so
// a peer can only ever write metrics under its own identity.
func (s *Service) handleMetricsReport(report protocol.MetricsReport) error {
	if err := s.store.SaveAgentMetrics(report); err != nil {
		s.logger.Warn("save metrics", zap.String("agent_id", report.AgentID), zap.Error(err))
		return err
	}
	s.plugins.Trigger("metrics_report", report)
	return nil
}

func (s *Service) activeTransfersCount() int {
	s.transferMu.RLock()
	defer s.transferMu.RUnlock()
	return len(s.transfers)
}
