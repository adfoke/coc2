package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"coc2/internal/agent"
	"coc2/internal/logging"
)

func main() {
	cfg := agent.Config{}
	var tags string
	var logOpts logging.Options
	flag.StringVar(&cfg.ServerURL, "server", "ws://127.0.0.1:8080/ws/agent", "server websocket url")
	flag.StringVar(&cfg.Token, "token", "coc2-dev-token", "shared agent token")
	flag.StringVar(&cfg.AgentID, "agent-id", "", "agent id")
	flag.StringVar(&tags, "tags", "", "comma separated tags")
	flag.DurationVar(&cfg.HeartbeatInterval, "heartbeat", 30*time.Second, "heartbeat interval")
	flag.DurationVar(&cfg.MaxBackoff, "max-backoff", 30*time.Second, "max reconnect backoff")
	flag.StringVar(&cfg.ServerName, "server-name", "", "tls server name")
	flag.StringVar(&cfg.CACertFile, "ca-cert", "", "ca cert file")
	flag.StringVar(&cfg.ClientCertFile, "client-cert", "", "client cert file")
	flag.StringVar(&cfg.ClientKeyFile, "client-key", "", "client key file")
	flag.StringVar(&logOpts.File, "log-file", "", "log to this file with size rotation (default: stderr)")
	flag.StringVar(&logOpts.Level, "log-level", "info", "debug|info|warn|error")
	flag.IntVar(&logOpts.MaxSizeMB, "log-max-size-mb", logging.DefaultMaxSizeMB, "rotate the log file above this size (MB)")
	flag.IntVar(&logOpts.MaxBackups, "log-max-backups", logging.DefaultMaxBackups, "rotated log files to keep")
	flag.IntVar(&logOpts.MaxAgeDays, "log-max-age-days", logging.DefaultMaxAgeDays, "days to keep rotated log files")
	flag.BoolVar(&logOpts.Compress, "log-compress", false, "gzip rotated log files")
	flag.Parse()

	if tags != "" {
		cfg.Tags = strings.Split(tags, ",")
	}

	logger, closeLogs, err := logging.New(logOpts)
	if err != nil {
		panic(err)
	}
	// Flush before the file closes (defers run LIFO, so Sync must be
	// registered after CloseLogs to run before it).
	defer closeLogs()
	defer logger.Sync()

	client, err := agent.New(cfg, logger)
	if err != nil {
		logger.Fatal("create agent", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := client.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Fatal("run agent", zap.Error(err))
	}
}
