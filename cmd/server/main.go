package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"coc2/internal/logging"
	"coc2/internal/server"
)

func main() {
	cfg, err := loadServerConfig(os.Args[1:])
	if err != nil {
		panic(err)
	}
	if err := validateServerConfig(cfg, cfg.AllowDevToken); err != nil {
		panic(err)
	}

	logger, closeLogs, err := logging.New(logging.Options{
		File:       cfg.LogFile,
		Level:      cfg.LogLevel,
		MaxSizeMB:  cfg.LogMaxSizeMB,
		MaxBackups: cfg.LogMaxBackups,
		MaxAgeDays: cfg.LogMaxAgeDays,
		Compress:   cfg.LogCompress,
	})
	if err != nil {
		panic(err)
	}
	// Flush before the file closes (defers run LIFO).
	defer closeLogs()
	defer logger.Sync()

	svc, err := server.New(cfg, logger)
	if err != nil {
		logger.Fatal("create server", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.Run()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := svc.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown server", zap.Error(err))
		}
	case err := <-errCh:
		if err != nil {
			logger.Fatal("run server", zap.Error(err))
		}
	}
}
