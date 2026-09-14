// Package logging builds the zap logger shared by server and agent.
//
// By default logs go to stderr (a service manager captures them). When a
// file path is configured, output moves to that file and is size-rotated
// (age- and backup-count-bounded) so a long-running deployment cannot fill
// the disk with its own logs.
package logging

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Options configure logger construction. Zero values select defaults.
type Options struct {
	// File is the log file path; empty means stderr.
	File string
	// Level is debug|info|warn|error (case-insensitive); empty means info.
	Level string
	// MaxSizeMB rotates the active file once it exceeds this size.
	MaxSizeMB int
	// MaxBackups is the number of rotated files kept.
	MaxBackups int
	// MaxAgeDays removes rotated files older than this.
	MaxAgeDays int
	// Compress gzips rotated files (ignored for stderr).
	Compress bool
}

// Defaults for file logging; conservative for a 1-core fleet tool.
const (
	DefaultMaxSizeMB  = 100
	DefaultMaxBackups = 5
	DefaultMaxAgeDays = 28
)

// New builds a logger from opts and returns it with a Close func (flush and
// release the rotating file; no-op for stderr).
func New(opts Options) (*zap.Logger, func() error, error) {
	level, err := parseLevel(opts.Level)
	if err != nil {
		return nil, nil, err
	}

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "ts"
	encCfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	encCfg.EncodeDuration = zapcore.SecondsDurationEncoder
	encoder := zapcore.NewJSONEncoder(encCfg)

	var sink zapcore.WriteSyncer
	var realClose func() error = func() error { return nil }

	if opts.File == "" {
		sink = zapcore.Lock(os.Stderr)
	} else {
		maxSize := opts.MaxSizeMB
		if maxSize <= 0 {
			maxSize = DefaultMaxSizeMB
		}
		maxBackups := opts.MaxBackups
		if maxBackups == 0 {
			maxBackups = DefaultMaxBackups
		}
		maxAge := opts.MaxAgeDays
		if maxAge == 0 {
			maxAge = DefaultMaxAgeDays
		}
		lj := &lumberjack.Logger{
			Filename:   opts.File,
			MaxSize:    maxSize,
			MaxBackups: maxBackups,
			MaxAge:     maxAge,
			Compress:   opts.Compress,
		}
		sink = zapcore.AddSync(lj)
		realClose = lj.Close
	}

	core := zapcore.NewCore(encoder, sink, level)
	// Sampling keeps parity with zap.NewProduction and matters here: one
	// misbehaving agent pumping bad frames must not be able to fill the
	// disk through the log path. First 100 of an identical message per
	// second pass, then every 100th.
	core = zapcore.NewSamplerWithOptions(core, time.Second, 100, 100)
	logger := zap.New(core, zap.AddCaller())

	// Close is idempotent: main()'s defer and an explicit pre-exit close
	// (or a double-defer in tests) must not double-close the file.
	var closeOnce sync.Once
	return logger, func() error {
		var err error
		closeOnce.Do(func() { err = realClose() })
		return err
	}, nil
}

func parseLevel(raw string) (zapcore.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "info":
		return zapcore.InfoLevel, nil
	case "debug":
		return zapcore.DebugLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (want debug|info|warn|error)", raw)
	}
}
