package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewStderrDefault(t *testing.T) {
	logger, closeFn, err := New(Options{})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer closeFn()
	if logger == nil {
		t.Fatal("nil logger")
	}
	logger.Info("to stderr")
}

func TestLevelFiltering(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	logger, closeFn, err := New(Options{File: path, Level: "warn"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	logger.Info("info-should-be-dropped")
	logger.Warn("warn-should-land")
	if err := closeFn(); err != nil {
		t.Fatalf("close: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Contains(string(raw), "info-should-be-dropped") {
		t.Fatal("warn level must drop info records")
	}
	if !strings.Contains(string(raw), "warn-should-land") {
		t.Fatal("warn record missing")
	}
	// JSON with caller field
	var rec map[string]any
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	for _, k := range []string{"ts", "level", "msg", "caller"} {
		if _, ok := rec[k]; !ok {
			t.Fatalf("missing field %q in %+v", k, rec)
		}
	}
}

func TestUnknownLevelRejected(t *testing.T) {
	if _, _, err := New(Options{Level: "verbose"}); err == nil {
		t.Fatal("bogus level must error at construction")
	}
}

func TestRotationCreatesBackups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rot.log")
	logger, closeFn, err := New(Options{File: path, MaxSizeMB: 1, MaxBackups: 3})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer closeFn()

	// ~1.6 MB of payload forces at least one rotation at MaxSize=1MB.
	// Each message must be unique: the core sampler drops repeats of an
	// identical (level, msg) pair beyond 100/sec, which would starve the
	// rotation before the size threshold is reached.
	padding := strings.Repeat("x", 1024)
	for i := 0; i < 1600; i++ {
		logger.Info(fmt.Sprintf("bulk-%d %s", i, padding))
	}
	if err := closeFn(); err != nil {
		t.Fatalf("close: %v", err)
	}

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var rotated int
	for _, e := range entries {
		if strings.Contains(e.Name(), ".log") && e.Name() != "rot.log" {
			rotated++
		}
	}
	if rotated == 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("no rotated backups created; dir=%v", names)
	}
	if rotated > 3 {
		t.Fatalf("MaxBackups=3 exceeded: %d backups", rotated)
	}
}

func TestFileModeNotWorldReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secure.log")
	logger, closeFn, err := New(Options{File: path})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	logger.Info("secret-ish")
	closeFn()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		t.Fatalf("log file is group/world readable: %o", fi.Mode().Perm())
	}
}
