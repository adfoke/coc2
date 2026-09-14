package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadServerConfigFromYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	raw := []byte("listen: \":9090\"\ntoken: from-yaml\napi_token: api-yaml\ndb: ./data/app.db\nplugins: ./hooks\nwrite_wait: 3s\npong_wait: 50s\nping_period: 15s\n")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadServerConfig([]string{"-config", path})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ListenAddr != ":9090" || cfg.AuthToken != "from-yaml" || cfg.APIToken != "api-yaml" {
		t.Fatalf("unexpected basic config: %+v", cfg)
	}
	if cfg.DBPath != "./data/app.db" || cfg.PluginDir != "./hooks" {
		t.Fatalf("unexpected paths: %+v", cfg)
	}
	if cfg.WriteWait != 3*time.Second || cfg.PongWait != 50*time.Second || cfg.PingPeriod != 15*time.Second {
		t.Fatalf("unexpected durations: %+v", cfg)
	}
}

func TestLoadServerConfigFlagsOverrideYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	raw := []byte("listen: \":9090\"\ntoken: from-yaml\napi_token: api-yaml\n")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadServerConfig([]string{"-config", path, "-listen", ":8088", "-token", "from-flag"})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ListenAddr != ":8088" {
		t.Fatalf("expected flag listen override, got %q", cfg.ListenAddr)
	}
	if cfg.AuthToken != "from-flag" {
		t.Fatalf("expected flag token override, got %q", cfg.AuthToken)
	}
	if cfg.APIToken != "api-yaml" {
		t.Fatalf("unexpected api token override, got %q", cfg.APIToken)
	}
}

func TestLoadServerConfigWithoutYAMLUsesDefaults(t *testing.T) {
	cfg, err := loadServerConfig(nil)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ListenAddr != ":8080" || cfg.AuthToken != "coc2-dev-token" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.DBPath != "coc2.db" || cfg.PluginDir != "plugins" {
		t.Fatalf("unexpected default paths: %+v", cfg)
	}
}

func TestLoadServerConfigLoggingFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	raw := []byte("listen: \":9090\"\ntoken: t\nlog_file: ./var/log/coc2.log\nlog_level: warn\nlog_max_size_mb: 32\nlog_max_backups: 2\nlog_max_age_days: 7\nlog_compress: true\naudit_retention_days: 90\n")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := loadServerConfig([]string{"-config", path})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.LogFile != "./var/log/coc2.log" || cfg.LogLevel != "warn" {
		t.Fatalf("yaml logging not parsed: %+v", cfg)
	}
	if cfg.LogMaxSizeMB != 32 || cfg.LogMaxBackups != 2 || cfg.LogMaxAgeDays != 7 || !cfg.LogCompress {
		t.Fatalf("yaml rotation knobs not parsed: %+v", cfg)
	}
	if cfg.AuditRetentionDays != 90 {
		t.Fatalf("audit_retention_days = %d", cfg.AuditRetentionDays)
	}

	// CLI flag must win over YAML (same contract as every other field).
	cfg, err = loadServerConfig([]string{"-config", path, "-log-file", "/tmp/other.log", "-log-level", "error"})
	if err != nil {
		t.Fatalf("load with flags: %v", err)
	}
	if cfg.LogFile != "/tmp/other.log" || cfg.LogLevel != "error" {
		t.Fatalf("flag override failed: %+v", cfg)
	}
}

func TestLoadServerConfigLoggingDefaultsOff(t *testing.T) {
	cfg, err := loadServerConfig(nil)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.LogFile != "" {
		t.Fatalf("default must log to stderr, got file %q", cfg.LogFile)
	}
	if cfg.AuditRetentionDays != 0 {
		t.Fatalf("retention must default to off (0), got %d", cfg.AuditRetentionDays)
	}
}
