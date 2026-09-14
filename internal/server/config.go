package server

import "time"

type Config struct {
	ListenAddr string `yaml:"listen"`
	AuthToken  string `yaml:"token"`
	APIToken   string `yaml:"api_token"`
	DBPath     string `yaml:"db"`
	PluginDir  string `yaml:"plugins"`

	// OperatorUDSPath is the Unix socket serving the operator (CLI/API)
	// plane. It defaults to ./coc2.sock; filesystem permissions are the
	// access boundary, so no token is required on this plane.
	OperatorUDSPath string `yaml:"operator_uds"`
	// OperatorListen additionally exposes the operator plane on TCP.
	// When set, token auth is enforced for every request on it.
	OperatorListen string `yaml:"operator_listen"`

	TLSCertFile  string `yaml:"tls_cert"`
	TLSKeyFile   string `yaml:"tls_key"`
	ClientCAFile string `yaml:"client_ca"`

	RequireTLS bool `yaml:"require_tls"`

	WriteWait  time.Duration `yaml:"write_wait"`
	PongWait   time.Duration `yaml:"pong_wait"`
	PingPeriod time.Duration `yaml:"ping_period"`

	// Logging. LogFile empty keeps logs on stderr (a service manager
	// captures them); when set, logs go to that file with size-based
	// rotation so a long-running server cannot fill the disk with itself.
	LogFile       string `yaml:"log_file"`
	LogLevel      string `yaml:"log_level"`
	LogMaxSizeMB  int    `yaml:"log_max_size_mb"`
	LogMaxBackups int    `yaml:"log_max_backups"`
	LogMaxAgeDays int    `yaml:"log_max_age_days"`
	LogCompress   bool   `yaml:"log_compress"`

	// AuditRetentionDays bounds growth of the audit tables (oplog,
	// transfer_audit) and the task/result history. 0 (default) keeps
	// everything forever: an ops tool must never delete evidence the
	// operator did not ask it to delete.
	AuditRetentionDays int `yaml:"audit_retention_days"`
}
