package server

import "time"

// DefaultAuthToken is the development token baked into config.yaml and the
// CLI defaults. It is public knowledge, so a server that still uses it is
// effectively unauthenticated: cmd/server refuses to start with it unless the
// operator explicitly opts in.
const DefaultAuthToken = "coc2-dev-token"

// DefaultAuditRetentionDays is how long finalized task history, completed
// transfer audit rows, and oplog entries are kept when the operator has not
// chosen a window. Two weeks is enough to answer "what happened during the
// last incident" without letting the tables grow without bound.
//
// Zero still means "keep everything forever"; that is an explicit choice and
// is distinguishable from an absent setting (see mergeServerConfigFile).
const DefaultAuditRetentionDays = 14

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
	// transfer_audit) and the task/result history. Defaults to
	// DefaultAuditRetentionDays (14); set it to 0 to keep everything
	// forever, which is an explicit operator choice.
	AuditRetentionDays int `yaml:"audit_retention_days"`

	// AllowDevToken permits starting with DefaultAuthToken. Off by default;
	// see validateServerConfig in cmd/server, which is what enforces it.
	AllowDevToken bool `yaml:"allow_dev_token"`
}
