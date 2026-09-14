package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/goccy/go-yaml"

	"coc2/internal/server"
)

func defaultServerConfig() server.Config {
	return server.Config{
		ListenAddr:      ":8080",
		OperatorUDSPath: "./coc2.sock",
		AuthToken:       "coc2-dev-token",
		DBPath:          "coc2.db",
		PluginDir:       "plugins",
		WriteWait:       10 * time.Second,
		PongWait:        70 * time.Second,
		PingPeriod:      25 * time.Second,
	}
}

func loadServerConfig(args []string) (server.Config, error) {
	cfg := defaultServerConfig()

	fs := flag.NewFlagSet("server", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	configPath := "config.yaml"
	listen := cfg.ListenAddr
	operatorUDS := cfg.OperatorUDSPath
	operatorListen := cfg.OperatorListen
	token := cfg.AuthToken
	apiToken := cfg.APIToken
	dbPath := cfg.DBPath
	pluginDir := cfg.PluginDir
	tlsCert := cfg.TLSCertFile
	tlsKey := cfg.TLSKeyFile
	clientCA := cfg.ClientCAFile
	requireTLS := cfg.RequireTLS
	writeWait := cfg.WriteWait
	pongWait := cfg.PongWait
	pingPeriod := cfg.PingPeriod
	logFile := cfg.LogFile
	logLevel := cfg.LogLevel

	fs.StringVar(&configPath, "config", configPath, "server config yaml path")
	fs.StringVar(&listen, "listen", listen, "agent plane listen address")
	fs.StringVar(&operatorUDS, "operator-uds", operatorUDS, "operator plane unix socket path (empty disables, then -operator-listen is required)")
	fs.StringVar(&operatorListen, "operator-listen", operatorListen, "also expose operator plane on TCP (token auth enforced)")
	fs.StringVar(&token, "token", token, "shared agent token")
	fs.StringVar(&apiToken, "api-token", apiToken, "operator plane token, defaults to -token")
	fs.StringVar(&dbPath, "db", dbPath, "sqlite db path")
	fs.StringVar(&pluginDir, "plugins", pluginDir, "plugin directory")
	fs.StringVar(&tlsCert, "tls-cert", tlsCert, "tls cert file")
	fs.StringVar(&tlsKey, "tls-key", tlsKey, "tls key file")
	fs.StringVar(&clientCA, "client-ca", clientCA, "client ca file")
	fs.BoolVar(&requireTLS, "require-tls", requireTLS, "refuse to start without TLS")
	fs.DurationVar(&writeWait, "write-wait", writeWait, "websocket write timeout")
	fs.DurationVar(&pongWait, "pong-wait", pongWait, "heartbeat timeout")
	fs.DurationVar(&pingPeriod, "ping-period", pingPeriod, "websocket ping interval")
	fs.StringVar(&logFile, "log-file", logFile, "log to this file with size rotation (default: stderr)")
	fs.StringVar(&logLevel, "log-level", logLevel, "debug|info|warn|error")

	if err := fs.Parse(args); err != nil {
		return server.Config{}, err
	}

	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})

	if err := mergeServerConfigFile(&cfg, configPath, visited["config"]); err != nil {
		return server.Config{}, err
	}

	if visited["listen"] {
		cfg.ListenAddr = listen
	}
	if visited["operator-uds"] {
		cfg.OperatorUDSPath = operatorUDS
	}
	if visited["operator-listen"] {
		cfg.OperatorListen = operatorListen
	}
	if visited["token"] {
		cfg.AuthToken = token
	}
	if visited["api-token"] {
		cfg.APIToken = apiToken
	}
	if visited["db"] {
		cfg.DBPath = dbPath
	}
	if visited["plugins"] {
		cfg.PluginDir = pluginDir
	}
	if visited["tls-cert"] {
		cfg.TLSCertFile = tlsCert
	}
	if visited["tls-key"] {
		cfg.TLSKeyFile = tlsKey
	}
	if visited["client-ca"] {
		cfg.ClientCAFile = clientCA
	}
	if visited["require-tls"] {
		cfg.RequireTLS = requireTLS
	}
	if visited["write-wait"] {
		cfg.WriteWait = writeWait
	}
	if visited["pong-wait"] {
		cfg.PongWait = pongWait
	}
	if visited["ping-period"] {
		cfg.PingPeriod = pingPeriod
	}
	if visited["log-file"] {
		cfg.LogFile = logFile
	}
	if visited["log-level"] {
		cfg.LogLevel = logLevel
	}

	return cfg, nil
}

func mergeServerConfigFile(cfg *server.Config, path string, required bool) error {
	if path == "" {
		return nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && !required {
			return nil
		}
		return fmt.Errorf("read config yaml: %w", err)
	}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return fmt.Errorf("decode config yaml: %w", err)
	}
	return nil
}
