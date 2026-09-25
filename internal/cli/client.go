// Package cli implements the coc2 operator command line client. It is
// designed for programmatic use: compact JSON on stdout, JSON errors on
// stderr, stable exit codes, and no interactive prompts.
package cli

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Exit codes (stable contract, documented in the schema command).
const (
	ExitOK      = 0
	ExitFailure = 1 // server-side failure, task failure, or transport error
	ExitConnect = 2 // cannot reach the operator plane
	ExitAuth    = 3 // rejected by token auth
	ExitUsage   = 4 // bad flags / arguments
)

// Error carries the exit code a failure maps to.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Exit    int    `json:"-"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func fail(code, msg string, exit int) error {
	return &Error{Code: code, Message: msg, Exit: exit}
}

// Client talks HTTP to a server operator plane endpoint. Target is either a
// URL (http://host:port — the TCP escape hatch) or a filesystem path to the
// Unix socket (the default plane).
type Client struct {
	target   string
	token    string
	timeout  time.Duration
	insecure bool
	// TLS material for the operator plane. A server started with client_ca
	// requires a client certificate on every operator connection, including
	// the TCP escape hatch, and without these flags the CLI simply could not
	// reach such a server over TCP at all — leaving the local socket as the
	// only option.
	caCert     string
	clientCert string
	clientKey  string
}

// TLSFiles names the optional certificate material for the operator plane.
type TLSFiles struct {
	CACert     string
	ClientCert string
	ClientKey  string
}

func NewClient(target, token string, timeout time.Duration, insecure bool, tlsFiles TLSFiles) *Client {
	return &Client{
		target:     target,
		token:      token,
		timeout:    timeout,
		insecure:   insecure,
		caCert:     tlsFiles.CACert,
		clientCert: tlsFiles.ClientCert,
		clientKey:  tlsFiles.ClientKey,
	}
}

// tlsClientConfig assembles the operator-plane TLS configuration, or nil when
// no material was supplied (then Go's defaults apply).
func (c *Client) tlsClientConfig() (*tls.Config, error) {
	if c.caCert == "" && c.clientCert == "" && c.clientKey == "" {
		return nil, nil
	}

	cfg := &tls.Config{}
	if c.insecure {
		cfg.InsecureSkipVerify = true
	}
	if c.caCert != "" {
		pem, err := os.ReadFile(c.caCert)
		if err != nil {
			return nil, fail("config", fmt.Sprintf("read -ca-cert: %v", err), ExitUsage)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fail("config", "-ca-cert contains no PEM certificate", ExitUsage)
		}
		cfg.RootCAs = pool
	}
	if c.clientCert != "" || c.clientKey != "" {
		if c.clientCert == "" || c.clientKey == "" {
			return nil, fail("config", "-client-cert and -client-key must be given together", ExitUsage)
		}
		cert, err := tls.LoadX509KeyPair(c.clientCert, c.clientKey)
		if err != nil {
			return nil, fail("config", fmt.Sprintf("load client certificate: %v", err), ExitUsage)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// IsUDS reports whether the target is a Unix socket path rather than a URL.
func IsUDS(target string) bool {
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		return false
	}
	// Anything without a URL scheme and without host:port shape is a path.
	return !strings.Contains(target, "://") && !strings.Contains(target, ":")
}

func (c *Client) httpClient() (*http.Client, string, error) {
	base := c.target
	if IsUDS(c.target) {
		path := c.target
		if strings.HasPrefix(path, "~") {
			home, err := os.UserHomeDir()
			if err != nil {
				return nil, "", fail("config", "cannot expand ~ in server path", ExitUsage)
			}
			path = home + path[1:]
		}
		return &http.Client{
			Timeout: c.timeout,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", path)
				},
			},
		}, "http://unix", nil
	}

	tlsCfg, err := c.tlsClientConfig()
	if err != nil {
		return nil, "", err
	}
	if strings.HasPrefix(base, "https://") {
		if tlsCfg == nil && c.insecure {
			tlsCfg = &tls.Config{InsecureSkipVerify: true}
		}
		if tlsCfg != nil {
			return &http.Client{
				Timeout:   c.timeout,
				Transport: &http.Transport{TLSClientConfig: tlsCfg},
			}, strings.TrimRight(base, "/"), nil
		}
	}
	return &http.Client{Timeout: c.timeout}, strings.TrimRight(base, "/"), nil
}

// Get performs a GET request and returns decoded JSON.
func (c *Client) Get(path string, out any) error {
	return c.Do(http.MethodGet, path, nil, out)
}

// Post performs a JSON POST request and returns decoded JSON. body may be nil.
func (c *Client) Post(path string, body any, out any) error {
	return c.Do(http.MethodPost, path, body, out)
}

// Do is the single request path: auth header, error mapping, JSON decode.
func (c *Client) Do(method, path string, body any, out any) error {
	httpClient, base, err := c.httpClient()
	if err != nil {
		return err
	}

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fail("encode", err.Error(), ExitUsage)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, base+path, reader)
	if err != nil {
		return fail("bad_request", err.Error(), ExitUsage)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) || os.IsTimeout(err) || strings.Contains(err.Error(), "timeout") {
			return fail("timeout", err.Error(), ExitConnect)
		}
		return fail("connect", err.Error(), ExitConnect)
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return fail("read_response", err.Error(), ExitConnect)
	}

	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return fail("unauthorized", serverMessage(payload), ExitAuth)
	case resp.StatusCode >= 400:
		return fail("server", fmt.Sprintf("%s %s: %d %s", method, path, resp.StatusCode, serverMessage(payload)), ExitFailure)
	}

	if out != nil {
		if err := json.Unmarshal(payload, out); err != nil {
			return fail("decode_response", err.Error(), ExitFailure)
		}
	}
	return nil
}

func serverMessage(payload []byte) string {
	var v struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(payload, &v) == nil && v.Error != "" {
		return v.Error
	}
	msg := strings.TrimSpace(string(payload))
	if len(msg) > 200 {
		msg = msg[:200] + "…"
	}
	return msg
}

// Emit writes a compact (or pretty when asked) JSON document to stdout.
func Emit(w io.Writer, v any, pretty bool) error {
	enc := json.NewEncoder(w)
	if pretty {
		enc.SetIndent("", "  ")
	}
	return enc.Encode(v)
}

// EmitError writes the JSON error envelope to stderr.
func EmitError(w io.Writer, err error) {
	var e *Error
	if errors.As(err, &e) {
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": e.Code, "message": e.Message}})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "internal", "message": err.Error()}})
}

// ExitCode maps an error to the process exit code.
func ExitCode(err error) int {
	var e *Error
	if errors.As(err, &e) {
		return e.Exit
	}
	if err != nil {
		return ExitFailure
	}
	return ExitOK
}
