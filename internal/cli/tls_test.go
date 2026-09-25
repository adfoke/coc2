package cli

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeTestCA generates a self-signed CA and returns the paths to its
// certificate and a client keypair signed by it.
func writeTestCA(t *testing.T) (caPath, certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	caPath = filepath.Join(dir, "ca.crt")
	writePEM(t, caPath, "CERTIFICATE", caDER)

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caTmpl, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}
	certPath = filepath.Join(dir, "client.crt")
	writePEM(t, certPath, "CERTIFICATE", clientDER)

	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	keyPath = filepath.Join(dir, "client.key")
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)

	return caPath, certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}

// TestTLSClientConfigNoMaterial: without any TLS flags the client must keep
// Go's defaults rather than install an empty (and therefore rejecting) config.
func TestTLSClientConfigNoMaterial(t *testing.T) {
	c := NewClient("https://example:8443", "tok", time.Second, false, TLSFiles{})
	cfg, err := c.tlsClientConfig()
	if err != nil {
		t.Fatalf("tlsClientConfig: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected no TLS override, got %+v", cfg)
	}
}

// TestTLSClientConfigWithMaterial covers the mTLS path: a server started with
// client_ca requires a client certificate on every operator connection, and
// before these flags existed the CLI could not reach such a server over TCP.
func TestTLSClientConfigWithMaterial(t *testing.T) {
	caPath, certPath, keyPath := writeTestCA(t)

	c := NewClient("https://example:8443", "tok", time.Second, false, TLSFiles{
		CACert:     caPath,
		ClientCert: certPath,
		ClientKey:  keyPath,
	})
	cfg, err := c.tlsClientConfig()
	if err != nil {
		t.Fatalf("tlsClientConfig: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected a TLS config")
	}
	if cfg.RootCAs == nil {
		t.Fatal("CA bundle not installed")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("client certificate not installed: %d certs", len(cfg.Certificates))
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("client certs must not imply skipping verification")
	}
}

// TestTLSClientConfigRejectsBadInput: misconfiguration must surface as a usage
// error with a usable message, not as a confusing connection failure later.
func TestTLSClientConfigRejectsBadInput(t *testing.T) {
	_, certPath, keyPath := writeTestCA(t)
	junk := filepath.Join(t.TempDir(), "junk.txt")
	if err := os.WriteFile(junk, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatalf("write junk: %v", err)
	}

	cases := []struct {
		name  string
		files TLSFiles
		want  string
	}{
		{"missing ca file", TLSFiles{CACert: filepath.Join(t.TempDir(), "nope.crt")}, "read -ca-cert"},
		{"ca has no certs", TLSFiles{CACert: junk}, "no PEM certificate"},
		{"cert without key", TLSFiles{ClientCert: certPath}, "given together"},
		{"key without cert", TLSFiles{ClientKey: keyPath}, "given together"},
		{"missing key file", TLSFiles{ClientCert: certPath, ClientKey: filepath.Join(t.TempDir(), "nope.key")}, "load client certificate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient("https://example:8443", "tok", time.Second, false, tc.files)
			_, err := c.tlsClientConfig()
			if err == nil {
				t.Fatalf("expected an error")
			}
			var cliErr *Error
			if !errors.As(err, &cliErr) {
				t.Fatalf("expected a *cli.Error, got %T", err)
			}
			if cliErr.Exit != ExitUsage {
				t.Fatalf("exit code = %d, want %d", cliErr.Exit, ExitUsage)
			}
			if !strings.Contains(cliErr.Message, tc.want) {
				t.Fatalf("message %q does not mention %q", cliErr.Message, tc.want)
			}
		})
	}

}

// TestTLSFlagsAreGlobal drives the real registry instead of inspecting the
// name maps: a flag can be listed as global and still never be registered on
// the FlagSet, in which case the parser rejects it with "flag provided but not
// defined" while map-based assertions pass anyway. Valid certificate material
// is used so a failure can only mean the flag itself was not accepted.
func TestTLSFlagsAreGlobal(t *testing.T) {
	r, ts := newFakeRegistry(t)
	caPath, certPath, keyPath := writeTestCA(t)

	for _, dash := range []string{"-", "--"} {
		args := []string{
			"-server", ts.URL,
			dash + "ca-cert", caPath,
			dash + "client-cert", certPath,
			dash + "client-key", keyPath,
			"agents", "list",
		}
		code, _, stderr := run(t, r, args...)
		if strings.Contains(stderr, "flag provided but not defined") {
			t.Fatalf("%s*: flag is not registered on the FlagSet: %s", dash, stderr)
		}
		if code != ExitOK {
			t.Fatalf("%s*: code=%d stderr=%s", dash, code, stderr)
		}
	}
}
