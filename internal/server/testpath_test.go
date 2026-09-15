package server

import (
	"os"
	"path/filepath"
	"testing"
)

// shortSockPath returns a Unix socket path guaranteed to fit in the kernel's
// sun_path buffer (104 bytes on macOS/BSD, 108 on Linux).
//
// t.TempDir() embeds the test name, so a descriptive test like
// TestOplogUDSPlaneRecordsUnixActor produces a path that overflows on macOS
// and fails with the opaque "bind: invalid argument" — intermittently, since
// the random suffix varies in length. Binding any UDS in a test must use this
// helper instead of filepath.Join(t.TempDir(), ...).
func shortSockPath(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "coc2s")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	path := filepath.Join(dir, "s.sock")
	if len(path) > 100 {
		t.Fatalf("socket path %q is %d bytes, too long for sun_path", path, len(path))
	}
	return path
}
