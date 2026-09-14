package server

import (
	"context"
	"net"

	"golang.org/x/sys/unix"
)

type peerIDKey struct{}

// peerID identifies the process on the other end of a Unix socket
// connection. The credentials come from the kernel (LOCAL_PEERCRED), so they
// cannot be forged by the client — this is the entire "who did it" answer on
// the tokenless UDS plane.
type peerID struct {
	pid int
	uid int
	gid int
}

func peerFromContext(ctx context.Context) *peerID {
	if p, ok := ctx.Value(peerIDKey{}).(*peerID); ok {
		return p
	}
	return nil
}

// withPeerCred attaches kernel-verified peer credentials to the request
// context. Returns ctx unchanged for TCP connections or when the lookup
// fails (a failed lookup must never break the request path).
//
// macOS has no SO_PEERCRED; the equivalent is LOCAL_PEERCRED (xucred), which
// carries only the uid — the pid comes from a separate LOCAL_PEERPID query,
// and gid is not available at all (-1).
func withPeerCred(ctx context.Context, nc net.Conn) context.Context {
	uc, ok := nc.(*net.UnixConn)
	if !ok {
		return ctx
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return ctx
	}
	var cred *peerID
	_ = raw.Control(func(fd uintptr) {
		xu, e := unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if e != nil {
			return
		}
		cred = &peerID{pid: -1, uid: int(xu.Uid), gid: -1}
		if pid, e := unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID); e == nil {
			cred.pid = pid
		}
	})
	if cred == nil {
		return ctx
	}
	return context.WithValue(ctx, peerIDKey{}, cred)
}
