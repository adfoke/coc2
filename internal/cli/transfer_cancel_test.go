package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// fakeCancelServer serves the two endpoints `transfers cancel` touches:
// the POST that requests cancellation, and the GET polled by --wait. The
// first poll reports cancel_requested (non-terminal), the second reports
// canceled — the exact sequence a real agent confirmation produces.
func fakeCancelServer(t *testing.T, polls int32, confirmStatus string, postStatus string, postCode int) (*Registry, *httptest.Server, *int32) {
	t.Helper()
	var getPolls int32

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/transfers/tx-9/cancel", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(postCode)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"transfer_id": "tx-9", "agent_id": "a1", "direction": "upload",
			"status": postStatus, "cancel_sent": true,
		})
	})
	mux.HandleFunc("GET /api/v1/transfers/tx-9", func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt32(&getPolls, 1)
		status := "cancel_requested"
		if n >= polls {
			status = confirmStatus
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"transfer_id": "tx-9", "status": status})
	})

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	r := &Registry{}
	RegisterAll(r)
	return r, ts, &getPolls
}

func TestTransfersCancelNoWait(t *testing.T) {
	r, ts, _ := fakeCancelServer(t, 99, "canceled", "cancel_requested", http.StatusAccepted)
	code, stdout, stderr := runWait(t, r, ts, "transfers", "cancel", "tx-9")
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "cancel_requested") || !strings.Contains(stdout, "cancel_sent") {
		t.Fatalf("POST ack must pass through: %s", stdout)
	}
}

// TestTransfersCancelWaitSurvivesIntermediateState is the regression: a
// canceled transfer must be terminal for the poll, and the intermediate
// cancel_requested must NOT be treated as terminal (it would exit early
// with the wrong status).
func TestTransfersCancelWaitSurvivesIntermediateState(t *testing.T) {
	r, ts, polls := fakeCancelServer(t, 2, "canceled", "cancel_requested", http.StatusAccepted)
	code, stdout, stderr := runWait(t, r, ts, "transfers", "cancel", "tx-9", "--wait", "--wait-timeout", "5s")
	if code != ExitOK {
		t.Fatalf("cancel to canceled is success: code=%d stderr=%s out=%s", code, stderr, stdout)
	}
	if !strings.Contains(stdout, "canceled") {
		t.Fatalf("final status must be canceled: %s", stdout)
	}
	if n := atomic.LoadInt32(polls); n < 2 {
		t.Fatalf("polling stopped at %d before cancel_requested expired: %s", n, stdout)
	}
}

func TestTransfersCancelWaitReportsNonCancelOutcome(t *testing.T) {
	// The transfer finished before the cancel landed: cancel POST is
	// idempotent (returns the terminal state) and --wait must surface
	// "ended as success" with a non-zero exit, not hang.
	r, ts, _ := fakeCancelServer(t, 1, "success", "success", http.StatusAccepted)
	code, _, stderr := runWait(t, r, ts, "transfers", "cancel", "tx-9", "--wait", "--wait-timeout", "5s")
	if code != ExitFailure {
		t.Fatalf("already-successful transfer cancel: code=%d want 1 (stderr=%s)", code, stderr)
	}
	if !strings.Contains(stderr, "transfer_not_canceled") {
		t.Fatalf("error envelope must explain the non-cancel outcome: %s", stderr)
	}
}

func TestTransfersCancelRequiresID(t *testing.T) {
	r, ts, _ := fakeCancelServer(t, 1, "canceled", "cancel_requested", http.StatusAccepted)
	code, _, _ := runWait(t, r, ts, "transfers", "cancel")
	if code != ExitUsage {
		t.Fatalf("missing arg: code=%d want 4", code)
	}
}

func TestTransfersCancelInSchema(t *testing.T) {
	r := &Registry{}
	RegisterAll(r)
	code, stdout, _ := run(t, r, "schema")
	if code != ExitOK {
		t.Fatalf("schema code=%d", code)
	}
	if !strings.Contains(stdout, "transfers cancel") {
		t.Fatal("schema missing `transfers cancel`")
	}
}
