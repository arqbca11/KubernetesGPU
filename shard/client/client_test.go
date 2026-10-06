package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// A scheduler that is down for the first N requests (connection-level or
// 5xx), then answers. The client must retry and succeed without the caller
// seeing an error (decision 61).
func TestRetriesTransientFailuresThenSucceeds(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k := n.Add(1)
		if k <= 2 {
			w.WriteHeader(503)
			w.Write([]byte(`{"error":"restarting"}`)) //nolint:errcheck
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"round_id": 7}`)) //nolint:errcheck
	}))
	defer srv.Close()
	c := New(srv.URL)
	c.RetryFor = 10 * time.Second
	start := time.Now()
	var out struct {
		RoundID int64 `json:"round_id"`
	}
	code, err := c.do(context.Background(), "POST", "/rounds", map[string]int{"n_shards": 1}, &out, 200)
	t.Logf("two 503s then 200: code=%d round_id=%d err=%v after %d requests in %s", code, out.RoundID, err, n.Load(), time.Since(start).Round(time.Millisecond))
	if err != nil || code != 200 || out.RoundID != 7 || n.Load() != 3 {
		t.Fatalf("unexpected: code=%d err=%v n=%d", code, err, n.Load())
	}
}

func TestGivesUpAfterRetryFor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(502) }))
	defer srv.Close()
	c := New(srv.URL)
	c.RetryFor = 700 * time.Millisecond
	start := time.Now()
	_, err := c.do(context.Background(), "GET", "/healthz", nil, nil, 200)
	t.Logf("always 502: err=%v after %s", err, time.Since(start).Round(time.Millisecond))
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestNonTransientErrorsAreNotRetried(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(404)
		w.Write([]byte(`{"error":"no such round"}`)) //nolint:errcheck
	}))
	defer srv.Close()
	c := New(srv.URL)
	_, err := c.do(context.Background(), "GET", "/rounds/9", nil, nil, 200)
	t.Logf("404: err=%v, requests=%d (no retry)", err, n.Load())
	if err == nil || n.Load() != 1 {
		t.Fatalf("a 4xx must fail immediately; requests=%d err=%v", n.Load(), err)
	}
}

// After a lost reply, the retried job start/done gets 409 (already in that
// state). That is success for the shard's purposes.
func TestJobStartTreats409AsRecorded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(409)
		w.Write([]byte(`{"error":"job cannot be started"}`)) //nolint:errcheck
	}))
	defer srv.Close()
	c := New(srv.URL)
	if err := c.JobStart(context.Background(), 1, 0, 3); err != nil {
		t.Fatalf("409 on start should be treated as recorded: %v", err)
	}
	if err := c.JobDone(context.Background(), 1, 0, 3); err != nil {
		t.Fatalf("409 on done should be treated as recorded: %v", err)
	}
	t.Log("409 on start and done accepted as 'already recorded'")
}
