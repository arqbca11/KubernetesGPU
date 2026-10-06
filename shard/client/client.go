// Package client is the shard's view of the scheduler: the HTTP calls a shard
// makes (design doc, API table). Nothing else in the shard knows about HTTP.
//
// Every call is idempotent on the scheduler side (build_id, seq, guarded
// updates, upserts), so the client retries transient failures (connection
// errors, 5xx) with backoff for up to RetryFor, and a scheduler restart
// mid-round is survivable (decision 61). A 409 on a job start/done after a
// retry means the first attempt landed; it is treated as success.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Client struct {
	base     string
	http     *http.Client
	RetryFor time.Duration // give up on a call after this long of transient failures
	Log      *slog.Logger  // optional; retries are logged at WARN
}

func New(base string) *Client {
	return &Client{base: base, http: &http.Client{Timeout: 10 * time.Second}, RetryFor: 60 * time.Second}
}

// transientError marks a failure worth retrying.
type transientError struct{ err error }

func (e transientError) Error() string { return e.err.Error() }
func (e transientError) Unwrap() error { return e.err }

// ---- request / response shapes (mirrors scheduler/api) ----------------------

type CreateRoundRequest struct {
	Scenario string `json:"scenario"`
	Seed     int64  `json:"seed"`
	NShards  int32  `json:"n_shards"`
}

type SubmitBuildRequest struct {
	RoundID  int64 `json:"round_id"`
	ShardID  int32 `json:"shard_id"`
	NVectors int64 `json:"n_vectors"`
	Dim      int32 `json:"dim"`
}

type SubmitBuildResponse struct {
	BuildID    string  `json:"build_id"`
	Placement  string  `json:"placement"`
	Priority   float64 `json:"priority"`
	Reason     string  `json:"reason"`
	MemBytes   int64   `json:"mem_bytes"`
	Created    bool    `json:"created"`
	CPUBuildMs int64   `json:"cpu_build_ms"`
	GPUTotalMs int64   `json:"gpu_total_ms"`
}

type ArrivalRequest struct {
	Seq        int32 `json:"seq"`
	DurationMs int64 `json:"duration_ms"`
	NeedsIndex bool  `json:"needs_index"`
}

type ReportRequest struct {
	QueueDepth        int32   `json:"queue_depth"`
	WaitingNeedsIndex int32   `json:"waiting_needs_index"`
	OldestWaitMs      int64   `json:"oldest_wait_ms"`
	BuildProgress     float32 `json:"build_progress"`
	StreamDone        bool    `json:"stream_done"`
}

type BuildSummary struct {
	BuildID   string `json:"build_id"`
	Placement string `json:"placement"`
	State     string `json:"state"`
	Attempt   int32  `json:"attempt"`
}

type ReportResponse struct {
	Build BuildSummary `json:"build"`
}

type Round struct {
	RoundID    int64      `json:"round_id"`
	Scenario   string     `json:"scenario"`
	Seed       int64      `json:"seed"`
	NShards    int32      `json:"n_shards"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

type BuildRow struct {
	BuildID    string     `json:"build_id"`
	RoundID    int64      `json:"round_id"`
	ShardID    int32      `json:"shard_id"`
	NVectors   int64      `json:"n_vectors"`
	Dim        int32      `json:"dim"`
	MemBytes   int64      `json:"mem_bytes"`
	Placement  string     `json:"placement"`
	State      string     `json:"state"`
	Priority   float64    `json:"priority"`
	Attempt    int32      `json:"attempt"`
	LeaseOwner *string    `json:"lease_owner"`
	FailReason *string    `json:"fail_reason"`
	EnqueuedAt time.Time  `json:"enqueued_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

type JobRow struct {
	RoundID    int64      `json:"round_id"`
	ShardID    int32      `json:"shard_id"`
	Seq        int32      `json:"seq"`
	DurationMs int64      `json:"duration_ms"`
	NeedsIndex bool       `json:"needs_index"`
	ArrivedAt  time.Time  `json:"arrived_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

type ShardStatus struct {
	ShardID           int32     `json:"shard_id"`
	QueueDepth        int32     `json:"queue_depth"`
	WaitingNeedsIndex int32     `json:"waiting_needs_index"`
	OldestWaitMs      int64     `json:"oldest_wait_ms"`
	BuildProgress     float32   `json:"build_progress"`
	StreamDone        bool      `json:"stream_done"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type RoundStatus struct {
	Round    Round         `json:"round"`
	Builds   []BuildRow    `json:"builds"`
	Jobs     []JobRow      `json:"jobs"`
	Statuses []ShardStatus `json:"shard_status"`
}

type PoolState struct {
	LiveWorkers int   `json:"live_workers"`
	LargestMem  int64 `json:"largest_mem_bytes"`
}

type WorkersResponse struct {
	Pool PoolState `json:"pool"`
}

// ---- calls ---------------------------------------------------------------------

func (c *Client) CreateRound(ctx context.Context, scenario string, seed int64, nShards int32) (int64, error) {
	var out struct {
		RoundID int64 `json:"round_id"`
	}
	_, err := c.do(ctx, "POST", "/rounds", CreateRoundRequest{Scenario: scenario, Seed: seed, NShards: nShards}, &out, 201)
	return out.RoundID, err
}

func (c *Client) Submit(ctx context.Context, req SubmitBuildRequest) (SubmitBuildResponse, error) {
	var out SubmitBuildResponse
	_, err := c.do(ctx, "POST", "/builds", req, &out, 200, 201)
	return out, err
}

func (c *Client) LocalDone(ctx context.Context, buildID string) (accepted bool, err error) {
	code, err := c.do(ctx, "POST", "/builds/"+buildID+"/done", nil, nil, 200, 409)
	return code == 200, err
}

func (c *Client) Arrival(ctx context.Context, round int64, shard int32, req ArrivalRequest) error {
	_, err := c.do(ctx, "POST", fmt.Sprintf("/jobs/%d/%d", round, shard), req, nil, 200, 201)
	return err
}

// JobStart and JobDone accept 409 as well as 200: after a retry, 409 means the
// earlier attempt was recorded before its reply was lost.
func (c *Client) JobStart(ctx context.Context, round int64, shard, seq int32) error {
	_, err := c.do(ctx, "POST", fmt.Sprintf("/jobs/%d/%d/%d/start", round, shard, seq), nil, nil, 200, 409)
	return err
}

func (c *Client) JobDone(ctx context.Context, round int64, shard, seq int32) error {
	_, err := c.do(ctx, "POST", fmt.Sprintf("/jobs/%d/%d/%d/done", round, shard, seq), nil, nil, 200, 409)
	return err
}

func (c *Client) Report(ctx context.Context, round int64, shard int32, req ReportRequest) (BuildSummary, error) {
	var out ReportResponse
	_, err := c.do(ctx, "POST", fmt.Sprintf("/shards/%d/%d/report", round, shard), req, &out, 200)
	return out.Build, err
}

func (c *Client) GetRound(ctx context.Context, round int64) (RoundStatus, error) {
	var out RoundStatus
	_, err := c.do(ctx, "GET", fmt.Sprintf("/rounds/%d", round), nil, &out, 200)
	return out, err
}

func (c *Client) GetBuild(ctx context.Context, id string) (BuildRow, error) {
	var out BuildRow
	_, err := c.do(ctx, "GET", "/builds/"+id, nil, &out, 200)
	return out, err
}

func (c *Client) LiveWorkers(ctx context.Context) (int, error) {
	var out WorkersResponse
	_, err := c.do(ctx, "GET", "/workers", nil, &out, 200)
	return out.Pool.LiveWorkers, err
}

func (c *Client) Healthy(ctx context.Context) bool {
	_, err := c.do(ctx, "GET", "/healthz", nil, nil, 200)
	return err == nil
}

// do sends one request, retrying transient failures with backoff for up to
// RetryFor, and decodes the JSON reply. A status outside ok is an error
// carrying the server's message.
func (c *Client) do(ctx context.Context, method, path string, body, out any, ok ...int) (int, error) {
	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		payload = b
	}
	deadline := time.Now().Add(c.RetryFor)
	wait := 200 * time.Millisecond
	attempts := 0
	for {
		attempts++
		code, err := c.once(ctx, method, path, payload, out, ok)
		var te transientError
		if err == nil || !errors.As(err, &te) || ctx.Err() != nil {
			if attempts > 1 && err == nil && c.Log != nil {
				c.Log.Info("scheduler call succeeded after retries", "call", method+" "+path, "attempts", attempts)
			}
			return code, err
		}
		if time.Now().Add(wait).After(deadline) {
			return code, fmt.Errorf("%s %s: gave up after %d attempts over %s: %w", method, path, attempts, c.RetryFor, te.err)
		}
		if c.Log != nil {
			c.Log.Warn("scheduler call failed, retrying", "call", method+" "+path, "attempt", attempts, "in", wait, "err", te.err.Error())
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return code, ctx.Err()
		}
		if wait < 3*time.Second {
			wait *= 2
		}
	}
}

func (c *Client) once(ctx context.Context, method, path string, payload []byte, out any, ok []int) (int, error) {
	var rd io.Reader
	if payload != nil {
		rd = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return 0, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, transientError{fmt.Errorf("%s %s: %w", method, path, err)}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 500 {
		return resp.StatusCode, transientError{fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(raw))}
	}
	accepted := false
	for _, k := range ok {
		if resp.StatusCode == k {
			accepted = true
		}
	}
	if !accepted {
		return resp.StatusCode, fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(raw))
	}
	if out != nil && len(raw) > 0 && resp.StatusCode != 409 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: decode: %w", method, path, err)
		}
	}
	return resp.StatusCode, nil
}
