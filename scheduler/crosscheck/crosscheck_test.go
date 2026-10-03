package crosscheck

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/arqbca11/KubernetesGPU/scheduler/api"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

// API table: POST /shards/{round}/{shard}/report upserts shard_status and
// replies 200 {build: {placement, state, attempt}} (decision 29).
func TestReportUpsertsStatusAndRepliesWithBuild(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 1)
	sub := e.submit(t, round, 4)
	t.Logf("submitted build %s, placement=%s (AlwaysGPU, zero live workers -> gpu, decision 19)", sub.BuildID, sub.Placement)

	first := api.ReportRequest{QueueDepth: 5, WaitingNeedsIndex: 2, OldestWaitMs: 300, BuildProgress: 0, StreamDone: false}
	var rep api.ReportResponse
	code, body := e.do(t, "POST", fmt.Sprintf("/shards/%d/4/report", round), first, &rep)
	t.Logf("first report -> %d %s", code, body)
	if code != http.StatusOK {
		t.Fatalf("API table promises 200 for a report from a shard with a build; got %d", code)
	}
	if rep.Build.BuildID != sub.BuildID || rep.Build.Placement != "gpu" || rep.Build.State != "queued" || rep.Build.Attempt != 0 {
		t.Errorf("reply build = %+v; want build_id=%s placement=gpu state=queued attempt=0", rep.Build, sub.BuildID)
	}

	second := api.ReportRequest{QueueDepth: 1, WaitingNeedsIndex: 1, OldestWaitMs: 900, BuildProgress: 0, StreamDone: true}
	code, body = e.do(t, "POST", fmt.Sprintf("/shards/%d/4/report", round), second, &rep)
	t.Logf("second report -> %d %s", code, body)

	sts, err := e.st.ListShardStatus(e.ctx, round)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[db] shard_status rows for round %d: %+v", round, sts)
	if len(sts) != 1 {
		t.Fatalf("shard_status holds the shard's latest report (one row per round+shard); got %d rows", len(sts))
	}
	s := sts[0]
	if s.ShardID != 4 || s.QueueDepth != 1 || s.WaitingNeedsIndex != 1 || s.OldestWaitMs != 900 || !s.StreamDone {
		t.Errorf("shard_status = %+v; want the second report's values (upsert)", s)
	}
}

// Decision 29: the report reply tracks the build as workers move it, including
// the attempt (fencing token) after a claim and state=done after complete.
func TestReportReplyTracksWorkerProgress(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 1)
	sub := e.submit(t, round, 0)
	path := fmt.Sprintf("/shards/%d/0/report", round)

	c, ok, err := e.st.Claim(e.ctx, "w1", 1<<40, 30*time.Second)
	if err != nil || !ok || c.BuildID != sub.BuildID {
		t.Fatalf("claim: %+v ok=%v err=%v", c, ok, err)
	}
	var rep api.ReportResponse
	code, body := e.do(t, "POST", path, report(false), &rep)
	t.Logf("after claim (attempt %d), report -> %d %s", c.Attempt, code, body)
	if rep.Build.State != "leased" || rep.Build.Attempt != 1 || rep.Build.Placement != "gpu" {
		t.Errorf("reply after claim = %+v; want gpu/leased/attempt 1", rep.Build)
	}

	if ok, err := e.st.Complete(e.ctx, c.BuildID, c.Attempt); err != nil || !ok {
		t.Fatalf("complete: ok=%v err=%v", ok, err)
	}
	code, body = e.do(t, "POST", path, report(false), &rep)
	t.Logf("after complete, report -> %d %s", code, body)
	logBuild(t, "after complete", e.mustBuild(t, sub.BuildID))
	if rep.Build.State != "done" || rep.Build.Attempt != 1 {
		t.Errorf("reply after complete = %+v; want state=done attempt=1 (the shard learns its index is ready from this reply)", rep.Build)
	}
}

// API table: report "404 if the shard has no build in the round"; nothing is
// written to shard_status for such a shard.
func TestReportFromShardWithoutBuildIs404(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 2)
	e.submit(t, round, 0)

	code, body := e.do(t, "POST", fmt.Sprintf("/shards/%d/1/report", round), report(true), nil)
	t.Logf("report from shard 1 (no build) -> %d %s", code, body)
	code2, body2 := e.do(t, "POST", fmt.Sprintf("/shards/%d/0/report", round+1000), report(true), nil)
	t.Logf("report into nonexistent round %d -> %d %s", round+1000, code2, body2)

	sts, err := e.st.ListShardStatus(e.ctx, round)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[db] shard_status rows: %+v", sts)
	if code != http.StatusNotFound {
		t.Errorf("API table: 404 if the shard has no build in the round; got %d", code)
	}
	if code2 != http.StatusNotFound {
		t.Errorf("API table: 404 if the shard has no build in the round (round does not exist); got %d", code2)
	}
	if len(sts) != 0 {
		t.Errorf("a refused report must not write shard_status; got %d rows", len(sts))
	}
}

// Schema CHECKs on shard_status (migration 0003) and decision 14: an invalid
// report is refused and does not replace the stored report.
func TestInvalidReportRejectedAndPreviousKept(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 1)
	e.submit(t, round, 0)
	path := fmt.Sprintf("/shards/%d/0/report", round)
	good := api.ReportRequest{QueueDepth: 3, WaitingNeedsIndex: 1, OldestWaitMs: 10, BuildProgress: 0.5}
	if code, body := e.do(t, "POST", path, good, nil); code != http.StatusOK {
		t.Fatalf("good report: %d %s", code, body)
	}
	bad := []api.ReportRequest{
		{QueueDepth: 1, WaitingNeedsIndex: 2, OldestWaitMs: 0, BuildProgress: 0}, // waiting_needs_index > queue_depth
		{QueueDepth: 1, WaitingNeedsIndex: 0, OldestWaitMs: 0, BuildProgress: 1.5},
		{QueueDepth: -1, WaitingNeedsIndex: 0, OldestWaitMs: 0, BuildProgress: 0},
	}
	for _, b := range bad {
		code, body := e.do(t, "POST", path, b, nil)
		t.Logf("bad report %+v -> %d %s", b, code, body)
		if code < 400 || code >= 500 {
			t.Errorf("bad report %+v: want a 4xx refusal (schema CHECK violated by the client's input); got %d", b, code)
		}
	}
	sts, _ := e.st.ListShardStatus(e.ctx, round)
	t.Logf("[db] shard_status: %+v", sts)
	if len(sts) != 1 || sts[0].QueueDepth != 3 || sts[0].BuildProgress != 0.5 {
		t.Errorf("stored report should still be the last good one; got %+v", sts)
	}
}

// API table: POST /jobs/{round}/{shard} -> 201; a repeat of the same seq ->
// 200, unchanged (decision 28, RecordArrival doc).
func TestArrivalRecordedOnceAndRepeatUnchanged(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 1)
	e.submit(t, round, 2)
	path := fmt.Sprintf("/jobs/%d/2", round)

	code, body := e.do(t, "POST", path, api.ArrivalRequest{Seq: 1, DurationMs: 250, NeedsIndex: true}, nil)
	t.Logf("first arrival seq=1 -> %d %s", code, body)
	if code != http.StatusCreated {
		t.Errorf("API table: arrival -> 201; got %d", code)
	}
	j1, ok, err := e.st.GetJob(e.ctx, round, 2, 1)
	if err != nil || !ok {
		t.Fatalf("GetJob: ok=%v err=%v", ok, err)
	}
	t.Logf("[db] job after arrival: %+v", j1)
	if j1.StartedAt != nil || j1.FinishedAt != nil || j1.ArrivedAt.IsZero() {
		t.Errorf("an arrival records arrived_at only; got %+v", j1)
	}

	code, body = e.do(t, "POST", path, api.ArrivalRequest{Seq: 1, DurationMs: 9999, NeedsIndex: false}, nil)
	t.Logf("repeat arrival seq=1 with different body -> %d %s", code, body)
	if code != http.StatusOK {
		t.Errorf("API table: repeat of the same seq -> 200; got %d", code)
	}
	j2, _, _ := e.st.GetJob(e.ctx, round, 2, 1)
	t.Logf("[db] job after repeat: %+v", j2)
	if j2.DurationMs != 250 || !j2.NeedsIndex || !j2.ArrivedAt.Equal(j1.ArrivedAt) {
		t.Errorf("repeat must leave the row unchanged; before %+v after %+v", j1, j2)
	}
	jobs, _ := e.st.ListJobs(e.ctx, round)
	if len(jobs) != 1 {
		t.Errorf("want exactly one job row; got %d", len(jobs))
	}
}

// API table: start/done -> 404 if no such query, 409 if wrong state.
func TestJobStartDoneStatusCodes(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 1)
	e.submit(t, round, 0)
	base := fmt.Sprintf("/jobs/%d/0", round)
	if code, body := e.do(t, "POST", base, api.ArrivalRequest{Seq: 1, DurationMs: 10}, nil); code != http.StatusCreated {
		t.Fatalf("arrival: %d %s", code, body)
	}
	steps := []struct {
		path string
		want int
		why  string
	}{
		{base + "/2/start", 404, "no such query"},
		{base + "/2/done", 404, "no such query"},
		{base + "/1/done", 409, "done before start is the wrong state"},
		{base + "/1/start", 200, "first start"},
		{base + "/1/start", 409, "already started"},
		{base + "/1/done", 200, "first done"},
		{base + "/1/done", 409, "already finished"},
	}
	for _, s := range steps {
		code, body := e.do(t, "POST", s.path, nil, nil)
		t.Logf("POST %s -> %d %s (%s)", s.path, code, body, s.why)
		if s.want == 200 {
			if code < 200 || code >= 300 {
				t.Errorf("POST %s: want 2xx (%s); got %d", s.path, s.why, code)
			}
		} else if code != s.want {
			t.Errorf("POST %s: want %d (%s); got %d", s.path, s.want, s.why, code)
		}
	}
	j, _, _ := e.st.GetJob(e.ctx, round, 0, 1)
	t.Logf("[db] job: %+v", j)
	if j.StartedAt == nil || j.FinishedAt == nil || j.FinishedAt.Before(*j.StartedAt) {
		t.Errorf("job should have started_at <= finished_at; got %+v", j)
	}
}

// Decision 31, preempt: running local -> queued gpu, started_at cleared,
// priority set, attempt untouched; the first claim then gives attempt 1; the
// shard's late /done is refused with 409 and its report says gpu.
func TestPreemptMovesRunningLocalToGPUQueue(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 1)
	id := e.submitLocal(t, round, 3)
	before := e.mustBuild(t, id)
	logBuild(t, "local build at submit", before)

	ok, err := e.st.PreemptToGPU(e.ctx, id, 7.5)
	if err != nil || !ok {
		t.Fatalf("PreemptToGPU on a running local build: ok=%v err=%v", ok, err)
	}
	after := e.mustBuild(t, id)
	logBuild(t, "after preempt", after)
	if after.Placement != "gpu" || after.State != "queued" || after.Priority != 7.5 || after.StartedAt != nil || after.Attempt != before.Attempt {
		t.Errorf("decision 31: want placement=gpu state=queued priority=7.5 started_at=nil attempt=%d; got %+v", before.Attempt, after)
	}
	if after.BuildID != id || !after.EnqueuedAt.Equal(before.EnqueuedAt) {
		t.Errorf("decision 31: same row, same idempotency key; decision 24: enqueued_at keeps the submit time. before enqueued_at=%s after=%s", before.EnqueuedAt.Format(time.RFC3339Nano), after.EnqueuedAt.Format(time.RFC3339Nano))
	}

	var rep api.ReportResponse
	code, body := e.do(t, "POST", fmt.Sprintf("/shards/%d/3/report", round), report(false), &rep)
	t.Logf("shard report after preempt -> %d %s", code, body)
	if rep.Build.Placement != "gpu" || rep.Build.State != "queued" {
		t.Errorf("decision 29: shard should learn of the preemption from its report reply; got %+v", rep.Build)
	}

	code, body = e.do(t, "POST", "/builds/"+id+"/done", nil, nil)
	t.Logf("shard's late POST /builds/%s/done -> %d %s", id, code, body)
	if code != http.StatusConflict {
		t.Errorf("API table: /done is 409 unless the build is a running local build; got %d", code)
	}

	c, ok, err := e.st.Claim(e.ctx, "w1", 1<<40, 30*time.Second)
	t.Logf("claim after preempt: %+v ok=%v err=%v", c, ok, err)
	if !ok || c.BuildID != id || c.Attempt != 1 {
		t.Errorf("PreemptToGPU doc: the first claim will bump attempt (to 1); got %+v ok=%v", c, ok)
	}
}

// Decision 31, renege: queued gpu -> running local, attempt untouched; the
// build is no longer claimable and the shard can finish it via /done.
func TestRenegeMovesQueuedGPUToLocal(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 1)
	sub := e.submit(t, round, 1)
	before := e.mustBuild(t, sub.BuildID)
	logBuild(t, "gpu build at submit", before)

	ok, err := e.st.RenegeToLocal(e.ctx, sub.BuildID)
	if err != nil || !ok {
		t.Fatalf("RenegeToLocal on a queued gpu build: ok=%v err=%v", ok, err)
	}
	after := e.mustBuild(t, sub.BuildID)
	logBuild(t, "after renege", after)
	if after.Placement != "local" || after.State != "running" || after.Attempt != before.Attempt {
		t.Errorf("decision 31: want placement=local state=running attempt=%d; got %+v", before.Attempt, after)
	}

	c, ok, err := e.st.Claim(e.ctx, "w1", 1<<40, 30*time.Second)
	t.Logf("claim after renege: %+v ok=%v err=%v", c, ok, err)
	if ok {
		t.Errorf("decision 5: a running local build is not matched by the claim; got %+v", c)
	}

	var rep api.ReportResponse
	e.do(t, "POST", fmt.Sprintf("/shards/%d/1/report", round), report(false), &rep)
	t.Logf("report reply after renege: %+v", rep.Build)
	if rep.Build.Placement != "local" || rep.Build.State != "running" {
		t.Errorf("decision 29: reply should tell the shard to start the build locally; got %+v", rep.Build)
	}

	code, body := e.do(t, "POST", "/builds/"+sub.BuildID+"/done", nil, nil)
	t.Logf("POST /builds/%s/done -> %d %s", sub.BuildID, code, body)
	done := e.mustBuild(t, sub.BuildID)
	logBuild(t, "after /done", done)
	if code < 200 || code >= 300 || done.State != "done" || done.FinishedAt == nil {
		t.Errorf("a reneged build is a running local build; /done should succeed and mark it done; got %d %+v", code, done)
	}
}

// Decision 31: a leased build is never moved; neither transition applies
// outside its guard; a refused transition changes nothing.
func TestTransitionsRefusedOutsideTheirGuards(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 3)
	leasedID := e.submit(t, round, 0).BuildID
	c, ok, err := e.st.Claim(e.ctx, "w1", 1<<40, 30*time.Second)
	if err != nil || !ok || c.BuildID != leasedID {
		t.Fatalf("claim: %+v %v %v", c, ok, err)
	}
	queuedID := e.submit(t, round, 1).BuildID
	localID := e.submitLocal(t, round, 2)
	if ok, err := e.st.CompleteLocal(e.ctx, localID); err != nil || !ok {
		t.Fatalf("CompleteLocal: %v %v", ok, err)
	}

	snap := map[string]store.BuildRow{}
	for _, id := range []string{leasedID, queuedID, localID} {
		snap[id] = e.mustBuild(t, id)
		logBuild(t, "before", snap[id])
	}
	cases := []struct {
		name string
		f    func() (bool, error)
		id   string
	}{
		{"renege leased gpu", func() (bool, error) { return e.st.RenegeToLocal(e.ctx, leasedID) }, leasedID},
		{"preempt leased gpu", func() (bool, error) { return e.st.PreemptToGPU(e.ctx, leasedID, 9) }, leasedID},
		{"preempt queued gpu", func() (bool, error) { return e.st.PreemptToGPU(e.ctx, queuedID, 9) }, queuedID},
		{"preempt done local", func() (bool, error) { return e.st.PreemptToGPU(e.ctx, localID, 9) }, localID},
		{"renege done local", func() (bool, error) { return e.st.RenegeToLocal(e.ctx, localID) }, localID},
	}
	for _, cs := range cases {
		ok, err := cs.f()
		t.Logf("%s -> ok=%v err=%v", cs.name, ok, err)
		if ok {
			t.Errorf("%s: decision 31 guard should refuse (false)", cs.name)
		}
	}
	for id, b := range snap {
		a := e.mustBuild(t, id)
		logBuild(t, "after", a)
		if a.Placement != b.Placement || a.State != b.State || a.Attempt != b.Attempt || a.Priority != b.Priority || deref(a.LeaseOwner) != deref(b.LeaseOwner) {
			t.Errorf("refused transition changed %s: before %+v after %+v", id, b, a)
		}
	}
	if ok, err := e.st.Renew(e.ctx, leasedID, c.Attempt, 30*time.Second); err != nil || !ok {
		t.Errorf("the leased worker must still own its build after refused moves; renew ok=%v err=%v", ok, err)
	}
}

// Decision 30: with every build done and every query finished, the round stays
// open until all n_shards shards have reported stream_done; then finished_at
// is the latest build or query finish, not the time of the check (decision 18).
func TestRoundWaitsForStreamDoneThenStampsLatestFinish(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 2)
	a := e.submit(t, round, 0).BuildID
	b := e.submitLocal(t, round, 1)
	for i := 0; i < 1; i++ {
		c, ok, err := e.st.Claim(e.ctx, "w1", 1<<40, 30*time.Second)
		if err != nil || !ok || c.BuildID != a {
			t.Fatalf("claim: %+v %v %v", c, ok, err)
		}
		if ok, err := e.st.Complete(e.ctx, c.BuildID, c.Attempt); err != nil || !ok {
			t.Fatalf("complete: %v %v", ok, err)
		}
	}
	if ok, err := e.st.CompleteLocal(e.ctx, b); err != nil || !ok {
		t.Fatalf("CompleteLocal: %v %v", ok, err)
	}
	for _, sh := range []int32{0, 1} {
		if code, body := e.do(t, "POST", fmt.Sprintf("/jobs/%d/%d", round, sh), api.ArrivalRequest{Seq: 1, DurationMs: 5}, nil); code != http.StatusCreated {
			t.Fatalf("arrival: %d %s", code, body)
		}
		runJob(t, e, round, sh, 1)
	}
	e.do(t, "POST", fmt.Sprintf("/shards/%d/0/report", round), report(true), nil)
	e.do(t, "POST", fmt.Sprintf("/shards/%d/1/report", round), report(false), nil)

	var rs api.RoundStatus
	code, _ := e.do(t, "GET", fmt.Sprintf("/rounds/%d", round), nil, &rs)
	t.Logf("GET /rounds/%d -> %d; finished_at=%v; statuses=%+v", round, code, tptr(rs.Round.FinishedAt), rs.Statuses)
	if rs.Round.FinishedAt != nil {
		t.Errorf("decision 30: shard 1 has not reported stream_done, round must stay open")
	}

	time.Sleep(50 * time.Millisecond) // so "now" differs measurably from the last child finish
	e.do(t, "POST", fmt.Sprintf("/shards/%d/1/report", round), report(true), nil)
	code, _ = e.do(t, "GET", fmt.Sprintf("/rounds/%d", round), nil, &rs)
	r := e.mustRound(t, round)
	t.Logf("after shard 1 stream_done: GET -> %d; [db] round finished_at=%v", code, tptr(r.FinishedAt))
	if r.FinishedAt == nil {
		t.Fatalf("decision 30: every condition now holds; round should be finished")
	}
	var latest time.Time
	builds, _ := e.st.ListBuilds(e.ctx, round)
	jobs, _ := e.st.ListJobs(e.ctx, round)
	for _, x := range builds {
		if x.FinishedAt != nil && x.FinishedAt.After(latest) {
			latest = *x.FinishedAt
		}
	}
	for _, x := range jobs {
		if x.FinishedAt != nil && x.FinishedAt.After(latest) {
			latest = *x.FinishedAt
		}
	}
	t.Logf("[db] latest child finish = %s", latest.Format(time.RFC3339Nano))
	if !r.FinishedAt.Equal(latest) {
		t.Errorf("decision 18 / FinishCompleteRounds doc: finished_at should equal the latest build or query finish %s; got %s", latest, r.FinishedAt)
	}
}

// Decision 30: a query that arrived but has not finished keeps the round open
// even when every shard has reported stream_done and every build is done.
func TestRoundStaysOpenWhileAQueryIsUnfinished(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 1)
	id := e.submitLocal(t, round, 0)
	if ok, err := e.st.CompleteLocal(e.ctx, id); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if _, err := e.st.RecordArrival(e.ctx, round, 0, 1, 5, true); err != nil {
		t.Fatal(err)
	}
	if _, err := e.st.RecordArrival(e.ctx, round, 0, 2, 5, false); err != nil {
		t.Fatal(err)
	}
	runJob(t, e, round, 0, 2)
	if ok, err := e.st.StartJob(e.ctx, round, 0, 1); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := e.st.ReportStatus(e.ctx, store.ShardStatus{RoundID: round, ShardID: 0, StreamDone: true}); err != nil {
		t.Fatal(err)
	}
	n, err := e.st.FinishCompleteRounds(e.ctx)
	jobs, _ := e.st.ListJobs(e.ctx, round)
	r := e.mustRound(t, round)
	t.Logf("FinishCompleteRounds -> %d %v; [db] jobs=%+v round.finished_at=%v", n, err, jobs, tptr(r.FinishedAt))
	if r.FinishedAt != nil {
		t.Errorf("decision 30: query seq 1 started but not finished; round must stay open")
	}
	if ok, err := e.st.FinishJob(e.ctx, round, 0, 1); err != nil || !ok {
		t.Fatal(ok, err)
	}
	e.st.FinishCompleteRounds(e.ctx)
	r = e.mustRound(t, round)
	t.Logf("after finishing seq 1: [db] round.finished_at=%v", tptr(r.FinishedAt))
	if r.FinishedAt == nil {
		t.Errorf("decision 30: all conditions hold now; round should finish")
	}
}

// Decision 30: "n_shards shards with stream_done, all their queries finished,
// all builds terminal". A stream_done report from a shard that has no build in
// the round (possible through the exported store.ReportStatus) must not stand
// in for a real shard that is still streaming.
func TestStreamDoneFromBuildlessShardDoesNotCount(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 2)
	for _, sh := range []int32{0, 1} {
		id := e.submitLocal(t, round, sh)
		if ok, err := e.st.CompleteLocal(e.ctx, id); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(e.st.ReportStatus(e.ctx, store.ShardStatus{RoundID: round, ShardID: 0, StreamDone: true}))
	must(e.st.ReportStatus(e.ctx, store.ShardStatus{RoundID: round, ShardID: 1, StreamDone: false}))
	// Spec resolution after this test first ran (decision 33): a report from a
	// shard with no build is refused at the store with store.ErrNoBuild, so it
	// can never be stored, let alone counted.
	err99 := e.st.ReportStatus(e.ctx, store.ShardStatus{RoundID: round, ShardID: 99, StreamDone: true})
	t.Logf("ReportStatus for shard 99 (no build) -> %v", err99)
	if !errors.Is(err99, store.ErrNoBuild) {
		t.Errorf("decision 33: a report from a buildless shard must be refused with ErrNoBuild; got %v", err99)
	}
	n, err := e.st.FinishCompleteRounds(e.ctx)
	sts, _ := e.st.ListShardStatus(e.ctx, round)
	r := e.mustRound(t, round)
	t.Logf("builds for shards 0,1 done; stream_done: shard0=true shard1=false shard99(no build)=true")
	t.Logf("FinishCompleteRounds -> %d %v; [db] shard_status=%+v; round.finished_at=%v", n, err, sts, tptr(r.FinishedAt))
	if r.FinishedAt != nil {
		t.Errorf("decision 30: shard 1 (which has a build) is still streaming; a buildless shard's stream_done must not complete the round")
	}
}

// Decision 27 / API table: 404 for an unknown round; 400 for n_shards <= 0;
// 409 for a new build once the round holds n_shards builds; a duplicate is
// still 200 created:false (ErrRoundFull doc).
func TestSubmitValidationAndCap(t *testing.T) {
	e := setup(t)
	for _, n := range []int32{0, -3} {
		code, body := e.do(t, "POST", "/rounds", api.CreateRoundRequest{Scenario: "x", Seed: 1, NShards: n}, nil)
		t.Logf("POST /rounds n_shards=%d -> %d %s", n, code, body)
		if code != http.StatusBadRequest {
			t.Errorf("decision 27: n_shards must be positive (400); got %d", code)
		}
	}
	code, body := e.do(t, "POST", "/builds", api.SubmitBuildRequest{RoundID: 424242, ShardID: 0, NVectors: 100, Dim: 8}, nil)
	t.Logf("POST /builds unknown round -> %d %s", code, body)
	if code != http.StatusNotFound {
		t.Errorf("decision 27: unknown round -> 404; got %d", code)
	}

	round := e.createRound(t, 2)
	e.submit(t, round, 5)
	e.submit(t, round, 17) // sparse ids are fine (decision 27)
	code, body = e.do(t, "POST", "/builds", api.SubmitBuildRequest{RoundID: round, ShardID: 3, NVectors: 100, Dim: 8}, nil)
	t.Logf("third distinct shard into n_shards=2 -> %d %s", code, body)
	if code != http.StatusConflict {
		t.Errorf("decision 27: new build beyond n_shards -> 409; got %d", code)
	}
	var dup api.SubmitBuildResponse
	code, body = e.do(t, "POST", "/builds", api.SubmitBuildRequest{RoundID: round, ShardID: 5, NVectors: 10000, Dim: 64}, &dup)
	t.Logf("duplicate of shard 5 into full round -> %d %s", code, body)
	if code != http.StatusOK || dup.Created {
		t.Errorf("ErrRoundFull doc / API table: duplicate is 200 created:false even when full; got %d %+v", code, dup)
	}
	_, err := e.st.SubmitBuild(e.ctx, store.Build{BuildID: store.BuildID(3, round), RoundID: round, ShardID: 3, NVectors: 1, Dim: 1, Placement: "gpu"})
	t.Logf("store.SubmitBuild third shard -> err=%v", err)
	if !errors.Is(err, store.ErrRoundFull) {
		t.Errorf("store: want ErrRoundFull; got %v", err)
	}
	builds, _ := e.st.ListBuilds(e.ctx, round)
	t.Logf("[db] %d builds in round", len(builds))
	if len(builds) != 2 {
		t.Errorf("round must hold exactly n_shards=2 builds; got %d", len(builds))
	}
}

// Decision 27: the round row lock serialises submits, so the n_shards cap
// holds under concurrency.
func TestConcurrentSubmitsRespectCap(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 3)
	const n = 12
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i], _ = e.do(t, "POST", "/builds", api.SubmitBuildRequest{RoundID: round, ShardID: int32(i), NVectors: 1000, Dim: 16}, nil)
		}(i)
	}
	wg.Wait()
	counts := map[int]int{}
	for _, c := range codes {
		counts[c]++
	}
	builds, _ := e.st.ListBuilds(e.ctx, round)
	t.Logf("%d concurrent submits of distinct shards into n_shards=3: status counts %v; [db] %d builds", n, counts, len(builds))
	if counts[http.StatusCreated] != 3 || counts[http.StatusConflict] != n-3 || len(builds) != 3 {
		t.Errorf("decision 27: want exactly 3x201, %dx409, 3 rows; got %v and %d rows", n-3, counts, len(builds))
	}
}

// Decision 28 + decision 21: submit carries no jobs, and request bodies use
// DisallowUnknownFields, so an old-spec submit with a "jobs" list is a 400 and
// inserts nothing.
func TestSubmitWithJobsFieldRejected(t *testing.T) {
	e := setup(t)
	round := e.createRound(t, 1)
	body := fmt.Sprintf(`{"round_id":%d,"shard_id":0,"n_vectors":1000,"dim":16,"jobs":[{"duration_ms":5,"needs_index":true}]}`, round)
	code, resp := e.do(t, "POST", "/builds", body, nil)
	builds, _ := e.st.ListBuilds(e.ctx, round)
	jobs, _ := e.st.ListJobs(e.ctx, round)
	t.Logf("POST /builds with old-spec jobs list -> %d %s; [db] builds=%d jobs=%d", code, resp, len(builds), len(jobs))
	if code != http.StatusBadRequest {
		t.Errorf("decisions 21/28: unknown field 'jobs' should be rejected with 400; got %d", code)
	}
	if len(builds) != 0 || len(jobs) != 0 {
		t.Errorf("a rejected submit must write nothing; got %d builds %d jobs", len(builds), len(jobs))
	}
}

// Decision 27 / SubmitBuild doc: "The round row is locked for the transaction,
// so concurrent submits into one round serialise and the n_shards cap holds."
// Same property as TestConcurrentSubmitsRespectCap, but through the store
// directly, to separate the store from the HTTP layer.
func TestConcurrentStoreSubmitsRespectCap(t *testing.T) {
	e := setup(t)
	rid, err := e.st.CreateRound(e.ctx, "crosscheck", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	const n = 12
	var mu sync.Mutex
	inserted, full, other := 0, 0, 0
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int32) {
			defer wg.Done()
			ok, err := e.st.SubmitBuild(e.ctx, store.Build{BuildID: store.BuildID(i, rid), RoundID: rid, ShardID: i, NVectors: 1000, Dim: 16, MemBytes: 1, Placement: "gpu"})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && ok:
				inserted++
			case errors.Is(err, store.ErrRoundFull):
				full++
			default:
				other++
				t.Logf("shard %d: ok=%v err=%v", i, ok, err)
			}
		}(int32(i))
	}
	wg.Wait()
	builds, _ := e.st.ListBuilds(e.ctx, rid)
	t.Logf("%d concurrent store.SubmitBuild into n_shards=3: inserted=%d ErrRoundFull=%d other=%d; [db] %d builds", n, inserted, full, other, len(builds))
	if inserted != 3 || full != n-3 || len(builds) != 3 {
		t.Errorf("SubmitBuild doc / decision 27: want 3 inserted, %d ErrRoundFull, 3 rows", n-3)
	}
}
