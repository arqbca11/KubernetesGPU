package crosscheck

// Black-box tests of the Python GPU worker (Phase 1 step 3). The worker is the
// real process; the test plays the scheduler, the reaper and other workers
// through scheduler/store and direct SQL, and reads the outcome from Postgres
// and from the worker's stdout.

import (
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/arqbca11/KubernetesGPU/scheduler/costmodel"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

const (
	smallN = 100_000   // 4.6 s modeled at 128 dims
	longN  = 2_000_000 // 65 s modeled at 128 dims
	dim128 = int32(128)
)

func scaled(n int64, dim int32, scale float64) time.Duration {
	return time.Duration(float64(costmodel.Default().GPUTotal(n, dim)) / scale)
}

// Config table: WORKER_ID, WORKER_MEM_BYTES, HEARTBEAT_INTERVAL_SECONDS.
// Promise: "heartbeat before the first claim and every interval after".
func TestWorkerHeartbeatsBeforeFirstClaimAndEveryInterval(t *testing.T) {
	e := setup(t)
	r := e.round(t, 1)
	id := queueGPU(t, e, r, 0, smallN, dim128, -1, 0)

	w := startWorker(t, "hb-worker", map[string]string{"WORKER_MEM_BYTES": "4000000000", "HEARTBEAT_INTERVAL_SECONDS": "0.4"})
	b := e.waitBuild(t, id, 10*time.Second, "done", func(b store.BuildRow) bool { return b.State == "done" })
	logBuild(t, "after worker finished", b)

	var memBytes int64
	var registered, seen1 time.Time
	if err := e.pool.QueryRow(e.ctx, `SELECT mem_bytes, registered_at, last_seen FROM workers WHERE worker_id = $1`, "hb-worker").Scan(&memBytes, &registered, &seen1); err != nil {
		t.Fatalf("workers row for hb-worker: %v", err)
	}
	t.Logf("[db] workers: worker_id=hb-worker mem_bytes=%d registered_at=%s last_seen=%s", memBytes, registered.Format(time.RFC3339Nano), seen1.Format(time.RFC3339Nano))
	t.Logf("[db] build started_at (claim time, set by now() in the claim) = %s", tptr(b.StartedAt))
	if memBytes != 4000000000 {
		t.Errorf("workers.mem_bytes = %d, want WORKER_MEM_BYTES 4000000000 (config table)", memBytes)
	}
	if b.StartedAt == nil || registered.After(*b.StartedAt) {
		t.Errorf("first heartbeat (registered_at %s) is after the first claim (started_at %s); the spec promises \"heartbeat before the first claim\"",
			registered.Format(time.RFC3339Nano), tptr(b.StartedAt))
	} else {
		t.Logf("first heartbeat precedes first claim, as promised")
	}

	time.Sleep(1500 * time.Millisecond)
	var seen2 time.Time
	if err := e.pool.QueryRow(e.ctx, `SELECT last_seen FROM workers WHERE worker_id = $1`, "hb-worker").Scan(&seen2); err != nil {
		t.Fatal(err)
	}
	t.Logf("[db] 1.5 s later, last_seen=%s (advanced %v)", seen2.Format(time.RFC3339Nano), seen2.Sub(seen1))
	if !seen2.After(seen1) {
		t.Errorf("last_seen did not advance over 1.5 s with HEARTBEAT_INTERVAL_SECONDS=0.4")
	}
	if age := time.Since(seen2); age > time.Second {
		t.Logf("note: last_seen is %v old by the test's clock (clock skew with the DB container is possible)", age)
	}
	_ = w
}

// Promise: claim, build for gpu_total / FAKE_TIME_SCALE (decision 38),
// complete; every build log line carries build_id, attempt, round_id
// (CLAUDE.md conventions).
func TestWorkerCompletesBuildForScaledModelTime(t *testing.T) {
	e := setup(t)
	r := e.round(t, 1)
	const n = 1_000_000
	id := queueGPU(t, e, r, 0, n, dim128, -1, 0)
	want := scaled(n, dim128, 20)

	w := startWorker(t, "done-worker", nil)
	b := e.waitBuild(t, id, 15*time.Second, "done", func(b store.BuildRow) bool { return b.State == "done" })
	logLease(t, "final", b)

	if b.Attempt != 1 {
		t.Errorf("attempt = %d, want 1 (invariant 4: one claim)", b.Attempt)
	}
	if b.StartedAt == nil || b.FinishedAt == nil {
		t.Fatalf("started_at/finished_at not set")
	}
	got := b.FinishedAt.Sub(*b.StartedAt)
	t.Logf("finished_at - started_at = %v; modeled GPU total %v / FAKE_TIME_SCALE 20 = %v", got, costmodel.Default().GPUTotal(n, dim128), want)
	if got < want-want/20 || got > want+800*time.Millisecond {
		t.Errorf("build took %v, want about %v (decision 38: FAKE_TIME_SCALE divides every fake build time)", got, want)
	}

	claimed := w.waitLog(t, 2*time.Second, "claimed", forBuild(id, 1, msgHas("claim")))
	completed := w.waitLog(t, 2*time.Second, "completed", forBuild(id, 1, msgHas("complet")))
	t.Logf("worker log: %s", claimed.str("raw"))
	t.Logf("worker log: %s", completed.str("raw"))
	for _, l := range w.find(func(l logLine) bool { return l.str("build_id") != "" }) {
		if l.str("round_id") == "" || l.str("attempt") == "" {
			t.Errorf("build log line without round_id/attempt: %s", l.str("raw"))
		}
		if l.str("round_id") != "" && l.str("round_id") != itoa(r) {
			t.Errorf("log round_id %s, want %d: %s", l.str("round_id"), r, l.str("raw"))
		}
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// Config table: LOG_FORMAT=text; conventions: build lines carry build_id,
// attempt and round_id in text mode too.
func TestWorkerTextLogFormatCarriesBuildFields(t *testing.T) {
	e := setup(t)
	r := e.round(t, 1)
	id := queueGPU(t, e, r, 0, smallN, dim128, -1, 0)
	w := startWorker(t, "text-worker", map[string]string{"LOG_FORMAT": "text"})
	e.waitBuild(t, id, 10*time.Second, "done", func(b store.BuildRow) bool { return b.State == "done" })
	time.Sleep(200 * time.Millisecond)
	var buildLines []string
	// Match build_id=<id> with a boundary, not the bare id: a bare "0:22" also
	// matches a timestamp like 00:22:20 (this bit once, when round 22 ran at 00:22).
	re := buildRE(id)
	for _, ln := range strings.Split(w.stdout.String(), "\n") {
		if re.MatchString(ln) {
			buildLines = append(buildLines, ln)
		}
		if strings.HasPrefix(strings.TrimSpace(ln), "{") {
			t.Errorf("JSON line in LOG_FORMAT=text output: %s", ln)
		}
	}
	if len(buildLines) < 2 {
		t.Fatalf("want at least claim and complete lines naming %s in text output, got %d:\n%s", id, len(buildLines), w.stdout.String())
	}
	for _, ln := range buildLines {
		t.Logf("text log: %s", ln)
		if !strings.Contains(ln, "attempt") || !strings.Contains(ln, "round_id") {
			t.Errorf("text build line lacks attempt or round_id: %s", ln)
		}
	}
}

// Config table: WORKER_MEM_BYTES, "the claim filters mem_bytes <= this";
// decision 26: fits means mem_bytes <= capacity; decision 7.
func TestWorkerClaimsOnlyWhatFitsItsMemory(t *testing.T) {
	e := setup(t)
	r := e.round(t, 3)
	big := queueGPU(t, e, r, 0, smallN, dim128, 5001, 10)
	exact := queueGPU(t, e, r, 1, smallN, dim128, 5000, 5)
	small := queueGPU(t, e, r, 2, smallN, dim128, 10, 1)

	w := startWorker(t, "small-gpu", map[string]string{"WORKER_MEM_BYTES": "5000"})
	e.waitBuild(t, exact, 10*time.Second, "done", func(b store.BuildRow) bool { return b.State == "done" })
	e.waitBuild(t, small, 10*time.Second, "done", func(b store.BuildRow) bool { return b.State == "done" })
	time.Sleep(500 * time.Millisecond) // a few more poll intervals in which it could wrongly take the big one
	bb := e.mustBuild(t, big)
	logBuild(t, "big (mem 5001, prio 10)", bb)
	logBuild(t, "exact (mem 5000, prio 5)", e.mustBuild(t, exact))
	logBuild(t, "small (mem 10, prio 1)", e.mustBuild(t, small))
	if bb.State != "queued" || bb.Attempt != 0 {
		t.Errorf("build needing 5001 bytes was touched by a 5000-byte worker: state=%s attempt=%d", bb.State, bb.Attempt)
	}
	if ls := w.find(func(l logLine) bool { return l.str("build_id") == big }); len(ls) > 0 {
		t.Errorf("worker logged about the build that does not fit: %s", ls[0].str("raw"))
	}
}

// Promise: "renew every interval for exactly the claimed attempt"; config
// table LEASE_SECONDS / RENEW_INTERVAL_SECONDS. A build three times longer
// than the lease must survive a reaper running all the while (decision 2).
func TestWorkerRenewKeepsLeaseAliveUnderReaper(t *testing.T) {
	e := setup(t)
	r := e.round(t, 1)
	id := queueGPU(t, e, r, 0, longN, dim128, -1, 0)
	t.Logf("scaled build time %v vs LEASE_SECONDS=1", scaled(longN, dim128, 20))

	startWorker(t, "renewer", map[string]string{"LEASE_SECONDS": "1", "RENEW_INTERVAL_SECONDS": "0.2"})
	first := e.waitBuild(t, id, 5*time.Second, "leased", leasedTo("renewer"))
	logLease(t, "just claimed", first)

	var maxLease time.Time
	advances := 0
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		reaped, err := e.st.Reap(e.ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, rp := range reaped {
			t.Errorf("reaper took a lease from a live, renewing worker: %+v", rp)
		}
		b := e.mustBuild(t, id)
		if b.State == "done" {
			logLease(t, "final", b)
			if b.Attempt != 1 {
				t.Errorf("attempt = %d, want 1", b.Attempt)
			}
			break
		}
		if b.LeaseUntil != nil && b.LeaseUntil.After(maxLease) {
			if !maxLease.IsZero() {
				advances++
			}
			maxLease = *b.LeaseUntil
		}
		if b.Attempt != 1 {
			t.Fatalf("attempt changed to %d mid-build", b.Attempt)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("lease_until advanced %d times during the build", advances)
	if e.mustBuild(t, id).State != "done" {
		t.Fatalf("build not done")
	}
	if advances < 5 {
		t.Errorf("lease_until advanced only %d times over a ~3 s build with a 0.2 s renew interval", advances)
	}
}

// Roadmap failure test 1 / flow "worker killed mid-build": kill -9 mid-build,
// lease expires, the reaper requeues, another worker completes it, once.
func TestWorkerKill9MidBuildAnotherWorkerCompletesOnce(t *testing.T) {
	e := setup(t)
	r := e.round(t, 1)
	id := queueGPU(t, e, r, 0, longN, dim128, -1, 0)

	a := startWorker(t, "gpu-A", map[string]string{"LEASE_SECONDS": "1", "RENEW_INTERVAL_SECONDS": "0.2"})
	e.waitBuild(t, id, 5*time.Second, "leased to A", leasedTo("gpu-A"))
	time.Sleep(700 * time.Millisecond)
	a.signal(t, syscall.SIGKILL)
	a.waitExit(t, 5*time.Second)
	logLease(t, "after kill -9 of A", e.mustBuild(t, id))

	var reaped []store.Reaped
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(reaped) == 0 {
		var err error
		if reaped, err = e.st.Reap(e.ctx, 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(reaped) != 1 || reaped[0].BuildID != id || reaped[0].Attempt != 1 || reaped[0].LeaseOwner != "gpu-A" {
		t.Fatalf("reap after expiry: %+v, want one entry for %s attempt 1 owner gpu-A", reaped, id)
	}
	t.Logf("reaper returned %s attempt 1 (owner gpu-A) to queued", id)

	bw := startWorker(t, "gpu-B", map[string]string{"LEASE_SECONDS": "1", "RENEW_INTERVAL_SECONDS": "0.2"})
	b := e.waitBuild(t, id, 15*time.Second, "done", func(b store.BuildRow) bool { return b.State == "done" })
	logLease(t, "final", b)
	if b.Attempt != 2 {
		t.Errorf("attempt = %d, want 2 (invariant 4)", b.Attempt)
	}
	time.Sleep(200 * time.Millisecond)
	ca := a.find(msgHas("complet"))
	cb := bw.find(forBuild(id, 2, msgHas("complet")))
	t.Logf("completion lines: A=%d B=%d", len(ca), len(cb))
	if len(ca) != 0 || len(cb) != 1 {
		t.Errorf("want exactly one completion, by B with attempt 2; A=%v B=%v", ca, cb)
	}
}

// Roadmap failure test 2, flow "SIGSTOP variant", decision 36: a worker paused
// past its lease, reaped and re-claimed by another, wakes, gets its renew
// rejected, cancels, logs "lost ownership" and writes nothing more to the row
// (invariant 3).
func TestWorkerSIGSTOPPastLeaseLosesOwnershipAndWritesNothing(t *testing.T) {
	e := setup(t)
	r := e.round(t, 1)
	id := queueGPU(t, e, r, 0, longN, dim128, -1, 0)
	t.Logf("scaled build time %v at FAKE_TIME_SCALE=10", scaled(longN, dim128, 10))

	a := startWorker(t, "gpu-A", map[string]string{"FAKE_TIME_SCALE": "10", "LEASE_SECONDS": "1", "RENEW_INTERVAL_SECONDS": "0.2"})
	e.waitBuild(t, id, 5*time.Second, "leased to A", leasedTo("gpu-A"))
	time.Sleep(500 * time.Millisecond)
	a.signal(t, syscall.SIGSTOP)

	var reaped []store.Reaped
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(reaped) == 0 {
		var err error
		if reaped, err = e.st.Reap(e.ctx, 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(reaped) != 1 {
		t.Fatalf("lease of a stopped worker was not reaped: %+v", reaped)
	}
	c, ok, err := e.st.Claim(e.ctx, "gpu-other", 1<<40, 30*time.Second)
	if err != nil || !ok || c.BuildID != id || c.Attempt != 2 {
		t.Fatalf("other worker claim: %+v ok=%v err=%v", c, ok, err)
	}
	before := e.mustBuild(t, id)
	logLease(t, "reaped and re-claimed by gpu-other while A is stopped", before)

	a.signal(t, syscall.SIGCONT)
	lost := a.waitLog(t, 3*time.Second, "lost ownership", forBuild(id, 1, msgHas("lost ownership")))
	t.Logf("worker A log: %s", lost.str("raw"))

	// Give A every chance to misbehave: well past when its build would have ended.
	time.Sleep(scaled(longN, dim128, 10))
	after := e.mustBuild(t, id)
	logLease(t, "after A resumed", after)
	if after.State != "leased" || after.Attempt != 2 || deref(after.LeaseOwner) != "gpu-other" ||
		!after.LeaseUntil.Equal(*before.LeaseUntil) || deref(after.FailReason) != "<nil>" {
		t.Errorf("A wrote to the row after losing it (invariant 3): before %+v after %+v", before, after)
	}
	for _, l := range a.find(forBuild(id, 1, func(l logLine) bool {
		m := strings.ToLower(l.str("msg"))
		return strings.Contains(m, "complet") || strings.Contains(m, "fail") || strings.Contains(m, "releas")
	})) {
		t.Errorf("A logged a write attempt after losing the lease (decision 36 says it only logs lost ownership): %s", l.str("raw"))
	}
	if ok, err := e.st.Complete(e.ctx, id, 2); err != nil || !ok {
		t.Errorf("rightful owner's complete(attempt 2): ok=%v err=%v", ok, err)
	}
	logBuild(t, "after gpu-other completes attempt 2", e.mustBuild(t, id))

	a.signal(t, syscall.SIGTERM)
	code := a.waitExit(t, 5*time.Second)
	t.Logf("A exit code after SIGTERM: %d", code)
	if code != 0 {
		t.Errorf("exit code %d after SIGTERM, want 0", code)
	}
}

// Decision 36: "the renewer's rejected renew sets lost and cancel; the fake
// builder checks cancel every 100 ms and raises; the main loop then neither
// completes nor releases, it only logs lost ownership". Here the worker is not
// paused: the reaper and another claimer act while it is building, and it must
// stop within about one renew interval rather than build to the end.
func TestWorkerReclaimedMidBuildCancelsWithinARenewInterval(t *testing.T) {
	e := setup(t)
	r := e.round(t, 2)
	id := queueGPU(t, e, r, 0, longN, dim128, -1, 0)
	buildTime := scaled(longN, dim128, 10)

	a := startWorker(t, "gpu-A", map[string]string{"FAKE_TIME_SCALE": "10", "LEASE_SECONDS": "30", "RENEW_INTERVAL_SECONDS": "1"})
	e.waitBuild(t, id, 5*time.Second, "leased to A", leasedTo("gpu-A"))
	time.Sleep(500 * time.Millisecond)

	// Play the reaper: expire the lease and reap. A renew may land between the
	// two statements (decision 25 allows renewing an expired lease), so retry.
	reapedOK := false
	for i := 0; i < 20 && !reapedOK; i++ {
		if _, err := e.pool.Exec(e.ctx, `UPDATE builds SET lease_until = now() - interval '1 second' WHERE build_id = $1 AND attempt = 1 AND state = 'leased'`, id); err != nil {
			t.Fatal(err)
		}
		reaped, err := e.st.Reap(e.ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, rp := range reaped {
			if rp.BuildID == id {
				reapedOK = true
			}
		}
	}
	if !reapedOK {
		t.Fatalf("could not reap %s", id)
	}
	c, ok, err := e.st.Claim(e.ctx, "gpu-other", 1<<40, 30*time.Second)
	if err != nil || !ok || c.Attempt != 2 {
		t.Fatalf("other claim: %+v ok=%v err=%v", c, ok, err)
	}
	tReclaim := time.Now()
	before := e.mustBuild(t, id)
	logLease(t, "reaped and re-claimed by gpu-other (attempt 2) while A builds attempt 1", before)

	lost := a.waitLog(t, 3*time.Second, "lost ownership", forBuild(id, 1, msgHas("lost ownership")))
	took := time.Since(tReclaim)
	t.Logf("worker A log %v after the re-claim (RENEW_INTERVAL_SECONDS=1, build would need ~%v more): %s", took, buildTime-time.Second, lost.str("raw"))
	if took > 2500*time.Millisecond {
		t.Errorf("A noticed the lost lease only after %v; decision 36 says the rejected renew cancels at once", took)
	}

	// The cancelled build must not reach complete/fail/release, now or later.
	time.Sleep(buildTime)
	after := e.mustBuild(t, id)
	logLease(t, "one full build time later", after)
	if after.State != "leased" || after.Attempt != 2 || deref(after.LeaseOwner) != "gpu-other" || !after.LeaseUntil.Equal(*before.LeaseUntil) {
		t.Errorf("row changed after A lost it (invariant 3): %+v", after)
	}
	for _, l := range a.find(forBuild(id, 1, func(l logLine) bool {
		m := strings.ToLower(l.str("msg"))
		return strings.Contains(m, "complet") || strings.Contains(m, "fail") || strings.Contains(m, "releas")
	})) {
		t.Errorf("A logged a write after losing ownership: %s", l.str("raw"))
	}

	// Not promised explicitly: does A go back to work after a lost lease?
	id2 := queueGPU(t, e, r, 1, smallN, dim128, -1, 0)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && e.mustBuild(t, id2).State != "done" {
		time.Sleep(50 * time.Millisecond)
	}
	b2 := e.mustBuild(t, id2)
	logBuild(t, "a new build queued after A lost its lease", b2)
	if b2.State != "done" {
		t.Logf("observation: A did not take new work after losing a lease (spec does not say whether it should)")
	}
}

// Decision 37 and the promise list: on SIGTERM, a build whose remaining
// (scaled) time exceeds SHUTDOWN_FINISH_BUDGET_SECONDS is cancelled and
// released; release clears lease columns and started_at (decision 24);
// exit code 0 after SIGTERM.
func TestWorkerSIGTERMReleasesBuildOverBudget(t *testing.T) {
	e := setup(t)
	r := e.round(t, 1)
	id := queueGPU(t, e, r, 0, longN, dim128, -1, 0)
	t.Logf("scaled build %v, budget 0.5 s", scaled(longN, dim128, 20))

	a := startWorker(t, "gpu-term", map[string]string{"SHUTDOWN_FINISH_BUDGET_SECONDS": "0.5", "LEASE_SECONDS": "30"})
	claimed := e.waitBuild(t, id, 5*time.Second, "leased", leasedTo("gpu-term"))
	logLease(t, "claimed", claimed)
	time.Sleep(700 * time.Millisecond)
	a.signal(t, syscall.SIGTERM)
	code := a.waitExit(t, 3*time.Second)
	b := e.mustBuild(t, id)
	logLease(t, "after SIGTERM and exit", b)
	t.Logf("exit code %d", code)
	for _, l := range a.find(func(l logLine) bool { return l.str("build_id") == id }) {
		t.Logf("worker log: %s", l.str("raw"))
	}
	if code != 0 {
		t.Errorf("exit code %d, want 0 after SIGTERM", code)
	}
	if b.State != "queued" || b.Attempt != 1 || b.LeaseOwner != nil || b.LeaseUntil != nil || b.StartedAt != nil || b.FinishedAt != nil {
		t.Errorf("want released row: queued, attempt 1, no owner, no lease_until, no started_at (decision 24); got %+v", b)
	}
	if len(a.find(forBuild(id, 1, msgHas("releas")))) == 0 {
		t.Errorf("no release log line carrying build_id and attempt")
	}
	// The released build is claimable at once, with the next attempt.
	c, ok, err := e.st.Claim(e.ctx, "gpu-next", 1<<40, 30*time.Second)
	if err != nil || !ok || c.BuildID != id || c.Attempt != 2 {
		t.Errorf("re-claim after release: %+v ok=%v err=%v", c, ok, err)
	}
}

// Decision 37: "stop claiming; finish the current build if its remaining time
// is within SHUTDOWN_FINISH_BUDGET_SECONDS" (default 5). A second queued
// build must not be claimed after SIGTERM.
func TestWorkerSIGTERMFinishesBuildWithinBudgetAndStopsClaiming(t *testing.T) {
	e := setup(t)
	r := e.round(t, 2)
	id := queueGPU(t, e, r, 0, longN, dim128, -1, 10)
	next := queueGPU(t, e, r, 1, smallN, dim128, -1, 1)
	t.Logf("scaled build %v, default budget 5 s", scaled(longN, dim128, 20))

	a := startWorker(t, "gpu-term", map[string]string{"LEASE_SECONDS": "30", "SHUTDOWN_FINISH_BUDGET_SECONDS": ""})
	e.waitBuild(t, id, 5*time.Second, "leased", leasedTo("gpu-term"))
	time.Sleep(500 * time.Millisecond)
	tTerm := time.Now()
	a.signal(t, syscall.SIGTERM)
	code := a.waitExit(t, 8*time.Second)
	t.Logf("exited %v after SIGTERM with code %d", time.Since(tTerm), code)
	b := e.mustBuild(t, id)
	n := e.mustBuild(t, next)
	logLease(t, "build in progress at SIGTERM", b)
	logLease(t, "second queued build", n)
	if code != 0 {
		t.Errorf("exit code %d, want 0", code)
	}
	if b.State != "done" || b.Attempt != 1 {
		t.Errorf("build with ~2.75 s left under a 5 s budget should finish: state=%s attempt=%d", b.State, b.Attempt)
	}
	if n.State != "queued" || n.Attempt != 0 {
		t.Errorf("worker claimed after SIGTERM (decision 37: stop claiming): state=%s attempt=%d", n.State, n.Attempt)
	}
}

// Promise: "on a build error, fail with the exception text as the reason".
// The only build error reachable from outside through documented config is a
// cost-model constant that makes the builder's arithmetic raise
// (COST_BANDWIDTH=0: transfer time divides by bandwidth).
// Spec resolution after this test first ran (decision 39): a bad COST_* value
// is refused at startup, before any claim, with exit status 2. The build is
// never touched.
func TestWorkerBadConfigIsRefusedBeforeAnyClaim(t *testing.T) {
	e := setup(t)
	r := e.round(t, 1)
	id := queueGPU(t, e, r, 0, smallN, dim128, -1, 0)

	a := startWorker(t, "gpu-badcfg", map[string]string{"COST_BANDWIDTH": "0"})
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) && !a.exited() {
		time.Sleep(50 * time.Millisecond)
	}
	if !a.exited() {
		t.Fatal("decision 39: a worker with a bad config must exit at startup")
	}
	code := a.cmd.ProcessState.ExitCode()
	tail := strings.TrimSpace(a.stderr.String())
	lines := strings.Split(tail, "\n")
	t.Logf("worker exited with code %d; last stderr line: %s", code, lines[len(lines)-1])
	if code != 2 {
		t.Errorf("decision 39: exit status 2 for bad config; got %d", code)
	}
	if !strings.Contains(tail, "COST_BANDWIDTH") {
		t.Errorf("decision 39: the message names the bad variable; got %q", lines[len(lines)-1])
	}
	b := e.mustBuild(t, id)
	logLease(t, "after the refused start", b)
	if b.State != "queued" || b.Attempt != 0 {
		t.Errorf("a refused worker must not have claimed anything; got state=%s attempt=%d", b.State, b.Attempt)
	}
	ws, _ := e.st.ListWorkers(e.ctx)
	if len(ws) != 0 {
		t.Errorf("a refused worker must not have registered; workers=%+v", ws)
	}
}

// "on a build error, fail with the exception text as the reason" (decisions 36/41),
// triggered through the documented FAKE_FAIL_BUILD_IDS knob (decision 40).
func TestWorkerBuildErrorRecordsFailWithReason(t *testing.T) {
	e := setup(t)
	r := e.round(t, 2)
	bad := queueGPU(t, e, r, 0, smallN, dim128, -1, 0)
	good := queueGPU(t, e, r, 1, smallN, dim128, -1, 0)

	a := startWorker(t, "gpu-err", map[string]string{"FAKE_FAIL_BUILD_IDS": bad})
	deadline := time.Now().Add(8 * time.Second)
	var b, g store.BuildRow
	for time.Now().Before(deadline) {
		b, g = e.mustBuild(t, bad), e.mustBuild(t, good)
		if b.State == "failed" && g.State == "done" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	logLease(t, "the injected-failure build", b)
	logLease(t, "the other build", g)
	if b.State != "failed" {
		t.Errorf("want state failed; got state=%s attempt=%d lease_owner=%s", b.State, b.Attempt, deref(b.LeaseOwner))
	} else if b.FailReason == nil || !strings.Contains(*b.FailReason, "injected failure") {
		t.Errorf("fail_reason should carry the exception text; got %v", b.FailReason)
	}
	if b.LeaseOwner != nil || b.LeaseUntil != nil {
		t.Errorf("a failed build must not keep its lease; owner=%v until=%v", b.LeaseOwner, b.LeaseUntil)
	}
	if g.State != "done" {
		t.Errorf("the worker must survive a build error and keep claiming; other build is %s", g.State)
	}
	if a.exited() {
		t.Errorf("decision 41: a build error must not crash the worker; it exited with %d", a.cmd.ProcessState.ExitCode())
	}
}
