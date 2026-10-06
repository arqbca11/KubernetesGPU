package crosscheck

// Helpers for the step 6 load cross-check: the whole Compose stack with the
// shard simulator running rounds forever (ROUNDS=0), observed only through
// the scheduler's HTTP API, the docker CLI and container logs.
//
// The stack is the same project and ports as the step 4 helpers
// (kgpu-xcheck, 28080, 25432) but with its own environment, so the load tests
// bring it up themselves. They set stackUp so TestMain's teardown
// (`down -v --remove-orphans`) runs after them.

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arqbca11/KubernetesGPU/experiments/workload"
)

// The environment the task prescribes for this run.
var ldEnv = map[string]string{
	"API_PORT":                   "28080",
	"PG_PORT":                    "25432",
	"FAKE_TIME_SCALE":            "2",
	"SCENARIO":                   "skewed",
	"N_SHARDS":                   "6",
	"SEED":                       "1",
	"ROUNDS":                     "0",
	"ROUND_GAP":                  "1s",
	"LOG_FORMAT":                 "text",
	"LEASE_SECONDS":              "5",
	"RENEW_INTERVAL_SECONDS":     "1",
	"HEARTBEAT_INTERVAL_SECONDS": "1",
	"REAP_INTERVAL":              "1s",
	"WORKER_STALE_AFTER":         "5s",
	"MIN_WORKERS":                "2",
}

const (
	ldShardsName    = xcProject + "-shards-1"
	ldSchedulerName = xcProject + "-scheduler-1"
	ldPostgresName  = xcProject + "-postgres-1"
	ldTimeScale     = 2.0 // FAKE_TIME_SCALE above
)

// The stack size is parametrised for the step 7 phase-boundary run:
// KGPU_XCHECK_SHARDS (default 6) and KGPU_XCHECK_WORKERS (default 2). At the
// defaults every setting and deadline is what the step 6 cross-check used.
var (
	ldNShards  = envInt("KGPU_XCHECK_SHARDS", 6)
	ldNWorkers = envInt("KGPU_XCHECK_WORKERS", 2)
)

func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		panic(fmt.Sprintf("%s=%q: want a positive integer", name, v))
	}
	return n
}

func init() {
	ldEnv["N_SHARDS"] = strconv.Itoa(ldNShards)
	ldEnv["MIN_WORKERS"] = strconv.Itoa(ldNWorkers)
}

// ldScaled stretches a step 6 deadline for a bigger stack: a 6-shard round
// takes about 15 s real, a 50-shard round about 25 to 30 s, and recovery waits
// sit behind a longer GPU queue. At 6 shards it returns d unchanged.
func ldScaled(d time.Duration) time.Duration {
	if ldNShards <= 6 {
		return d
	}
	return d * 3 / 2
}

var (
	ldOnce sync.Once
	ldErr  error
)

func ldComposeCmd(args ...string) *exec.Cmd {
	root := xcRepoRoot()
	full := append([]string{"compose", "-p", xcProject, "-f", filepath.Join(root, "deploy", "compose.yaml")}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Dir = root
	cmd.Env = os.Environ()
	for k, v := range ldEnv {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd
}

func ldCompose(t *testing.T, args ...string) string {
	t.Helper()
	start := time.Now()
	out, err := ldComposeCmd(args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	t.Logf("[docker] compose -p %s %s (%.1fs)", xcProject, strings.Join(args, " "), time.Since(start).Seconds())
	return string(out)
}

// requireLoadStack skips unless KGPU_COMPOSE=1. The first call wipes any
// leftover xcheck project and brings up a fresh stack with the load settings;
// every call then checks the baseline: scheduler healthy, ldNWorkers live workers,
// the simulator still running (its restart policy is "no", so a dead
// simulator must not be silently restarted by `up`).
func requireLoadStack(t *testing.T) {
	t.Helper()
	if !composeEnabled() {
		t.Skip("KGPU_COMPOSE=1 not set: compose load tests need Docker and take minutes")
	}
	ldOnce.Do(func() {
		stackUp = true // TestMain tears down even if up fails half way
		_ = ldComposeCmd("down", "-v", "--remove-orphans").Run()
		args := []string{"up", "-d", "--build", "--wait"}
		if ldNWorkers != 2 {
			args = append(args, "--scale", "worker="+strconv.Itoa(ldNWorkers))
		}
		out, err := ldComposeCmd(args...).CombinedOutput()
		if err != nil {
			ldErr = fmt.Errorf("compose up: %v\n%s", err, out)
		}
	})
	if ldErr != nil {
		t.Fatal(ldErr)
	}
	if st := ldInspect(t, ldShardsName, "{{.State.Status}}"); st != "running" {
		t.Fatalf("shard simulator is %s, not running (ROUNDS=0 should run forever):\n%s", st, tail(containerLogs(t, ldShardsName), 20))
	}
	// Restore any paused or stopped worker from an earlier failed test.
	for _, c := range serviceContainers(t, "worker", true) {
		switch ldInspect(t, c.Name, "{{.State.Status}}") {
		case "paused":
			dockerCmd(t, "unpause", c.Name)
		case "exited", "created":
			dockerCmd(t, "start", c.Name)
		}
	}
	waitHealthy(t, 60*time.Second)
	waitLiveWorkers(t, ldNWorkers, 30*time.Second)
	t.Logf("[setup] stack %s (%d shards, %d workers): %s", xcProject, ldNShards, ldNWorkers, psSummary(t))
}

func ldInspect(t *testing.T, name, format string) string {
	t.Helper()
	out, err := exec.Command("docker", "inspect", "-f", format, name).CombinedOutput()
	if err != nil {
		t.Fatalf("docker inspect %s: %v %s", name, err, out)
	}
	return strings.TrimSpace(string(out))
}

func ldRestartCount(t *testing.T, name string) int {
	t.Helper()
	n, _ := strconv.Atoi(ldInspect(t, name, "{{.RestartCount}}"))
	return n
}

func logsSince(t *testing.T, name string, since time.Time) string {
	t.Helper()
	out, err := exec.Command("docker", "logs", "--since", since.UTC().Format(time.RFC3339Nano), name).CombinedOutput()
	if err != nil {
		t.Logf("docker logs --since %s: %v", name, err)
	}
	return string(out)
}

// ---- round status over the API ----

type ldJob struct {
	ShardID    int32      `json:"shard_id"`
	Seq        int32      `json:"seq"`
	DurationMs int64      `json:"duration_ms"`
	NeedsIndex bool       `json:"needs_index"`
	ArrivedAt  time.Time  `json:"arrived_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

type ldStatus struct {
	ShardID    int32     `json:"shard_id"`
	QueueDepth int       `json:"queue_depth"`
	WaitingIdx int       `json:"waiting_needs_index"`
	StreamDone bool      `json:"stream_done"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type ldRound struct {
	Round struct {
		RoundID    int64      `json:"round_id"`
		Scenario   string     `json:"scenario"`
		Seed       int64      `json:"seed"`
		NShards    int        `json:"n_shards"`
		StartedAt  time.Time  `json:"started_at"`
		FinishedAt *time.Time `json:"finished_at"`
	} `json:"round"`
	Builds      []xBuild   `json:"builds"`
	Jobs        []ldJob    `json:"jobs"`
	ShardStatus []ldStatus `json:"shard_status"`
}

func (r ldRound) build(id string) (xBuild, bool) {
	for _, b := range r.Builds {
		if b.BuildID == id {
			return b, true
		}
	}
	return xBuild{}, false
}

func (r ldRound) largest() (xBuild, bool) {
	var best xBuild
	ok := false
	for _, b := range r.Builds {
		if !ok || b.NVectors > best.NVectors {
			best, ok = b, true
		}
	}
	return best, ok
}

func getRoundTry(id int64) (ldRound, int, error) {
	var r ldRound
	code, _, err := apiTry("GET", "/rounds/"+strconv.FormatInt(id, 10), nil, &r)
	return r, code, err
}

var ldLastRound int64 = 1

// latestRound returns the highest round id the scheduler knows (0 if none).
func latestRound(t *testing.T) int64 {
	t.Helper()
	id := ldLastRound
	for {
		_, code, err := getRoundTry(id)
		if err != nil {
			t.Fatalf("GET /rounds/%d: %v", id, err)
		}
		if code != http.StatusOK {
			break
		}
		id++
	}
	ldLastRound = max(id-1, 1)
	return id - 1
}

// waitFreshRound waits for a round that started after now (id > the current
// latest) and has all n_shards builds submitted, so a failure injected into
// it lands mid-round.
func waitFreshRound(t *testing.T, d time.Duration) ldRound {
	t.Helper()
	after := latestRound(t)
	deadline := time.Now().Add(d)
	for {
		if id := latestRound(t); id > after {
			r, code, err := getRoundTry(id)
			if err == nil && code == http.StatusOK && len(r.Builds) == r.Round.NShards && r.Round.FinishedAt == nil {
				t.Logf("[api] fresh round %d started at %s with %d builds submitted", id, r.Round.StartedAt.Format("15:04:05.000"), len(r.Builds))
				return r
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no fresh round within %v (latest %d)", d, latestRound(t))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitRoundBuild polls the round until pick returns a build, and returns it.
func waitRoundBuild(t *testing.T, round int64, d time.Duration, desc string, pick func(ldRound) (xBuild, bool)) (xBuild, ldRound) {
	t.Helper()
	deadline := time.Now().Add(d)
	var r ldRound
	for {
		cur, code, err := getRoundTry(round)
		if err == nil && code == http.StatusOK {
			r = cur
			if b, ok := pick(r); ok {
				return b, r
			}
		}
		if time.Now().After(deadline) {
			var states []string
			for _, b := range r.Builds {
				states = append(states, b.String())
			}
			t.Fatalf("round %d: timed out after %v waiting for %s; builds: %s", round, d, desc, strings.Join(states, "; "))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// freshlyLeased picks a build leased at attempt 1 that started under maxAge
// ago, so the failure lands well before its fake build ends. If largest is
// set, only the round's largest build qualifies.
func freshlyLeased(largest bool, minN int64, maxAge time.Duration) func(ldRound) (xBuild, bool) {
	return func(r ldRound) (xBuild, bool) {
		var cands []xBuild
		if largest {
			if b, ok := r.largest(); ok {
				cands = []xBuild{b}
			}
		} else {
			cands = append(cands, r.Builds...)
		}
		sort.Slice(cands, func(i, j int) bool { return cands[i].NVectors > cands[j].NVectors })
		for _, b := range cands {
			if b.State == "leased" && b.Attempt == 1 && b.NVectors >= minN && b.StartedAt != nil && time.Since(*b.StartedAt) < maxAge && b.LeaseOwner != nil {
				return b, true
			}
		}
		return xBuild{}, false
	}
}

// ---- worker logs ----

var ldCompletedRE = regexp.MustCompile(`\scompleted build_id=(\S+) attempt=(\d+)`)

// completions maps build_id -> every "completed" line (with its container)
// across every worker container, including restarted and stopped ones.
type ldCompletion struct {
	Container string
	Attempt   int
	Line      string
}

func allCompletions(t *testing.T) map[string][]ldCompletion {
	t.Helper()
	res := map[string][]ldCompletion{}
	for _, c := range serviceContainers(t, "worker", true) {
		for _, l := range strings.Split(containerLogs(t, c.Name), "\n") {
			m := ldCompletedRE.FindStringSubmatch(l)
			if m == nil {
				continue
			}
			a, _ := strconv.Atoi(m[2])
			res[m[1]] = append(res[m[1]], ldCompletion{Container: c.Name, Attempt: a, Line: strings.TrimSpace(l)})
		}
	}
	return res
}

// linesWith returns lines that mention build_id=id and attempt=a (a<0: any)
// and contain every substring in subs.
func linesWith(logs, id string, a int, subs ...string) []string {
	re := buildRE(id)
	are := regexp.MustCompile(`attempt=` + strconv.Itoa(a) + `(\s|$)`)
	var out []string
	for _, l := range strings.Split(logs, "\n") {
		if !re.MatchString(l) {
			continue
		}
		if a >= 0 && !are.MatchString(l) {
			continue
		}
		ok := true
		for _, s := range subs {
			if !strings.Contains(l, s) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, strings.TrimSpace(l))
		}
	}
	return out
}

// ldCrashWorker SIGKILLs the python process inside a worker container (not
// PID 1, which is tini under init: true) and waits for the restart policy to
// bring the container back (RestartCount goes up). Decision 47.
func ldCrashWorker(t *testing.T, name string) {
	t.Helper()
	if !strings.HasPrefix(name, xcProject+"-") {
		t.Fatalf("refusing to touch %s", name)
	}
	before := ldRestartCount(t, name)
	script := `for p in /proc/[0-9]*; do pid=${p#/proc/}; [ "$pid" = 1 ] && continue; case "$(tr "\0" " " < $p/cmdline 2>/dev/null)" in python*) kill -9 $pid;; esac; done`
	start := time.Now()
	out, err := exec.Command("docker", "exec", name, "sh", "-c", script).CombinedOutput()
	// The exec may itself report an error when its container dies under it.
	t.Logf("[docker] SIGKILL python inside %s (exec err=%v %s) in %.2fs", name, err, strings.TrimSpace(string(out)), time.Since(start).Seconds())
	deadline := time.Now().Add(30 * time.Second)
	for ldRestartCount(t, name) <= before {
		if time.Now().After(deadline) {
			t.Fatalf("%s: RestartCount stayed %d after the crash; status %s", name, before, ldInspect(t, name, "{{.State.Status}}"))
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("[docker] %s restarted by policy: RestartCount %d -> %d, %.1fs after the kill", name, before, ldRestartCount(t, name), time.Since(start).Seconds())
}

// ---- the simulator's timeline ----

type ldTLRow struct {
	Shard    int
	NVectors int64
	Place    string
	Att      int
	BDone    float64 // seconds into the round
	Finish   float64
	Queries  int
	MaxWait  float64
}

type ldTimeline struct {
	Header   string
	Summary  string
	Rows     map[int]ldTLRow
	Finished string // the structured "round finished" line
}

var ldRowRE = regexp.MustCompile(`^(\d+)\s+(\d+)\s+(gpu|local)\s+(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\d+)\s+(\S+)`)

func secs(s string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSuffix(s, "s"), 64)
	if err != nil {
		return math.NaN()
	}
	return v
}

// waitTimeline waits for the simulator to print round N's "round finished"
// line and its text timeline, and parses the per-shard rows.
func waitTimeline(t *testing.T, round int64, d time.Duration) ldTimeline {
	t.Helper()
	deadline := time.Now().Add(d)
	hdr := regexp.MustCompile(`^round ` + strconv.FormatInt(round, 10) + `\s+scenario=`)
	fin := regexp.MustCompile(`msg="round finished" round_id=` + strconv.FormatInt(round, 10) + `\s`)
	for {
		lines := strings.Split(containerLogs(t, ldShardsName), "\n")
		var tl ldTimeline
		tl.Rows = map[int]ldTLRow{}
		for i, l := range lines {
			if fin.MatchString(l) {
				tl.Finished = strings.TrimSpace(l)
			}
			if !hdr.MatchString(l) {
				continue
			}
			tl.Header = strings.TrimSpace(l)
			if i+1 < len(lines) {
				tl.Summary = strings.TrimSpace(lines[i+1])
			}
			for _, r := range lines[i+1 : min(len(lines), i+5+ldNShards+5)] {
				m := ldRowRE.FindStringSubmatch(strings.TrimSpace(r))
				if m == nil {
					continue
				}
				sh, _ := strconv.Atoi(m[1])
				n, _ := strconv.ParseInt(m[2], 10, 64)
				att, _ := strconv.Atoi(m[4])
				q, _ := strconv.Atoi(m[9])
				tl.Rows[sh] = ldTLRow{Shard: sh, NVectors: n, Place: m[3], Att: att, BDone: secs(m[7]), Finish: secs(m[8]), Queries: q, MaxWait: secs(m[10])}
			}
		}
		if tl.Finished != "" && len(tl.Rows) == ldNShards {
			return tl
		}
		if time.Now().After(deadline) {
			t.Fatalf("simulator did not print round %d's timeline within %v (finished line %q, %d rows); log tail:\n%s",
				round, d, tl.Finished, len(tl.Rows), tail(containerLogs(t, ldShardsName), 30))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// ---- the round-completes-correctly property ----

type ldVerify struct {
	Round    ldRound
	Timeline ldTimeline
}

// verifyRound checks the property every load test asserts once its failure
// has been injected: the round still completes correctly.
//
//   - the round is stamped (finished_at set) and finished_at is the latest
//     build or query finish (decision 18);
//   - it holds n_shards builds, every one done, and every one completed exactly
//     once across all worker containers' logs, by the attempt the row records
//     (invariants 3 and 4);
//   - every recorded query has started and finished, in order, and no
//     needs_index query started before its shard's build was done
//     (decision 32);
//   - the recorded queries are exactly the seeded workload's: every query of
//     every shard, including those that arrived before the shard's DDL
//     (decision 59, backfilled), in arrival order, with its needs_index and
//     modeled duration, and arrived on schedule (ldCheckWorkload);
//   - every shard reported stream_done with an empty queue (decision 30);
//   - the simulator logged "round finished" with the same query count and
//     printed a timeline whose attempt column matches Postgres; it logged no
//     ERROR for this round and is still running.
func verifyRound(t *testing.T, round int64, d time.Duration) ldVerify {
	t.Helper()
	deadline := time.Now().Add(d)
	var r ldRound
	for {
		cur, code, err := getRoundTry(round)
		if err == nil && code == http.StatusOK {
			r = cur
			if r.Round.FinishedAt != nil {
				break
			}
		}
		if time.Now().After(deadline) {
			var states []string
			for _, b := range r.Builds {
				states = append(states, b.String())
			}
			unfinished := 0
			for _, j := range r.Jobs {
				if j.FinishedAt == nil {
					unfinished++
				}
			}
			t.Fatalf("round %d not stamped within %v: builds %s; %d of %d queries unfinished; shard_status %+v",
				round, d, strings.Join(states, "; "), unfinished, len(r.Jobs), r.ShardStatus)
		}
		time.Sleep(250 * time.Millisecond)
	}
	fin := *r.Round.FinishedAt
	t.Logf("[api] round %d stamped finished_at=%s (%.2fs after start)", round, fin.Format("15:04:05.000"), fin.Sub(r.Round.StartedAt).Seconds())

	// Builds.
	if len(r.Builds) != r.Round.NShards {
		t.Errorf("round %d holds %d builds, n_shards=%d", round, len(r.Builds), r.Round.NShards)
	}
	comps := allCompletions(t)
	latest := r.Round.StartedAt
	for _, b := range r.Builds {
		if b.State != "done" || b.FinishedAt == nil {
			t.Errorf("build not done: %s", b)
			continue
		}
		if b.FinishedAt.After(latest) {
			latest = *b.FinishedAt
		}
		if b.Placement != "gpu" {
			continue
		}
		cs := comps[b.BuildID]
		if len(cs) != 1 {
			t.Errorf("build %s: %d worker 'completed' lines across all worker containers, want exactly 1: %+v", b.BuildID, len(cs), cs)
		} else if cs[0].Attempt != int(b.Attempt) {
			t.Errorf("build %s: completed by attempt %d but the row says attempt %d (%s)", b.BuildID, cs[0].Attempt, b.Attempt, cs[0].Line)
		}
	}

	// Queries.
	doneAt := map[int32]time.Time{}
	for _, b := range r.Builds {
		if b.FinishedAt != nil {
			doneAt[b.ShardID] = *b.FinishedAt
		}
	}
	unfinished, early := 0, 0
	for _, j := range r.Jobs {
		if j.StartedAt == nil || j.FinishedAt == nil {
			unfinished++
			continue
		}
		if j.StartedAt.Before(j.ArrivedAt) || j.FinishedAt.Before(*j.StartedAt) {
			t.Errorf("query %d/%d out of order: arrived %s started %s finished %s", j.ShardID, j.Seq, j.ArrivedAt, j.StartedAt, j.FinishedAt)
		}
		if j.NeedsIndex && j.StartedAt.Before(doneAt[j.ShardID]) {
			early++
			if early <= 3 {
				t.Errorf("needs_index query %d/%d started %s before its build finished %s", j.ShardID, j.Seq, j.StartedAt.Format("15:04:05.000"), doneAt[j.ShardID].Format("15:04:05.000"))
			}
		}
		if j.FinishedAt.After(latest) {
			latest = *j.FinishedAt
		}
	}
	if unfinished > 0 {
		t.Errorf("round %d stamped with %d of %d queries unfinished", round, unfinished, len(r.Jobs))
	}
	if early > 0 {
		t.Errorf("%d needs_index queries started before their build was done", early)
	}
	if diff := fin.Sub(latest); diff > time.Millisecond || diff < -time.Millisecond {
		t.Errorf("round finished_at %s is not the latest child finish %s (decision 18)", fin.Format(time.RFC3339Nano), latest.Format(time.RFC3339Nano))
	}

	// Every recorded query against the seeded workload (invariant 10,
	// decisions 59 and 60).
	ldCheckWorkload(t, r)

	// Shard status.
	if len(r.ShardStatus) != r.Round.NShards {
		t.Errorf("round %d: %d shard_status rows, want %d", round, len(r.ShardStatus), r.Round.NShards)
	}
	for _, s := range r.ShardStatus {
		if !s.StreamDone {
			t.Errorf("shard %d final status: stream_done=false in a stamped round (decision 30)", s.ShardID)
		}
		if s.QueueDepth != 0 {
			// Audited separately by TestLoadFinalReportShowsEmptyBacklog so
			// one finding does not mask each failure test's own property.
			t.Logf("[finding] shard %d's last stored report: stream_done=%v queue_depth=%d waiting_needs_index=%d although every query finished",
				s.ShardID, s.StreamDone, s.QueueDepth, s.WaitingIdx)
		}
	}

	// The simulator's own record.
	tl := waitTimeline(t, round, 30*time.Second)
	t.Logf("[shards] %s", tl.Finished)
	t.Logf("[shards] %s | %s", tl.Header, tl.Summary)
	if !strings.Contains(tl.Finished, fmt.Sprintf("queries=%d", len(r.Jobs))) {
		t.Errorf("simulator's round-finished line disagrees with the API's %d queries: %s", len(r.Jobs), tl.Finished)
	}
	for _, b := range r.Builds {
		row, ok := tl.Rows[int(b.ShardID)]
		if !ok {
			t.Errorf("timeline has no row for shard %d", b.ShardID)
			continue
		}
		t.Logf("[timeline] shard %d n_vectors=%d place=%s att=%d b.done=%.1fs finish=%.1fs queries=%d max_wait=%.1fs",
			row.Shard, row.NVectors, row.Place, row.Att, row.BDone, row.Finish, row.Queries, row.MaxWait)
		if row.Att != int(b.Attempt) || row.NVectors != b.NVectors {
			t.Errorf("timeline row for shard %d (att=%d n=%d) disagrees with the build row %s", b.ShardID, row.Att, row.NVectors, b)
		}
	}
	errRE := regexp.MustCompile(`level=ERROR.*round_id=` + strconv.FormatInt(round, 10) + `(\s|$)`)
	for _, l := range strings.Split(containerLogs(t, ldShardsName), "\n") {
		if errRE.MatchString(l) {
			t.Errorf("simulator logged an error for round %d: %s", round, strings.TrimSpace(l))
		}
	}
	if st := ldInspect(t, ldShardsName, "{{.State.Status}}"); st != "running" {
		t.Errorf("simulator is %s after round %d", st, round)
	}
	if !t.Failed() {
		t.Logf("[verify] round %d complete and correct: %d builds done once each, %d queries finished, stamped at the latest child finish", round, len(r.Builds), len(r.Jobs))
	}
	return ldVerify{Round: r, Timeline: tl}
}

// shardLines returns the simulator's structured lines for one build.
func shardLines(t *testing.T, id string) []string {
	t.Helper()
	return linesWith(containerLogs(t, ldShardsName), id, -1)
}

// reapLines returns the scheduler's lease-expired lines for build id/attempt.
func reapLines(t *testing.T, id string, attempt int) []string {
	t.Helper()
	return linesWith(containerLogs(t, ldSchedulerName), id, attempt, "lease expired")
}

func logAll(t *testing.T, prefix string, ls []string) {
	t.Helper()
	for _, l := range ls {
		t.Logf("[%s] %s", prefix, l)
	}
}

func workerIDs(t *testing.T) []string {
	t.Helper()
	var ids []string
	for _, w := range workers(t).Workers {
		ids = append(ids, w.WorkerID)
	}
	return ids
}

// ldExpected regenerates the round's workload from the scenario and seed the
// round row carries (invariant 10: workloads are seeded and reproducible).
func ldExpected(t *testing.T, r ldRound) workload.Workload {
	t.Helper()
	sc, err := workload.PresetWithShards(r.Round.Scenario, r.Round.NShards)
	if err != nil {
		t.Fatalf("PresetWithShards(%q, %d): %v", r.Round.Scenario, r.Round.NShards, err)
	}
	w := workload.Generate(sc, r.Round.Seed)
	if err := w.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	return w
}

// ldJobsByShard groups a round's jobs by shard, sorted by seq.
func ldJobsByShard(r ldRound) map[int32][]ldJob {
	m := map[int32][]ldJob{}
	for _, j := range r.Jobs {
		m[j.ShardID] = append(m[j.ShardID], j)
	}
	for k := range m {
		sort.Slice(m[k], func(a, b int) bool { return m[k][a].Seq < m[k][b].Seq })
	}
	return m
}

// ldCheckWorkload compares every recorded query with the workload the
// simulator replays. Decision 59 (step 7): arrivals start at round start and
// pre-DDL queries are backfilled, so every query of every shard must be in
// shard_jobs, in arrival order (seq), with the workload's needs_index and
// duration (duration_ms within 1 ms of the modeled duration), and its
// arrived_at at round start + arrival offset / time scale. The arrival check
// is loose (late by at most 500 ms, early by at most 100 ms): the round row's
// started_at is the scheduler's clock and the arrivals are the shard's
// (decision 60), one machine here.
func ldCheckWorkload(t *testing.T, r ldRound) {
	t.Helper()
	w := ldExpected(t, r)
	got := ldJobsByShard(r)
	total, mism, late, early := 0, 0, 0, 0
	var worstLate, worstEarly time.Duration
	for _, s := range w.Shards {
		total += len(s.Queries)
		js := got[s.ShardID]
		if len(js) != len(s.Queries) {
			t.Errorf("round %d shard %d: %d queries recorded, the workload (%s seed %d) has %d (%d before the DDL at %v, which decision 59 backfills)",
				r.Round.RoundID, s.ShardID, len(js), r.Round.Scenario, r.Round.Seed, len(s.Queries), len(s.Queries)-len(postDDL(s)), s.DDLOffset)
			continue
		}
		for i, j := range js {
			q := s.Queries[i]
			dm := q.Duration.Milliseconds()
			if j.NeedsIndex != q.NeedsIndex || j.DurationMs < dm-1 || j.DurationMs > dm+1 {
				mism++
				if mism <= 3 {
					t.Errorf("round %d shard %d seq %d (arrival #%d): recorded needs_index=%v duration_ms=%d, workload needs_index=%v duration=%v",
						r.Round.RoundID, s.ShardID, j.Seq, i, j.NeedsIndex, j.DurationMs, q.NeedsIndex, q.Duration)
				}
			}
			want := r.Round.StartedAt.Add(time.Duration(float64(q.Arrival) / ldTimeScale))
			d := j.ArrivedAt.Sub(want)
			if d > worstLate {
				worstLate = d
			}
			if d < worstEarly {
				worstEarly = d
			}
			if d > 500*time.Millisecond {
				late++
			}
			if d < -100*time.Millisecond {
				early++
			}
		}
	}
	if len(r.Jobs) != total {
		t.Errorf("round %d: %d queries recorded, the workload has %d", r.Round.RoundID, len(r.Jobs), total)
	}
	if mism > 0 {
		t.Errorf("round %d: %d recorded queries disagree with the workload in arrival order", r.Round.RoundID, mism)
	}
	if late > 0 || early > 0 {
		t.Errorf("round %d: %d queries arrived >500 ms late and %d >100 ms early against round start + offset/scale (worst %+.3fs / %+.3fs)",
			r.Round.RoundID, late, early, worstLate.Seconds(), worstEarly.Seconds())
	}
	t.Logf("[workload] round %d (%s seed %d): %d recorded queries vs %d in the workload, %d field mismatches; arrival vs schedule worst late %+.3fs, worst early %+.3fs",
		r.Round.RoundID, r.Round.Scenario, r.Round.Seed, len(r.Jobs), total, mism, worstLate.Seconds(), worstEarly.Seconds())
}
