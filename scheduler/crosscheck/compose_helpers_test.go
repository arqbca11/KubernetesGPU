package crosscheck

// Helpers for black-box tests of the Docker Compose stack (deploy/compose.yaml).
// The stack is driven only through the scheduler's HTTP API and the docker
// CLI; it is observed through the API, `docker ps`/`docker inspect` and
// container logs. The project name and host ports are distinct from the
// implementer's `kgpu` stack so the two never collide.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	xcProject = "kgpu-xcheck"
	xcAPI     = "http://127.0.0.1:28080"
	// 2,000,000 x 128 is about 65.5 s modeled, 6.55 s at FAKE_TIME_SCALE=10.
	xcBigN = 2000000
	// 100,000 x 128 is about half a second at FAKE_TIME_SCALE=10.
	xcSmallN = 100000
)

// Fast failure-test settings, passed to compose as environment variables.
var xcEnv = map[string]string{
	"API_PORT":                   "28080",
	"PG_PORT":                    "25432",
	"LEASE_SECONDS":              "5",
	"RENEW_INTERVAL_SECONDS":     "1",
	"HEARTBEAT_INTERVAL_SECONDS": "1",
	"FAKE_TIME_SCALE":            "10",
	"REAP_INTERVAL":              "1s",
	"WORKER_STALE_AFTER":         "5s",
	"LOG_FORMAT":                 "text",
}

var (
	stackOnce sync.Once
	stackErr  error
	stackUp   bool
)

func xcRepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func composeEnabled() bool { return os.Getenv("KGPU_COMPOSE") == "1" }

func composeCmd(args ...string) *exec.Cmd {
	root := xcRepoRoot()
	full := append([]string{"compose", "-p", xcProject, "-f", filepath.Join(root, "deploy", "compose.yaml")}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Dir = root
	cmd.Env = os.Environ()
	for k, v := range xcEnv {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd
}

func composeTeardown() {
	out, err := composeCmd("down", "-v", "--remove-orphans").CombinedOutput()
	fmt.Fprintf(os.Stderr, "compose down -v --remove-orphans (project %s): err=%v\n%s", xcProject, err, tail(string(out), 5))
}

func TestMain(m *testing.M) {
	code := m.Run()
	if composeEnabled() && stackUp {
		composeTeardown()
	}
	os.Exit(code)
}

// compose runs a docker compose subcommand on the xcheck project and fails
// the test on error.
func compose(t *testing.T, args ...string) string {
	t.Helper()
	start := time.Now()
	out, err := composeCmd(args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	t.Logf("[docker] compose %s (%.1fs)", strings.Join(args, " "), time.Since(start).Seconds())
	return string(out)
}

// dockerCmd runs a plain docker command and fails the test on error.
func dockerCmd(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// requireStack skips unless KGPU_COMPOSE=1, brings the stack up once, and
// before every test restores the baseline: scheduler healthy, 2 workers live.
func requireStack(t *testing.T) {
	t.Helper()
	if !composeEnabled() {
		t.Skip("KGPU_COMPOSE=1 not set: compose tests need Docker and take minutes")
	}
	stackOnce.Do(func() {
		stackUp = true // tear down even if up fails half way
		out, err := composeCmd("up", "-d", "--build", "--wait").CombinedOutput()
		if err != nil {
			stackErr = fmt.Errorf("compose up: %v\n%s", err, out)
		}
	})
	if stackErr != nil {
		t.Fatal(stackErr)
	}
	compose(t, "up", "-d", "--wait", "--no-recreate", "--scale", "worker=2")
	waitHealthy(t, 60*time.Second)
	waitLiveWorkers(t, 2, 30*time.Second)
	t.Logf("[setup] docker server %s", dockerCmd(t, "version", "--format", "{{.Server.Version}}"))
	t.Logf("[setup] stack %s up: %s", xcProject, psSummary(t))
}

// ---- HTTP ----

func apiTry(method, path string, body any, out any) (int, string, error) {
	var rd io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return 0, "", err
		}
		rd = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, xcAPI+path, rd)
	if err != nil {
		return 0, "", err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, string(raw), fmt.Errorf("decode %q: %v", raw, err)
		}
	}
	return resp.StatusCode, string(raw), nil
}

func callAPI(t *testing.T, method, path string, body any, out any) (int, string) {
	t.Helper()
	code, raw, err := apiTry(method, path, body, out)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return code, raw
}

type xBuild struct {
	BuildID    string     `json:"build_id"`
	RoundID    int64      `json:"round_id"`
	ShardID    int32      `json:"shard_id"`
	NVectors   int64      `json:"n_vectors"`
	Dim        int32      `json:"dim"`
	MemBytes   int64      `json:"mem_bytes"`
	Placement  string     `json:"placement"`
	State      string     `json:"state"`
	Attempt    int32      `json:"attempt"`
	LeaseOwner *string    `json:"lease_owner"`
	LeaseUntil *time.Time `json:"lease_until"`
	FailReason *string    `json:"fail_reason"`
	EnqueuedAt time.Time  `json:"enqueued_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
}

func (b xBuild) owner() string {
	if b.LeaseOwner == nil {
		return "<nil>"
	}
	return *b.LeaseOwner
}

func (b xBuild) String() string {
	fin := "<nil>"
	if b.FinishedAt != nil {
		fin = b.FinishedAt.Format("15:04:05.000")
	}
	fr := ""
	if b.FailReason != nil {
		fr = " fail_reason=" + *b.FailReason
	}
	return fmt.Sprintf("build_id=%s placement=%s state=%s attempt=%d lease_owner=%s finished_at=%s%s",
		b.BuildID, b.Placement, b.State, b.Attempt, b.owner(), fin, fr)
}

type xSubmit struct {
	BuildID    string  `json:"build_id"`
	Placement  string  `json:"placement"`
	Priority   float64 `json:"priority"`
	Reason     string  `json:"reason"`
	MemBytes   int64   `json:"mem_bytes"`
	Created    bool    `json:"created"`
	CPUBuildMs int64   `json:"cpu_build_ms"`
	GPUTotalMs int64   `json:"gpu_total_ms"`
}

type xRoundStatus struct {
	Round struct {
		RoundID    int64      `json:"round_id"`
		NShards    int32      `json:"n_shards"`
		FinishedAt *time.Time `json:"finished_at"`
	} `json:"round"`
	Builds []xBuild `json:"builds"`
}

type xWorkers struct {
	Pool struct {
		LiveWorkers int   `json:"live_workers"`
		LargestMem  int64 `json:"largest_mem_bytes"`
	} `json:"pool"`
	Workers []struct {
		WorkerID string    `json:"worker_id"`
		LastSeen time.Time `json:"last_seen"`
	} `json:"workers"`
}

func newRound(t *testing.T, nShards int) int64 {
	t.Helper()
	var r struct {
		RoundID int64 `json:"round_id"`
	}
	code, raw := callAPI(t, "POST", "/rounds", map[string]any{"scenario": "xcheck-compose", "seed": 42, "n_shards": nShards}, &r)
	if code != http.StatusCreated {
		t.Fatalf("POST /rounds: %d %s", code, raw)
	}
	t.Logf("[api] POST /rounds n_shards=%d -> 201 round_id=%d", nShards, r.RoundID)
	return r.RoundID
}

func submit(t *testing.T, round int64, shard int, n int64) (int, xSubmit) {
	t.Helper()
	var s xSubmit
	code, raw := callAPI(t, "POST", "/builds", map[string]any{"round_id": round, "shard_id": shard, "n_vectors": n, "dim": 128}, &s)
	t.Logf("[api] POST /builds round=%d shard=%d n_vectors=%d -> %d %s", round, shard, n, code, strings.TrimSpace(raw))
	return code, s
}

func mustSubmitNew(t *testing.T, round int64, shard int, n int64) xSubmit {
	t.Helper()
	code, s := submit(t, round, shard, n)
	if code != http.StatusCreated || !s.Created || s.Placement != "gpu" {
		t.Fatalf("expected 201 created gpu build, got %d %+v", code, s)
	}
	return s
}

func getBuild(t *testing.T, id string) xBuild {
	t.Helper()
	var b xBuild
	code, raw := callAPI(t, "GET", "/builds/"+id, nil, &b)
	if code != http.StatusOK {
		t.Fatalf("GET /builds/%s: %d %s", id, code, raw)
	}
	return b
}

// waitBuild polls GET /builds/{id} until pred holds, logging every change of
// (state, attempt, lease_owner) it sees on the way. API errors (scheduler
// down) are tolerated and logged once per outage.
func waitBuild(t *testing.T, id string, d time.Duration, desc string, pred func(xBuild) bool) xBuild {
	t.Helper()
	deadline := time.Now().Add(d)
	start := time.Now()
	last := ""
	down := false
	var b xBuild
	for {
		var cur xBuild
		code, raw, err := apiTry("GET", "/builds/"+id, nil, &cur)
		if err != nil || code != http.StatusOK {
			if !down {
				t.Logf("[api] +%.1fs GET /builds/%s unavailable: code=%d err=%v %s", time.Since(start).Seconds(), id, code, err, strings.TrimSpace(raw))
				down = true
			}
		} else {
			if down {
				t.Logf("[api] +%.1fs GET /builds/%s answering again", time.Since(start).Seconds(), id)
				down = false
			}
			b = cur
			key := fmt.Sprintf("%s/%d/%s", b.State, b.Attempt, b.owner())
			if key != last {
				t.Logf("[api] +%.1fs %s", time.Since(start).Seconds(), b)
				last = key
			}
			if pred(b) {
				return b
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s; last seen: %s", d, desc, b)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitHealthy(t *testing.T, d time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for {
		code, _, err := apiTry("GET", "/healthz", nil, nil)
		if err == nil && code == http.StatusOK {
			return time.Since(start)
		}
		if time.Since(start) > d {
			t.Fatalf("scheduler /healthz not 200 within %v (code=%d err=%v)", d, code, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func workers(t *testing.T) xWorkers {
	t.Helper()
	var w xWorkers
	code, raw := callAPI(t, "GET", "/workers", nil, &w)
	if code != http.StatusOK {
		t.Fatalf("GET /workers: %d %s", code, raw)
	}
	return w
}

func waitLiveWorkers(t *testing.T, n int, d time.Duration) xWorkers {
	t.Helper()
	start := time.Now()
	for {
		var w xWorkers
		code, _, err := apiTry("GET", "/workers", nil, &w)
		if err == nil && code == http.StatusOK && w.Pool.LiveWorkers == n {
			return w
		}
		if time.Since(start) > d {
			t.Fatalf("pool did not reach %d live workers within %v (last: %+v err=%v)", n, d, w.Pool, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ---- docker observation ----

type ctr struct{ ID, Name string }

func serviceContainers(t *testing.T, service string, all bool) []ctr {
	t.Helper()
	args := []string{"ps"}
	if all {
		args = append(args, "-a")
	}
	args = append(args, "--filter", "label=com.docker.compose.project="+xcProject,
		"--filter", "label=com.docker.compose.service="+service, "--format", "{{.ID}} {{.Names}}")
	out := dockerCmd(t, args...)
	var cs []ctr
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			cs = append(cs, ctr{ID: f[0], Name: f[1]})
		}
	}
	return cs
}

// nameOf maps a lease_owner (container hostname = short id) to a container name.
func nameOf(t *testing.T, shortID string) string {
	t.Helper()
	n := dockerCmd(t, "ps", "-a", "--filter", "id="+shortID, "--format", "{{.Names}}")
	if n == "" {
		t.Fatalf("no container with id %s", shortID)
	}
	if !strings.HasPrefix(n, xcProject+"-") {
		t.Fatalf("container %s (%s) is not in project %s; refusing to touch it", n, shortID, xcProject)
	}
	return n
}

func psSummary(t *testing.T) string {
	t.Helper()
	return strings.ReplaceAll(dockerCmd(t, "ps", "-a", "--filter", "label=com.docker.compose.project="+xcProject,
		"--format", "{{.Names}}({{.ID}}):{{.Status}}"), "\n", "; ")
}

func inspect(t *testing.T, name, format string) string {
	t.Helper()
	return dockerCmd(t, "inspect", "-f", format, name)
}

func containerLogs(t *testing.T, name string) string {
	t.Helper()
	out, err := exec.Command("docker", "logs", name).CombinedOutput()
	if err != nil {
		t.Logf("docker logs %s: %v", name, err)
	}
	return string(out)
}

func buildRE(id string) *regexp.Regexp {
	return regexp.MustCompile(`build_id=` + regexp.QuoteMeta(id) + `(\s|$)`)
}

// linesFor returns the lines in logs that mention build id and contain msg.
func linesFor(logs, id, msg string) []string {
	re := buildRE(id)
	var out []string
	for _, l := range strings.Split(logs, "\n") {
		if re.MatchString(l) && strings.Contains(l, msg) {
			out = append(out, strings.TrimSpace(l))
		}
	}
	return out
}

// logsByContainer collects logs from every worker container (including
// stopped ones) and the scheduler.
func logsByContainer(t *testing.T) map[string]string {
	t.Helper()
	m := map[string]string{}
	for _, c := range serviceContainers(t, "worker", true) {
		m[c.Name+"("+c.ID+")"] = containerLogs(t, c.Name)
	}
	for _, c := range serviceContainers(t, "scheduler", true) {
		m[c.Name] = containerLogs(t, c.Name)
	}
	return m
}

// completedLines returns every worker "completed" line for the build, keyed
// by container.
func completedLines(t *testing.T, id string) map[string][]string {
	t.Helper()
	res := map[string][]string{}
	for _, c := range serviceContainers(t, "worker", true) {
		if ls := linesFor(containerLogs(t, c.Name), id, "completed"); len(ls) > 0 {
			res[c.Name+"("+c.ID+")"] = ls
		}
	}
	return res
}

func countLines(m map[string][]string) int {
	n := 0
	for _, v := range m {
		n += len(v)
	}
	return n
}

// narrateBuildLogs logs every line mentioning the build from every container.
func narrateBuildLogs(t *testing.T, id string) {
	t.Helper()
	re := buildRE(id)
	for name, logs := range logsByContainer(t) {
		for _, l := range strings.Split(logs, "\n") {
			if re.MatchString(l) {
				t.Logf("[log %s] %s", name, strings.TrimSpace(l))
			}
		}
	}
}

func tail(s string, n int) string {
	ls := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(ls) > n {
		ls = ls[len(ls)-n:]
	}
	return strings.Join(ls, "\n") + "\n"
}

// execQuiet runs a command for cleanup, ignoring its output.
func execQuiet(name string, args ...string) error {
	return exec.Command(name, args...).Run()
}
