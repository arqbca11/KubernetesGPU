package crosscheck

// Helpers for the cross-check of Phase 2 step 3 (the GPU workers on kind).
// Black box: kubectl, the scheduler API through a port-forward, the worker's
// probe port 8081 through a port-forward, and a shell inside the worker
// container (the image has sh; no ps, so processes are read from /proc).
//
// Logs are captured by following every worker and scheduler container with
// `kubectl logs -f` as soon as it appears, so the last lines of a pod that is
// deleted or drained are not lost.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	kwProbePort = 28092 // local port for a worker's probe server (8081)
	kwSmallN    = 2000000
	kwMidN      = 4000000
	kwBigN      = 8000000
	kwDim       = 128
)

// ---- scheduler API through a self-healing port-forward ----

type kwAPI struct {
	mu sync.Mutex
	pf *portForward
}

func (a *kwAPI) do(t *testing.T, method, path string, body any) (int, string) {
	t.Helper()
	for i := 0; i < 4; i++ {
		a.mu.Lock()
		pf := a.pf
		a.mu.Unlock()
		code, raw, err := pf.do(method, path, body)
		if err == nil {
			return code, raw
		}
		t.Logf("[api] %s %s: %v; re-establishing port-forward to svc/scheduler", method, path, err)
		a.mu.Lock()
		a.pf.stop()
		a.pf = forward(t, "svc/scheduler", kPortA)
		a.mu.Unlock()
	}
	t.Fatalf("%s %s: scheduler API unreachable", method, path)
	return 0, ""
}

func (a *kwAPI) round(t *testing.T, scenario string, nShards int) int64 {
	t.Helper()
	code, raw := a.do(t, "POST", "/rounds", map[string]any{"scenario": scenario, "seed": 7, "n_shards": nShards})
	var r struct {
		RoundID int64 `json:"round_id"`
	}
	if code != http.StatusCreated || json.Unmarshal([]byte(raw), &r) != nil || r.RoundID == 0 {
		t.Fatalf("POST /rounds -> %d %s", code, raw)
	}
	t.Logf("[api] POST /rounds {scenario:%s n_shards:%d} -> 201 round_id=%d", scenario, nShards, r.RoundID)
	return r.RoundID
}

func (a *kwAPI) submit(t *testing.T, round int64, shard int, n int64) xSubmit {
	t.Helper()
	code, raw := a.do(t, "POST", "/builds", map[string]any{"round_id": round, "shard_id": shard, "n_vectors": n, "dim": kwDim})
	var s xSubmit
	if code != http.StatusCreated || json.Unmarshal([]byte(raw), &s) != nil || !s.Created {
		t.Fatalf("POST /builds shard %d -> %d %s", shard, code, raw)
	}
	t.Logf("[api] POST /builds {round:%d shard:%d n:%d} -> 201 build_id=%s placement=%s gpu_total_ms=%d (~%.1fs at 10x)",
		round, shard, n, s.BuildID, s.Placement, s.GPUTotalMs, float64(s.GPUTotalMs)/10000)
	if s.Placement != "gpu" {
		t.Fatalf("build %s placed %s, want gpu (POLICY=always_gpu)", s.BuildID, s.Placement)
	}
	return s
}

func (a *kwAPI) build(t *testing.T, id string) xBuild {
	t.Helper()
	code, raw := a.do(t, "GET", "/builds/"+id, nil)
	var b xBuild
	if code != http.StatusOK || json.Unmarshal([]byte(raw), &b) != nil {
		t.Fatalf("GET /builds/%s -> %d %s", id, code, raw)
	}
	return b
}

func (a *kwAPI) workers(t *testing.T) xWorkers {
	t.Helper()
	code, raw := a.do(t, "GET", "/workers", nil)
	var w xWorkers
	if code != http.StatusOK || json.Unmarshal([]byte(raw), &w) != nil {
		t.Fatalf("GET /workers -> %d %s", code, raw)
	}
	return w
}

func (w xWorkers) ids() []string {
	var ids []string
	for _, x := range w.Workers {
		ids = append(ids, x.WorkerID)
	}
	sort.Strings(ids)
	return ids
}

// waitBuildK polls GET /builds/{id} until pred holds, narrating state changes.
func (a *kwAPI) waitBuild(t *testing.T, id string, d time.Duration, desc string, pred func(xBuild) bool) xBuild {
	t.Helper()
	var b xBuild
	last := ""
	start := time.Now()
	poll(t, d, 200*time.Millisecond, desc+" ("+id+")", func() bool {
		b = a.build(t, id)
		s := fmt.Sprintf("%s/%d/%s", b.State, b.Attempt, b.owner())
		if s != last {
			t.Logf("[build t=%.1fs] %s", time.Since(start).Seconds(), b)
			last = s
		}
		return pred(b)
	})
	return b
}

func isDone(b xBuild) bool { return b.State == "done" }

// ---- pods ----

// kwPods returns the pods matching selector that are not being deleted.
func kwPods(t *testing.T, ns, selector string) (live []kPod, terminating []string) {
	t.Helper()
	out := kc(t, "get", "pods", "-n", ns, "-l", selector, "-o", "json")
	var list struct {
		Items []kPod `json:"items"`
	}
	var del struct {
		Items []struct {
			Metadata struct {
				Name              string  `json:"name"`
				DeletionTimestamp *string `json:"deletionTimestamp"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if json.Unmarshal([]byte(out), &list) != nil || json.Unmarshal([]byte(out), &del) != nil {
		t.Fatalf("decode pods")
	}
	for i, p := range list.Items {
		if del.Items[i].Metadata.DeletionTimestamp != nil {
			terminating = append(terminating, p.Metadata.Name)
			continue
		}
		live = append(live, p)
	}
	sort.Slice(live, func(i, j int) bool { return live[i].Metadata.Name < live[j].Metadata.Name })
	return live, terminating
}

// waitWorkers waits for exactly n live, Ready worker pods and none terminating.
func waitWorkers(t *testing.T, ns string, n int, d time.Duration) []kPod {
	t.Helper()
	var ps []kPod
	took := poll(t, d, time.Second, fmt.Sprintf("%d Ready worker pods in %s", n, ns), func() bool {
		var term []string
		ps, term = kwPods(t, ns, "app=worker")
		if len(ps) != n || len(term) > 0 {
			return false
		}
		for _, p := range ps {
			if !p.ready() {
				return false
			}
		}
		return true
	})
	t.Logf("[k8s] %d worker pod(s) Ready in %s after %.1fs: %v", n, ns, took.Seconds(), ps)
	return ps
}

func podNames(ps []kPod) []string {
	var n []string
	for _, p := range ps {
		n = append(n, p.Metadata.Name)
	}
	sort.Strings(n)
	return n
}

// ---- inside the worker container ----

type kwProc struct {
	PID, PPID int
	Cmd       string
}

func procs(t *testing.T, pod string) []kwProc {
	t.Helper()
	out := kc(t, "exec", "-n", kNS, pod, "-c", "worker", "--", "sh", "-c",
		`for p in /proc/[0-9]*; do [ -r $p/stat ] || continue; echo "${p#/proc/} $(cut -d' ' -f4 $p/stat) $(tr '\0' ' ' < $p/cmdline)"; done`)
	var ps []kwProc
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.SplitN(l, " ", 3)
		if len(f) < 3 {
			continue
		}
		pid, _ := strconv.Atoi(f[0])
		ppid, _ := strconv.Atoi(f[1])
		ps = append(ps, kwProc{pid, ppid, strings.TrimSpace(f[2])})
	}
	return ps
}

// pythonPID returns the pid of the worker's python process.
func pythonPID(t *testing.T, pod string) int {
	t.Helper()
	for _, p := range procs(t, pod) {
		if strings.HasPrefix(p.Cmd, "python") {
			return p.PID
		}
	}
	t.Fatalf("no python process in %s", pod)
	return 0
}

func signalPython(t *testing.T, pod, sig string) int {
	t.Helper()
	pid := pythonPID(t, pod)
	kc(t, "exec", "-n", kNS, pod, "-c", "worker", "--", "sh", "-c", fmt.Sprintf("kill -%s %d", sig, pid))
	t.Logf("[exec %s] kill -%s %d (python)", pod, sig, pid)
	return pid
}

// ---- the worker's probe server ----

type probeForward struct{ pf *portForward }

func forwardProbe(t *testing.T, pod string) *probeForward {
	t.Helper()
	for attempt := 1; attempt <= 3; attempt++ {
		buf := &syncBuffer{}
		cmd := exec.Command("kubectl", "--context", kContext, "-n", kNS, "port-forward", "pod/"+pod, fmt.Sprintf("%d:8081", kwProbePort))
		cmd.Stdout, cmd.Stderr = buf, buf
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		pf := &portForward{cmd: cmd, port: kwProbePort, out: buf}
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if code, _, err := pf.get("/livez"); err == nil && code > 0 {
				t.Logf("[port-forward] pod/%s:8081 on 127.0.0.1:%d", pod, kwProbePort)
				t.Cleanup(pf.stop)
				return &probeForward{pf}
			}
			time.Sleep(300 * time.Millisecond)
		}
		pf.stop()
		time.Sleep(time.Second)
	}
	t.Fatalf("probe port-forward to %s never answered", pod)
	return nil
}

var probeHTTP = &http.Client{Timeout: 2 * time.Second}

// probe returns the status code of GET path on the worker's probe server, or
// -1 on a transport error (timeout, refused).
func (p *probeForward) probe(path string) (int, string) {
	resp, err := probeHTTP.Get(fmt.Sprintf("http://127.0.0.1:%d%s", p.pf.port, path))
	if err != nil {
		return -1, err.Error()
	}
	defer resp.Body.Close()
	var b [256]byte
	n, _ := resp.Body.Read(b[:])
	return resp.StatusCode, strings.TrimSpace(string(b[:n]))
}

// ---- log collector ----

type kwLine struct {
	Pod string
	Raw string
	M   map[string]any
}

func (l kwLine) str(k string) string {
	switch v := l.M[k].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

type logCollector struct {
	mu        sync.Mutex
	lines     []kwLine
	seen      map[string]bool
	following map[string]bool
	cmds      []*exec.Cmd
	stopc     chan struct{}
	wg        sync.WaitGroup
}

func startLogCollector(t *testing.T) *logCollector {
	c := &logCollector{seen: map[string]bool{}, following: map[string]bool{}, stopc: make(chan struct{})}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		tick := time.NewTicker(700 * time.Millisecond)
		defer tick.Stop()
		for {
			c.scan()
			select {
			case <-c.stopc:
				return
			case <-tick.C:
			}
		}
	}()
	t.Cleanup(c.stop)
	return c
}

func (c *logCollector) scan() {
	so, _, err := kcRun("", "get", "pods", "-n", kNS, "-l", "app in (worker,scheduler)", "-o",
		`jsonpath={range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[0].containerID}{"\n"}{end}`)
	if err != nil {
		return
	}
	for _, l := range strings.Split(strings.TrimSpace(so), "\n") {
		f := strings.Fields(l)
		if len(f) != 2 {
			continue
		}
		key := f[0] + " " + f[1]
		c.mu.Lock()
		done := c.following[key]
		c.following[key] = true
		c.mu.Unlock()
		if done {
			continue
		}
		pod := f[0]
		cmd := exec.Command("kubectl", "--context", kContext, "-n", kNS, "logs", "-f", pod)
		out, err := cmd.StdoutPipe()
		if err != nil || cmd.Start() != nil {
			continue
		}
		c.mu.Lock()
		c.cmds = append(c.cmds, cmd)
		c.mu.Unlock()
		go func() {
			sc := bufio.NewScanner(out)
			sc.Buffer(make([]byte, 1<<20), 1<<20)
			for sc.Scan() {
				raw := sc.Text()
				var m map[string]any
				_ = json.Unmarshal([]byte(raw), &m)
				c.mu.Lock()
				if !c.seen[pod+"\x00"+raw] {
					c.seen[pod+"\x00"+raw] = true
					c.lines = append(c.lines, kwLine{pod, raw, m})
				}
				c.mu.Unlock()
			}
			_ = cmd.Wait()
		}()
	}
}

func (c *logCollector) stop() {
	select {
	case <-c.stopc:
		return
	default:
	}
	close(c.stopc)
	c.wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cmd := range c.cmds {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}
}

// forBuild returns the collected lines about a build, in arrival order.
func (c *logCollector) forBuild(id string) []kwLine {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []kwLine
	for _, l := range c.lines {
		if l.str("build_id") == id {
			out = append(out, l)
		}
	}
	return out
}

func (c *logCollector) all() []kwLine {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]kwLine(nil), c.lines...)
}

func isWorkerPod(p string) bool    { return strings.HasPrefix(p, "worker-") }
func isSchedulerPod(p string) bool { return strings.HasPrefix(p, "scheduler-") }

// workerMsgs returns the worker lines about build id with the given msg.
func (c *logCollector) workerMsgs(id, msg string) []kwLine {
	var out []kwLine
	for _, l := range c.forBuild(id) {
		if isWorkerPod(l.Pod) && l.str("msg") == msg {
			out = append(out, l)
		}
	}
	return out
}

// reapLines returns the scheduler WARN/ERROR lines about build id: the reaper
// logs one warning line per reaped build (Phase 1, reaper row of the file table).
func (c *logCollector) reapLines(id string) []kwLine {
	var out []kwLine
	for _, l := range c.forBuild(id) {
		lv := strings.ToUpper(l.str("level"))
		if isSchedulerPod(l.Pod) && (strings.HasPrefix(lv, "WARN") || lv == "ERROR") {
			out = append(out, l)
		}
	}
	return out
}

// narrate logs every collected line about a build.
func (c *logCollector) narrate(t *testing.T, id string) {
	t.Helper()
	for _, l := range c.forBuild(id) {
		if l.str("msg") == "build placed" {
			continue
		}
		t.Logf("[log %s] %s attempt=%s %s", l.Pod, l.str("msg"), l.str("attempt"), tailFields(l))
	}
}

func tailFields(l kwLine) string {
	var parts []string
	for _, k := range []string{"lease_owner", "worker_id", "elapsed_s", "remaining_s", "reason", "err", "level"} {
		if v := l.str(k); v != "" {
			parts = append(parts, k+"="+v)
		}
	}
	return strings.Join(parts, " ")
}

// assertOneCompletion checks exactly one worker "completed" line for the
// build, at the given attempt (0 = any), and that it matches the row.
func assertOneCompletion(t *testing.T, c *logCollector, b xBuild, wantAttempt int) {
	t.Helper()
	// The completed line may land a moment after the row flips.
	var lines []kwLine
	deadline := time.Now().Add(5 * time.Second)
	for {
		lines = c.workerMsgs(b.BuildID, "completed")
		if len(lines) >= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	time.Sleep(time.Second)
	lines = c.workerMsgs(b.BuildID, "completed")
	var desc []string
	for _, l := range lines {
		desc = append(desc, fmt.Sprintf("%s@attempt%s", l.Pod, l.str("attempt")))
	}
	t.Logf("[check] build %s: row %s; worker completed lines: %v", b.BuildID, b, desc)
	if b.State != "done" {
		t.Errorf("build %s state %s, want done", b.BuildID, b.State)
	}
	if len(lines) != 1 {
		t.Errorf("build %s: %d worker 'completed' lines %v, want exactly 1", b.BuildID, len(lines), desc)
		return
	}
	if got := lines[0].str("attempt"); got != strconv.Itoa(int(b.Attempt)) {
		t.Errorf("build %s completed at attempt %s but the row says attempt %d", b.BuildID, got, b.Attempt)
	}
	if wantAttempt > 0 && int(b.Attempt) != wantAttempt {
		t.Errorf("build %s done at attempt %d, want %d", b.BuildID, b.Attempt, wantAttempt)
	}
}

// ---- implementer's namespace, read only ----

func origWorkers(t *testing.T) []kPod {
	t.Helper()
	ps, _ := kwPods(t, kOrigNS, "app=worker")
	return ps
}
