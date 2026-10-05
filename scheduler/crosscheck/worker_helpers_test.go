package crosscheck

// Helpers for driving the real Python worker (python -m kgpu_worker) as a
// black-box subprocess. Configured only through the environment variables in
// the design doc's worker configuration table.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/arqbca11/KubernetesGPU/scheduler/costmodel"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

// syncBuffer is a goroutine-safe bytes.Buffer.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type workerProc struct {
	id     string
	cmd    *exec.Cmd
	stdout *syncBuffer
	stderr *syncBuffer
	done   chan struct{}
	err    error // set before done is closed
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// scheduler/crosscheck/<file> -> repo root
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

// startWorker launches the worker with sensible fast defaults; extra overrides
// or adds variables (an empty value removes a default).
func startWorker(t *testing.T, id string, extra map[string]string) *workerProc {
	t.Helper()
	root := repoRoot(t)
	py := filepath.Join(root, "worker", ".venv", "bin", "python")
	if _, err := os.Stat(py); err != nil {
		t.Skipf("worker venv not found at %s", py)
	}
	vars := map[string]string{
		"DATABASE_URL":               os.Getenv("TEST_DATABASE_URL"),
		"PATH":                       os.Getenv("PATH"),
		"HOME":                       os.Getenv("HOME"),
		"WORKER_ID":                  id,
		"FAKE_TIME_SCALE":            "20",
		"LEASE_SECONDS":              "2",
		"RENEW_INTERVAL_SECONDS":     "0.3",
		"HEARTBEAT_INTERVAL_SECONDS": "0.5",
		"POLL_INTERVAL_SECONDS":      "0.1",
	}
	for k, v := range extra {
		if v == "" {
			delete(vars, k)
		} else {
			vars[k] = v
		}
	}
	var envv []string
	for k, v := range vars {
		envv = append(envv, k+"="+v)
	}
	cmd := exec.Command(py, "-m", "kgpu_worker")
	cmd.Dir = filepath.Join(root, "worker")
	cmd.Env = envv
	w := &workerProc{id: id, cmd: cmd, stdout: &syncBuffer{}, stderr: &syncBuffer{}, done: make(chan struct{})}
	cmd.Stdout = w.stdout
	cmd.Stderr = w.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker %s: %v", id, err)
	}
	go func() {
		w.err = cmd.Wait()
		close(w.done)
	}()
	t.Cleanup(func() {
		select {
		case <-w.done:
		default:
			_ = cmd.Process.Signal(syscall.SIGCONT) // in case a test left it stopped
			_ = cmd.Process.Kill()
			<-w.done
		}
		t.Logf("--- worker %s stdout ---\n%s", id, w.stdout.String())
		if s := w.stderr.String(); s != "" {
			t.Logf("--- worker %s stderr ---\n%s", id, s)
		}
	})
	t.Logf("started worker %s (pid %d) with FAKE_TIME_SCALE=%s LEASE_SECONDS=%s RENEW_INTERVAL_SECONDS=%s",
		id, cmd.Process.Pid, vars["FAKE_TIME_SCALE"], vars["LEASE_SECONDS"], vars["RENEW_INTERVAL_SECONDS"])
	return w
}

func (w *workerProc) signal(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := w.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("signal %v to %s: %v", sig, w.id, err)
	}
	t.Logf("sent %v to worker %s", sig, w.id)
}

// waitExit waits for the process to exit and returns its exit code (-1 if
// killed by a signal).
func (w *workerProc) waitExit(t *testing.T, d time.Duration) int {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(d):
		t.Fatalf("worker %s did not exit within %v", w.id, d)
	}
	return w.cmd.ProcessState.ExitCode()
}

func (w *workerProc) exited() bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

type logLine map[string]any

func (l logLine) str(k string) string {
	v, ok := l[k]
	if !ok || v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// lines parses the JSON-lines stdout; non-JSON lines are returned with only
// a "raw" key.
func (w *workerProc) lines() []logLine {
	var out []logLine
	sc := bufio.NewScanner(strings.NewReader(w.stdout.String()))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		txt := sc.Text()
		var m map[string]any
		if err := json.Unmarshal([]byte(txt), &m); err != nil {
			out = append(out, logLine{"raw": txt})
			continue
		}
		m["raw"] = txt
		out = append(out, logLine(m))
	}
	return out
}

func (w *workerProc) find(pred func(logLine) bool) []logLine {
	var out []logLine
	for _, l := range w.lines() {
		if pred(l) {
			out = append(out, l)
		}
	}
	return out
}

func (w *workerProc) waitLog(t *testing.T, d time.Duration, what string, pred func(logLine) bool) logLine {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ls := w.find(pred); len(ls) > 0 {
			return ls[0]
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("worker %s: no log line %q within %v", w.id, what, d)
	return nil
}

func msgHas(sub string) func(logLine) bool {
	return func(l logLine) bool { return strings.Contains(strings.ToLower(l.str("msg")), sub) }
}

func forBuild(id string, attempt int32, pred func(logLine) bool) func(logLine) bool {
	return func(l logLine) bool {
		return l.str("build_id") == id && l.str("attempt") == fmt.Sprint(attempt) && pred(l)
	}
}

// queueGPU submits a GPU build sized (n, dim) with the cost model's memory
// need, through the store as the scheduler would.
func queueGPU(t *testing.T, e *env, round int64, shard int32, n int64, dim int32, mem int64, prio float64) string {
	t.Helper()
	id := store.BuildID(shard, round)
	if mem < 0 {
		mem = costmodel.Default().GPUMemBytes(n, dim)
	}
	ok, err := e.st.SubmitBuild(e.ctx, store.Build{BuildID: id, RoundID: round, ShardID: shard, NVectors: n, Dim: dim, MemBytes: mem, Placement: "gpu", Priority: prio})
	if err != nil || !ok {
		t.Fatalf("SubmitBuild %s: ok=%v err=%v", id, ok, err)
	}
	t.Logf("queued GPU build %s: n=%d dim=%d mem=%d priority=%v modeled gpu_total=%v", id, n, dim, mem, prio, costmodel.Default().GPUTotal(n, dim))
	return id
}

func (e *env) round(t *testing.T, n int32) int64 {
	t.Helper()
	r, err := e.st.CreateRound(e.ctx, "crosscheck-worker", 11, n)
	if err != nil {
		t.Fatalf("CreateRound: %v", err)
	}
	return r
}

// waitBuild polls Postgres until pred holds for the build row.
func (e *env) waitBuild(t *testing.T, id string, d time.Duration, what string, pred func(store.BuildRow) bool) store.BuildRow {
	t.Helper()
	deadline := time.Now().Add(d)
	var b store.BuildRow
	for time.Now().Before(deadline) {
		b = e.mustBuild(t, id)
		if pred(b) {
			return b
		}
		time.Sleep(25 * time.Millisecond)
	}
	logBuild(t, "timed out waiting: "+what, b)
	t.Fatalf("build %s: %s not reached within %v", id, what, d)
	return b
}

func leasedTo(owner string) func(store.BuildRow) bool {
	return func(b store.BuildRow) bool {
		return b.State == "leased" && b.LeaseOwner != nil && *b.LeaseOwner == owner
	}
}

func logLease(t *testing.T, label string, b store.BuildRow) {
	t.Helper()
	logBuild(t, label, b)
	t.Logf("[db] %s: lease_until=%v fail_reason=%v enqueued_at=%v", label, tptr(b.LeaseUntil), deref(b.FailReason), b.EnqueuedAt.Format(time.RFC3339Nano))
}
