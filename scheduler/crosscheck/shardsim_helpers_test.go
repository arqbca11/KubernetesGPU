package crosscheck

// Helpers for the step 5 cross-check: the shard simulator run as a black-box
// process against an in-process scheduler (httptest), a reaper loop, and a
// fake GPU worker played through the store's exported worker operations.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/arqbca11/KubernetesGPU/scheduler/costmodel"
	"github.com/arqbca11/KubernetesGPU/scheduler/reaper"
	"github.com/arqbca11/KubernetesGPU/scheduler/store"
)

var (
	shardsimOnce sync.Once
	shardsimBin  string
	shardsimErr  error
	shardsimOut  []byte
)

// shardsimBinary builds ./shard/cmd/shardsim once per test process.
func shardsimBinary(t *testing.T) string {
	t.Helper()
	shardsimOnce.Do(func() {
		dir, err := os.MkdirTemp("", "xc-shardsim-")
		if err != nil {
			shardsimErr = err
			return
		}
		shardsimBin = filepath.Join(dir, "shardsim")
		cmd := exec.Command("go", "build", "-o", shardsimBin, "./shard/cmd/shardsim")
		cmd.Dir = repoRoot(t)
		shardsimOut, shardsimErr = cmd.CombinedOutput()
	})
	if shardsimErr != nil {
		t.Fatalf("go build shardsim: %v\n%s", shardsimErr, shardsimOut)
	}
	return shardsimBin
}

type simProc struct {
	cmd    *exec.Cmd
	stdout *syncBuffer
	stderr *syncBuffer
	done   chan struct{}
	start  time.Time
	exitAt time.Time
}

// startShardsim runs the binary with exactly the given environment (plus PATH
// and HOME), so no default from the developer's shell leaks in.
func startShardsim(t *testing.T, vars map[string]string) *simProc {
	t.Helper()
	bin := shardsimBinary(t)
	cmd := exec.Command(bin)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		cmd.Env = append(cmd.Env, k+"="+vars[k])
	}
	p := &simProc{cmd: cmd, stdout: &syncBuffer{}, stderr: &syncBuffer{}, done: make(chan struct{})}
	cmd.Stdout = p.stdout
	cmd.Stderr = p.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start shardsim: %v", err)
	}
	p.start = time.Now()
	go func() {
		_ = cmd.Wait()
		p.exitAt = time.Now()
		close(p.done)
	}()
	t.Cleanup(func() {
		if !p.exited() {
			_ = cmd.Process.Signal(syscall.SIGKILL)
			<-p.done
		}
	})
	t.Logf("started shardsim pid %d with %v", cmd.Process.Pid, cmd.Env[2:])
	return p
}

func (p *simProc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// wait waits for exit and returns the exit code; fatal on timeout.
func (p *simProc) wait(t *testing.T, d time.Duration) int {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(d):
		t.Logf("shardsim stderr tail:\n%s", tail(p.stderr.String(), 30))
		t.Logf("shardsim stdout tail:\n%s", tail(p.stdout.String(), 30))
		t.Fatalf("shardsim did not exit within %v", d)
	}
	return p.cmd.ProcessState.ExitCode()
}

// simEnv is the in-process scheduler plus a reaper loop.
type simEnv struct {
	*env
	cancel context.CancelFunc
}

func setupSim(t *testing.T) *simEnv {
	t.Helper()
	e := setup(t) // TRUNCATEs, migrates, starts the API (policy AlwaysGPU, costmodel.Default)
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(200 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				reaper.Tick(ctx, e.st, 5, logger)
			}
		}
	}()
	t.Cleanup(func() { cancel(); wg.Wait() })
	return &simEnv{env: e, cancel: cancel}
}

// fakeGPU plays one GPU worker through the store: heartbeat with heartMem,
// claim with claimMem (normally the same), sleep for the cost model's GPU
// time divided by scale, complete. Claiming waits until gate is closed.
type fakeGPU struct {
	id        string
	heartMem  int64
	claimMem  int64
	scale     float64
	gate      chan struct{}
	mu        sync.Mutex
	claims    []store.Claimed
	completes map[string]bool
	completed map[string]time.Time
}

func startFakeGPU(t *testing.T, e *simEnv, id string, heartMem, claimMem int64, scale float64, open bool) *fakeGPU {
	t.Helper()
	g := &fakeGPU{id: id, heartMem: heartMem, claimMem: claimMem, scale: scale, gate: make(chan struct{}),
		completes: map[string]bool{}, completed: map[string]time.Time{}}
	if open {
		close(g.gate)
	}
	if err := e.st.Heartbeat(e.ctx, id, heartMem); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(500 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				_ = e.st.Heartbeat(ctx, id, heartMem)
			}
		}
	}()
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-g.gate:
		}
		cm := costmodel.Default()
		for ctx.Err() == nil {
			c, ok, err := e.st.Claim(ctx, id, claimMem, 30*time.Second)
			if err != nil || !ok {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			g.mu.Lock()
			g.claims = append(g.claims, c)
			g.mu.Unlock()
			d := time.Duration(float64(cm.GPUTotal(c.NVectors, c.Dim)) / scale)
			select {
			case <-ctx.Done():
				return
			case <-time.After(d):
			}
			ok, err = e.st.Complete(ctx, c.BuildID, c.Attempt)
			g.mu.Lock()
			g.completes[c.BuildID] = ok && err == nil
			g.completed[c.BuildID] = time.Now()
			g.mu.Unlock()
		}
	}()
	t.Cleanup(func() { cancel(); wg.Wait() })
	return g
}

func (g *fakeGPU) open() { close(g.gate) }

func (g *fakeGPU) claimed() []store.Claimed {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]store.Claimed(nil), g.claims...)
}

// waitRoundFinished polls the store until the round has finished_at.
func (e *simEnv) waitRoundFinished(t *testing.T, round int64, d time.Duration) store.Round {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := e.st.FinishCompleteRounds(e.ctx); err != nil {
			t.Fatalf("FinishCompleteRounds: %v", err)
		}
		r, ok, err := e.st.GetRound(e.ctx, round)
		if err == nil && ok && r.FinishedAt != nil {
			return r
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("round %d not finished within %v", round, d)
	return store.Round{}
}

// waitFirstRound polls until a round row exists and returns its id.
func (e *simEnv) waitFirstRound(t *testing.T, d time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		var id int64
		err := e.pool.QueryRow(e.ctx, `SELECT round_id FROM rounds ORDER BY round_id LIMIT 1`).Scan(&id)
		if err == nil {
			return id
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no round created within %v", d)
	return 0
}
