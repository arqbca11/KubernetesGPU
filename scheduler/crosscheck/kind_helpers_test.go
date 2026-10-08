package crosscheck

// Helpers for black-box tests of the Phase 2 kind cluster (deploy/kind,
// deploy/k8s). Everything is driven through kubectl (always with an explicit
// --context), the scheduler's HTTP API through `kubectl port-forward`, psql
// inside the cross-check's own Postgres pod, and, for the SIGKILL test only,
// `docker exec` into the kind node container that runs the scheduler pod.
//
// The stack is a copy of deploy/k8s rendered into namespace kgpu-xcheck; the
// implementer's namespace kgpu is only ever read.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	kContext  = "kind-kgpu"
	kNS       = "kgpu-xcheck"
	kOrigNS   = "kgpu"
	kPortA    = 28090
	kPortB    = 28091
	kGPUKey   = "kgpu.io/gpu"
	kPauseImg = "registry.k8s.io/pause:3.10"
)

func kindEnabled() bool { return os.Getenv("KGPU_KIND") == "1" }

// kcRun runs kubectl with the fixed context, returning stdout and stderr
// separately (kubectl prints deprecation warnings on stderr).
func kcRun(stdin string, args ...string) (string, string, error) {
	full := append([]string{"--context", kContext}, args...)
	cmd := exec.Command("kubectl", full...)
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	err := cmd.Run()
	return so.String(), se.String(), err
}

// kc runs kubectl and fails the test on error.
func kc(t *testing.T, args ...string) string {
	t.Helper()
	so, se, err := kcRun("", args...)
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s%s", strings.Join(args, " "), err, so, se)
	}
	return so
}

// kcApply applies a manifest from a string.
func kcApply(t *testing.T, manifest string) {
	t.Helper()
	so, se, err := kcRun(manifest, "apply", "-f", "-")
	if err != nil {
		t.Fatalf("kubectl apply: %v\n%s%s", err, so, se)
	}
	t.Logf("[kubectl] apply:\n%s", strings.TrimSpace(so))
}

// ---- rendering ----

// renderXcheck renders deploy/k8s with kustomize and moves it into kgpu-xcheck.
// Only the namespace changes, plus the scheduler's replica count.
func renderXcheck(t *testing.T, schedReplicas int) string {
	t.Helper()
	out := kc(t, "kustomize", xcRepoRoot()+"/deploy/k8s")
	docs := strings.Split(out, "\n---\n")
	for i, d := range docs {
		lines := strings.Split(d, "\n")
		isNS := strings.Contains(d, "kind: Namespace")
		isDep := strings.Contains(d, "kind: Deployment")
		for j, l := range lines {
			if strings.TrimSpace(l) == "namespace: "+kOrigNS {
				lines[j] = strings.Replace(l, "namespace: "+kOrigNS, "namespace: "+kNS, 1)
			}
			if isNS && strings.TrimSpace(l) == "name: "+kOrigNS {
				lines[j] = strings.Replace(l, "name: "+kOrigNS, "name: "+kNS, 1)
			}
			if isDep && l == "  replicas: 1" {
				lines[j] = fmt.Sprintf("  replicas: %d", schedReplicas)
			}
		}
		docs[i] = strings.Join(lines, "\n")
	}
	res := strings.Join(docs, "\n---\n")
	for _, l := range strings.Split(res, "\n") {
		s := strings.TrimSpace(l)
		if s == "namespace: "+kOrigNS || s == "name: "+kOrigNS {
			t.Fatalf("render left a reference to namespace %s: %q", kOrigNS, l)
		}
	}
	return res
}

// docOfKind returns the rendered documents of the given kinds.
func docsOfKind(rendered string, kinds ...string) string {
	var keep []string
	for _, d := range strings.Split(rendered, "\n---\n") {
		for _, k := range kinds {
			if regexp.MustCompile(`(?m)^kind: ` + k + `$`).MatchString(d) {
				keep = append(keep, d)
			}
		}
	}
	return strings.Join(keep, "\n---\n")
}

func deleteXcheckNS() {
	fmt.Fprintf(os.Stderr, "[cleanup] kubectl delete namespace %s --wait\n", kNS)
	so, se, err := kcRun("", "delete", "namespace", kNS, "--wait", "--timeout=5m", "--ignore-not-found")
	fmt.Fprintf(os.Stderr, "[cleanup] err=%v %s%s", err, so, se)
}

// ---- pods ----

type kPod struct {
	Metadata struct {
		Name   string            `json:"name"`
		UID    string            `json:"uid"`
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		NodeName string `json:"nodeName"`
		Volumes  []struct {
			Name string `json:"name"`
			PVC  *struct {
				ClaimName string `json:"claimName"`
			} `json:"persistentVolumeClaim"`
		} `json:"volumes"`
	} `json:"spec"`
	Status struct {
		Phase      string `json:"phase"`
		PodIP      string `json:"podIP"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		ContainerStatuses []struct {
			Ready        bool   `json:"ready"`
			RestartCount int    `json:"restartCount"`
			ContainerID  string `json:"containerID"`
			LastState    struct {
				Terminated *struct {
					ExitCode int    `json:"exitCode"`
					Reason   string `json:"reason"`
					Signal   int    `json:"signal"`
				} `json:"terminated"`
			} `json:"lastState"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (p kPod) ready() bool {
	for _, c := range p.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}
	return false
}

func (p kPod) restarts() int {
	n := 0
	for _, c := range p.Status.ContainerStatuses {
		n += c.RestartCount
	}
	return n
}

func (p kPod) String() string {
	return fmt.Sprintf("%s(uid=%s node=%s phase=%s ready=%v restarts=%d ip=%s)",
		p.Metadata.Name, short(p.Metadata.UID), p.Spec.NodeName, p.Status.Phase, p.ready(), p.restarts(), p.Status.PodIP)
}

func short(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

func pods(t *testing.T, ns, selector string) []kPod {
	t.Helper()
	args := []string{"get", "pods", "-n", ns, "-o", "json"}
	if selector != "" {
		args = append(args, "-l", selector)
	}
	var list struct {
		Items []kPod `json:"items"`
	}
	if err := json.Unmarshal([]byte(kc(t, args...)), &list); err != nil {
		t.Fatalf("decode pods: %v", err)
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Metadata.Name < list.Items[j].Metadata.Name })
	return list.Items
}

// getPod returns the pod, or ok=false if it does not exist.
func getPod(t *testing.T, ns, name string) (kPod, bool) {
	t.Helper()
	so, se, err := kcRun("", "get", "pod", name, "-n", ns, "-o", "json")
	if err != nil {
		if strings.Contains(se, "NotFound") {
			return kPod{}, false
		}
		t.Fatalf("get pod %s: %v %s", name, err, se)
	}
	var p kPod
	if err := json.Unmarshal([]byte(so), &p); err != nil {
		t.Fatalf("decode pod: %v", err)
	}
	return p, true
}

// poll calls f every interval until it returns true or the deadline passes.
func poll(t *testing.T, d, every time.Duration, desc string, f func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for {
		if f() {
			return time.Since(start)
		}
		if time.Since(start) > d {
			t.Fatalf("timed out after %s waiting for: %s", d, desc)
		}
		time.Sleep(every)
	}
}

// waitPodsReady waits until exactly n pods match selector and all are Ready.
func waitPodsReady(t *testing.T, ns, selector string, n int, d time.Duration) []kPod {
	t.Helper()
	var ps []kPod
	took := poll(t, d, time.Second, fmt.Sprintf("%d ready pods with %s", n, selector), func() bool {
		ps = pods(t, ns, selector)
		if len(ps) != n {
			return false
		}
		for _, p := range ps {
			if !p.ready() {
				return false
			}
		}
		return true
	})
	t.Logf("[k8s] %d pod(s) %s ready after %.1fs: %v", n, selector, took.Seconds(), ps)
	return ps
}

// schedulingEvents returns the FailedScheduling messages for a pod.
func schedulingEvents(t *testing.T, pod string) string {
	t.Helper()
	so, _, _ := kcRun("", "get", "events", "-n", kNS, "--field-selector",
		"involvedObject.name="+pod+",reason=FailedScheduling", "-o", "jsonpath={range .items[*]}{.message}{\"\\n\"}{end}")
	return strings.TrimSpace(so)
}

func podLogs(t *testing.T, pod string, previous bool) string {
	t.Helper()
	args := []string{"logs", "-n", kNS, pod}
	if previous {
		args = append(args, "--previous")
	}
	so, se, err := kcRun("", args...)
	if err != nil {
		return "logs error: " + se
	}
	return so
}

// grepLines returns log lines matching re.
func grepLines(logs string, re *regexp.Regexp) []string {
	var out []string
	for _, l := range strings.Split(logs, "\n") {
		if re.MatchString(l) {
			out = append(out, l)
		}
	}
	return out
}

// ---- psql in the cross-check's own Postgres pod ----

func psql(t *testing.T, sql string) string {
	t.Helper()
	return strings.TrimSpace(kc(t, "exec", "-n", kNS, "postgres-0", "-c", "postgres", "--",
		"psql", "-U", "postgres", "-d", "kgpu", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql))
}

// ---- port-forward and HTTP ----

type portForward struct {
	cmd  *exec.Cmd
	port int
	out  *syncBuffer
}

// forward starts `kubectl port-forward` to a pod (or svc/NAME) and waits until
// /livez answers through it.
func forward(t *testing.T, target string, port int) *portForward {
	t.Helper()
	var last error
	for attempt := 1; attempt <= 3; attempt++ {
		buf := &syncBuffer{}
		cmd := exec.Command("kubectl", "--context", kContext, "-n", kNS, "port-forward", target, fmt.Sprintf("%d:8080", port))
		cmd.Stdout, cmd.Stderr = buf, buf
		if err := cmd.Start(); err != nil {
			t.Fatalf("start port-forward: %v", err)
		}
		pf := &portForward{cmd: cmd, port: port, out: buf}
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			code, _, err := pf.get("/livez")
			if err == nil && code > 0 {
				t.Logf("[port-forward] %s on 127.0.0.1:%d", target, port)
				t.Cleanup(pf.stop)
				return pf
			}
			last = err
			time.Sleep(300 * time.Millisecond)
		}
		pf.stop()
		t.Logf("[port-forward] attempt %d to %s failed: %v %s", attempt, target, last, buf.String())
		time.Sleep(time.Second)
	}
	t.Fatalf("port-forward to %s never answered: %v", target, last)
	return nil
}

func (pf *portForward) stop() {
	if pf.cmd.Process != nil {
		_ = pf.cmd.Process.Kill()
		_ = pf.cmd.Wait()
		pf.cmd.Process = nil
	}
}

var kHTTP = &http.Client{Timeout: 5 * time.Second}

func (pf *portForward) do(method, path string, body any) (int, string, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", pf.port, path), rd)
	if err != nil {
		return 0, "", err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := kHTTP.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b)), nil
}

func (pf *portForward) get(path string) (int, string, error) { return pf.do("GET", path, nil) }

// newRoundVia creates a round through the API and returns its id.
func (pf *portForward) newRound(t *testing.T, scenario string, seed int64) int64 {
	t.Helper()
	code, body, err := pf.do("POST", "/rounds", map[string]any{"scenario": scenario, "seed": seed, "n_shards": 1})
	if err != nil || code != http.StatusCreated {
		t.Fatalf("POST /rounds via :%d: code=%d err=%v body=%s", pf.port, code, err, body)
	}
	var r struct {
		RoundID int64 `json:"round_id"`
	}
	if err := json.Unmarshal([]byte(body), &r); err != nil || r.RoundID == 0 {
		t.Fatalf("POST /rounds body %q: %v", body, err)
	}
	t.Logf("[api :%d] POST /rounds {scenario:%s seed:%d n_shards:1} -> 201 round_id=%d", pf.port, scenario, seed, r.RoundID)
	return r.RoundID
}

// mustGetRound checks GET /rounds/{id} returns 200 with the expected scenario.
func (pf *portForward) mustGetRound(t *testing.T, id int64, scenario string) {
	t.Helper()
	code, body, err := pf.get(fmt.Sprintf("/rounds/%d", id))
	if err != nil || code != http.StatusOK || !strings.Contains(body, scenario) {
		t.Fatalf("GET /rounds/%d via :%d: code=%d err=%v body=%.300s", id, pf.port, code, err, body)
	}
	t.Logf("[api :%d] GET /rounds/%d -> 200 (scenario %s present)", pf.port, id, scenario)
}

// ---- snapshot of the implementer's namespace (read only) ----

func snapshotOrig(t *testing.T) map[string]string {
	t.Helper()
	so, _, err := kcRun("", "get", "pods,deployments,statefulsets,services,pvc,configmaps,secrets", "-n", kOrigNS, "-o", "json")
	if err != nil {
		t.Fatalf("snapshot %s: %v", kOrigNS, err)
	}
	var list struct {
		Items []struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name       string `json:"name"`
				UID        string `json:"uid"`
				Generation int    `json:"generation"`
			} `json:"metadata"`
			Status struct {
				ContainerStatuses []struct {
					RestartCount int `json:"restartCount"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(so), &list); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	m := map[string]string{}
	for _, it := range list.Items {
		r := 0
		for _, c := range it.Status.ContainerStatuses {
			r += c.RestartCount
		}
		m[it.Kind+"/"+it.Metadata.Name] = fmt.Sprintf("uid=%s gen=%d restarts=%d", it.Metadata.UID, it.Metadata.Generation, r)
	}
	return m
}
