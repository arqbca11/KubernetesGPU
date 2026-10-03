package policy

import "testing"

func TestAlwaysGPU(t *testing.T) {
	p := AlwaysGPU{}
	cases := []struct {
		name string
		in   Input
		want string
	}{
		{"fits", Input{Build: BuildSpec{MemBytes: 1 << 30}, Pool: PoolState{LiveWorkers: 2, LargestMem: 8 << 30}}, "gpu"},
		{"exactly fits", Input{Build: BuildSpec{MemBytes: 8 << 30}, Pool: PoolState{LiveWorkers: 1, LargestMem: 8 << 30}}, "gpu"},
		{"too big for every live worker", Input{Build: BuildSpec{MemBytes: 16 << 30}, Pool: PoolState{LiveWorkers: 2, LargestMem: 8 << 30}}, "local"},
		{"no live workers: still queue (pool may be cold)", Input{Build: BuildSpec{MemBytes: 16 << 30}, Pool: PoolState{}}, "gpu"},
	}
	for _, c := range cases {
		d := p.Decide(c.in)
		t.Logf("%-45s -> %-5s (%s)", c.name, d.Placement, d.Reason)
		if d.Placement != c.want {
			t.Fatalf("%s: got %s want %s", c.name, d.Placement, c.want)
		}
	}
	// Purity: same input, same answer.
	a, b := p.Decide(cases[0].in), p.Decide(cases[0].in)
	if a != b {
		t.Fatal("decision is not deterministic")
	}
}
