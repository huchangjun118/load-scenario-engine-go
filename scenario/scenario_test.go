package scenario

import (
	"testing"
	"time"
)

func TestDeterministicSequence(t *testing.T) {
	s := &Scenario{
		Name: "det",
		Seed: 42,
		Steps: []Step{
			{Name: "a", Op: "read", Weight: 3},
			{Name: "b", Op: "write", Weight: 2},
		},
	}
	collect := func() []Action {
		it := NewIterator(s)
		var out []Action
		for {
			a, ok := it.Next()
			if !ok {
				return out
			}
			out = append(out, a)
		}
	}
	r1, r2 := collect(), collect()
	if len(r1) != 5 {
		t.Fatalf("expected 5 actions, got %d", len(r1))
	}
	for i := range r1 {
		if r1[i] != r2[i] {
			t.Fatalf("action %d differs between runs: %+v vs %+v", i, r1[i], r2[i])
		}
	}
	// Step order and weights.
	for i := 0; i < 3; i++ {
		if r1[i].StepIndex != 0 || r1[i].Op != "read" {
			t.Fatalf("action %d should be step 0 read, got %+v", i, r1[i])
		}
	}
	if r1[3].StepIndex != 1 || r1[4].StepIndex != 1 {
		t.Fatalf("last two actions should be step 1")
	}
	// Keys unique.
	seen := map[string]bool{}
	for _, a := range r1 {
		if seen[a.Key] {
			t.Fatalf("duplicate key %q", a.Key)
		}
		seen[a.Key] = true
	}
}

func TestDifferentSeedsDiffer(t *testing.T) {
	mk := func(seed int64) *Scenario {
		return &Scenario{Seed: seed, Steps: []Step{{Name: "a", Op: "read", Weight: 4}}}
	}
	k1 := NewIterator(mk(1))
	k2 := NewIterator(mk(2))
	a1, _ := k1.Next()
	a2, _ := k2.Next()
	if a1.Key == a2.Key {
		t.Fatalf("different seeds produced identical keys")
	}
}

func TestFailureGotoBranch(t *testing.T) {
	s := &Scenario{
		Seed: 7,
		Steps: []Step{
			{Name: "try", Op: "write", Weight: 1,
				OnFailure: &FailurePolicy{Kind: FailureGoto, Step: 1}},
			{Name: "compensate", Op: "delete", Weight: 1},
		},
	}
	it := NewIterator(s)
	a, ok := it.Next()
	if !ok || a.Op != "write" {
		t.Fatalf("expected write, got %+v", a)
	}
	kind := it.Resolve(a)
	if kind != FailureGoto {
		t.Fatalf("expected goto, got %q", kind)
	}
	b, ok := it.Next()
	if !ok || b.Op != "delete" {
		t.Fatalf("expected delete after goto, got %+v", b)
	}
}

func TestWaitBeforeAfter(t *testing.T) {
	s := &Scenario{Steps: []Step{{
		Name: "a", Op: "read", WaitBefore: 10 * time.Millisecond, WaitAfter: 5 * time.Millisecond,
	}}}
	it := NewIterator(s)
	a, _ := it.Next()
	if it.WaitBefore(a) != 10*time.Millisecond {
		t.Fatalf("wait_before mismatch")
	}
	if it.WaitAfter(a) != 5*time.Millisecond {
		t.Fatalf("wait_after mismatch")
	}
}

func TestValidate(t *testing.T) {
	if err := (&Scenario{}).Validate(); err == nil {
		t.Fatal("empty scenario should be invalid")
	}
	bad := &Scenario{Steps: []Step{{Name: "x"}}}
	if err := bad.Validate(); err == nil {
		t.Fatal("step without op should be invalid")
	}
	badGoto := &Scenario{Steps: []Step{{
		Name: "x", Op: "read",
		OnFailure: &FailurePolicy{Kind: FailureGoto, Step: 5},
	}}}
	if err := badGoto.Validate(); err == nil {
		t.Fatal("goto out of range should be invalid")
	}
	ok := &Scenario{Steps: []Step{{
		Name: "x", Op: "read",
		OnFailure: &FailurePolicy{Kind: FailureAbort},
	}}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid scenario rejected: %v", err)
	}
}
