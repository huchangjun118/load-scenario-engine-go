package scenario

import "testing"

func TestValidate(t *testing.T) {
	valid := &Scenario{
		Name: "ok",
		Steps: []Step{
			{ID: "a", Action: ActionSpec{Type: "http", URL: "http://x"}},
			{ID: "b", Action: ActionSpec{Type: "sleep", Duration: Duration(1000000)}},
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid scenario rejected: %v", err)
	}

	cases := []struct {
		name  string
		steps []Step
	}{
		{"no steps", nil},
		{"missing id", []Step{{ID: "", Action: ActionSpec{Type: "log"}}}},
		{"duplicate id", []Step{
			{ID: "a", Action: ActionSpec{Type: "log"}},
			{ID: "a", Action: ActionSpec{Type: "log"}},
		}},
		{"missing action type", []Step{{ID: "a", Action: ActionSpec{}}}},
		{"bad on_error action", []Step{{ID: "a", Action: ActionSpec{Type: "log"}, OnError: &OnError{Action: "bogus"}}}},
		{"goto missing target", []Step{{ID: "a", Action: ActionSpec{Type: "log"}, OnError: &OnError{Action: "goto"}}}},
		{"goto unknown id", []Step{{ID: "a", Action: ActionSpec{Type: "log"}, OnError: &OnError{Action: "goto", Goto: "zzz"}}}},
	}
	for _, tc := range cases {
		s := &Scenario{Name: tc.name, Steps: tc.steps}
		if err := s.Validate(); err == nil {
			t.Errorf("%s: expected validation error", tc.name)
		}
	}
}

func TestDeriveSeedDeterministic(t *testing.T) {
	a := DeriveSeed(42, 0, 0)
	b := DeriveSeed(42, 0, 0)
	if a != b {
		t.Fatal("same inputs produced different seeds")
	}
	if DeriveSeed(42, 1, 0) == a {
		t.Fatal("different vu produced same seed")
	}
	if DeriveSeed(42, 0, 1) == a {
		t.Fatal("different iteration produced same seed")
	}
	if DeriveSeed(43, 0, 0) == a {
		t.Fatal("different base seed produced same seed")
	}
}

func TestParseDuration(t *testing.T) {
	var d Duration
	if err := d.UnmarshalJSON([]byte(`"100ms"`)); err != nil {
		t.Fatal(err)
	}
	if d.Value().Milliseconds() != 100 {
		t.Fatalf("got %v", d.Value())
	}
	if err := d.UnmarshalJSON([]byte(`2.5`)); err != nil {
		t.Fatal(err)
	}
	if d.Value().Seconds() != 2.5 {
		t.Fatalf("got %v", d.Value())
	}
}
