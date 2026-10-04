package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/stats"
)

func TestFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ckpt.json")
	fs := &FileStore{Path: path}

	if _, ok, err := fs.Load(); err != nil || ok {
		t.Fatalf("empty store should report ok=false, got ok=%v err=%v", ok, err)
	}

	st := State{
		CompletedActions: 42,
		Elapsed:          3 * time.Second,
		Stats: stats.Snapshot{
			Completed: 42, Succeeded: 40, Failed: 2,
			Errors:  map[string]int64{"boom": 2},
			Buckets: []int64{1, 2, 3},
		},
	}
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	got, ok, err := fs.Load()
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if got.CompletedActions != 42 || got.Elapsed != 3*time.Second {
		t.Fatalf("state mismatch: %+v", got)
	}
	if got.Stats.Completed != 42 || got.Stats.Errors["boom"] != 2 || len(got.Stats.Buckets) != 3 {
		t.Fatalf("stats mismatch: %+v", got.Stats)
	}
}

func TestFileStoreAtomicNoTempLeft(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ckpt.json")
	fs := &FileStore{Path: path}
	if err := fs.Save(State{CompletedActions: 1}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "ckpt.json" {
		t.Fatalf("temp files left behind: %v", entries)
	}
}

func TestMemStore(t *testing.T) {
	m := &MemStore{}
	if _, ok, _ := m.Load(); ok {
		t.Fatal("empty mem store should be invalid")
	}
	_ = m.Save(State{CompletedActions: 7})
	st, ok, _ := m.Load()
	if !ok || st.CompletedActions != 7 {
		t.Fatalf("mem store mismatch: %+v ok=%v", st, ok)
	}
}
