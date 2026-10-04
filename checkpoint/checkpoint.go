// Package checkpoint persists run progress so a restarted process can resume
// from the latest checkpoint without double-counting completed statistics.
package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/huchangjun118/load-scenario-engine-go/stats"
)

// State is the resumable progress of a run.
type State struct {
	// CompletedActions is the number of scenario actions fully executed
	// and already reflected in Stats. On resume, the iterator skips
	// exactly this many actions, so nothing is counted twice.
	CompletedActions int64 `json:"completed_actions"`
	// Elapsed is the curve offset already consumed.
	Elapsed time.Duration `json:"elapsed"`
	// Stats is the merged statistics snapshot at checkpoint time.
	Stats stats.Snapshot `json:"stats"`
}

// Store persists and loads checkpoint states.
type Store interface {
	Save(s State) error
	Load() (State, bool, error)
}

// FileStore is a JSON-file backed Store. Writes are atomic (temp file +
// rename) so a crash mid-save never corrupts the last good checkpoint.
type FileStore struct {
	Path string
}

// Save writes the state atomically.
func (f *FileStore) Save(s State) error {
	if f.Path == "" {
		return errors.New("checkpoint: empty path")
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}
	dir := filepath.Dir(f.Path)
	tmp, err := os.CreateTemp(dir, ".checkpoint-*.tmp")
	if err != nil {
		return fmt.Errorf("checkpoint: create temp: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("checkpoint: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("checkpoint: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("checkpoint: close: %w", err)
	}
	if err := os.Rename(tmpName, f.Path); err != nil {
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	return nil
}

// Load reads the last saved state; ok is false when none exists.
func (f *FileStore) Load() (State, bool, error) {
	data, err := os.ReadFile(f.Path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return State{}, false, nil
		}
		return State{}, false, fmt.Errorf("checkpoint: read: %w", err)
	}
	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return State{}, false, fmt.Errorf("checkpoint: unmarshal: %w", err)
	}
	return s, true, nil
}

// MemStore is an in-memory Store useful for tests.
type MemStore struct {
	State State
	Valid bool
}

// Save records the state in memory.
func (m *MemStore) Save(s State) error {
	m.State = s
	m.Valid = true
	return nil
}

// Load returns the in-memory state.
func (m *MemStore) Load() (State, bool, error) {
	return m.State, m.Valid, nil
}
