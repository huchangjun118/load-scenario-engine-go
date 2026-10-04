package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Checkpoint is the persistent state of a run, allowing resume without
// double-counting statistics.
type Checkpoint struct {
	Version      int              `json:"version"`
	ScenarioName string           `json:"scenario_name"`
	Seed         int64            `json:"seed"`
	Elapsed      time.Duration    `json:"elapsed"`
	VUStates     map[int]*VUState `json:"vu_states"`
	Stats        Snapshot         `json:"stats"`
	SavedAt      time.Time        `json:"saved_at"`
}

// VUState is the resumable state of one virtual user.
type VUState struct {
	Iteration int64 `json:"iteration"`
	StepIndex int   `json:"step_index"`
}

const checkpointVersion = 1

// SaveCheckpoint writes a checkpoint to disk atomically (temp file + rename).
func SaveCheckpoint(path string, cp *Checkpoint) error {
	if path == "" {
		return nil
	}
	if cp.Version == 0 {
		cp.Version = checkpointVersion
	}
	cp.SavedAt = time.Now()
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".checkpoint-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return nil
}

// LoadCheckpoint reads a checkpoint from disk.
func LoadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	if cp.Version != checkpointVersion {
		return nil, fmt.Errorf("unsupported checkpoint version %d", cp.Version)
	}
	return &cp, nil
}
