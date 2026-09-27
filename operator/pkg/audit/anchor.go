package audit

import (
	"fmt"
	"os"
	"sync"
)

// AnchorBackend persists checkpoint anchors outside the main chain.
// Implementations:
//   - MemoryAnchorBackend  : kind/dev — in-process, survives pod restarts only if state is serialised
//   - FileAnchorBackend    : kind/dev — persists checkpoints to a local file
//   - Planned: azure-blob, s3, transparency-log (interfaces defined, not yet implemented)
type AnchorBackend interface {
	// Anchor stores a checkpoint. Returns an error if the backend is unavailable.
	Anchor(cp Checkpoint) error
	// Latest returns the most recently anchored checkpoint, or an error if none exists.
	Latest() (Checkpoint, error)
}

// --- MemoryAnchorBackend ---

// MemoryAnchorBackend stores checkpoints in-process. Suitable for kind/dev only.
// It is NOT durable across process restarts.
type MemoryAnchorBackend struct {
	mu      sync.RWMutex
	history []Checkpoint
}

// Anchor implements AnchorBackend.
func (m *MemoryAnchorBackend) Anchor(cp Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.history = append(m.history, cp)
	return nil
}

// Latest implements AnchorBackend.
func (m *MemoryAnchorBackend) Latest() (Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.history) == 0 {
		return Checkpoint{}, fmt.Errorf("no checkpoint anchored yet")
	}
	return m.history[len(m.history)-1], nil
}

// All returns all anchored checkpoints (oldest first).
func (m *MemoryAnchorBackend) All() []Checkpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Checkpoint, len(m.history))
	copy(out, m.history)
	return out
}

// --- FileAnchorBackend ---

// FileAnchorBackend appends checkpoints to a newline-delimited JSON file.
// Suitable for kind/dev persistent storage. One JSON object per line.
type FileAnchorBackend struct {
	path string
	mu   sync.Mutex
}

// NewFileAnchorBackend creates a FileAnchorBackend writing to the given path.
func NewFileAnchorBackend(path string) *FileAnchorBackend {
	return &FileAnchorBackend{path: path}
}

// Anchor implements AnchorBackend.
func (f *FileAnchorBackend) Anchor(cp Checkpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	encoded, err := EncodeCheckpoint(cp)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open checkpoint file %q: %w", f.path, err)
	}
	defer file.Close()
	_, err = fmt.Fprintln(file, encoded)
	return err
}

// Latest implements AnchorBackend by reading the last line of the file.
func (f *FileAnchorBackend) Latest() (Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Checkpoint{}, fmt.Errorf("no checkpoint file yet at %q", f.path)
		}
		return Checkpoint{}, fmt.Errorf("read checkpoint file: %w", err)
	}
	lines := splitLines(data)
	if len(lines) == 0 {
		return Checkpoint{}, fmt.Errorf("checkpoint file is empty")
	}
	return DecodeCheckpoint(lines[len(lines)-1])
}

func splitLines(data []byte) []string {
	var lines []string
	start := 0
	for i, b := range data {
		if b == '\n' {
			line := string(data[start:i])
			if line != "" {
				lines = append(lines, line)
			}
			start = i + 1
		}
	}
	if start < len(data) {
		if line := string(data[start:]); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// --- Planned backends (interfaces only) ---
// These are NOT implemented yet. They are documented as "planned" per the spec.

// PlannedAzureBlobAnchorBackend is a future implementation for AKS private deployments.
// Status: PLANNED — not implemented.
// When implemented, it will write checkpoint JSON to an Azure Blob container
// via the Azure SDK, using a Managed Identity or Workload Identity credential.
type PlannedAzureBlobAnchorBackend struct{}

func (PlannedAzureBlobAnchorBackend) Anchor(_ Checkpoint) error {
	return fmt.Errorf("azure-blob anchor backend is PLANNED and not yet implemented")
}

func (PlannedAzureBlobAnchorBackend) Latest() (Checkpoint, error) {
	return Checkpoint{}, fmt.Errorf("azure-blob anchor backend is PLANNED and not yet implemented")
}

// PlannedS3AnchorBackend is a future implementation for S3-compatible storage.
// Status: PLANNED — not implemented.
type PlannedS3AnchorBackend struct{}

func (PlannedS3AnchorBackend) Anchor(_ Checkpoint) error {
	return fmt.Errorf("s3 anchor backend is PLANNED and not yet implemented")
}

func (PlannedS3AnchorBackend) Latest() (Checkpoint, error) {
	return Checkpoint{}, fmt.Errorf("s3 anchor backend is PLANNED and not yet implemented")
}
