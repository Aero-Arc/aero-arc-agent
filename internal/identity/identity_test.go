package identity

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeIDTrimsWhitespaceAndNulls(t *testing.T) {
	raw := "\x00  abc-123  \n\x00"
	got := normalizeID(raw)
	if want := "abc-123"; got != want {
		t.Fatalf("normalizeID(%q) = %q, want %q", raw, got, want)
	}
}

func TestComputeFinalIDMatchesSHA256OfUUID(t *testing.T) {
	uuid := "550e8400-e29b-41d4-a716-446655440000"
	sum := sha256.Sum256([]byte(uuid))
	want := hex.EncodeToString(sum[:])

	got := computeFinalID(uuid)
	if got != want {
		t.Fatalf("computeFinalID(%q) = %q, want %q", uuid, got, want)
	}
}

func TestGeneratedIDPersistence(t *testing.T) {
	tempHome := t.TempDir()

	env := resolverEnv{
		readFile:    os.ReadFile,
		interfaces:  nil,
		userHomeDir: func() (string, error) { return tempHome, nil },
		mkdirAll:    os.MkdirAll,
		writeFile:   os.WriteFile,
	}

	// First resolution should create and persist a new UUID.
	id1 := resolveWithEnv(env)
	if id1.GeneratedID == "" {
		t.Fatalf("expected generated ID to be set")
	}

	idFile := filepath.Join(tempHome, ".aeroarc", "agent-id")
	data, err := os.ReadFile(idFile)
	if err != nil {
		t.Fatalf("expected agent-id file to exist: %v", err)
	}
	if got := string(data); got != id1.GeneratedID {
		t.Fatalf("expected file contents %q, got %q", id1.GeneratedID, got)
	}

	// Second resolution should load the same ID from disk.
	id2 := resolveWithEnv(env)
	if id2.GeneratedID != id1.GeneratedID {
		t.Fatalf("expected persisted generated ID %q, got %q", id1.GeneratedID, id2.GeneratedID)
	}
}

func TestResolveWithMissingHomeReturnsEmptyGeneratedID(t *testing.T) {
	env := resolverEnv{
		readFile:    os.ReadFile,
		interfaces:  nil,
		userHomeDir: func() (string, error) { return "", os.ErrNotExist },
		mkdirAll:    os.MkdirAll,
		writeFile:   os.WriteFile,
	}

	id := resolveWithEnv(env)
	if id.GeneratedID != "" {
		t.Fatalf("expected empty GeneratedID when home directory is unavailable, got %q", id.GeneratedID)
	}

	sum := sha256.Sum256([]byte(""))
	want := hex.EncodeToString(sum[:])
	if id.FinalID != want {
		t.Fatalf("expected FinalID %q when GeneratedID empty, got %q", want, id.FinalID)
	}
}

func TestResolveStableWithinProcess(t *testing.T) {
	id1 := Resolve()
	id2 := Resolve()

	if id1 != id2 {
		t.Fatalf("expected Resolve to return same identity within a process, got %+v and %+v", id1, id2)
	}
}

