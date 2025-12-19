package wal

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_Lifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "wal_lifecycle.db")

	w, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Check if file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("WAL file was not created at %s", dbPath)
	}
}

func TestWAL_AppendAndRead(t *testing.T) {
	w := mustNewWAL(t)
	defer w.Close()
	ctx := context.Background()

	payloads := [][]byte{
		[]byte("frame1"),
		[]byte("frame2"),
		[]byte("frame3"),
	}

	for _, p := range payloads {
		if _, err := w.Append(ctx, p); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Read all 3
	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatalf("ReadUndelivered failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	for i, e := range entries {
		if !bytes.Equal(e.Payload, payloads[i]) {
			t.Errorf("Entry %d mismatch: got %s, want %s", i, e.Payload, payloads[i])
		}
	}
}

func TestWAL_MarkDelivered_Idempotency(t *testing.T) {
	w := mustNewWAL(t)
	defer w.Close()
	ctx := context.Background()

	id, err := w.Append(ctx, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	// First mark should succeed (RowsAffected = 1)
	rows, err := w.MarkDelivered(ctx, id)
	if err != nil {
		t.Fatalf("MarkDelivered failed: %v", err)
	}
	if rows != 1 {
		t.Errorf("First mark: expected 1 row affected, got %d", rows)
	}

	// Second mark should reflect no change (RowsAffected = 0)
	rows, err = w.MarkDelivered(ctx, id)
	if err != nil {
		t.Fatalf("MarkDelivered (2nd) failed: %v", err)
	}
	if rows != 0 {
		t.Errorf("Second mark: expected 0 rows affected, got %d", rows)
	}

	// Should not be returned by ReadUndelivered anymore
	entries, err := w.ReadUndelivered(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 undelivered entries, got %d", len(entries))
	}
}

func TestWAL_CleanupDelivered(t *testing.T) {
	w := mustNewWAL(t)
	defer w.Close()
	ctx := context.Background()

	// Append 10 items
	var ids []uint64
	for i := uint64(0); i < 10; i++ {
		id, err := w.Append(ctx, []byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// Mark all as delivered
	for _, id := range ids {
		if _, err := w.MarkDelivered(ctx, id); err != nil {
			t.Fatal(err)
		}
	}

	// Keep last 3
	if err := w.CleanupDelivered(ctx, 3); err != nil {
		t.Fatalf("CleanupDelivered failed: %v", err)
	}

	// Verify manually via SQL (since ReadUndelivered ignores them anyway)
	var count int
	row := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM telemetry_frames")
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}

	if count != 3 {
		t.Errorf("Expected 3 rows remaining, got %d", count)
	}

	// Verify we kept the *latest* 3 (IDs 8, 9, 10 if 1-based, or 7,8,9 if 0-based... sqlite is 1-based autoinc usually)
	// We can check if the min ID is correct.
	// We inserted 10 items. IDs likely 1..10. Keeping last 3 means keeping 8, 9, 10.
	// So ID 1 should be gone.
	var id1Exists int
	err := w.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM telemetry_frames WHERE seq = ?", ids[0]).Scan(&id1Exists)
	if err != nil {
		t.Fatal(err)
	}
	if id1Exists != 0 {
		t.Error("Expected old ID to be deleted, but it exists")
	}
}

func TestWAL_ReadLimit(t *testing.T) {
	w := mustNewWAL(t)
	defer w.Close()
	ctx := context.Background()

	// Append 5 items
	for i := 0; i < 5; i++ {
		w.Append(ctx, []byte("data"))
	}

	// Read with limit 2
	entries, err := w.ReadUndelivered(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}

	// Test invalid limit
	_, err = w.ReadUndelivered(ctx, 0)
	if err == nil {
		t.Error("Expected error for limit=0, got nil")
	}
}

func mustNewWAL(t *testing.T) *WAL {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	w, err := New(dbPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	return w
}
