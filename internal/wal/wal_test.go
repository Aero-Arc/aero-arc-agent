package wal

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestWAL_Lifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "wal_lifecycle.db")

	w, err := New(context.Background(), dbPath, 0, 0)
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
		frame := &agentv1.TelemetryFrame{
			RawMavlink: p,
		}
		if _, err := w.Append(ctx, frame); err != nil {
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
		var frame agentv1.TelemetryFrame
		if err := proto.Unmarshal(e.Payload, &frame); err != nil {
			t.Fatalf("Failed to unmarshal frame: %v", err)
		}
		if !bytes.Equal(frame.RawMavlink, payloads[i]) {
			t.Errorf("Entry %d mismatch: got %s, want %s", i, frame.RawMavlink, payloads[i])
		}
	}
}

func TestWAL_AsyncBatching(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_async.db")
	// Use small batch size and short timeout for testing
	w, err := New(context.Background(), dbPath, 2, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer w.Close()
	ctx := context.Background()

	// 1. AppendAsync 1 frame (should buffer)
	err = w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("frame1")})
	if err != nil {
		t.Fatalf("AppendAsync 1 failed: %v", err)
	}

	// Read immediately - should be empty (buffered)
	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatalf("ReadUndelivered failed: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries (buffered), got %d", len(entries))
	}

	// 2. AppendAsync 2nd frame (should trigger batch flush due to size=2)
	err = w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("frame2")})
	if err != nil {
		t.Fatalf("AppendAsync 2 failed: %v", err)
	}

	// Wait for signal
	select {
	case <-w.signalChan:
		// Got signal
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for WAL signal")
	}

	// Read - should have 2 entries
	entries, err = w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatalf("ReadUndelivered failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}

	// 3. AppendAsync 3rd frame (should wait for timeout)
	err = w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("frame3")})
	if err != nil {
		t.Fatalf("AppendAsync 3 failed: %v", err)
	}

	// Wait for signal (triggered by timeout)
	select {
	case <-w.signalChan:
		// Got signal
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for WAL signal (timeout flush)")
	}

	// Read - should have 1 new entry (total 3 undelivered if we didn't mark them)
	entries, err = w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatalf("ReadUndelivered failed: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries total, got %d", len(entries))
	}
}

func TestWAL_SpoolAndDrain(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_spool.db")
	w, err := New(context.Background(), dbPath, 2, time.Hour)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer w.Close()
	ctx := context.Background()

	frames := []*agentv1.TelemetryFrame{
		{RawMavlink: []byte("spool1")},
		{RawMavlink: []byte("spool2")},
	}

	spoolPath, err := w.spoolBatch(frames)
	if err != nil {
		t.Fatalf("spoolBatch failed: %v", err)
	}
	if spoolPath == "" {
		t.Fatal("expected spool file to be created")
	}
	if _, err := os.Stat(spoolPath); err != nil {
		t.Fatalf("spool file missing: %v", err)
	}

	if err := w.drainSpool(); err != nil {
		t.Fatalf("drainSpool failed: %v", err)
	}
	if _, err := os.Stat(spoolPath); !os.IsNotExist(err) {
		t.Fatalf("expected spool file to be removed, got: %v", err)
	}

	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatalf("ReadUndelivered failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}
}

func TestWAL_MarkDelivered_Idempotency(t *testing.T) {
	w := mustNewWAL(t)
	defer w.Close()
	ctx := context.Background()

	id, err := w.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	// First mark should succeed (RowsAffected = 1)
	rows, err := w.MarkDelivered(ctx, uint64(id))
	if err != nil {
		t.Fatalf("MarkDelivered failed: %v", err)
	}
	if rows != 1 {
		t.Errorf("First mark: expected 1 row affected, got %d", rows)
	}

	// Second mark should reflect no change (RowsAffected = 0)
	rows, err = w.MarkDelivered(ctx, uint64(id))
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
		id, err := w.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte{byte(i)}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, uint64(id))
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
		w.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("data")})
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
	w, err := New(context.Background(), dbPath, 0, 0)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	return w
}
