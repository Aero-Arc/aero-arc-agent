package wal

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func TestWALGenerationRotatesAndPersistedFramesRetainIdentity(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "generation.db")
	w, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	firstID := w.GenerationID()
	if _, err := uuid.Parse(firstID); err != nil {
		t.Fatalf("generation ID %q is not a UUID: %v", firstID, err)
	}
	if _, err := w.Append(ctx, &agentv1.TelemetryFrame{WalId: "caller-supplied"}); err != nil {
		t.Fatal(err)
	}
	entries, err := w.ReadUndelivered(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	var stored agentv1.TelemetryFrame
	if err := proto.Unmarshal(entries[0].Payload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.GetWalId() != firstID {
		t.Fatalf("stored WAL ID = %q, want %q", stored.GetWalId(), firstID)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	secondID := reopened.GenerationID()
	if secondID == firstID {
		t.Fatalf("reopened WAL reused generation ID %q", firstID)
	}
	entries, err = reopened.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(entries[0].Payload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.GetWalId() != firstID {
		t.Fatalf("reopened stored WAL ID = %q, want %q", stored.GetWalId(), firstID)
	}
	if _, err := reopened.Append(ctx, &agentv1.TelemetryFrame{}); err != nil {
		t.Fatal(err)
	}
	entries, err = reopened.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(entries[1].Payload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.GetWalId() != secondID {
		t.Fatalf("new stored WAL ID = %q, want %q", stored.GetWalId(), secondID)
	}

	other, err := New(ctx, filepath.Join(t.TempDir(), "new-generation.db"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := other.Close(); err != nil {
			t.Errorf("close other WAL: %v", err)
		}
	})
	if other.GenerationID() == firstID {
		t.Fatalf("new WAL reused generation ID %q", firstID)
	}
}

func TestWALGenerationRotationPreventsRestoredSequenceReuse(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wal.db")
	snapshotPath := filepath.Join(dir, "wal.snapshot.db")

	original, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	firstGeneration := original.GenerationID()
	if id, err := original.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("before-snapshot")}); err != nil || id != 1 {
		t.Fatalf("first append = %d, %v; want seq 1", id, err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	copyFile(t, dbPath, snapshotPath)

	continued, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	continuedGeneration := continued.GenerationID()
	if continuedGeneration == firstGeneration {
		t.Fatalf("continued WAL reused generation ID %q", firstGeneration)
	}
	if id, err := continued.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("after-snapshot")}); err != nil || id != 2 {
		t.Fatalf("continued append = %d, %v; want seq 2", id, err)
	}
	if err := continued.Close(); err != nil {
		t.Fatal(err)
	}

	copyFile(t, snapshotPath, dbPath)
	restored, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Errorf("close restored WAL: %v", err)
		}
	})
	restoredGeneration := restored.GenerationID()
	if restoredGeneration == firstGeneration || restoredGeneration == continuedGeneration {
		t.Fatalf("restored generation ID %q reused a prior generation", restoredGeneration)
	}
	if id, err := restored.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("after-restore")}); err != nil || id != 2 {
		t.Fatalf("restored append = %d, %v; want reused SQLite seq 2", id, err)
	}

	entries, err := restored.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("restored entries = %d, want 2", len(entries))
	}
	var restoredFrame agentv1.TelemetryFrame
	if err := proto.Unmarshal(entries[1].Payload, &restoredFrame); err != nil {
		t.Fatal(err)
	}
	if restoredFrame.GetWalId() != restoredGeneration {
		t.Fatalf("restored seq 2 WAL ID = %q, want %q", restoredFrame.GetWalId(), restoredGeneration)
	}
	if restoredFrame.GetWalId() == continuedGeneration {
		t.Fatalf("restored seq 2 reused cursor (%q, 2)", continuedGeneration)
	}
}

func TestWALGenerationRotationStampsLegacyFramesBeforeChangingGeneration(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	w, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	previousGeneration := w.GenerationID()
	if _, err := w.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("legacy")}); err != nil {
		t.Fatal(err)
	}
	legacyPayload, err := proto.Marshal(&agentv1.TelemetryFrame{RawMavlink: []byte("legacy")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET payload = ? WHERE seq = 1`, legacyPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `DELETE FROM wal_identity_migration`); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	if reopened.GenerationID() == previousGeneration {
		t.Fatalf("reopened WAL reused generation ID %q", previousGeneration)
	}
	entries, err := reopened.ReadUndelivered(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("drained entries = %d, want 1", len(entries))
	}
	var stored agentv1.TelemetryFrame
	if err := proto.Unmarshal(entries[0].Payload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.GetWalId() != previousGeneration {
		t.Fatalf("legacy frame WAL ID = %q, want previous generation %q", stored.GetWalId(), previousGeneration)
	}
}

func TestWALGenerationMigrationCommitsBatchesAndResumesAfterFailure(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-backlog.db")
	w, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	previousGeneration := w.GenerationID()
	frameCount := legacyFrameMigrationBatchSize + 2
	for i := 0; i < frameCount; i++ {
		if _, err := w.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	legacyPayload, err := proto.Marshal(&agentv1.TelemetryFrame{RawMavlink: []byte("legacy")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET payload = ?`, legacyPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `DELETE FROM wal_identity_migration`); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET payload = ? WHERE seq = ?`, []byte{0xff}, frameCount); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := New(ctx, dbPath, 0, 0); err == nil {
		t.Fatal("migration with corrupt frame succeeded, want error")
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var generationAfterFailure string
	if err := db.QueryRowContext(ctx, `SELECT generation_id FROM wal_metadata WHERE id = 1`).Scan(&generationAfterFailure); err != nil {
		t.Fatal(err)
	}
	if generationAfterFailure != previousGeneration {
		t.Fatalf("generation after rollback = %q, want %q", generationAfterFailure, previousGeneration)
	}
	var migrationGeneration string
	var lastSeq, completed int64
	if err := db.QueryRowContext(ctx, `SELECT legacy_generation_id, last_seq, completed
		FROM wal_identity_migration WHERE id = 1`).Scan(&migrationGeneration, &lastSeq, &completed); err != nil {
		t.Fatal(err)
	}
	if migrationGeneration != previousGeneration || lastSeq != legacyFrameMigrationBatchSize || completed != 0 {
		t.Fatalf("migration state after interruption = (%q, %d, %d), want (%q, %d, 0)",
			migrationGeneration, lastSeq, completed, previousGeneration, legacyFrameMigrationBatchSize)
	}
	var firstPayload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM telemetry_frames WHERE seq = 1`).Scan(&firstPayload); err != nil {
		t.Fatal(err)
	}
	var firstFrame agentv1.TelemetryFrame
	if err := proto.Unmarshal(firstPayload, &firstFrame); err != nil {
		t.Fatal(err)
	}
	if firstFrame.GetWalId() != previousGeneration {
		t.Fatalf("committed first-batch WAL ID = %q, want %q", firstFrame.GetWalId(), previousGeneration)
	}
	var rolledBackPayload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM telemetry_frames WHERE seq = ?`, legacyFrameMigrationBatchSize+1).Scan(&rolledBackPayload); err != nil {
		t.Fatal(err)
	}
	var rolledBackFrame agentv1.TelemetryFrame
	if err := proto.Unmarshal(rolledBackPayload, &rolledBackFrame); err != nil {
		t.Fatal(err)
	}
	if rolledBackFrame.GetWalId() != "" {
		t.Fatalf("failed second batch retained WAL ID %q, want rollback", rolledBackFrame.GetWalId())
	}
	if _, err := db.ExecContext(ctx, `UPDATE telemetry_frames SET payload = ? WHERE seq = ?`, legacyPayload, frameCount); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := migrated.Close(); err != nil {
			t.Errorf("close migrated WAL: %v", err)
		}
	})
	entries, err := migrated.ReadUndelivered(ctx, frameCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != frameCount {
		t.Fatalf("migrated entries = %d, want %d", len(entries), frameCount)
	}
	if err := migrated.db.QueryRowContext(ctx, `SELECT legacy_generation_id, last_seq, completed
		FROM wal_identity_migration WHERE id = 1`).Scan(&migrationGeneration, &lastSeq, &completed); err != nil {
		t.Fatal(err)
	}
	if migrationGeneration != previousGeneration || lastSeq != int64(frameCount) || completed != 1 {
		t.Fatalf("completed migration state = (%q, %d, %d), want (%q, %d, 1)",
			migrationGeneration, lastSeq, completed, previousGeneration, frameCount)
	}
	for _, entry := range entries {
		var frame agentv1.TelemetryFrame
		if err := proto.Unmarshal(entry.Payload, &frame); err != nil {
			t.Fatal(err)
		}
		if frame.GetWalId() != previousGeneration {
			t.Fatalf("frame %d WAL ID = %q, want %q", entry.ID, frame.GetWalId(), previousGeneration)
		}
	}
}

func TestWALGenerationMigrationCompletionSkipsPayloadRescan(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "completed-migration.db")
	w, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	previousGeneration := w.GenerationID()
	if _, err := w.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("will-corrupt")}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET payload = ? WHERE seq = 1`, []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatalf("completed migration rescanned payloads: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	if reopened.GenerationID() == previousGeneration {
		t.Fatalf("reopened WAL reused generation ID %q", previousGeneration)
	}
}

func TestWALGenerationMigrationInitializesLegacyDatabaseWithoutMetadata(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "pre-metadata.db")
	w, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("legacy")}); err != nil {
		t.Fatal(err)
	}
	legacyPayload, err := proto.Marshal(&agentv1.TelemetryFrame{RawMavlink: []byte("legacy")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET payload = ?`, legacyPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `DELETE FROM wal_identity_migration`); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `DELETE FROM wal_metadata`); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := migrated.Close(); err != nil {
			t.Errorf("close migrated WAL: %v", err)
		}
	})
	entries, err := migrated.ReadUndelivered(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("migrated entries = %d, want 1", len(entries))
	}
	var frame agentv1.TelemetryFrame
	if err := proto.Unmarshal(entries[0].Payload, &frame); err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(frame.GetWalId()); err != nil {
		t.Fatalf("legacy frame WAL ID %q is not a UUID: %v", frame.GetWalId(), err)
	}
	if frame.GetWalId() == migrated.GenerationID() {
		t.Fatalf("legacy migration generation %q reused current append generation", frame.GetWalId())
	}
}

func copyFile(t *testing.T, source, destination string) {
	t.Helper()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

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

func TestWALSpoolUsesGenerationThatAssignsSequence(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "spool-generation.db")
	w, err := New(ctx, dbPath, 2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	firstGeneration := w.GenerationID()
	if _, err := w.spoolBatch([]*agentv1.TelemetryFrame{{RawMavlink: []byte("spooled")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(ctx, dbPath, 2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	if reopened.GenerationID() == firstGeneration {
		t.Fatalf("reopened WAL reused generation ID %q", firstGeneration)
	}
	select {
	case <-reopened.signalChan:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reopened WAL to drain spool")
	}
	entries, err := reopened.ReadUndelivered(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("drained entries = %d, want 1", len(entries))
	}
	var stored agentv1.TelemetryFrame
	if err := proto.Unmarshal(entries[0].Payload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.GetWalId() != reopened.GenerationID() {
		t.Fatalf("spooled frame WAL ID = %q, want assigning generation %q", stored.GetWalId(), reopened.GenerationID())
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

func TestWAL_OperationContextPersistsAndCommandsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wal.db")
	w, err := New(ctx, path, 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	want := OperationContext{FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 3}
	applied, err := w.SetOperationContext(ctx, "set-1", want)
	if err != nil || !applied {
		t.Fatalf("SetOperationContext() = %v, %v", applied, err)
	}
	applied, err = w.SetOperationContext(ctx, "set-1", OperationContext{FlightID: "wrong"})
	if err != nil || applied {
		t.Fatalf("duplicate SetOperationContext() = %v, %v", applied, err)
	}
	got, ok, err := w.LoadOperationContext(ctx)
	if err != nil || !ok || got != want {
		t.Fatalf("LoadOperationContext() = %#v, %v, %v; want %#v", got, ok, err, want)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = New(ctx, path, 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	got, ok, err = w.LoadOperationContext(ctx)
	if err != nil || !ok || got != want {
		t.Fatalf("context after reopen = %#v, %v, %v; want %#v", got, ok, err, want)
	}

	applied, err = w.ClearOperationContext(ctx, "clear-empty", "")
	if err == nil || applied {
		t.Fatalf("empty-flight clear = %v, %v; want false, error", applied, err)
	}
	if got, ok, err = w.LoadOperationContext(ctx); err != nil || !ok || got != want {
		t.Fatalf("context after empty-flight clear = %#v, %v, %v; want %#v", got, ok, err, want)
	}

	applied, err = w.ClearOperationContext(ctx, "clear-old", "another-flight")
	if err != nil || !applied {
		t.Fatalf("conditional clear = %v, %v", applied, err)
	}
	if got, ok, err = w.LoadOperationContext(ctx); err != nil || !ok || got != want {
		t.Fatalf("context after mismatched clear = %#v, %v, %v", got, ok, err)
	}
	applied, err = w.ClearOperationContext(ctx, "clear-1", want.FlightID)
	if err != nil || !applied {
		t.Fatalf("matching clear = %v, %v", applied, err)
	}
	if _, ok, err = w.LoadOperationContext(ctx); err != nil || ok {
		t.Fatalf("context after clear: ok=%v err=%v", ok, err)
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
