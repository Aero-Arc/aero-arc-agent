package wal

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type cancelAfterFirstRead struct {
	reader io.Reader
	cancel context.CancelFunc
	once   sync.Once
}

func (r *cancelAfterFirstRead) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	if n > 0 {
		r.once.Do(r.cancel)
	}
	return n, err
}

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

func TestInitDBAddsSpoolImportCleanupTokenToExistingSchema(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "schema.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close schema database: %v", err)
		}
	})
	if _, err := db.Exec(`CREATE TABLE spool_imports (
		spool_id TEXT PRIMARY KEY,
		imported_at INTEGER NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO spool_imports(spool_id, imported_at) VALUES('existing', 1)`); err != nil {
		t.Fatal(err)
	}
	if err := initDB(db); err != nil {
		t.Fatal(err)
	}
	var seenToken string
	if err := db.QueryRow(`SELECT seen_token FROM spool_imports WHERE spool_id = 'existing'`).Scan(&seenToken); err != nil {
		t.Fatal(err)
	}
	if seenToken != "" {
		t.Fatalf("migrated cleanup token = %q, want empty", seenToken)
	}
}

func TestInitDBMigratesPendingTimestampsTransactionallyAndIdempotently(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "pending-schema.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close schema database: %v", err)
		}
	})
	if _, err := db.Exec(`CREATE TABLE telemetry_frames (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at INTEGER NOT NULL,
		payload BLOB NOT NULL,
		delivery_status INTEGER NOT NULL DEFAULT 0
	)`); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		createdAt int64
		status    DeliveryStatus
	}{{11, DeliveryStatusWritten}, {22, DeliveryStatusPending}, {33, DeliveryStatusDelivered}} {
		if _, err := db.Exec(`INSERT INTO telemetry_frames(created_at, payload, delivery_status) VALUES(?, X'00', ?)`, row.createdAt, row.status); err != nil {
			t.Fatal(err)
		}
	}
	if err := initDB(db); err != nil {
		t.Fatal(err)
	}
	if err := initDB(db); err != nil {
		t.Fatalf("idempotent initDB: %v", err)
	}
	rows, err := db.Query(`SELECT seq, pending_since_unix_ns, pending_owner FROM telemetry_frames ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close migrated telemetry rows: %v", err)
		}
	}()
	for sequence := int64(1); rows.Next(); sequence++ {
		var seq int64
		var pendingSince sql.NullInt64
		var pendingOwner sql.NullString
		if err := rows.Scan(&seq, &pendingSince, &pendingOwner); err != nil {
			t.Fatal(err)
		}
		if seq == 2 {
			if !pendingSince.Valid || pendingSince.Int64 != 22 {
				t.Fatalf("legacy pending timestamp = %+v, want 22", pendingSince)
			}
		} else if pendingSince.Valid {
			t.Fatalf("non-pending sequence %d retained timestamp %d", seq, pendingSince.Int64)
		}
		if pendingOwner.Valid {
			t.Fatalf("migrated sequence %d retained pending owner %q", seq, pendingOwner.String)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestInitDBAddsOperationCommandFingerprintsToExistingSchema(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "schema.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close schema database: %v", err)
		}
	})
	if _, err := db.Exec(`CREATE TABLE operation_context_commands (
		command_id TEXT PRIMARY KEY,
		processed_at INTEGER NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO operation_context_commands(command_id, processed_at) VALUES('legacy', 1)`); err != nil {
		t.Fatal(err)
	}
	if err := initDB(db); err != nil {
		t.Fatal(err)
	}
	var kind, fingerprint string
	if err := db.QueryRow(`SELECT command_kind, payload_fingerprint FROM operation_context_commands WHERE command_id = 'legacy'`).Scan(&kind, &fingerprint); err != nil {
		t.Fatal(err)
	}
	if kind != "" || fingerprint != "" {
		t.Fatalf("legacy fingerprint = (%q, %q), want empty", kind, fingerprint)
	}

	// Payload history cannot be reconstructed for an old command row. Retries
	// remain no-ops even after later context changes, and the row stays explicitly
	// unknown rather than adopting whichever payload happens to arrive first.
	w := &WAL{db: db}
	want := OperationContext{FlightID: "flight-legacy", IntentID: "intent-legacy", IntentVersion: 4}
	if _, err := db.Exec(`INSERT INTO operation_context(id, flight_id, intent_id, intent_version, updated_at) VALUES(1, ?, ?, ?, 1)`, want.FlightID, want.IntentID, want.IntentVersion); err != nil {
		t.Fatal(err)
	}
	applied, err := w.SetOperationContext(context.Background(), "legacy", want)
	if err != nil || applied {
		t.Fatalf("legacy matching retry = %v, %v", applied, err)
	}
	if err := db.QueryRow(`SELECT command_kind, payload_fingerprint FROM operation_context_commands WHERE command_id = 'legacy'`).Scan(&kind, &fingerprint); err != nil {
		t.Fatal(err)
	}
	if kind != "" || fingerprint != "" {
		t.Fatalf("legacy fingerprint = (%q, %q), want payload-unknown", kind, fingerprint)
	}
	if applied, err = w.SetOperationContext(context.Background(), "legacy", OperationContext{FlightID: "different"}); err != nil || applied {
		t.Fatalf("payload-unknown legacy retry = %v, %v", applied, err)
	}
	current, ok, err := w.LoadOperationContext(context.Background())
	if err != nil || !ok || current != want {
		t.Fatalf("legacy retry changed current context = %+v, %v, %v", current, ok, err)
	}
	newer := OperationContext{FlightID: "flight-new", IntentID: "intent-new", IntentVersion: 5}
	if applied, err = w.SetOperationContext(context.Background(), "newer", newer); err != nil || !applied {
		t.Fatalf("later context update = %v, %v", applied, err)
	}
	if applied, err = w.SetOperationContext(context.Background(), "legacy", want); err != nil || applied {
		t.Fatalf("older exact retry after context change = %v, %v", applied, err)
	}
	current, ok, err = w.LoadOperationContext(context.Background())
	if err != nil || !ok || current != newer {
		t.Fatalf("older retry rewound current context = %+v, %v, %v", current, ok, err)
	}

	if _, err := db.Exec(`INSERT INTO operation_context_commands(command_id, processed_at, command_kind, payload_fingerprint) VALUES('legacy-clear', 2, '', '')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM operation_context WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	applied, err = w.ClearOperationContext(context.Background(), "legacy-clear", want.FlightID)
	if err != nil || applied {
		t.Fatalf("legacy clear retry = %v, %v", applied, err)
	}
}

func TestMissionDeploymentJournalIsImmutableAndDurable(t *testing.T) {
	ctx := context.Background()
	w, err := New(ctx, filepath.Join(t.TempDir(), "mission-journal.db"), 1, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	record, created, err := w.ReserveMissionDeployment(ctx, "command-1", "fingerprint-1", []byte("payload-1"))
	if err != nil || !created || record.State != "prepared" {
		t.Fatalf("reserve = %+v, %v, %v", record, created, err)
	}
	if _, created, err = w.ReserveMissionDeployment(ctx, "command-1", "fingerprint-1", []byte("payload-1")); err != nil || created {
		t.Fatalf("exact reserve retry = %v, %v", created, err)
	}
	if _, _, err = w.ReserveMissionDeployment(ctx, "command-1", "fingerprint-2", []byte("payload-2")); !errors.Is(err, ErrMissionDeploymentConflict) {
		t.Fatalf("conflicting reserve error = %v", err)
	}
	if err := w.MarkMissionDeploymentEffectStarted(ctx, "command-1", "fingerprint-1"); err != nil {
		t.Fatal(err)
	}
	if err := w.StoreMissionDeploymentResult(ctx, "command-1", "fingerprint-1", []byte("result"), true); err != nil {
		t.Fatal(err)
	}
	record, err = w.LoadMissionDeployment(ctx, "command-1")
	if err != nil || record.State != "outcome_unknown" || string(record.ResultPayload) != "result" {
		t.Fatalf("stored record = %+v, %v", record, err)
	}
	if err := w.StoreMissionDeploymentResult(ctx, "command-1", "wrong", []byte("mutation"), false); !errors.Is(err, ErrMissionDeploymentConflict) {
		t.Fatalf("wrong-row mutation error = %v", err)
	}
	if err := w.StoreMissionDeploymentResult(ctx, "command-1", "fingerprint-1", []byte("terminal"), false); err != nil {
		t.Fatal(err)
	}
	if err := w.StoreMissionDeploymentResult(ctx, "command-1", "fingerprint-1", []byte("regression"), true); err == nil {
		t.Fatal("terminal mission deployment regressed to outcome_unknown")
	}
}

func TestDurableCommandIDsCannotCrossOperationAndMissionKinds(t *testing.T) {
	w := mustNewWAL(t)
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()
	ctx := context.Background()
	operation := OperationContext{AircraftID: "aircraft-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 1}
	if applied, err := w.SetOperationContext(ctx, "shared-operation-first", operation); err != nil || !applied {
		t.Fatalf("SetOperationContext() = %v, %v", applied, err)
	}
	if _, _, err := w.ReserveMissionDeployment(ctx, "shared-operation-first", "fingerprint-1", []byte("mission-1")); !errors.Is(err, ErrMissionDeploymentConflict) {
		t.Fatalf("mission reuse of operation ID error = %v", err)
	}
	if _, err := w.LoadMissionDeployment(ctx, "shared-operation-first"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-kind mission row exists: %v", err)
	}

	if _, created, err := w.ReserveMissionDeployment(ctx, "shared-mission-first", "fingerprint-2", []byte("mission-2")); err != nil || !created {
		t.Fatalf("ReserveMissionDeployment() = %v, %v", created, err)
	}
	if applied, err := w.SetOperationContext(ctx, "shared-mission-first", operation); !errors.Is(err, ErrOperationCommandConflict) || applied {
		t.Fatalf("operation reuse of mission ID = %v, %v", applied, err)
	}
	record, err := w.LoadMissionDeployment(ctx, "shared-mission-first")
	if err != nil || record.PayloadFingerprint != "fingerprint-2" || record.State != "prepared" {
		t.Fatalf("mission record after collision = %+v, %v", record, err)
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

func TestWALGenerationMigrationQuarantinesBeyondBatchBoundaryAndResumes(t *testing.T) {
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

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureDB(db); err != nil {
		t.Fatal(err)
	}
	if err := initDB(db); err != nil {
		t.Fatal(err)
	}
	state, err := loadOrCreateLegacyMigration(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	state, err = migrateLegacyFrameBatch(ctx, db, state.generationID)
	if err != nil {
		t.Fatal(err)
	}
	if state.lastSeq != legacyFrameMigrationBatchSize || state.completed {
		t.Fatalf("first migration batch state = (%d, %v), want (%d, false)", state.lastSeq, state.completed, legacyFrameMigrationBatchSize)
	}
	var generationDuringMigration string
	if err := db.QueryRowContext(ctx, `SELECT generation_id FROM wal_metadata WHERE id = 1`).Scan(&generationDuringMigration); err != nil {
		t.Fatal(err)
	}
	if generationDuringMigration != previousGeneration {
		t.Fatalf("generation during migration = %q, want %q", generationDuringMigration, previousGeneration)
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
	var notYetMigratedPayload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM telemetry_frames WHERE seq = ?`, legacyFrameMigrationBatchSize+1).Scan(&notYetMigratedPayload); err != nil {
		t.Fatal(err)
	}
	var notYetMigratedFrame agentv1.TelemetryFrame
	if err := proto.Unmarshal(notYetMigratedPayload, &notYetMigratedFrame); err != nil {
		t.Fatal(err)
	}
	if notYetMigratedFrame.GetWalId() != "" {
		t.Fatalf("uncommitted next-batch frame retained WAL ID %q", notYetMigratedFrame.GetWalId())
	}
	var corruptPayload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM telemetry_frames WHERE seq = ?`, frameCount).Scan(&corruptPayload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(corruptPayload, []byte{0xff}) {
		t.Fatalf("corrupt payload changed before resume: %x", corruptPayload)
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
	if len(entries) != frameCount-1 {
		t.Fatalf("migrated entries = %d, want %d valid rows", len(entries), frameCount-1)
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
	var quarantinedPayload []byte
	var quarantineReason string
	var originalStatus, currentStatus int
	if err := migrated.db.QueryRowContext(ctx, `SELECT telemetry_frames.payload,
		telemetry_frame_quarantine.reason, telemetry_frame_quarantine.original_delivery_status,
		telemetry_frames.delivery_status
		FROM telemetry_frame_quarantine
		JOIN telemetry_frames USING(seq)
		WHERE seq = ?`, legacyFrameMigrationBatchSize+2).Scan(&quarantinedPayload, &quarantineReason, &originalStatus, &currentStatus); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(quarantinedPayload, []byte{0xff}) {
		t.Fatalf("quarantined payload = %x, want ff", quarantinedPayload)
	}
	if !strings.Contains(quarantineReason, "protobuf decode failed") || originalStatus != int(DeliveryStatusWritten) || currentStatus != int(DeliveryStatusQuarantined) {
		t.Fatalf("quarantine diagnostic = (%q, original=%d, current=%d)", quarantineReason, originalStatus, currentStatus)
	}
}

func TestWALGenerationMigrationQuarantinesMalformedRowsAcrossBatch(t *testing.T) {
	tests := []struct {
		name           string
		corruptSeq     int
		originalStatus DeliveryStatus
	}{
		{name: "before boundary", corruptSeq: 1, originalStatus: DeliveryStatusWritten},
		{name: "within batch", corruptSeq: legacyFrameMigrationBatchSize / 2, originalStatus: DeliveryStatusDelivered},
		{name: "at boundary", corruptSeq: legacyFrameMigrationBatchSize, originalStatus: DeliveryStatusWritten},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), "malformed-legacy.db")
			w, err := New(ctx, dbPath, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			legacyGeneration := w.GenerationID()
			for i := 0; i < legacyFrameMigrationBatchSize; i++ {
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
			if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames
				SET payload = ?, delivery_status = ? WHERE seq = ?`, []byte{0xff}, test.originalStatus, test.corruptSeq); err != nil {
				t.Fatal(err)
			}
			if _, err := w.db.ExecContext(ctx, `DELETE FROM wal_identity_migration`); err != nil {
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
			entries, err := migrated.ReadUndelivered(ctx, legacyFrameMigrationBatchSize)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != legacyFrameMigrationBatchSize-1 {
				t.Fatalf("deliverable frames = %d, want %d", len(entries), legacyFrameMigrationBatchSize-1)
			}
			count, err := migrated.CountUndelivered(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if count != int64(len(entries)) {
				t.Fatalf("undelivered count = %d, want %d", count, len(entries))
			}
			for _, entry := range entries {
				var frame agentv1.TelemetryFrame
				if err := proto.Unmarshal(entry.Payload, &frame); err != nil {
					t.Fatal(err)
				}
				if frame.GetWalId() != legacyGeneration {
					t.Fatalf("valid frame %d WAL ID = %q, want %q", entry.ID, frame.GetWalId(), legacyGeneration)
				}
				if _, err := migrated.MarkDelivered(ctx, uint64(entry.ID)); err != nil {
					t.Fatal(err)
				}
			}

			var payload []byte
			var reason string
			var originalStatus, currentStatus int
			if err := migrated.db.QueryRowContext(ctx, `SELECT telemetry_frames.payload,
				telemetry_frame_quarantine.reason, telemetry_frame_quarantine.original_delivery_status,
				telemetry_frames.delivery_status
				FROM telemetry_frame_quarantine JOIN telemetry_frames USING(seq)
				WHERE seq = ?`, test.corruptSeq).Scan(&payload, &reason, &originalStatus, &currentStatus); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(payload, []byte{0xff}) {
				t.Fatalf("quarantined payload = %x, want ff", payload)
			}
			if !strings.Contains(reason, "protobuf decode failed") || originalStatus != int(test.originalStatus) || currentStatus != int(DeliveryStatusQuarantined) {
				t.Fatalf("quarantine diagnostic = (%q, original=%d, current=%d), want original %d and quarantine status", reason, originalStatus, currentStatus, test.originalStatus)
			}
			var lastSeq, completed int64
			if err := migrated.db.QueryRowContext(ctx, `SELECT last_seq, completed
				FROM wal_identity_migration WHERE id = 1`).Scan(&lastSeq, &completed); err != nil {
				t.Fatal(err)
			}
			if lastSeq != legacyFrameMigrationBatchSize || completed != 1 {
				t.Fatalf("migration completion = (%d, %d), want (%d, 1)", lastSeq, completed, legacyFrameMigrationBatchSize)
			}

			if err := migrated.CleanupDelivered(ctx, 0); err != nil {
				t.Fatal(err)
			}
			var remaining int
			if err := migrated.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frames`).Scan(&remaining); err != nil {
				t.Fatal(err)
			}
			if remaining != 1 {
				t.Fatalf("rows after delivered cleanup = %d, want only quarantined row", remaining)
			}
			deleted, err := migrated.CleanupQuarantined(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			if deleted != 1 {
				t.Fatalf("explicit quarantine cleanup deleted %d rows, want 1", deleted)
			}
		})
	}
}

func TestWALGenerationMigrationQuarantineAndProgressRollbackTogether(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "quarantine-rollback.db")
	w, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	legacyGeneration := w.GenerationID()
	legacyPayload, err := proto.Marshal(&agentv1.TelemetryFrame{RawMavlink: []byte("valid")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(ctx, &agentv1.TelemetryFrame{}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(ctx, &agentv1.TelemetryFrame{}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET payload = ?`, legacyPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET payload = ? WHERE seq = 2`, []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `DELETE FROM wal_identity_migration`); err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `CREATE TRIGGER fail_quarantine
		BEFORE INSERT ON telemetry_frame_quarantine BEGIN SELECT RAISE(ABORT, 'forced quarantine failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := New(ctx, dbPath, 0, 0); err == nil || !strings.Contains(err.Error(), "forced quarantine failure") {
		t.Fatalf("migration error = %v, want forced quarantine failure", err)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureDB(db); err != nil {
		t.Fatal(err)
	}
	var lastSeq, completed, quarantineCount int
	if err := db.QueryRowContext(ctx, `SELECT last_seq, completed FROM wal_identity_migration WHERE id = 1`).Scan(&lastSeq, &completed); err != nil {
		t.Fatal(err)
	}
	if lastSeq != 0 || completed != 0 {
		t.Fatalf("rolled-back migration state = (%d, %d), want (0, 0)", lastSeq, completed)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frame_quarantine`).Scan(&quarantineCount); err != nil {
		t.Fatal(err)
	}
	if quarantineCount != 0 {
		t.Fatalf("rolled-back quarantine count = %d, want 0", quarantineCount)
	}
	var validPayload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM telemetry_frames WHERE seq = 1`).Scan(&validPayload); err != nil {
		t.Fatal(err)
	}
	var validFrame agentv1.TelemetryFrame
	if err := proto.Unmarshal(validPayload, &validFrame); err != nil {
		t.Fatal(err)
	}
	if validFrame.GetWalId() != "" {
		t.Fatalf("rolled-back valid frame WAL ID = %q, want empty", validFrame.GetWalId())
	}
	if _, err := db.ExecContext(ctx, `DROP TRIGGER fail_quarantine`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recovered.Close(); err != nil {
			t.Errorf("close recovered WAL: %v", err)
		}
	})
	entries, err := recovered.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("recovered deliverable frames = %d, want 1", len(entries))
	}
	if err := proto.Unmarshal(entries[0].Payload, &validFrame); err != nil {
		t.Fatal(err)
	}
	if validFrame.GetWalId() != legacyGeneration {
		t.Fatalf("recovered valid frame WAL ID = %q, want %q", validFrame.GetWalId(), legacyGeneration)
	}
}

func TestWALCleanupQuarantinedRetainsNewestAcrossBoundedBatches(t *testing.T) {
	ctx := context.Background()
	w := mustOpenWALWithoutWriter(t, filepath.Join(t.TempDir(), "quarantine-cleanup.db"))
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})

	frameCount := quarantineCleanupBatchSize*2 + 1
	for i := 1; i <= frameCount; i++ {
		result, err := w.db.ExecContext(ctx, `INSERT INTO telemetry_frames(
			created_at, payload, delivery_status) VALUES(?, ?, ?)`, i, []byte{0xff}, DeliveryStatusQuarantined)
		if err != nil {
			t.Fatal(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.db.ExecContext(ctx, `INSERT INTO telemetry_frame_quarantine(
			seq, quarantined_at, reason, original_delivery_status) VALUES(?, ?, ?, ?)`,
			seq, i, "test corruption", DeliveryStatusWritten); err != nil {
			t.Fatal(err)
		}
	}

	deleted, err := w.CleanupQuarantined(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != int64(frameCount-1) {
		t.Fatalf("deleted quarantined frames = %d, want %d", deleted, frameCount-1)
	}
	var frameRows, diagnosticRows int
	var retainedSeq int64
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*), MAX(seq) FROM telemetry_frames`).Scan(&frameRows, &retainedSeq); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frame_quarantine`).Scan(&diagnosticRows); err != nil {
		t.Fatal(err)
	}
	if frameRows != 1 || diagnosticRows != 1 || retainedSeq != int64(frameCount) {
		t.Fatalf("retained frames=%d diagnostics=%d seq=%d, want 1,1,%d", frameRows, diagnosticRows, retainedSeq, frameCount)
	}
}

func TestWALGenerationMigrationReopensForRowsBeyondCompletedCursor(t *testing.T) {
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
		t.Fatalf("reopen migration for new corrupt tail: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	if reopened.GenerationID() == previousGeneration {
		t.Fatalf("reopened WAL reused generation ID %q", previousGeneration)
	}
	var status, quarantineCount int
	if err := reopened.db.QueryRowContext(ctx, `SELECT delivery_status FROM telemetry_frames WHERE seq = 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frame_quarantine WHERE seq = 1`).Scan(&quarantineCount); err != nil {
		t.Fatal(err)
	}
	if status != int(DeliveryStatusQuarantined) || quarantineCount != 1 {
		t.Fatalf("new corrupt tail status=%d quarantine rows=%d, want %d,1",
			status, quarantineCount, DeliveryStatusQuarantined)
	}
}

func TestWALGenerationMigrationStampsRollbackTailWithPersistedGeneration(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "rollback-tail.db")
	w, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rollbackGeneration := w.GenerationID()
	if _, err := w.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("native-before-rollback")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureDB(db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	legacyPayload, err := proto.Marshal(&agentv1.TelemetryFrame{RawMavlink: []byte("written-by-rollback")})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO telemetry_frames(created_at, payload, delivery_status)
		VALUES(?, ?, ?)`, time.Now().UnixNano(), legacyPayload, DeliveryStatusWritten); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.GenerationID() == rollbackGeneration {
		t.Fatalf("post-rollback WAL reused generation ID %q", rollbackGeneration)
	}
	var storedPayload []byte
	if err := reopened.db.QueryRowContext(ctx, `SELECT payload FROM telemetry_frames WHERE seq = 2`).Scan(&storedPayload); err != nil {
		t.Fatal(err)
	}
	var stored agentv1.TelemetryFrame
	if err := proto.Unmarshal(storedPayload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.GetWalId() != rollbackGeneration {
		t.Fatalf("rollback tail WAL ID = %q, want persisted generation %q", stored.GetWalId(), rollbackGeneration)
	}
	var migrationGeneration string
	var lastSeq int64
	var completed int
	if err := reopened.db.QueryRowContext(ctx, `SELECT legacy_generation_id, last_seq, completed
		FROM wal_identity_migration WHERE id = 1`).Scan(&migrationGeneration, &lastSeq, &completed); err != nil {
		t.Fatal(err)
	}
	if migrationGeneration != rollbackGeneration || lastSeq != 2 || completed != 1 {
		t.Fatalf("rollback migration state = (%q,%d,%d), want (%q,2,1)",
			migrationGeneration, lastSeq, completed, rollbackGeneration)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := New(ctx, dbPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := again.Close(); err != nil {
			t.Errorf("close second reopened WAL: %v", err)
		}
	})
	if err := again.db.QueryRowContext(ctx, `SELECT payload FROM telemetry_frames WHERE seq = 2`).Scan(&storedPayload); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(storedPayload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.GetWalId() != rollbackGeneration {
		t.Fatalf("rollback tail WAL ID after ACK-boundary restart = %q, want %q",
			stored.GetWalId(), rollbackGeneration)
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
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})

	// Check if file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("WAL file was not created at %s", dbPath)
	}
}

func TestWAL_AppendAndRead(t *testing.T) {
	w := mustNewWAL(t)
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
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
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
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

func TestWALAppendAsyncRejectsPoisonFrameWithoutBlockingValidBatches(t *testing.T) {
	ctx := context.Background()
	w, err := New(ctx, filepath.Join(t.TempDir(), "async-poison.db"), 2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})

	first := &agentv1.TelemetryFrame{
		RawMavlink: []byte("before"),
		Fields:     map[string]string{"source": "original"},
	}
	if err := w.AppendAsync(ctx, first); err != nil {
		t.Fatal(err)
	}
	// Mutation after acceptance must not alter or poison the queued copy.
	first.RawMavlink = []byte("mutated")
	first.Fields["source"] = string([]byte{0xff})

	poison := &agentv1.TelemetryFrame{Fields: map[string]string{"invalid": string([]byte{0xff})}}
	if err := w.AppendAsync(ctx, poison); err == nil {
		t.Fatal("AppendAsync accepted a frame containing invalid UTF-8")
	}
	if err := w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("after")}); err != nil {
		t.Fatalf("append after rejected poison frame: %v", err)
	}
	select {
	case <-w.signalChan:
	case <-time.After(time.Second):
		t.Fatal("valid batch behind poison frame did not flush")
	}

	// A second batch proves the writer did not enter a retry loop or stop
	// consuming queue capacity after rejecting the poison frame.
	for _, payload := range []string{"later-1", "later-2"} {
		if err := w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte(payload)}); err != nil {
			t.Fatalf("append %q after rejected poison frame: %v", payload, err)
		}
	}
	select {
	case <-w.signalChan:
	case <-time.After(time.Second):
		t.Fatal("second valid batch did not flush")
	}

	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("valid entries after poison rejection = %d, want 4", len(entries))
	}
	want := []string{"before", "after", "later-1", "later-2"}
	for i, entry := range entries {
		var frame agentv1.TelemetryFrame
		if err := proto.Unmarshal(entry.Payload, &frame); err != nil {
			t.Fatal(err)
		}
		if got := string(frame.GetRawMavlink()); got != want[i] {
			t.Fatalf("entry %d payload = %q, want %q", i, got, want[i])
		}
		if frame.GetWalId() != w.GenerationID() {
			t.Fatalf("entry %d WAL ID = %q, want %q", i, frame.GetWalId(), w.GenerationID())
		}
	}
}

func TestWALClosePathsPreserveValidFramesAroundRejectedPoison(t *testing.T) {
	for _, test := range []struct {
		name  string
		close func(*WAL) error
	}{
		{name: "Close", close: func(w *WAL) error { return w.Close() }},
		{name: "CloseContext", close: func(w *WAL) error {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			return w.CloseContext(ctx)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), "close-poison.db")
			w, err := New(ctx, dbPath, 100, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := w.Close(); err != nil {
					t.Errorf("close original WAL: %v", err)
				}
			})

			if err := w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("before-close")}); err != nil {
				t.Fatal(err)
			}
			poison := &agentv1.TelemetryFrame{Fields: map[string]string{"invalid": string([]byte{0xff})}}
			if err := w.AppendAsync(ctx, poison); err == nil {
				t.Fatal("AppendAsync accepted a frame containing invalid UTF-8")
			}
			if err := w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("after-close")}); err != nil {
				t.Fatal(err)
			}
			if err := test.close(w); err != nil {
				t.Fatal(err)
			}

			reopened, err := New(ctx, dbPath, 100, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("close reopened WAL: %v", err)
				}
			})
			select {
			case <-reopened.signalChan:
			case <-time.After(time.Second):
				t.Fatal("reopened WAL did not drain close-path spool")
			}
			entries, err := reopened.ReadUndelivered(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 2 {
				t.Fatalf("valid close-path entries = %d, want 2", len(entries))
			}
			for i, want := range []string{"before-close", "after-close"} {
				var frame agentv1.TelemetryFrame
				if err := proto.Unmarshal(entries[i].Payload, &frame); err != nil {
					t.Fatal(err)
				}
				if got := string(frame.GetRawMavlink()); got != want {
					t.Fatalf("entry %d payload = %q, want %q", i, got, want)
				}
			}
		})
	}
}

func TestWAL_SpoolAndDrain(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_spool.db")
	w, err := New(context.Background(), dbPath, 2, time.Hour)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
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
	var importCount int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports`).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if importCount != 0 {
		t.Fatalf("completed spool import markers = %d, want 0", importCount)
	}
}

func TestWALDrainQuarantinesMalformedSpoolAndContinues(t *testing.T) {
	tests := []struct {
		name         string
		corruptBytes func(t *testing.T) []byte
	}{
		{
			name: "current format",
			corruptBytes: func(t *testing.T) []byte {
				t.Helper()
				var length [4]byte
				binary.LittleEndian.PutUint32(length[:], 1)
				return bytes.Join([][]byte{[]byte(spoolFileMagic), []byte(uuid.NewString()), length[:], []byte{0xff}}, nil)
			},
		},
		{
			name: "legacy format",
			corruptBytes: func(t *testing.T) []byte {
				t.Helper()
				return []byte{0x01, 0x00, 0x00}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), "malformed-spool.db")
			w := mustOpenWALWithoutWriter(t, dbPath)
			corruptPath := filepath.Join(w.spoolDir, "000-corrupt.batch")
			corruptBytes := test.corruptBytes(t)
			if err := os.WriteFile(corruptPath, corruptBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			validPath, err := w.spoolBatch([]*agentv1.TelemetryFrame{{RawMavlink: []byte("valid-after-corruption")}})
			if err != nil {
				t.Fatal(err)
			}
			validSortedPath := filepath.Join(w.spoolDir, "999-valid.batch")
			if err := os.Rename(validPath, validSortedPath); err != nil {
				t.Fatal(err)
			}

			drainErr := w.drainSpool()
			if drainErr == nil || !strings.Contains(drainErr.Error(), "quarantined malformed spool file") {
				t.Fatalf("drain error = %v, want quarantine diagnostic", drainErr)
			}
			select {
			case <-w.signalChan:
			default:
				t.Fatal("valid batch imported without signaling data")
			}
			entries, err := w.ReadUndelivered(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Fatalf("valid imported rows = %d, want 1", len(entries))
			}
			var frame agentv1.TelemetryFrame
			if err := proto.Unmarshal(entries[0].Payload, &frame); err != nil {
				t.Fatal(err)
			}
			if string(frame.GetRawMavlink()) != "valid-after-corruption" {
				t.Fatalf("imported payload = %q", frame.GetRawMavlink())
			}
			if _, err := os.Stat(corruptPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("active corrupt spool still exists: %v", err)
			}
			quarantineEntries, err := os.ReadDir(w.spoolQuarantineDir)
			if err != nil {
				t.Fatal(err)
			}
			if len(quarantineEntries) != 1 {
				t.Fatalf("quarantine artifacts = %d, want 1", len(quarantineEntries))
			}
			artifactName := quarantineEntries[0].Name()
			if !strings.Contains(artifactName, "-"+filepath.Base(corruptPath)+".corrupt") {
				t.Fatalf("quarantine artifact name = %q", artifactName)
			}
			artifactPath := filepath.Join(w.spoolQuarantineDir, artifactName)
			preserved, err := os.ReadFile(artifactPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(preserved, corruptBytes) {
				t.Fatalf("quarantined bytes changed: got %x want %x", preserved, corruptBytes)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			reopened := mustOpenWALWithoutWriter(t, dbPath)
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("close reopened WAL: %v", err)
				}
			})
			if err := reopened.drainSpool(); err != nil {
				t.Fatalf("restart retried quarantined spool: %v", err)
			}
			entries, err = reopened.ReadUndelivered(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Fatalf("restart imported rows = %d, want stable 1", len(entries))
			}
			preserved, err = os.ReadFile(artifactPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(preserved, corruptBytes) {
				t.Fatalf("restart changed quarantined bytes: got %x want %x", preserved, corruptBytes)
			}
			deleted, err := reopened.CleanupSpoolQuarantine(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
			if deleted != 1 {
				t.Fatalf("explicit quarantine cleanup deleted %d files, want 1", deleted)
			}
		})
	}
}

func TestWALDrainContinuesWhenSpoolQuarantineMoveFails(t *testing.T) {
	ctx := context.Background()
	w := mustOpenWALWithoutWriter(t, filepath.Join(t.TempDir(), "quarantine-move-failure.db"))
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	corruptPath := filepath.Join(w.spoolDir, "000-corrupt.batch")
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], 1)
	corruptBytes := bytes.Join([][]byte{[]byte(spoolFileMagic), []byte(uuid.NewString()), length[:], []byte{0xff}}, nil)
	if err := os.WriteFile(corruptPath, corruptBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	validPath, err := w.spoolBatch([]*agentv1.TelemetryFrame{{RawMavlink: []byte("later-valid")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(validPath, filepath.Join(w.spoolDir, "999-valid.batch")); err != nil {
		t.Fatal(err)
	}
	moveErr := errors.New("simulated quarantine move failure")
	w.renameFile = func(source, destination string) error {
		if source == corruptPath {
			return moveErr
		}
		return os.Rename(source, destination)
	}

	if err := w.drainSpool(); !errors.Is(err, moveErr) {
		t.Fatalf("drain error = %v, want %v", err, moveErr)
	}
	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("valid rows after quarantine move failure = %d, want 1", len(entries))
	}
	select {
	case <-w.signalChan:
	default:
		t.Fatal("valid batch imported without signaling data")
	}
	if preserved, err := os.ReadFile(corruptPath); err != nil || !bytes.Equal(preserved, corruptBytes) {
		t.Fatalf("active corrupt spool after failed move = %x, %v", preserved, err)
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

func TestWALSpoolImportIsIdempotentAfterCleanupFailureAndRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "spool-retry.db")
	w := mustOpenWALWithoutWriter(t, dbPath)
	firstGeneration := w.GenerationID()
	spoolPath, err := w.spoolBatch([]*agentv1.TelemetryFrame{
		{RawMavlink: []byte("one")},
		{RawMavlink: []byte("two")},
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupErr := errors.New("simulated cleanup failure")
	w.removeFile = func(path string) error {
		if path == spoolPath {
			return cleanupErr
		}
		return os.Remove(path)
	}
	if err := w.drainSpool(); !errors.Is(err, cleanupErr) {
		t.Fatalf("first drain error = %v, want %v", err, cleanupErr)
	}
	before, err := w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 2 {
		t.Fatalf("rows after committed import = %d, want 2", len(before))
	}
	var importCount int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports`).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if importCount != 1 {
		t.Fatalf("spool import markers = %d, want 1", importCount)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := mustOpenWALWithoutWriter(t, dbPath)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	if reopened.GenerationID() == firstGeneration {
		t.Fatalf("reopened WAL reused generation ID %q", firstGeneration)
	}
	if err := reopened.drainSpool(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(spoolPath); !os.IsNotExist(err) {
		t.Fatalf("spool file after cleanup retry: %v", err)
	}
	after, err := reopened.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("rows after restart retry = %d, want %d", len(after), len(before))
	}
	for i := range before {
		if after[i].ID != before[i].ID || !bytes.Equal(after[i].Payload, before[i].Payload) {
			t.Fatalf("cursor %d changed after spool retry", i)
		}
		var frame agentv1.TelemetryFrame
		if err := proto.Unmarshal(after[i].Payload, &frame); err != nil {
			t.Fatal(err)
		}
		if frame.GetWalId() != firstGeneration {
			t.Fatalf("frame %d WAL ID = %q, want original import generation %q", after[i].ID, frame.GetWalId(), firstGeneration)
		}
	}
	if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports`).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if importCount != 0 {
		t.Fatalf("spool markers after cleanup retry = %d, want 0", importCount)
	}
}

func TestWALSpoolImportMarkerDeletionFollowsDurableUnlink(t *testing.T) {
	ctx := context.Background()
	w := mustOpenWALWithoutWriter(t, filepath.Join(t.TempDir(), "spool-unlink-order.db"))
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})

	spoolPath, err := w.spoolBatch([]*agentv1.TelemetryFrame{{RawMavlink: []byte("ordered")}})
	if err != nil {
		t.Fatal(err)
	}
	spoolID, _, err := readSpoolFile(spoolPath)
	if err != nil {
		t.Fatal(err)
	}

	markerPresentAtUnlinkSync := false
	w.syncDir = func(path string) error {
		if path == w.spoolDir && !markerPresentAtUnlinkSync {
			var count int
			if err := w.db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM spool_imports WHERE spool_id = ?`, spoolID).Scan(&count); err != nil {
				return err
			}
			markerPresentAtUnlinkSync = count == 1
		}
		return syncDirectory(path)
	}

	if err := w.drainSpool(); err != nil {
		t.Fatal(err)
	}
	if !markerPresentAtUnlinkSync {
		t.Fatal("spool import marker was not retained through the unlink directory sync")
	}
	var importCount int
	if err := w.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM spool_imports WHERE spool_id = ?`, spoolID).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if importCount != 0 {
		t.Fatalf("spool import markers after durable unlink = %d, want 0", importCount)
	}
}

func TestWALSpoolImportMarkerSurvivesUnlinkSyncFailureAndResurrection(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "spool-unlink-sync-retry.db")
	w := mustOpenWALWithoutWriter(t, dbPath)
	spoolPath, err := w.spoolBatch([]*agentv1.TelemetryFrame{{RawMavlink: []byte("once")}})
	if err != nil {
		t.Fatal(err)
	}
	spoolBytes, err := os.ReadFile(spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	spoolID, _, err := readSpoolFile(spoolPath)
	if err != nil {
		t.Fatal(err)
	}

	syncErr := errors.New("simulated spool directory sync failure")
	w.syncDir = func(path string) error {
		if path == w.spoolDir {
			return syncErr
		}
		return syncDirectory(path)
	}
	if err := w.drainSpool(); !errors.Is(err, syncErr) {
		t.Fatalf("drain error = %v, want %v", err, syncErr)
	}
	if _, err := os.Stat(spoolPath); !os.IsNotExist(err) {
		t.Fatalf("spool path after successful unlink = %v, want not exist", err)
	}
	var frameCount, importCount int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frames`).Scan(&frameCount); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM spool_imports WHERE spool_id = ?`, spoolID).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if frameCount != 1 || importCount != 1 {
		t.Fatalf("after unlink sync failure frames=%d markers=%d, want 1,1", frameCount, importCount)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Model filesystem recovery resurrecting the unlink whose directory entry
	// was never durably synced before the process stopped.
	if err := os.WriteFile(spoolPath, spoolBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened := mustOpenWALWithoutWriter(t, dbPath)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	if err := reopened.drainSpool(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frames`).Scan(&frameCount); err != nil {
		t.Fatal(err)
	}
	if err := reopened.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM spool_imports WHERE spool_id = ?`, spoolID).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if frameCount != 1 || importCount != 0 {
		t.Fatalf("after resurrected spool retry frames=%d markers=%d, want 1,0", frameCount, importCount)
	}
}

func TestWALFinalizedSpoolSyncRetryDoesNotCreateDuplicate(t *testing.T) {
	ctx := context.Background()
	w := mustOpenWALWithoutWriter(t, filepath.Join(t.TempDir(), "spool-finalize-sync.db"))
	w.batchChan = make(chan *agentv1.TelemetryFrame, 2)
	w.writerDone = make(chan struct{})
	w.batchSize = 1
	w.batchTimeout = time.Hour

	var renameCalls atomic.Int32
	w.renameFile = func(oldPath, newPath string) error {
		renameCalls.Add(1)
		return os.Rename(oldPath, newPath)
	}
	var syncsWithBatch atomic.Int32
	syncErr := errors.New("simulated finalized spool directory sync failure")
	w.syncDir = func(path string) error {
		if path == w.spoolDir {
			entries, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if !entry.IsDir() && filepath.Ext(entry.Name()) == ".batch" {
					if syncsWithBatch.Add(1) == 1 {
						return syncErr
					}
					break
				}
			}
		}
		return syncDirectory(path)
	}
	go w.runBatchWriter(ctx)
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})

	if err := w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("single-spool")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.signalChan:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for spool directory sync retry")
	}
	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("persisted entries after finalized spool retry = %d, want 1", len(entries))
	}
	if got := renameCalls.Load(); got != 1 {
		t.Fatalf("final spool renames = %d, want 1", got)
	}
	if got := syncsWithBatch.Load(); got < 2 {
		t.Fatalf("spool directory sync attempts with final batch = %d, want at least 2", got)
	}
}

func TestWALSpoolImportMarkerRollsBackWithFailedFrameInsert(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "spool-atomicity.db")
	w := mustOpenWALWithoutWriter(t, dbPath)
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	spoolPath, err := w.spoolBatch([]*agentv1.TelemetryFrame{{RawMavlink: []byte("retry-me")}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.db.ExecContext(ctx, `CREATE TRIGGER fail_spool_insert
		BEFORE INSERT ON telemetry_frames BEGIN SELECT RAISE(ABORT, 'forced insert failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := w.drainSpool(); err == nil {
		t.Fatal("drain with failing insert succeeded")
	}
	var frameCount, importCount int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frames`).Scan(&frameCount); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports`).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if frameCount != 0 || importCount != 0 {
		t.Fatalf("failed import persisted frames=%d markers=%d, want 0,0", frameCount, importCount)
	}
	if _, err := os.Stat(spoolPath); err != nil {
		t.Fatalf("spool file lost after failed import: %v", err)
	}
	if _, err := w.db.ExecContext(ctx, `DROP TRIGGER fail_spool_insert`); err != nil {
		t.Fatal(err)
	}
	if err := w.drainSpool(); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frames`).Scan(&frameCount); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports`).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if frameCount != 1 || importCount != 0 {
		t.Fatalf("recovered import persisted frames=%d markers=%d, want 1,0", frameCount, importCount)
	}
}

func TestWALDrainsLegacySpoolFiles(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-spool.db")
	w := mustOpenWALWithoutWriter(t, dbPath)
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	spoolPath := filepath.Join(w.spoolDir, "00000000000000000001-000001.batch")
	frames := []*agentv1.TelemetryFrame{{RawMavlink: []byte("legacy-spool")}}
	writeLegacySpoolFile(t, spoolPath, frames)
	spoolID, err := readSpoolIdentityFromPath(spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(spoolID, "legacy:") {
		t.Fatalf("legacy spool identity = %q", spoolID)
	}
	if err := w.drainSpool(); err != nil {
		t.Fatal(err)
	}
	var frameCount, importCount int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frames`).Scan(&frameCount); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports`).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if frameCount != 1 || importCount != 0 {
		t.Fatalf("legacy spool import persisted frames=%d markers=%d, want 1,0", frameCount, importCount)
	}
}

func TestWALPrunesOrphanedSpoolImportsAfterRestartInBoundedBatches(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "orphaned-spool-imports.db")
	w := mustOpenWALWithoutWriter(t, dbPath)
	spoolPath, err := w.spoolBatch([]*agentv1.TelemetryFrame{{RawMavlink: []byte("committed-before-crash")}})
	if err != nil {
		t.Fatal(err)
	}
	spoolID, frames, err := readSpoolFile(spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	if imported, err := w.appendSpoolBatch(ctx, spoolID, frames); err != nil || !imported {
		t.Fatalf("appendSpoolBatch() = %v, %v", imported, err)
	}
	if err := os.Remove(spoolPath); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < spoolImportCleanupBatchSize*2; i++ {
		if _, err := w.db.ExecContext(ctx, `INSERT INTO spool_imports(spool_id, imported_at) VALUES(?, ?)`, uuid.NewString(), time.Now().UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := mustOpenWALWithoutWriter(t, dbPath)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	if err := reopened.drainSpool(); err != nil {
		t.Fatal(err)
	}
	var frameCount, importCount int
	if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frames`).Scan(&frameCount); err != nil {
		t.Fatal(err)
	}
	if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports`).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if frameCount != 1 || importCount != 0 {
		t.Fatalf("orphan cleanup retained frames=%d markers=%d, want 1,0", frameCount, importCount)
	}
}

func TestWALSpoolImportCleanupPreservesMarkersWithLiveFiles(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "live-spool-import.db")
	w := mustOpenWALWithoutWriter(t, dbPath)
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	spoolPath, err := w.spoolBatch([]*agentv1.TelemetryFrame{{RawMavlink: []byte("live")}})
	if err != nil {
		t.Fatal(err)
	}
	spoolID, frames, err := readSpoolFile(spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	if imported, err := w.appendSpoolBatch(ctx, spoolID, frames); err != nil || !imported {
		t.Fatalf("appendSpoolBatch() = %v, %v", imported, err)
	}
	if err := w.CleanupDelivered(ctx, 0); err != nil {
		t.Fatal(err)
	}
	var importCount int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports WHERE spool_id = ?`, spoolID).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if importCount != 1 {
		t.Fatalf("live spool marker count = %d, want 1", importCount)
	}
	if err := w.drainSpool(); err != nil {
		t.Fatal(err)
	}
	var frameCount int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM telemetry_frames`).Scan(&frameCount); err != nil {
		t.Fatal(err)
	}
	if frameCount != 1 {
		t.Fatalf("live spool re-imported rows = %d, want 1", frameCount)
	}
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports`).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if importCount != 0 {
		t.Fatalf("completed live spool markers = %d, want 0", importCount)
	}
}

func TestWALSpoolIdentityDoesNotReuseProcessSequence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "spool-identities.db")
	w := mustOpenWALWithoutWriter(t, dbPath)
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	frame := []*agentv1.TelemetryFrame{{RawMavlink: []byte("same")}}
	firstPath, err := w.spoolBatch(frame)
	if err != nil {
		t.Fatal(err)
	}
	w.spoolSeq = 0
	secondPath, err := w.spoolBatch(frame)
	if err != nil {
		t.Fatal(err)
	}
	firstID, _, err := readSpoolFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	secondID, _, err := readSpoolFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if firstID == secondID {
		t.Fatalf("recreated spool sequence reused identity %q", firstID)
	}
	if _, err := uuid.Parse(firstID); err != nil {
		t.Fatalf("first spool identity %q is not a UUID: %v", firstID, err)
	}
	if _, err := uuid.Parse(secondID); err != nil {
		t.Fatalf("second spool identity %q is not a UUID: %v", secondID, err)
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

func TestApplyTelemetryAckPermanentlyQuarantinesEvidenceAndIsIdempotent(t *testing.T) {
	w := mustNewWAL(t)
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()
	ctx := context.Background()
	frame := &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 1234, RawMavlink: []byte("evidence")}
	id, err := w.Append(ctx, frame)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := w.MarkPendingBatch(ctx, []uint64{uint64(id)}); err != nil || rows != 1 {
		t.Fatalf("MarkPendingBatch() = %d, %v", rows, err)
	}
	frameID := "7:agent-1:1234:1"
	result, err := w.ApplyTelemetryAck(ctx, uint64(id), frameID, TelemetryAckPermanentReject, "normalization rejected payload")
	if err != nil || !result.Changed || !result.CorrelatedByFrameID || result.PreviousStatus != DeliveryStatusPending {
		t.Fatalf("ApplyTelemetryAck() = %#v, %v", result, err)
	}
	var status DeliveryStatus
	var reason string
	var original DeliveryStatus
	if err = w.db.QueryRowContext(ctx, `SELECT delivery_status, reason, original_delivery_status FROM telemetry_frames JOIN telemetry_frame_quarantine USING(seq) WHERE seq = ?`, id).Scan(&status, &reason, &original); err != nil {
		t.Fatal(err)
	}
	if status != DeliveryStatusQuarantined || reason != "normalization rejected payload" || original != DeliveryStatusPending {
		t.Fatalf("quarantine = status %d reason %q original %d", status, reason, original)
	}
	result, err = w.ApplyTelemetryAck(ctx, uint64(id), frameID, TelemetryAckPermanentReject, "normalization rejected payload")
	if err != nil || result.Changed {
		t.Fatalf("duplicate permanent ACK = %#v, %v", result, err)
	}
	if _, err = w.ApplyTelemetryAck(ctx, uint64(id), frameID, TelemetryAckDelivered, ""); !errors.Is(err, ErrTelemetryAckConflict) {
		t.Fatalf("contradictory delivered ACK error = %v", err)
	}
}

func TestResetPendingUsesDurableSendEpochInsteadOfCaptureAge(t *testing.T) {
	w := mustNewWAL(t)
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()
	ctx := context.Background()
	id, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 1234})
	if err != nil {
		t.Fatal(err)
	}
	capturedAt := time.Now().Add(-24 * time.Hour).UnixNano()
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET created_at = ? WHERE seq = ?`, capturedAt, id); err != nil {
		t.Fatal(err)
	}
	if rows, err := w.MarkPendingBatch(ctx, []uint64{uint64(id)}); err != nil || rows != 1 {
		t.Fatalf("MarkPendingBatch() = %d, %v", rows, err)
	}
	var pendingSince int64
	if err := w.db.QueryRowContext(ctx, `SELECT pending_since_unix_ns FROM telemetry_frames WHERE seq = ?`, id).Scan(&pendingSince); err != nil {
		t.Fatal(err)
	}
	if pendingSince <= capturedAt {
		t.Fatalf("pending epoch %d did not advance beyond capture %d", pendingSince, capturedAt)
	}
	if rows, err := w.ResetPending(ctx, 5*time.Minute); err != nil || rows != 0 {
		t.Fatalf("fresh replay send reset = %d, %v", rows, err)
	}
	result, err := w.ApplyTelemetryAck(ctx, uint64(id), "7:agent-1:1234:1", TelemetryAckDelivered, "")
	if err != nil || !result.Changed {
		t.Fatalf("exact ACK after cleanup tick = %#v, %v", result, err)
	}
}

func TestRefreshPendingBatchRenewsOnlyLivePendingRows(t *testing.T) {
	w := mustNewWAL(t)
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()
	ctx := context.Background()
	ids := make([]uint64, 2)
	for index := range ids {
		id, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: int64(index + 1)})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = uint64(id)
	}
	if rows, err := w.MarkPendingBatch(ctx, ids); err != nil || rows != 2 {
		t.Fatalf("MarkPendingBatch() = %d, %v", rows, err)
	}
	oldEpoch := time.Now().Add(-time.Hour).UnixNano()
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET pending_since_unix_ns = ? WHERE seq IN (?, ?)`, oldEpoch, ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ApplyTelemetryAck(ctx, ids[0], "", TelemetryAckDelivered, ""); err != nil {
		t.Fatal(err)
	}
	if rows, err := w.RefreshPendingBatch(ctx, ids); err != nil || rows != 1 {
		t.Fatalf("RefreshPendingBatch() = %d, %v", rows, err)
	}
	var deliveredStatus DeliveryStatus
	var pendingStatus DeliveryStatus
	var refreshedEpoch int64
	if err := w.db.QueryRowContext(ctx, `SELECT delivery_status FROM telemetry_frames WHERE seq = ?`, ids[0]).Scan(&deliveredStatus); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRowContext(ctx, `SELECT delivery_status, pending_since_unix_ns FROM telemetry_frames WHERE seq = ?`, ids[1]).Scan(&pendingStatus, &refreshedEpoch); err != nil {
		t.Fatal(err)
	}
	if deliveredStatus != DeliveryStatusDelivered || pendingStatus != DeliveryStatusPending || refreshedEpoch <= oldEpoch {
		t.Fatalf("refresh states = delivered %d pending %d epoch %d, want delivered/pending and epoch > %d", deliveredStatus, pendingStatus, refreshedEpoch, oldEpoch)
	}
}

func TestResetPendingAndTeardownRequeueClearEpochWithoutRegressingTerminalACKs(t *testing.T) {
	w := mustNewWAL(t)
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()
	ctx := context.Background()
	ids := make([]uint64, 3)
	for index := range ids {
		id, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: int64(index + 1)})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = uint64(id)
	}
	if rows, err := w.MarkPendingBatch(ctx, ids); err != nil || rows != 3 {
		t.Fatalf("MarkPendingBatch() = %d, %v", rows, err)
	}
	if _, err := w.ApplyTelemetryAck(ctx, ids[0], "", TelemetryAckDelivered, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ApplyTelemetryAck(ctx, ids[1], "", TelemetryAckPermanentReject, "bad frame"); err != nil {
		t.Fatal(err)
	}
	if rows, err := w.RequeueAllPending(ctx); err != nil || rows != 1 {
		t.Fatalf("RequeueAllPending() = %d, %v", rows, err)
	}

	var deliveredStatus, quarantinedStatus, writtenStatus DeliveryStatus
	var writtenPendingSince sql.NullInt64
	if err := w.db.QueryRowContext(ctx, `SELECT delivery_status FROM telemetry_frames WHERE seq = ?`, ids[0]).Scan(&deliveredStatus); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRowContext(ctx, `SELECT delivery_status FROM telemetry_frames WHERE seq = ?`, ids[1]).Scan(&quarantinedStatus); err != nil {
		t.Fatal(err)
	}
	if err := w.db.QueryRowContext(ctx, `SELECT delivery_status, pending_since_unix_ns FROM telemetry_frames WHERE seq = ?`, ids[2]).Scan(&writtenStatus, &writtenPendingSince); err != nil {
		t.Fatal(err)
	}
	if deliveredStatus != DeliveryStatusDelivered || quarantinedStatus != DeliveryStatusQuarantined || writtenStatus != DeliveryStatusWritten || writtenPendingSince.Valid {
		t.Fatalf("states after requeue = delivered %d quarantined %d written %d pending_since %+v", deliveredStatus, quarantinedStatus, writtenStatus, writtenPendingSince)
	}

	if rows, err := w.MarkPendingBatch(ctx, []uint64{ids[2]}); err != nil || rows != 1 {
		t.Fatalf("second MarkPendingBatch() = %d, %v", rows, err)
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE telemetry_frames SET pending_since_unix_ns = ? WHERE seq = ?`, time.Now().Add(-time.Hour).UnixNano(), ids[2]); err != nil {
		t.Fatal(err)
	}
	if rows, err := w.ResetPending(ctx, time.Minute); err != nil || rows != 1 {
		t.Fatalf("ResetPending() = %d, %v", rows, err)
	}
}

func TestPendingStreamOwnershipFencesOverlappingTeardownAndSenderCleanup(t *testing.T) {
	w := mustNewWAL(t)
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()
	ctx := context.Background()
	ids := make([]uint64, 2)
	for index := range ids {
		id, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: int64(index + 1)})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = uint64(id)
	}
	if rows, err := w.MarkPendingBatchOwned(ctx, []uint64{ids[0]}, "old-stream"); err != nil || rows != 1 {
		t.Fatalf("old stream reserve = %d, %v", rows, err)
	}
	if rows, err := w.MarkPendingBatchOwned(ctx, []uint64{ids[1]}, "new-stream"); err != nil || rows != 1 {
		t.Fatalf("new stream reserve = %d, %v", rows, err)
	}

	// The later stream quiesces while the older timed-out sender is still
	// running. Its teardown may expose only its own row.
	if rows, err := w.RequeuePendingOwner(ctx, "new-stream"); err != nil || rows != 1 {
		t.Fatalf("new stream teardown requeue = %d, %v", rows, err)
	}
	if rows, err := w.MarkWrittenBatchOwned(ctx, ids, "old-stream"); err != nil || rows != 1 {
		t.Fatalf("late old sender cleanup = %d, %v", rows, err)
	}
	if rows, err := w.MarkPendingBatchOwned(ctx, []uint64{ids[1]}, "replacement-stream"); err != nil || rows != 1 {
		t.Fatalf("replacement stream reserve = %d, %v", rows, err)
	}
	if rows, err := w.MarkWrittenBatchOwned(ctx, []uint64{ids[1]}, "old-stream"); err != nil || rows != 0 {
		t.Fatalf("old sender stole replacement ownership = %d, %v", rows, err)
	}
	if _, err := w.ApplyTelemetryAckOwned(ctx, ids[1], "", TelemetryAckRetry, "late old retry", "old-stream"); !errors.Is(err, ErrTelemetryAckConflict) {
		t.Fatalf("late old ACK crossed stream ownership: %v", err)
	}
	if result, err := w.ApplyTelemetryAckOwned(ctx, ids[1], "", TelemetryAckDelivered, "", "replacement-stream"); err != nil || !result.Changed {
		t.Fatalf("replacement exact ACK = %#v, %v", result, err)
	}
}

func TestDeliveredTelemetryACKBatchOwnedIsAtomicIdentityCheckedAndIdempotent(t *testing.T) {
	w := mustNewWAL(t)
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()
	ctx := context.Background()
	ids := make([]uint64, 4)
	for index := range ids {
		id, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: int64(index + 1)})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = uint64(id)
	}
	if rows, err := w.MarkPendingBatchOwned(ctx, ids, "stream-1"); err != nil || rows != int64(len(ids)) {
		t.Fatalf("reserve owned batch = %d, %v", rows, err)
	}
	frameID := fmt.Sprintf("7:agent-1:%d:%d", 1, ids[0])
	results, err := w.ApplyDeliveredTelemetryAckBatchOwned(ctx, []TelemetryDeliveredAck{
		{Sequence: ids[0], FrameID: frameID},
		{Sequence: ids[1]},
		{Sequence: ids[1]}, // duplicate within the same transaction is idempotent
	}, "stream-1")
	if err != nil || len(results) != 3 || !results[0].Changed || !results[1].Changed || results[2].Changed || !results[0].CorrelatedByFrameID {
		t.Fatalf("delivered batch = %#v, %v", results, err)
	}

	// A later identity failure must roll back the valid transition before it.
	if _, err := w.ApplyDeliveredTelemetryAckBatchOwned(ctx, []TelemetryDeliveredAck{
		{Sequence: ids[2]},
		{Sequence: ids[3], FrameID: "wrong"},
	}, "stream-1"); !errors.Is(err, ErrTelemetryFrameIdentityMismatch) {
		t.Fatalf("identity failure = %v", err)
	}
	if outstanding, err := w.CountOutstanding(ctx); err != nil || outstanding != 2 {
		t.Fatalf("outstanding after rolled-back batch = %d, %v; want 2", outstanding, err)
	}
	if _, err := w.ApplyDeliveredTelemetryAckBatchOwned(ctx, []TelemetryDeliveredAck{{Sequence: ids[2]}}, "another-stream"); !errors.Is(err, ErrTelemetryAckConflict) {
		t.Fatalf("cross-owner batch = %v", err)
	}
	results, err = w.ApplyDeliveredTelemetryAckBatchOwned(ctx, []TelemetryDeliveredAck{{Sequence: ids[2]}, {Sequence: ids[3]}}, "stream-1")
	if err != nil || len(results) != 2 || !results[0].Changed || !results[1].Changed {
		t.Fatalf("replayed exact batch = %#v, %v", results, err)
	}
	if outstanding, err := w.CountOutstanding(ctx); err != nil || outstanding != 0 {
		t.Fatalf("final outstanding = %d, %v; want 0", outstanding, err)
	}
}

func TestConcurrentTerminalACKsAndTeardownRequeueNeverRegressCommittedRows(t *testing.T) {
	w := mustNewWAL(t)
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()
	ctx := context.Background()
	const frameCount = 32
	ids := make([]uint64, frameCount)
	for index := range ids {
		id, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: int64(index + 1)})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = uint64(id)
	}
	if rows, err := w.MarkPendingBatch(ctx, ids); err != nil || rows != frameCount {
		t.Fatalf("MarkPendingBatch() = %d, %v", rows, err)
	}

	start := make(chan struct{})
	type ackOutcome struct {
		id  uint64
		err error
	}
	outcomes := make(chan ackOutcome, frameCount)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			<-start
			_, err := w.ApplyTelemetryAck(ctx, id, "", TelemetryAckDelivered, "")
			outcomes <- ackOutcome{id: id, err: err}
		}(id)
	}
	requeueDone := make(chan error, 1)
	go func() {
		<-start
		_, err := w.RequeueAllPending(ctx)
		requeueDone <- err
	}()
	close(start)
	wg.Wait()
	close(outcomes)
	if err := <-requeueDone; err != nil {
		t.Fatal(err)
	}

	for outcome := range outcomes {
		var status DeliveryStatus
		if err := w.db.QueryRowContext(ctx, `SELECT delivery_status FROM telemetry_frames WHERE seq = ?`, outcome.id).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if outcome.err == nil {
			if status != DeliveryStatusDelivered {
				t.Fatalf("committed ACK sequence %d regressed to %d", outcome.id, status)
			}
			continue
		}
		if !errors.Is(outcome.err, ErrTelemetryAckConflict) || status != DeliveryStatusWritten {
			t.Fatalf("racing ACK sequence %d = status %d error %v", outcome.id, status, outcome.err)
		}
	}
}

func TestTelemetryAckMismatchAndPendingBatchConflictDoNotMutateWrongRows(t *testing.T) {
	w := mustNewWAL(t)
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()
	ctx := context.Background()
	firstID, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 100})
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 200})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.ApplyTelemetryAck(ctx, uint64(firstID), "wrong-frame", TelemetryAckDelivered, ""); !errors.Is(err, ErrTelemetryFrameIdentityMismatch) {
		t.Fatalf("mismatched frame ID error = %v", err)
	}
	if _, err = w.ApplyTelemetryAck(ctx, uint64(secondID+100), "", TelemetryAckDelivered, ""); !errors.Is(err, ErrTelemetryFrameNotFound) {
		t.Fatalf("unknown sequence error = %v", err)
	}
	if rows, err := w.MarkDelivered(ctx, uint64(secondID)); err != nil || rows != 1 {
		t.Fatalf("prepare delivered row = %d, %v", rows, err)
	}
	if rows, err := w.MarkPendingBatch(ctx, []uint64{uint64(firstID), uint64(secondID)}); !errors.Is(err, ErrTelemetryAckConflict) || rows != 0 {
		t.Fatalf("conflicting pending batch = %d, %v", rows, err)
	}
	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil || len(entries) != 1 || entries[0].ID != firstID {
		t.Fatalf("all-or-nothing batch mutated rows: entries=%#v err=%v", entries, err)
	}
}

func TestWAL_OperationContextPersistsAndCommandsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wal.db")
	w, err := New(ctx, path, 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	want := OperationContext{AircraftID: "aircraft-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 3}
	applied, err := w.SetOperationContext(ctx, "set-1", want)
	if err != nil || !applied {
		t.Fatalf("SetOperationContext() = %v, %v", applied, err)
	}
	applied, err = w.SetOperationContext(ctx, "set-1", OperationContext{FlightID: "wrong"})
	if !errors.Is(err, ErrOperationCommandConflict) || applied {
		t.Fatalf("conflicting SetOperationContext() = %v, %v", applied, err)
	}
	applied, err = w.SetOperationContext(ctx, "set-1", want)
	if err != nil || applied {
		t.Fatalf("idempotent SetOperationContext() = %v, %v", applied, err)
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
	applied, err = w.SetOperationContext(ctx, "set-1", want)
	if err != nil || applied {
		t.Fatalf("durable idempotent set after reopen = %v, %v", applied, err)
	}
	applied, err = w.ClearOperationContext(ctx, "set-1", want.FlightID)
	if !errors.Is(err, ErrOperationCommandConflict) || applied {
		t.Fatalf("cross-kind command ID reuse = %v, %v", applied, err)
	}

	applied, err = w.ClearOperationContext(ctx, "clear-empty", "")
	if err != nil || !applied {
		t.Fatalf("empty-flight clear = %v, %v; want applied", applied, err)
	}
	if _, ok, err = w.LoadOperationContext(ctx); err != nil || ok {
		t.Fatalf("context after empty-flight clear: ok=%v err=%v", ok, err)
	}
	applied, err = w.SetOperationContext(ctx, "set-2", want)
	if err != nil || !applied {
		t.Fatalf("restore context after reconciliation = %v, %v", applied, err)
	}
	applied, err = w.ClearOperationContext(ctx, "clear-empty", "")
	if err != nil || applied {
		t.Fatalf("late empty-clear retry = %v, %v; want durable no-op", applied, err)
	}
	if got, ok, err = w.LoadOperationContext(ctx); err != nil || !ok || got != want {
		t.Fatalf("context after late empty-clear retry = %#v, %v, %v; want %#v", got, ok, err, want)
	}
	applied, err = w.ClearOperationContext(ctx, "clear-empty", want.FlightID)
	if !errors.Is(err, ErrOperationCommandConflict) || applied {
		t.Fatalf("empty-clear command ID conflict = %v, %v", applied, err)
	}

	applied, err = w.ClearOperationContext(ctx, "clear-old", "another-flight")
	if err != nil || !applied {
		t.Fatalf("conditional clear = %v, %v", applied, err)
	}
	applied, err = w.ClearOperationContext(ctx, "clear-old", want.FlightID)
	if !errors.Is(err, ErrOperationCommandConflict) || applied {
		t.Fatalf("conflicting conditional clear = %v, %v", applied, err)
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
	if _, err := w.db.ExecContext(ctx, `INSERT INTO spool_imports(spool_id, imported_at) VALUES(?, ?)`, uuid.NewString(), time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
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
	var importCount int
	if err := w.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM spool_imports`).Scan(&importCount); err != nil {
		t.Fatal(err)
	}
	if importCount != 0 {
		t.Fatalf("orphaned spool import markers after cleanup = %d, want 0", importCount)
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

func TestSpoolReadHelpersHonorCancellationBetweenChunks(t *testing.T) {
	payload := bytes.Repeat([]byte{0x5a}, spoolReadChunkSize*2)

	readCtx, cancelRead := context.WithCancel(context.Background())
	reader := &cancelAfterFirstRead{reader: bytes.NewReader(payload), cancel: cancelRead}
	buffer := make([]byte, len(payload))
	n, err := readFullContext(readCtx, reader, buffer)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("readFullContext() error = %v, want canceled", err)
	}
	if n != spoolReadChunkSize {
		t.Fatalf("bytes read before cancellation = %d, want %d", n, spoolReadChunkSize)
	}

	copyCtx, cancelCopy := context.WithCancel(context.Background())
	copyReader := &cancelAfterFirstRead{reader: bytes.NewReader(payload), cancel: cancelCopy}
	var destination bytes.Buffer
	written, err := copyContext(copyCtx, &destination, copyReader)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("copyContext() error = %v, want canceled", err)
	}
	if written != spoolReadChunkSize || destination.Len() != spoolReadChunkSize {
		t.Fatalf("bytes copied before cancellation = (%d,%d), want (%d,%d)",
			written, destination.Len(), spoolReadChunkSize, spoolReadChunkSize)
	}
}

func TestWALDrainCancellationRetainsValidSpool(t *testing.T) {
	w := mustOpenWALWithoutWriter(t, filepath.Join(t.TempDir(), "cancel-spool-read.db"))
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	spoolPath, err := w.spoolBatch([]*agentv1.TelemetryFrame{{RawMavlink: []byte("retain-on-cancel")}})
	if err != nil {
		t.Fatal(err)
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.drainSpoolContext(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("drainSpoolContext() error = %v, want canceled", err)
	}
	if _, err := os.Stat(spoolPath); err != nil {
		t.Fatalf("valid spool was not retained after canceled read: %v", err)
	}
	quarantined, err := os.ReadDir(w.spoolQuarantineDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(quarantined) != 0 {
		t.Fatalf("quarantine files after canceled read = %d, want 0", len(quarantined))
	}
}

func TestWALOrphanCleanupLockHonorsCancellation(t *testing.T) {
	w := mustOpenWALWithoutWriter(t, filepath.Join(t.TempDir(), "cancel-cleanup-lock.db"))
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	w.spoolMu.Lock()
	defer w.spoolMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := w.cleanupOrphanedSpoolImports(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanupOrphanedSpoolImports() error = %v, want deadline exceeded", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("cleanup lock ignored context for %v", time.Since(started))
	}
}

func TestWALCloseContextInterruptsBlockedStartupDrain(t *testing.T) {
	w := mustOpenWALWithoutWriter(t, filepath.Join(t.TempDir(), "blocked-drain.db"))
	w.writerDone = make(chan struct{})
	w.batchTimeout = time.Hour
	w.batchSize = 100
	w.batchChan = make(chan *agentv1.TelemetryFrame, 200)
	w.spoolMu.Lock()
	go w.runBatchWriter(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	err := w.CloseContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("CloseContext() did not interrupt blocked drain for %v", time.Since(started))
	}
	if err := w.db.PingContext(context.Background()); err == nil {
		t.Fatal("database remained open after interrupted drain shutdown")
	}

	w.spoolMu.Unlock()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALCloseInterruptsActiveFlushAndRetriesWithCloseContext(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "interrupt-active-flush.db")
	w := mustOpenWALWithoutWriter(t, dbPath)
	w.writerDone = make(chan struct{})
	w.batchTimeout = time.Hour
	w.batchSize = 2
	w.batchChan = make(chan *agentv1.TelemetryFrame, 4)

	marshalStarted := make(chan struct{})
	releaseMarshal := make(chan struct{})
	var marshalCalls atomic.Int32
	w.marshalSpoolFrame = func(frame *agentv1.TelemetryFrame) ([]byte, error) {
		if marshalCalls.Add(1) == 1 {
			close(marshalStarted)
			<-releaseMarshal
		}
		return proto.Marshal(frame)
	}
	go w.runBatchWriter(ctx)

	for _, payload := range []string{"before-cancel", "after-cancel"} {
		if err := w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-marshalStarted:
	case <-time.After(time.Second):
		t.Fatal("normal WAL flush did not start")
	}

	closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- w.CloseContext(closeCtx)
	}()
	closingDeadline := time.After(time.Second)
	for !w.closing.Load() {
		select {
		case <-closingDeadline:
			t.Fatal("close request did not reach WAL writer")
		case <-time.After(time.Millisecond):
		}
	}
	close(releaseMarshal)
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("close did not retry the interrupted batch")
	}
	if calls := marshalCalls.Load(); calls != 3 {
		t.Fatalf("spool marshal calls = %d, want 3 (one interrupted plus two close retries)", calls)
	}

	reopened := mustOpenWALWithoutWriter(t, dbPath)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	if err := reopened.drainSpool(); err != nil {
		t.Fatal(err)
	}
	entries, err := reopened.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("durable frames after interrupted flush = %d, want 2", len(entries))
	}
	for i, want := range []string{"before-cancel", "after-cancel"} {
		var frame agentv1.TelemetryFrame
		if err := proto.Unmarshal(entries[i].Payload, &frame); err != nil {
			t.Fatal(err)
		}
		if string(frame.GetRawMavlink()) != want {
			t.Fatalf("durable frame %d = %q, want %q", i, frame.GetRawMavlink(), want)
		}
	}
}

func TestWALCloseContextDeadlineDuringFinalFilesystemOperation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "blocked-filesystem.db")
	w, err := New(ctx, dbPath, 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("durable-after-timeout")}); err != nil {
		t.Fatal(err)
	}
	renameStarted := make(chan struct{})
	releaseRename := make(chan struct{})
	var startedOnce sync.Once
	w.renameFile = func(source, destination string) error {
		if strings.HasSuffix(source, ".tmp") {
			startedOnce.Do(func() { close(renameStarted) })
			<-releaseRename
		}
		return os.Rename(source, destination)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- w.CloseContext(closeCtx)
	}()
	select {
	case <-renameStarted:
	case <-time.After(time.Second):
		t.Fatal("final spool rename did not start")
	}
	select {
	case err := <-closeResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("CloseContext() error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CloseContext did not return after filesystem deadline")
	}
	if err := w.db.PingContext(context.Background()); err != nil {
		t.Fatalf("database closed while writer remained active: %v", err)
	}
	close(releaseRename)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dbPath + ".spool")
	if err != nil {
		t.Fatal(err)
	}
	batchCount := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".batch" {
			batchCount++
		}
	}
	if batchCount != 1 {
		t.Fatalf("durable batches after timed-out close = %d, want 1", batchCount)
	}
}

func TestWALCloseContextRetriesUnspooledBatchAfterExpiredRequest(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "retry-close.db")
	w, err := New(ctx, dbPath, 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendAsync(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("retry-me")}); err != nil {
		t.Fatal(err)
	}
	expiredCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.CloseContext(expiredCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expired CloseContext() error = %v, want canceled", err)
	}
	if err := w.AppendAsync(context.Background(), &agentv1.TelemetryFrame{}); err == nil {
		t.Fatal("AppendAsync succeeded while a close retry was pending")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dbPath + ".spool")
	if err != nil {
		t.Fatal(err)
	}
	batchCount := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".batch" {
			batchCount++
		}
	}
	if batchCount != 1 {
		t.Fatalf("retried close durable batches = %d, want 1", batchCount)
	}
}

func TestWALCloseContextDeadlineDuringDatabaseClose(t *testing.T) {
	w := mustOpenWALWithoutWriter(t, filepath.Join(t.TempDir(), "blocked-database-close.db"))
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	underlyingClose := w.db.Close
	w.closeDB = func() error {
		close(closeStarted)
		<-releaseClose
		return underlyingClose()
	}
	w.finalizeOnce = sync.Once{}
	w.closeDone = make(chan struct{})
	w.writerDone = nil

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- w.CloseContext(ctx)
	}()
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("database close did not start")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("CloseContext() error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CloseContext did not return while database close blocked")
	}
	close(releaseClose)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALCloseIsIdempotentAcrossConcurrentCallers(t *testing.T) {
	w, err := New(context.Background(), filepath.Join(t.TempDir(), "concurrent-close.db"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	underlyingClose := w.db.Close
	var closeCalls atomic.Int64
	w.closeDB = func() error {
		closeCalls.Add(1)
		return underlyingClose()
	}

	const callers = 32
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- w.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if closeCalls.Load() != 1 {
		t.Fatalf("database close calls = %d, want 1", closeCalls.Load())
	}
	if err := w.AppendAsync(context.Background(), &agentv1.TelemetryFrame{}); err == nil {
		t.Fatal("AppendAsync succeeded after close")
	}
}

func TestWALConcurrentAppendAndClosePreservesAcceptedFrames(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "append-close-race.db")
	w, err := New(context.Background(), dbPath, 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	const appenders = 64
	start := make(chan struct{})
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < appenders; i++ {
		wg.Add(1)
		go func(value byte) {
			defer wg.Done()
			<-start
			if err := w.AppendAsync(context.Background(), &agentv1.TelemetryFrame{RawMavlink: []byte{value}}); err == nil {
				accepted.Add(1)
			}
		}(byte(i))
	}
	closeResult := make(chan error, 1)
	go func() {
		<-start
		closeResult <- w.Close()
	}()
	close(start)
	wg.Wait()
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}

	reopened := mustOpenWALWithoutWriter(t, dbPath)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	})
	if err := reopened.drainSpool(); err != nil {
		t.Fatal(err)
	}
	entries, err := reopened.ReadUndelivered(context.Background(), appenders)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(entries)) != accepted.Load() {
		t.Fatalf("durable accepted frames = %d, want %d", len(entries), accepted.Load())
	}
}

func TestWALLifecycleCancellationTriggersDurableClose(t *testing.T) {
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	dbPath := filepath.Join(t.TempDir(), "lifecycle-close.db")
	w, err := New(lifecycleCtx, dbPath, 100, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendAsync(context.Background(), &agentv1.TelemetryFrame{RawMavlink: []byte("lifecycle")}); err != nil {
		t.Fatal(err)
	}
	cancelLifecycle()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dbPath + ".spool")
	if err != nil {
		t.Fatal(err)
	}
	batchCount := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".batch" {
			batchCount++
		}
	}
	if batchCount != 1 {
		t.Fatalf("lifecycle cancellation durable batches = %d, want 1", batchCount)
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

func mustOpenWALWithoutWriter(t *testing.T, dbPath string) *WAL {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureDB(db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := initDB(db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	generationID, err := startGenerationID(context.Background(), db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	spoolDir := dbPath + ".spool"
	spoolQuarantineDir := filepath.Join(spoolDir, "quarantine")
	if err := os.MkdirAll(spoolQuarantineDir, 0o755); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return &WAL{
		db:                 db,
		generationID:       generationID,
		closeWake:          make(chan struct{}, 1),
		closeDone:          make(chan struct{}),
		signalChan:         make(chan struct{}, 1),
		spoolDir:           spoolDir,
		spoolQuarantineDir: spoolQuarantineDir,
		removeFile:         os.Remove,
		renameFile:         os.Rename,
		syncDir:            syncDirectory,
		marshalSpoolFrame: func(frame *agentv1.TelemetryFrame) ([]byte, error) {
			return proto.Marshal(frame)
		},
		closeDB: db.Close,
	}
}

func writeLegacySpoolFile(t *testing.T, path string, frames []*agentv1.TelemetryFrame) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	writer := bufio.NewWriter(file)
	for _, frame := range frames {
		payload, err := proto.Marshal(frame)
		if err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		var length [4]byte
		binary.LittleEndian.PutUint32(length[:], uint32(len(payload)))
		if _, err := writer.Write(length[:]); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if _, err := writer.Write(payload); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
