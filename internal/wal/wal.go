package wal

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

// Keep legacy migration memory bounded on constrained companion computers.
const legacyFrameMigrationBatchSize = 128

// Keep durable marker reclamation transactions bounded as well.
const spoolImportCleanupBatchSize = 128

// Keep explicit quarantine cleanup transactions bounded on constrained disks.
const quarantineCleanupBatchSize = 128

const (
	spoolFileMagic           = "AEROARC-SPOOL\x00\x01"
	spoolIDLength            = 36
	maxSpoolFramePayloadSize = 16 << 20
)

// Entry represents a single log entry in the WAL.
type Entry struct {
	ID        int64
	Payload   []byte
	CreatedAt int64
}

// WAL implements a Write-Ahead Log using SQLite.
type WAL struct {
	db                 *sql.DB
	generationID       string
	doneChan           chan struct{}
	writerDone         chan struct{}
	batchChan          chan *agentv1.TelemetryFrame
	signalChan         chan struct{}
	batchSize          int64
	batchTimeout       time.Duration
	spoolDir           string
	spoolQuarantineDir string
	spoolSeq           uint64
	spoolMu            sync.Mutex
	removeFile         func(string) error
	renameFile         func(string, string) error
	closeOnce          sync.Once
	closeErr           error
}

// New creates or opens a WAL and starts a new durable append generation.
// Frames appended through the returned WAL are stamped before persistence, so
// retries retain their original `(generation, sequence)` cursor across process
// restarts. Starting a new generation on every open prevents a restored or
// cloned database from reusing cursors allocated after its snapshot.
//
// Parameters:
//   - ctx: bounds schema initialization and generation rotation.
//   - path: identifies the SQLite database and its adjacent spill directory.
//   - batchSize: controls asynchronous transaction size; non-positive values
//     select the default.
//   - batchTimeout: controls asynchronous flush latency; non-positive values
//     select the default.
//
// Returns:
//   - wal: owns the database, spill directory, and background writer.
//   - error: reports database configuration, schema, generation identity, or
//     spill-directory initialization failures.
//
// TODO: Add time.Duration for the WAL cleanup interval.
func New(ctx context.Context, path string, batchSize int64, batchTimeout time.Duration) (*WAL, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal db: %w", err)
	}

	if err := configureDB(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to configure db: %w", err)
	}

	if err := initDB(db); err != nil {
		db.Close()
		return nil, err
	}
	generationID, err := startGenerationID(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	// Default values if not provided
	if batchSize <= 0 {
		batchSize = 100
	}
	if batchTimeout <= 0 {
		batchTimeout = 100 * time.Millisecond
	}

	wal := &WAL{
		db:           db,
		generationID: generationID,
		doneChan:     make(chan struct{}),
		writerDone:   make(chan struct{}),
		batchChan:    make(chan *agentv1.TelemetryFrame, batchSize*2), // Buffer a bit more than one batch
		signalChan:   make(chan struct{}, 1),                          // Buffer 1 to prevent blocking
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		spoolDir:     path + ".spool",
		removeFile:   os.Remove,
		renameFile:   os.Rename,
	}
	wal.spoolQuarantineDir = filepath.Join(wal.spoolDir, "quarantine")

	if err := os.MkdirAll(wal.spoolQuarantineDir, 0o755); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create spool quarantine dir: %w", err)
	}

	// Start the background writer
	go wal.runBatchWriter(ctx)

	return wal, nil
}

func configureDB(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=FULL;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA busy_timeout=5000;",
	}

	db.SetMaxOpenConns(1)

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("failed to exec pragma %q: %w", p, err)
		}
	}
	return nil
}

func initDB(db *sql.DB) error {
	// for seq we would need to emit 1000frames a second over 200million years to overflow
	query := `
	CREATE TABLE IF NOT EXISTS telemetry_frames (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at INTEGER NOT NULL,
		payload BLOB NOT NULL,
		delivery_status INTEGER NOT NULL DEFAULT 0
	);
	CREATE TABLE IF NOT EXISTS operation_context (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		flight_id TEXT NOT NULL,
		intent_id TEXT NOT NULL,
		intent_version INTEGER NOT NULL,
		updated_at INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS operation_context_commands (
		command_id TEXT PRIMARY KEY,
		processed_at INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS wal_metadata (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		generation_id TEXT NOT NULL
	);
	CREATE TABLE IF NOT EXISTS wal_identity_migration (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		legacy_generation_id TEXT NOT NULL,
		last_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
		completed INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1))
	);
	CREATE TABLE IF NOT EXISTS telemetry_frame_quarantine (
		seq INTEGER PRIMARY KEY,
		quarantined_at INTEGER NOT NULL,
		reason TEXT NOT NULL,
		original_delivery_status INTEGER NOT NULL,
		FOREIGN KEY(seq) REFERENCES telemetry_frames(seq) ON DELETE CASCADE
	);
	CREATE TABLE IF NOT EXISTS spool_imports (
		spool_id TEXT PRIMARY KEY,
		imported_at INTEGER NOT NULL,
		seen_token TEXT NOT NULL DEFAULT ''
	);
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	indexQuery := `
	CREATE INDEX IF NOT EXISTS idx_telemetry_undelivered
	ON telemetry_frames (delivery_status, seq);
	CREATE INDEX IF NOT EXISTS idx_telemetry_frame_quarantine_newest
	ON telemetry_frame_quarantine (quarantined_at DESC, seq DESC);
	`
	_, err = db.Exec(indexQuery)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	if err := ensureSpoolImportSeenToken(db); err != nil {
		return err
	}

	return nil
}

func ensureSpoolImportSeenToken(db *sql.DB) error {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('spool_imports') WHERE name = 'seen_token'`).Scan(&count); err != nil {
		return fmt.Errorf("inspect spool import schema: %w", err)
	}
	if count != 0 {
		return nil
	}
	if _, err := db.Exec(`ALTER TABLE spool_imports ADD COLUMN seen_token TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("add spool import cleanup token: %w", err)
	}
	return nil
}

func startGenerationID(ctx context.Context, db *sql.DB) (string, error) {
	state, err := loadOrCreateLegacyMigration(ctx, db)
	if err != nil {
		return "", err
	}
	for !state.completed {
		state, err = migrateLegacyFrameBatch(ctx, db, state.generationID)
		if err != nil {
			return "", err
		}
	}
	return rotateGenerationID(ctx, db)
}

type legacyMigrationState struct {
	generationID string
	lastSeq      int64
	completed    bool
}

// loadOrCreateLegacyMigration durably selects the one generation used for all
// pre-wal_id rows before any payload batches are rewritten.
func loadOrCreateLegacyMigration(ctx context.Context, db *sql.DB) (legacyMigrationState, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return legacyMigrationState{}, fmt.Errorf("begin WAL identity migration setup: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	state, found, err := loadLegacyMigrationState(ctx, tx)
	if err != nil {
		return legacyMigrationState{}, err
	}
	if found {
		var currentGenerationID string
		if err := tx.QueryRowContext(ctx, `SELECT generation_id FROM wal_metadata WHERE id = 1`).Scan(&currentGenerationID); err != nil {
			return legacyMigrationState{}, fmt.Errorf("load current WAL generation identity: %w", err)
		}
		if _, err := uuid.Parse(currentGenerationID); err != nil {
			return legacyMigrationState{}, fmt.Errorf("load current WAL generation identity: invalid UUID %q: %w", currentGenerationID, err)
		}
		if !state.completed && currentGenerationID != state.generationID {
			return legacyMigrationState{}, fmt.Errorf("incomplete WAL identity migration generation %q does not match current generation %q", state.generationID, currentGenerationID)
		}
		return state, nil
	}

	var generationID string
	err = tx.QueryRowContext(ctx, `SELECT generation_id FROM wal_metadata WHERE id = 1`).Scan(&generationID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return legacyMigrationState{}, fmt.Errorf("load WAL generation for identity migration: %w", err)
	}
	if errors.Is(err, sql.ErrNoRows) {
		generationID = uuid.NewString()
		if _, err := tx.ExecContext(ctx, `INSERT INTO wal_metadata(id, generation_id) VALUES(1, ?)`, generationID); err != nil {
			return legacyMigrationState{}, fmt.Errorf("initialize WAL generation for identity migration: %w", err)
		}
	} else if _, err := uuid.Parse(generationID); err != nil {
		return legacyMigrationState{}, fmt.Errorf("load WAL generation for identity migration: invalid UUID %q: %w", generationID, err)
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO wal_identity_migration(
		id, legacy_generation_id, last_seq, completed) VALUES(1, ?, 0, 0)`, generationID); err != nil {
		return legacyMigrationState{}, fmt.Errorf("initialize WAL identity migration: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return legacyMigrationState{}, fmt.Errorf("commit WAL identity migration setup: %w", err)
	}
	return legacyMigrationState{generationID: generationID}, nil
}

func loadLegacyMigrationState(ctx context.Context, tx *sql.Tx) (legacyMigrationState, bool, error) {
	var state legacyMigrationState
	var completed int
	err := tx.QueryRowContext(ctx, `SELECT legacy_generation_id, last_seq, completed
		FROM wal_identity_migration WHERE id = 1`).Scan(&state.generationID, &state.lastSeq, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return legacyMigrationState{}, false, nil
	}
	if err != nil {
		return legacyMigrationState{}, false, fmt.Errorf("load WAL identity migration: %w", err)
	}
	if _, err := uuid.Parse(state.generationID); err != nil {
		return legacyMigrationState{}, false, fmt.Errorf("load WAL identity migration: invalid UUID %q: %w", state.generationID, err)
	}
	if state.lastSeq < 0 || (completed != 0 && completed != 1) {
		return legacyMigrationState{}, false, fmt.Errorf("load WAL identity migration: invalid progress last_seq=%d completed=%d", state.lastSeq, completed)
	}
	state.completed = completed == 1
	return state, true, nil
}

// migrateLegacyFrameBatch commits at most one bounded payload batch together
// with its cursor. Valid frames are stamped, while malformed frames and their
// diagnostic reason are atomically quarantined so later startups can resume.
func migrateLegacyFrameBatch(ctx context.Context, db *sql.DB, generationID string) (legacyMigrationState, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return legacyMigrationState{}, fmt.Errorf("begin WAL identity migration batch: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	state, found, err := loadLegacyMigrationState(ctx, tx)
	if err != nil {
		return legacyMigrationState{}, err
	}
	if !found {
		return legacyMigrationState{}, errors.New("WAL identity migration state is missing")
	}
	if state.generationID != generationID {
		return legacyMigrationState{}, fmt.Errorf("WAL identity migration generation changed from %q to %q", generationID, state.generationID)
	}
	if state.completed {
		return state, nil
	}

	type update struct {
		seq                    int64
		payload                []byte
		quarantineReason       string
		originalDeliveryStatus int
	}

	rows, err := tx.QueryContext(ctx, `SELECT seq, payload, delivery_status
		FROM telemetry_frames
		WHERE seq > ?
		ORDER BY seq
		LIMIT ?`, state.lastSeq, legacyFrameMigrationBatchSize)
	if err != nil {
		return legacyMigrationState{}, fmt.Errorf("query legacy WAL frames after sequence %d: %w", state.lastSeq, err)
	}

	updates := make([]update, 0, legacyFrameMigrationBatchSize)
	scanned := 0
	for rows.Next() {
		var seq int64
		var payload []byte
		var deliveryStatus int
		if err := rows.Scan(&seq, &payload, &deliveryStatus); err != nil {
			return legacyMigrationState{}, closeLegacyRows(rows, fmt.Errorf("scan legacy WAL frame: %w", err))
		}
		scanned++
		state.lastSeq = seq

		var frame agentv1.TelemetryFrame
		if err := proto.Unmarshal(payload, &frame); err != nil {
			updates = append(updates, update{
				seq:                    seq,
				quarantineReason:       fmt.Sprintf("legacy WAL identity migration protobuf decode failed: %v", err),
				originalDeliveryStatus: deliveryStatus,
			})
			continue
		}
		if frame.GetWalId() != "" {
			continue
		}
		frame.WalId = generationID
		encoded, err := proto.Marshal(&frame)
		if err != nil {
			return legacyMigrationState{}, closeLegacyRows(rows, fmt.Errorf("marshal legacy WAL frame %d: %w", seq, err))
		}
		updates = append(updates, update{seq: seq, payload: encoded})
	}
	if err := rows.Err(); err != nil {
		return legacyMigrationState{}, closeLegacyRows(rows, fmt.Errorf("iterate legacy WAL frames: %w", err))
	}
	if err := closeLegacyRows(rows, nil); err != nil {
		return legacyMigrationState{}, err
	}

	for _, item := range updates {
		if item.quarantineReason != "" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO telemetry_frame_quarantine(
				seq, quarantined_at, reason, original_delivery_status) VALUES(?, ?, ?, ?)
				ON CONFLICT(seq) DO NOTHING`,
				item.seq, time.Now().UnixNano(), item.quarantineReason, item.originalDeliveryStatus); err != nil {
				return legacyMigrationState{}, fmt.Errorf("quarantine malformed legacy WAL frame %d: %w", item.seq, err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE telemetry_frames SET delivery_status = ? WHERE seq = ?`,
				DeliveryStatusQuarantined, item.seq); err != nil {
				return legacyMigrationState{}, fmt.Errorf("mark malformed legacy WAL frame %d quarantined: %w", item.seq, err)
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE telemetry_frames SET payload = ? WHERE seq = ?`, item.payload, item.seq); err != nil {
			return legacyMigrationState{}, fmt.Errorf("stamp legacy WAL frame %d: %w", item.seq, err)
		}
	}
	state.completed = scanned < legacyFrameMigrationBatchSize
	result, err := tx.ExecContext(ctx, `UPDATE wal_identity_migration
		SET last_seq = ?, completed = ? WHERE id = 1`, state.lastSeq, state.completed)
	if err != nil {
		return legacyMigrationState{}, fmt.Errorf("persist WAL identity migration progress: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil {
		return legacyMigrationState{}, fmt.Errorf("inspect WAL identity migration progress: %w", err)
	} else if rows != 1 {
		return legacyMigrationState{}, fmt.Errorf("persist WAL identity migration progress: updated %d rows, want 1", rows)
	}
	if err := tx.Commit(); err != nil {
		return legacyMigrationState{}, fmt.Errorf("commit WAL identity migration batch: %w", err)
	}
	return state, nil
}

// rotateGenerationID advances the append epoch only after legacy migration is
// durably complete, so an interrupted migration continues using one identity.
func rotateGenerationID(ctx context.Context, db *sql.DB) (string, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin WAL generation rotation: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	state, found, err := loadLegacyMigrationState(ctx, tx)
	if err != nil {
		return "", err
	}
	if !found || !state.completed {
		return "", errors.New("cannot rotate WAL generation before identity migration completes")
	}
	nextID := uuid.NewString()
	result, err := tx.ExecContext(ctx, `UPDATE wal_metadata SET generation_id = ? WHERE id = 1`, nextID)
	if err != nil {
		return "", fmt.Errorf("rotate WAL generation identity: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil {
		return "", fmt.Errorf("inspect WAL generation rotation: %w", err)
	} else if rows != 1 {
		return "", fmt.Errorf("rotate WAL generation identity: updated %d rows, want 1", rows)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit WAL generation rotation: %w", err)
	}
	return nextID, nil
}

func closeLegacyRows(rows *sql.Rows, cause error) error {
	if err := rows.Close(); err != nil {
		return errors.Join(cause, fmt.Errorf("close legacy WAL frame rows: %w", err))
	}
	return cause
}

// GenerationID returns the append generation assigned to frames newly
// persisted by this WAL instance. A new value is created on every open while
// generation IDs already stored in queued frames remain unchanged.
func (w *WAL) GenerationID() string {
	return w.generationID
}

// OperationContext is the capture-time flight attribution persisted by the agent.
type OperationContext struct {
	FlightID      string
	IntentID      string
	IntentVersion uint32
}

// LoadOperationContext returns the currently active context, if one exists.
func (w *WAL) LoadOperationContext(ctx context.Context) (OperationContext, bool, error) {
	var value OperationContext
	var version int64
	err := w.db.QueryRowContext(ctx, `SELECT flight_id, intent_id, intent_version FROM operation_context WHERE id = 1`).
		Scan(&value.FlightID, &value.IntentID, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return OperationContext{}, false, nil
	}
	if err != nil {
		return OperationContext{}, false, fmt.Errorf("load operation context: %w", err)
	}
	if version < 0 || version > int64(^uint32(0)) {
		return OperationContext{}, false, fmt.Errorf("load operation context: invalid intent version %d", version)
	}
	value.IntentVersion = uint32(version)
	return value, true, nil
}

// SetOperationContext atomically applies a command once. Repeated command IDs
// are successful no-ops so command acknowledgements can safely be retried.
func (w *WAL) SetOperationContext(ctx context.Context, commandID string, value OperationContext) (bool, error) {
	return w.applyOperationCommand(ctx, commandID, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO operation_context(id, flight_id, intent_id, intent_version, updated_at)
			VALUES(1, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET flight_id=excluded.flight_id,
			intent_id=excluded.intent_id, intent_version=excluded.intent_version, updated_at=excluded.updated_at`,
			value.FlightID, value.IntentID, value.IntentVersion, time.Now().UnixNano())
		return err
	})
}

// ClearOperationContext atomically clears the active context once.
func (w *WAL) ClearOperationContext(ctx context.Context, commandID, flightID string) (bool, error) {
	if commandID == "" {
		return false, errors.New("operation command ID is required")
	}
	if flightID == "" {
		return false, errors.New("flight ID is required")
	}

	return w.applyOperationCommand(ctx, commandID, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM operation_context WHERE id = 1 AND flight_id = ?`, flightID)
		return err
	})
}

func (w *WAL) applyOperationCommand(ctx context.Context, commandID string, apply func(*sql.Tx) error) (bool, error) {
	if commandID == "" {
		return false, errors.New("operation command ID is required")
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin operation command: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO operation_context_commands(command_id, processed_at) VALUES(?, ?)`, commandID, time.Now().UnixNano())
	if err != nil {
		return false, fmt.Errorf("record operation command: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect operation command: %w", err)
	}
	if rows == 0 {
		return false, nil
	}
	if err := apply(tx); err != nil {
		return false, fmt.Errorf("apply operation command: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit operation command: %w", err)
	}
	return true, nil
}

// AppendAsync queues a frame for writing.
// This is non-blocking unless the buffer is completely full.
func (w *WAL) AppendAsync(ctx context.Context, tFrame *agentv1.TelemetryFrame) error {
	select {
	case w.batchChan <- tFrame:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// If buffer is full, we can choose to drop or block.
		// For safety, let's block with a short timeout or just block until context.
		// For now, let's block on the channel send to provide backpressure.
		select {
		case w.batchChan <- tFrame:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *WAL) runBatchWriter(ctx context.Context) {
	defer close(w.writerDone)

	var batch []*agentv1.TelemetryFrame
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

	if err := w.drainSpool(); err != nil {
		slog.Error("WAL spool drain failed", "error", err)
	}

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if _, err := w.spoolBatch(batch); err != nil {
			return err
		}

		batch = nil
		return nil
	}

	flushPending := false
	retryDelay := 200 * time.Millisecond
	shutdown := func(drain bool) {
		if err := flush(); err != nil {
			slog.Error("WAL Batch Spool Failed", "error", err)
		}
		if drain {
			if err := w.drainSpool(); err != nil {
				slog.Error("WAL Spool Drain Failed", "error", err)
			}
		}
	}

	for {
		if flushPending {
			if err := flush(); err != nil {
				slog.Error("WAL Batch Spool Failed", "error", err)
				select {
				case <-time.After(retryDelay):
					continue
				case <-ctx.Done():
					shutdown(true)
					return
				case <-w.doneChan:
					shutdown(false)
					return
				}
			}

			if err := w.drainSpool(); err != nil {
				slog.Error("WAL Spool Drain Failed", "error", err)
			}

			flushPending = false
			ticker.Reset(w.batchTimeout)
		}

		select {
		case frame := <-w.batchChan:
			batch = append(batch, frame)
			if int64(len(batch)) >= w.batchSize {
				flushPending = true
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushPending = true
			} else if err := w.drainSpool(); err != nil {
				slog.Error("WAL Spool Drain Failed", "error", err)
			}
		case <-ctx.Done():
			shutdown(true)
			return
		case <-w.doneChan:
			shutdown(false)
			return
		}
	}
}

func (w *WAL) spoolBatch(frames []*agentv1.TelemetryFrame) (string, error) {
	if len(frames) == 0 {
		return "", nil
	}

	payloads := make([][]byte, 0, len(frames))
	for _, frame := range frames {
		encoded, err := proto.Marshal(frame)
		if err != nil {
			return "", fmt.Errorf("failed to marshal frame for spool: %w", err)
		}
		if len(encoded) > maxSpoolFramePayloadSize {
			return "", fmt.Errorf("spool frame exceeds %d-byte safety limit: %d", maxSpoolFramePayloadSize, len(encoded))
		}
		payloads = append(payloads, encoded)
	}

	if len(payloads) == 0 {
		return "", nil
	}

	spoolID := uuid.NewString()
	seq := atomic.AddUint64(&w.spoolSeq, 1)
	name := fmt.Sprintf("%020d-%06d-%s.batch", time.Now().UnixNano(), seq, spoolID)
	path := filepath.Join(w.spoolDir, name)
	tmpPath := path + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("failed to create spool file: %w", err)
	}

	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	writer := bufio.NewWriter(file)
	if _, err := writer.WriteString(spoolFileMagic); err != nil {
		return "", fmt.Errorf("failed to write spool header magic: %w", err)
	}
	if _, err := writer.WriteString(spoolID); err != nil {
		return "", fmt.Errorf("failed to write spool identity: %w", err)
	}
	for _, payload := range payloads {
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		if _, err := writer.Write(lenBuf[:]); err != nil {
			return "", fmt.Errorf("failed to write spool length: %w", err)
		}
		if _, err := writer.Write(payload); err != nil {
			return "", fmt.Errorf("failed to write spool payload: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return "", fmt.Errorf("failed to flush spool file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return "", fmt.Errorf("failed to sync spool file: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("failed to close spool file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return "", fmt.Errorf("failed to finalize spool file: %w", err)
	}

	cleanup = false
	return path, nil
}

func (w *WAL) drainSpool() error {
	w.spoolMu.Lock()
	defer w.spoolMu.Unlock()

	entries, err := os.ReadDir(w.spoolDir)
	if err != nil {
		return fmt.Errorf("failed to read spool dir: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	wrote := false
	var drainErr error
	defer func() {
		if wrote {
			select {
			case w.signalChan <- struct{}{}:
			default:
			}
		}
	}()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".batch" {
			continue
		}
		path := filepath.Join(w.spoolDir, entry.Name())
		spoolID, frames, err := readSpoolFile(path)
		if err != nil {
			quarantinePath, quarantineErr := w.quarantineSpoolFile(path)
			if quarantineErr != nil {
				drainErr = errors.Join(drainErr,
					fmt.Errorf("read malformed spool file %s: %w", path, err),
					fmt.Errorf("quarantine malformed spool file %s: %w", path, quarantineErr))
				if quarantinePath != "" {
					drainErr = errors.Join(drainErr, fmt.Errorf("malformed spool file moved to %s before quarantine sync failed", quarantinePath))
				}
			} else {
				drainErr = errors.Join(drainErr, fmt.Errorf("quarantined malformed spool file %s at %s: %w", path, quarantinePath, err))
			}
			continue
		}
		if len(frames) == 0 {
			if err := w.removeFile(path); err != nil {
				drainErr = errors.Join(drainErr, fmt.Errorf("failed to remove empty spool file %s: %w", path, err))
				continue
			}
			if err := w.deleteSpoolImport(context.Background(), spoolID); err != nil {
				drainErr = errors.Join(drainErr, err)
			}
			continue
		}
		imported, err := w.appendSpoolBatch(context.Background(), spoolID, frames)
		if err != nil {
			drainErr = errors.Join(drainErr, fmt.Errorf("import spool file %s: %w", path, err))
			continue
		}
		wrote = wrote || imported
		if err := w.removeFile(path); err != nil {
			drainErr = errors.Join(drainErr, fmt.Errorf("failed to remove spool file %s: %w", path, err))
			continue
		}
		if err := w.deleteSpoolImport(context.Background(), spoolID); err != nil {
			drainErr = errors.Join(drainErr, err)
		}
	}

	remainingEntries, err := os.ReadDir(w.spoolDir)
	if err != nil {
		drainErr = errors.Join(drainErr, fmt.Errorf("failed to reread spool dir for import cleanup: %w", err))
	} else if err := w.pruneOrphanedSpoolImports(context.Background(), remainingEntries); err != nil {
		drainErr = errors.Join(drainErr, err)
	}

	return drainErr
}

func (w *WAL) quarantineSpoolFile(path string) (string, error) {
	if err := os.MkdirAll(w.spoolQuarantineDir, 0o755); err != nil {
		return "", fmt.Errorf("create spool quarantine dir: %w", err)
	}
	if err := syncFile(path); err != nil {
		return "", fmt.Errorf("sync malformed spool file before quarantine: %w", err)
	}
	destination := filepath.Join(w.spoolQuarantineDir,
		fmt.Sprintf("%020d-%s-%s.corrupt", time.Now().UnixNano(), uuid.NewString(), filepath.Base(path)))
	if err := w.renameFile(path, destination); err != nil {
		return "", fmt.Errorf("move spool file to quarantine: %w", err)
	}
	if err := syncDir(w.spoolQuarantineDir); err != nil {
		return destination, fmt.Errorf("sync spool quarantine dir: %w", err)
	}
	if err := syncDir(w.spoolDir); err != nil {
		return destination, fmt.Errorf("sync active spool dir: %w", err)
	}
	return destination, nil
}

func syncFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	return file.Sync()
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = dir.Close()
	}()
	return dir.Sync()
}

func (w *WAL) deleteSpoolImport(ctx context.Context, spoolID string) error {
	if _, err := w.db.ExecContext(ctx, `DELETE FROM spool_imports WHERE spool_id = ?`, spoolID); err != nil {
		return fmt.Errorf("delete spool import %q: %w", spoolID, err)
	}
	return nil
}

func (w *WAL) cleanupOrphanedSpoolImports(ctx context.Context) error {
	w.spoolMu.Lock()
	defer w.spoolMu.Unlock()

	entries, err := os.ReadDir(w.spoolDir)
	if err != nil {
		return fmt.Errorf("failed to read spool dir for import cleanup: %w", err)
	}
	return w.pruneOrphanedSpoolImports(ctx, entries)
}

func (w *WAL) pruneOrphanedSpoolImports(ctx context.Context, entries []os.DirEntry) error {
	seenToken := uuid.NewString()
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".batch" {
			continue
		}
		path := filepath.Join(w.spoolDir, entry.Name())
		spoolID, err := readSpoolIdentityFromPath(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read live spool identity %s: %w", path, err)
		}
		if _, err := w.db.ExecContext(ctx, `UPDATE spool_imports SET seen_token = ? WHERE spool_id = ?`, seenToken, spoolID); err != nil {
			return fmt.Errorf("mark live spool import %q: %w", spoolID, err)
		}
	}

	for {
		result, err := w.db.ExecContext(ctx, `DELETE FROM spool_imports WHERE spool_id IN (
			SELECT spool_id FROM spool_imports
			WHERE seen_token <> ?
			ORDER BY spool_id
			LIMIT ?
		)`, seenToken, spoolImportCleanupBatchSize)
		if err != nil {
			return fmt.Errorf("prune orphaned spool imports: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect orphaned spool import cleanup: %w", err)
		}
		if rows < spoolImportCleanupBatchSize {
			return nil
		}
	}
}

func readSpoolIdentityFromPath(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = file.Close()
	}()
	spoolID, _, err := readSpoolIdentity(file, path)
	return spoolID, err
}

func readSpoolFile(path string) (string, []*agentv1.TelemetryFrame, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	spoolID, payloadOffset, err := readSpoolIdentity(file, path)
	if err != nil {
		return "", nil, err
	}
	if _, err := file.Seek(payloadOffset, io.SeekStart); err != nil {
		return "", nil, fmt.Errorf("seek to spool payload: %w", err)
	}

	reader := bufio.NewReader(file)
	var frames []*agentv1.TelemetryFrame
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return "", nil, fmt.Errorf("truncated spool record: %w", err)
			}
			return "", nil, err
		}

		length := binary.LittleEndian.Uint32(lenBuf[:])
		if length == 0 {
			continue
		}
		if length > maxSpoolFramePayloadSize {
			return "", nil, fmt.Errorf("spool payload length %d exceeds %d-byte safety limit", length, maxSpoolFramePayloadSize)
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return "", nil, fmt.Errorf("truncated spool payload: %w", err)
		}

		var frame agentv1.TelemetryFrame
		if err := proto.Unmarshal(payload, &frame); err != nil {
			return "", nil, fmt.Errorf("failed to unmarshal spool frame: %w", err)
		}
		frames = append(frames, &frame)
	}

	return spoolID, frames, nil
}

func readSpoolIdentity(file *os.File, path string) (string, int64, error) {
	headerSize := len(spoolFileMagic) + spoolIDLength
	header := make([]byte, headerSize)
	n, err := file.ReadAt(header, 0)
	if n >= len(spoolFileMagic) && string(header[:len(spoolFileMagic)]) == spoolFileMagic {
		if err != nil || n != headerSize {
			return "", 0, fmt.Errorf("truncated spool identity header: read %d bytes, want %d", n, headerSize)
		}
		spoolID := string(header[len(spoolFileMagic):])
		parsed, err := uuid.Parse(spoolID)
		if err != nil {
			return "", 0, fmt.Errorf("invalid spool identity %q: %w", spoolID, err)
		}
		return parsed.String(), int64(headerSize), nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return "", 0, fmt.Errorf("inspect spool identity header: %w", err)
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", 0, fmt.Errorf("seek legacy spool file: %w", err)
	}
	hash := sha256.New()
	if _, err := io.WriteString(hash, filepath.Base(path)); err != nil {
		return "", 0, fmt.Errorf("hash legacy spool filename: %w", err)
	}
	if _, err := hash.Write([]byte{0}); err != nil {
		return "", 0, fmt.Errorf("hash legacy spool separator: %w", err)
	}
	if _, err := io.Copy(hash, file); err != nil {
		return "", 0, fmt.Errorf("hash legacy spool payload: %w", err)
	}
	return "legacy:" + hex.EncodeToString(hash.Sum(nil)), 0, nil
}

// Append appends a raw telemetry frame payload to the log and returns its ID.
// This is the synchronous version.
func (w *WAL) Append(ctx context.Context, tFrame *agentv1.TelemetryFrame) (int64, error) {
	query := `INSERT INTO telemetry_frames (created_at, payload, delivery_status) VALUES (?, ?, ?)`
	w.stampGeneration(tFrame)
	encoded, err := proto.Marshal(tFrame)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal telemetry frame: %w", err)
	}

	res, err := w.db.ExecContext(ctx, query, time.Now().UnixNano(), encoded, DeliveryStatusWritten)
	if err != nil {
		return 0, fmt.Errorf("failed to append frame to wal: %w", err)
	}
	return res.LastInsertId()
}

// AppendBatch writes multiple frames in a single transaction.
func (w *WAL) AppendBatch(ctx context.Context, frames []*agentv1.TelemetryFrame) (int64, error) {
	lastID, _, err := w.appendBatch(ctx, frames, "")
	return lastID, err
}

func (w *WAL) appendSpoolBatch(ctx context.Context, spoolID string, frames []*agentv1.TelemetryFrame) (bool, error) {
	if spoolID == "" {
		return false, errors.New("spool identity is required")
	}
	_, imported, err := w.appendBatch(ctx, frames, spoolID)
	return imported, err
}

func (w *WAL) appendBatch(ctx context.Context, frames []*agentv1.TelemetryFrame, spoolID string) (int64, bool, error) {
	if len(frames) == 0 {
		return 0, false, nil
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if spoolID != "" {
		result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO spool_imports(spool_id, imported_at) VALUES(?, ?)`, spoolID, time.Now().UnixNano())
		if err != nil {
			return 0, false, fmt.Errorf("record spool import %q: %w", spoolID, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return 0, false, fmt.Errorf("inspect spool import %q: %w", spoolID, err)
		}
		if rows == 0 {
			return 0, false, nil
		}
	}

	query := `INSERT INTO telemetry_frames (created_at, payload, delivery_status) VALUES (?, ?, ?)`
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return 0, false, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	var lastID int64
	now := time.Now().UnixNano()

	for _, frame := range frames {
		w.stampGeneration(frame)
		encoded, err := proto.Marshal(frame)
		if err != nil {
			return 0, false, fmt.Errorf("failed to marshal frame: %w", err)
		}

		res, err := stmt.ExecContext(ctx, now, encoded, DeliveryStatusWritten)
		if err != nil {
			return 0, false, fmt.Errorf("failed to insert frames: %w", err)
		}
		lastID, err = res.LastInsertId()
		if err != nil {
			return 0, false, fmt.Errorf("failed to inspect inserted frame: %w", err)
		}
	}

	if err := stmt.Close(); err != nil {
		return 0, false, fmt.Errorf("failed to close frame insert statement: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return lastID, true, nil
}

func (w *WAL) stampGeneration(frame *agentv1.TelemetryFrame) {
	if frame != nil {
		frame.WalId = w.generationID
	}
}

// ReadUndelivered reads up to the requested number of written entries in
// sequence order. Malformed legacy frames recorded in the durable quarantine
// are excluded from delivery without deleting their original payloads.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for the query.
//   - limit: bounds returned entries and must be greater than zero.
//
// Returns:
//   - entries: contains deliverable WAL records in ascending sequence order.
//   - error: reports an invalid limit, SQLite failure, or context cancellation.
func (w *WAL) ReadUndelivered(ctx context.Context, limit int) ([]Entry, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	// Only read DeliveryStatusWritten (0). Ignore Pending (1) and Delivered (2).
	query := `
	SELECT seq, created_at, payload
	FROM telemetry_frames
	WHERE delivery_status = ?
	AND NOT EXISTS (
		SELECT 1 FROM telemetry_frame_quarantine
		WHERE telemetry_frame_quarantine.seq = telemetry_frames.seq
	)
	ORDER BY seq ASC
	LIMIT ?
	`
	rows, err := w.db.QueryContext(ctx, query, DeliveryStatusWritten, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query undelivered frames: %w", err)
	}
	defer rows.Close()

	var entries []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.CreatedAt, &e.Payload); err != nil {
			return nil, fmt.Errorf("failed to scan entry: %w", err)
		}
		entries = append(entries, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return entries, nil
}

// CountUndelivered returns the number of written entries eligible for
// delivery, excluding durable quarantine records.
func (w *WAL) CountUndelivered(ctx context.Context) (int64, error) {
	query := `
	SELECT COUNT(1)
	FROM telemetry_frames
	WHERE delivery_status = ?
	AND NOT EXISTS (
		SELECT 1 FROM telemetry_frame_quarantine
		WHERE telemetry_frame_quarantine.seq = telemetry_frames.seq
	)
	`
	var count int64
	if err := w.db.QueryRowContext(ctx, query, DeliveryStatusWritten).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count undelivered frames: %w", err)
	}
	return count, nil
}

// WaitForData blocks until new data is signaled or the context is cancelled.
func (w *WAL) WaitForData(ctx context.Context) error {
	select {
	case <-w.signalChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *WAL) updateDeliveryStatus(ctx context.Context, seq uint64, status DeliveryStatus) (int64, error) {
	// Only update if the status is different to ensure idempotency.
	query := `UPDATE telemetry_frames SET delivery_status = ? WHERE seq = ? AND delivery_status != ?`

	res, err := w.db.ExecContext(ctx, query, status, seq, status)
	if err != nil {
		return 0, fmt.Errorf("failed to update delivery status: %w", err)
	}

	return res.RowsAffected()
}

// MarkDelivered marks a specific log entry as delivered.
func (w *WAL) MarkDelivered(ctx context.Context, seq uint64) (int64, error) {
	return w.updateDeliveryStatus(ctx, seq, DeliveryStatusDelivered)
}

// MarkPending marks one already-sent WAL entry as awaiting Relay acknowledgment
// when its state actually differs, preserving idempotent retry accounting.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for the operation.
//   - seq: identifies the sent WAL entry awaiting acknowledgment.
//
// Returns:
//   - rowsAffected: is one when the state changed and zero for an idempotent call.
//   - error: reports a SQLite update or context failure.
func (w *WAL) MarkPending(ctx context.Context, seq uint64) (int64, error) {
	return w.updateDeliveryStatus(ctx, seq, DeliveryStatusPending)
}

// MarkPendingBatch marks already-sent WAL entries as awaiting Relay
// acknowledgment in one transaction. Individual update failures are logged so
// remaining entries can still be marked; the returned count is currently zero.
//
// Parameters:
//   - ctx: contributes its deadline but not its cancellation signal. Without a
//     deadline, the detached transaction receives an independent two-second timeout.
//   - seqs: identifies the sent WAL entries awaiting acknowledgment.
//
// Returns:
//   - rowsAffected: is currently zero; callers must not use it as a batch count.
//   - error: reports transaction setup or commit failure.
func (w *WAL) MarkPendingBatch(ctx context.Context, seqs []uint64) (int64, error) {
	return w.updateDeliveryStatusBatch(ctx, seqs, DeliveryStatusPending)
}

func (w *WAL) updateDeliveryStatusBatch(ctx context.Context, seqs []uint64, status DeliveryStatus) (int64, error) {
	query := `UPDATE telemetry_frames SET delivery_status = ? WHERE seq=? AND delivery_status != ?`
	// Detach from stream cancellation so the batch can still commit.
	baseCtx := context.WithoutCancel(ctx)
	var txCtx context.Context
	var cancel context.CancelFunc
	if deadline, ok := ctx.Deadline(); ok {
		txCtx, cancel = context.WithDeadline(baseCtx, deadline)
	} else {
		txCtx, cancel = context.WithTimeout(baseCtx, 2*time.Second)
	}
	defer cancel()

	tx, err := w.db.BeginTx(txCtx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(txCtx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, seq := range seqs {
		if _, err := stmt.ExecContext(txCtx, status, seq, status); err != nil {
			slog.Error("failed to update delivery status", "error", err)
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return 0, nil
}

// MarkWritten moves one WAL entry to the written state when its state differs.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for the operation.
//   - seq: identifies the WAL entry whose persistence state is changing.
//
// Returns:
//   - rowsAffected: is one when the state changed and zero for an idempotent call.
//   - error: reports a SQLite update or context failure.
func (w *WAL) MarkWritten(ctx context.Context, seq uint64) (int64, error) {
	return w.updateDeliveryStatus(ctx, seq, DeliveryStatusWritten)
}

// ResetPending resets frames that have been in 'Pending' state for longer than ttl.
// This allows retrying frames that were marked as pending but never acked.
func (w *WAL) ResetPending(ctx context.Context, ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, nil
	}

	// created_at is stored as unix nano
	// We want rows where delivery_status = Pending AND created_at < (now - ttl)
	// Note: created_at is when it was inserted, not when it was marked pending.
	// Since we don't track "updated_at", we use "created_at" as a proxy.
	// If a frame is pending and old enough, we retry it.
	cutoff := time.Now().Add(-ttl).UnixNano()

	query := `
	UPDATE telemetry_frames 
	SET delivery_status = ? 
	WHERE delivery_status = ? 
	AND created_at < ?
	`

	res, err := w.db.ExecContext(ctx, query, DeliveryStatusWritten, DeliveryStatusPending, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to reset pending frames: %w", err)
	}

	rows, err := res.RowsAffected()

	if rows != 0 {
		w.signalChan <- struct{}{}
	}

	return rows, err
}

// CleanupDelivered deletes delivered frames older than the requested retention
// count and reclaims completed spool-import markers that have no live spool
// file. Live-file markers are retained so a pending cleanup retry cannot
// re-import its frames.
//
// Parameters:
//   - ctx: bounds both telemetry deletion and bounded marker reclamation.
//   - retentionCount: keeps this many of the newest delivered frames; negative
//     values are treated as zero.
//
// Returns:
//   - error: reports SQLite, spool-directory, or live-file inspection failures.
func (w *WAL) CleanupDelivered(ctx context.Context, retentionCount int) error {
	if retentionCount < 0 {
		retentionCount = 0
	}

	// Find the ID threshold. We want to keep the last `retentionCount` delivered frames.
	// We delete everything where delivered=1 AND seq < (SELECT min(seq) FROM (SELECT seq FROM telemetry_frames WHERE delivered=1 ORDER BY seq DESC LIMIT retentionCount))
	// Or simpler: DELETE FROM telemetry_frames WHERE delivered=1 AND id NOT IN (SELECT id FROM telemetry_frames WHERE delivered=1 ORDER BY id DESC LIMIT ?)

	query := `
	DELETE FROM telemetry_frames 
	WHERE delivery_status = ? 
	AND NOT EXISTS (
		SELECT 1 FROM telemetry_frame_quarantine
		WHERE telemetry_frame_quarantine.seq = telemetry_frames.seq
	)
	AND seq NOT IN (
		SELECT seq FROM telemetry_frames 
		WHERE delivery_status = ?
		AND NOT EXISTS (
			SELECT 1 FROM telemetry_frame_quarantine
			WHERE telemetry_frame_quarantine.seq = telemetry_frames.seq
		)
		ORDER BY seq DESC 
		LIMIT ?
	)`

	_, err := w.db.ExecContext(ctx, query, DeliveryStatusDelivered, DeliveryStatusDelivered, retentionCount)
	if err != nil {
		return fmt.Errorf("failed to cleanup delivered frames: %w", err)
	}
	if err := w.cleanupOrphanedSpoolImports(ctx); err != nil {
		return fmt.Errorf("cleanup spool imports: %w", err)
	}
	return nil
}

// CleanupQuarantined explicitly deletes older malformed legacy frames while
// retaining the requested number of newest quarantine records. Quarantined
// payloads are otherwise preserved indefinitely and CleanupDelivered never
// removes them.
//
// Parameters:
//   - ctx: controls cancellation and deadlines for each bounded transaction.
//   - retentionCount: keeps this many newest quarantined rows; negative values
//     are treated as zero.
//
// Returns:
//   - rowsDeleted: is the number of quarantined telemetry payloads removed,
//     including committed batches if a later batch fails.
//   - error: reports bounded-batch transaction, SQLite, or context failures.
func (w *WAL) CleanupQuarantined(ctx context.Context, retentionCount int) (int64, error) {
	if retentionCount < 0 {
		retentionCount = 0
	}

	var totalDeleted int64
	for {
		selected, deleted, err := w.cleanupQuarantinedBatch(ctx, retentionCount)
		if err != nil {
			return totalDeleted, err
		}
		totalDeleted += deleted
		if selected < quarantineCleanupBatchSize {
			return totalDeleted, nil
		}
	}
}

// CleanupSpoolQuarantine explicitly deletes older malformed spool files while
// retaining the requested number of newest artifacts. Normal spool draining
// never removes quarantined files.
//
// Parameters:
//   - ctx: stops cleanup between individual file removals when cancelled.
//   - retentionCount: keeps this many newest artifacts; negative values are
//     treated as zero.
//
// Returns:
//   - filesDeleted: is the number of quarantine artifacts removed.
//   - error: joins file removal and context failures after making
//     as much progress as possible.
func (w *WAL) CleanupSpoolQuarantine(ctx context.Context, retentionCount int) (int, error) {
	if retentionCount < 0 {
		retentionCount = 0
	}

	w.spoolMu.Lock()
	defer w.spoolMu.Unlock()

	entries, err := os.ReadDir(w.spoolQuarantineDir)
	if err != nil {
		return 0, fmt.Errorf("read spool quarantine dir: %w", err)
	}
	artifacts := make([]string, 0, len(entries))
	var cleanupErr error
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		artifacts = append(artifacts, entry.Name())
	}
	sort.Slice(artifacts, func(i, j int) bool {
		return artifacts[i] > artifacts[j]
	})

	filesDeleted := 0
	for _, name := range artifacts[min(retentionCount, len(artifacts)):] {
		if err := ctx.Err(); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			break
		}
		path := filepath.Join(w.spoolQuarantineDir, name)
		if err := w.removeFile(path); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete spool quarantine artifact %s: %w", path, err))
			continue
		}
		filesDeleted++
	}
	if filesDeleted > 0 {
		if err := syncDir(w.spoolQuarantineDir); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("sync spool quarantine cleanup: %w", err))
		}
	}
	return filesDeleted, cleanupErr
}

func (w *WAL) cleanupQuarantinedBatch(ctx context.Context, retentionCount int) (int64, int64, error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin quarantined frame cleanup: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	rows, err := tx.QueryContext(ctx, `SELECT telemetry_frame_quarantine.seq
		FROM telemetry_frame_quarantine
		JOIN telemetry_frames USING(seq)
		ORDER BY quarantined_at DESC, telemetry_frame_quarantine.seq DESC
		LIMIT ? OFFSET ?`, quarantineCleanupBatchSize, retentionCount)
	if err != nil {
		return 0, 0, fmt.Errorf("select quarantined telemetry cleanup batch: %w", err)
	}
	seqs := make([]int64, 0, quarantineCleanupBatchSize)
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return 0, 0, closeQuarantineRows(rows, fmt.Errorf("scan quarantined telemetry cleanup batch: %w", err))
		}
		seqs = append(seqs, seq)
	}
	if err := rows.Err(); err != nil {
		return 0, 0, closeQuarantineRows(rows, fmt.Errorf("iterate quarantined telemetry cleanup batch: %w", err))
	}
	if err := closeQuarantineRows(rows, nil); err != nil {
		return 0, 0, err
	}

	var rowsDeleted int64
	for _, seq := range seqs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM telemetry_frame_quarantine WHERE seq = ?`, seq); err != nil {
			return 0, 0, fmt.Errorf("delete quarantined frame %d diagnostics: %w", seq, err)
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM telemetry_frames WHERE seq = ?`, seq)
		if err != nil {
			return 0, 0, fmt.Errorf("delete quarantined telemetry frame %d: %w", seq, err)
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			return 0, 0, fmt.Errorf("inspect quarantined telemetry frame %d cleanup: %w", seq, err)
		}
		rowsDeleted += deleted
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit quarantined frame cleanup: %w", err)
	}
	return int64(len(seqs)), rowsDeleted, nil
}

func closeQuarantineRows(rows *sql.Rows, cause error) error {
	if err := rows.Close(); err != nil {
		return errors.Join(cause, fmt.Errorf("close quarantined telemetry cleanup rows: %w", err))
	}
	return cause
}

// Close durably spools any in-memory asynchronous batch, stops and waits for
// the background writer, and then closes the database. Existing spool files
// are left for the next open to import under its new append generation.
//
// Returns:
//   - error: reports failure to close the underlying SQLite connection.
func (w *WAL) Close() error {
	w.closeOnce.Do(func() {
		if w.writerDone != nil {
			close(w.doneChan)
			<-w.writerDone
		}
		w.closeErr = w.db.Close()
	})
	return w.closeErr
}
