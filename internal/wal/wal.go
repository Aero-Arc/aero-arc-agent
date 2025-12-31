package wal

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

// Entry represents a single log entry in the WAL.
type Entry struct {
	ID        int64
	Payload   []byte
	CreatedAt int64
}

// WAL implements a Write-Ahead Log using SQLite.
type WAL struct {
	db           *sql.DB
	doneChan     chan struct{}
	batchChan    chan *agentv1.TelemetryFrame
	signalChan   chan struct{}
	batchSize    int64
	batchTimeout time.Duration
	spoolDir     string
	spoolSeq     uint64
}

// New creates or opens a WAL at the specified path.
// TODO: Add time.Duration for the WAL cleanup interval.
func New(path string, batchSize int64, batchTimeout time.Duration) (*WAL, error) {
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

	// Default values if not provided
	if batchSize <= 0 {
		batchSize = 100
	}
	if batchTimeout <= 0 {
		batchTimeout = 100 * time.Millisecond
	}

	wal := &WAL{
		db:           db,
		doneChan:     make(chan struct{}),
		batchChan:    make(chan *agentv1.TelemetryFrame, batchSize*2), // Buffer a bit more than one batch
		signalChan:   make(chan struct{}, 1),                          // Buffer 1 to prevent blocking
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		spoolDir:     path + ".spool",
	}

	if err := os.MkdirAll(wal.spoolDir, 0o755); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create spool dir: %w", err)
	}

	// Start the background writer
	go wal.runBatchWriter()

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
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	indexQuery := `
	CREATE INDEX IF NOT EXISTS idx_telemetry_undelivered
	ON telemetry_frames (delivery_status, seq);
	`
	_, err = db.Exec(indexQuery)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	return nil
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

func (w *WAL) runBatchWriter() {
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

	for {
		if flushPending {
			if err := flush(); err != nil {
				slog.Error("WAL Batch Spool Failed", "error", err)
				select {
				case <-time.After(retryDelay):
					continue
				case <-w.doneChan:
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
		case <-w.doneChan:
			if err := flush(); err != nil {
				slog.Error("WAL Batch Spool Failed", "error", err)
			}
			if err := w.drainSpool(); err != nil {
				slog.Error("WAL Spool Drain Failed", "error", err)
			}
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
			slog.Warn("failed to marshal frame for spool, skipping", "error", err)
			continue
		}
		if len(encoded) > int(^uint32(0)) {
			return "", fmt.Errorf("spool frame too large: %d", len(encoded))
		}
		payloads = append(payloads, encoded)
	}

	if len(payloads) == 0 {
		return "", nil
	}

	seq := atomic.AddUint64(&w.spoolSeq, 1)
	name := fmt.Sprintf("%020d-%06d.batch", time.Now().UnixNano(), seq)
	path := filepath.Join(w.spoolDir, name)
	tmpPath := path + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("failed to create spool file: %w", err)
	}

	cleanup := true
	defer func() {
		if cleanup {
			file.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	writer := bufio.NewWriter(file)
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
	entries, err := os.ReadDir(w.spoolDir)
	if err != nil {
		return fmt.Errorf("failed to read spool dir: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	wrote := false
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".batch" {
			continue
		}
		path := filepath.Join(w.spoolDir, entry.Name())
		frames, err := readSpoolFile(path)
		if err != nil {
			return fmt.Errorf("failed to read spool file %s: %w", path, err)
		}
		if len(frames) == 0 {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("failed to remove empty spool file: %w", err)
			}
			continue
		}
		if _, err := w.AppendBatch(context.Background(), frames); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to remove spool file: %w", err)
		}
		wrote = true
	}

	if wrote {
		select {
		case w.signalChan <- struct{}{}:
		default:
		}
	}

	return nil
}

func readSpoolFile(path string) ([]*agentv1.TelemetryFrame, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var frames []*agentv1.TelemetryFrame
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("truncated spool record: %w", err)
			}
			return nil, err
		}

		length := binary.LittleEndian.Uint32(lenBuf[:])
		if length == 0 {
			continue
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("truncated spool payload: %w", err)
		}

		var frame agentv1.TelemetryFrame
		if err := proto.Unmarshal(payload, &frame); err != nil {
			slog.Warn("failed to unmarshal spool frame, skipping", "error", err)
			continue
		}
		frames = append(frames, &frame)
	}

	return frames, nil
}

// Append appends a raw telemetry frame payload to the log and returns its ID.
// This is the synchronous version.
func (w *WAL) Append(ctx context.Context, tFrame *agentv1.TelemetryFrame) (int64, error) {
	query := `INSERT INTO telemetry_frames (created_at, payload, delivery_status) VALUES (?, ?, ?)`
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
	if len(frames) == 0 {
		return 0, nil
	}

	// start the transaction
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// defer rollback. If Commit() is called then RollBack is a no-op
	defer tx.Rollback()

	// prepare the statement
	query := `INSERT INTO telemetry_frames (created_at, payload, delivery_status) VALUES (?, ?, ?)`
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}

	defer stmt.Close()

	var lastID int64
	now := time.Now().UnixNano()

	// loop and insert
	for _, frame := range frames {
		encoded, err := proto.Marshal(frame)
		if err != nil {
			// Skip malformed frames instead of failing the whole batch
			slog.Warn("failed to marshal frame, skipping", "error", err)
			continue
		}

		// Execute against the *statement* (which is bound to the transaction)
		res, err := stmt.ExecContext(ctx, now, encoded, DeliveryStatusWritten)
		if err != nil {
			return 0, fmt.Errorf("failed to insert frames: %w", err)
		}

		lastID, _ = res.LastInsertId()
	}

	// commit
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return lastID, nil
}

// ReadUndelivered reads up to limit undelivered entries from the log.
func (w *WAL) ReadUndelivered(ctx context.Context, limit int) ([]Entry, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	// Only read DeliveryStatusWritten (0). Ignore Pending (1) and Delivered (2).
	query := `
	SELECT seq, created_at, payload
	FROM telemetry_frames
	WHERE delivery_status = ?
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

func (w *WAL) MarkPending(ctx context.Context, seq uint64) (int64, error) {
	return w.updateDeliveryStatus(ctx, seq, DeliveryStatusPending)
}

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

	return res.RowsAffected()
}

// CleanupDelivered deletes delivered frames that are older than the specified retention count.
// It ensures that at most `retentionCount` delivered frames remain in the WAL.
// This is a basic form of garbage collection to prevent unbounded growth.
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
	AND seq NOT IN (
		SELECT seq FROM telemetry_frames 
		WHERE delivery_status = ? 
		ORDER BY seq DESC 
		LIMIT ?
	)`

	_, err := w.db.ExecContext(ctx, query, DeliveryStatusDelivered, DeliveryStatusDelivered, retentionCount)
	if err != nil {
		return fmt.Errorf("failed to cleanup delivered frames: %w", err)
	}
	return nil
}

// Close closes the underlying database connection.
func (w *WAL) Close() error {
	close(w.doneChan) // Signal writer to stop
	return w.db.Close()
}
