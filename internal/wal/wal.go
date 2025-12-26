package wal

import (
	"context"
	"database/sql"
	"fmt"
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
	batchSize    int64
	batchTimeout time.Duration
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

	wal := &WAL{
		db:           db,
		doneChan:     make(chan struct{}),
		batchChan:    make(chan *agentv1.TelemetryFrame),
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
	}

	return wal, nil
}

func configureDB(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=FULL;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA busy_timeout=5000;",
	}

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

func (w *WAL) AppendAsync(ctx context.Context, tFrame *agentv1.TelemetryFrame) error {
	select {
	case w.batchChan <- tFrame:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// TODO: Figure out if we should drop or other backpressure methods
		return nil
	}
}

func (w *WAL) runBatchWriter() {
	var batch []*agentv1.TelemetryFrame
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		if _, err := w.AppendBatch(context.Background(), batch); err != nil {
			// TODO: prolly should retry, dont panic, but need some sort of retry
			fmt.Printf("WAL Batch Write Failed: %w", err)
		}

		batch = nil
	}

	for {
		select {
		case frame := <-w.batchChan:
			batch = append(batch, frame)
			if len(batch) >= int(w.batchSize) {
				flush()
				ticker.Reset(w.batchTimeout)
			}
		case <-ticker.C:
			flush()
		case <-w.doneChan:
			flush()
			return
		}
	}
}

// Append appends a raw telemetry frame payload to the log and returns its ID.
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

func (w *WAL) AppendBatch(ctx context.Context, frames []*agentv1.TelemetryFrame) (int64, error) {
	if len(frames) == 0 {
		return 0, nil
	}

	// start the transaction
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction")
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
			return 0, fmt.Errorf("failed to marshal frame")
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
	query := `
	SELECT seq, created_at, payload
	FROM telemetry_frames
	WHERE delivery_status < ?
	ORDER BY seq ASC
	LIMIT ?
	`
	rows, err := w.db.QueryContext(ctx, query, DeliveryStatusDelivered, limit)
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

func (w *WAL) MarkWritten(ctx context.Context, seq uint64) (int64, error) {
	return w.updateDeliveryStatus(ctx, seq, DeliveryStatusWritten)
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
	return w.db.Close()
}
