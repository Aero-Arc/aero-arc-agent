package wal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

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
	db *sql.DB
}

// New creates or opens a WAL at the specified path.
func New(path string) (*WAL, error) {
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

	return &WAL{db: db}, nil
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
	query := `
	CREATE TABLE IF NOT EXISTS telemetry_frames (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at INTEGER NOT NULL,
		payload BLOB NOT NULL,
		delivered INTEGER NOT NULL DEFAULT 0
	);
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	indexQuery := `
	CREATE INDEX IF NOT EXISTS idx_telemetry_undelivered
	ON telemetry_frames (delivered, id);
	`
	_, err = db.Exec(indexQuery)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	return nil
}

// Append appends a raw telemetry frame payload to the log and returns its ID.
func (w *WAL) Append(ctx context.Context, payload []byte) (int64, error) {
	query := `INSERT INTO telemetry_frames (created_at, payload, delivered) VALUES (?, ?, 0)`
	res, err := w.db.ExecContext(ctx, query, time.Now().UnixNano(), payload)
	if err != nil {
		return 0, fmt.Errorf("failed to append frame to wal: %w", err)
	}
	return res.LastInsertId()
}

// ReadUndelivered reads up to limit undelivered entries from the log.
func (w *WAL) ReadUndelivered(ctx context.Context, limit int) ([]Entry, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	query := `
	SELECT id, created_at, payload
	FROM telemetry_frames
	WHERE delivered = 0
	ORDER BY id ASC
	LIMIT ?
	`
	rows, err := w.db.QueryContext(ctx, query, limit)
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

// MarkDelivered marks a specific log entry as delivered.
func (w *WAL) MarkDelivered(ctx context.Context, id int64) error {
	query := `UPDATE telemetry_frames SET delivered = 1 WHERE id = ? AND delivered = 0`
	res, err := w.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to mark frame %d delivered: %w", id, err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		// This is not necessarily an error - it could have been marked delivered already
		// or concurrently by another goroutine (if we had concurrency).
		// For now, we return nil as the goal "ensure it is marked delivered" is met.
		// If debugging is needed, we could return a specific error or bool.
		return nil
	}

	return nil
}

// CleanupDelivered deletes delivered frames that are older than the specified retention count.
// It ensures that at most `retentionCount` delivered frames remain in the WAL.
// This is a basic form of garbage collection to prevent unbounded growth.
func (w *WAL) CleanupDelivered(ctx context.Context, retentionCount int) error {
	if retentionCount < 0 {
		retentionCount = 0
	}

	// Find the ID threshold. We want to keep the last `retentionCount` delivered frames.
	// We delete everything where delivered=1 AND id < (SELECT min(id) FROM (SELECT id FROM telemetry_frames WHERE delivered=1 ORDER BY id DESC LIMIT retentionCount))
	// Or simpler: DELETE FROM telemetry_frames WHERE delivered=1 AND id NOT IN (SELECT id FROM telemetry_frames WHERE delivered=1 ORDER BY id DESC LIMIT ?)

	query := `
	DELETE FROM telemetry_frames 
	WHERE delivered = 1 
	AND id NOT IN (
		SELECT id FROM telemetry_frames 
		WHERE delivered = 1 
		ORDER BY id DESC 
		LIMIT ?
	)`

	_, err := w.db.ExecContext(ctx, query, retentionCount)
	if err != nil {
		return fmt.Errorf("failed to cleanup delivered frames: %w", err)
	}
	return nil
}

// Close closes the underlying database connection.
func (w *WAL) Close() error {
	return w.db.Close()
}
