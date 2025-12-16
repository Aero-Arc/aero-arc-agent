package wal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

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

	if err := initDB(db); err != nil {
		db.Close()
		return nil, err
	}

	return &WAL{db: db}, nil
}

func initDB(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS telemetry_frames (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp INTEGER NOT NULL,
		payload BLOB NOT NULL
	);
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	return nil
}

// Write appends a raw telemetry frame payload to the log.
func (w *WAL) Write(ctx context.Context, payload []byte) error {
	query := `INSERT INTO telemetry_frames (timestamp, payload) VALUES (?, ?)`
	_, err := w.db.ExecContext(ctx, query, time.Now().UnixNano(), payload)
	if err != nil {
		return fmt.Errorf("failed to write frame to wal: %w", err)
	}
	return nil
}

// Close closes the underlying database connection.
func (w *WAL) Close() error {
	return w.db.Close()
}

