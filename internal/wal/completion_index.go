// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package wal

import (
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/aero-arc/aero-arc-protos/missiondigest"
	"log/slog"
)

// ensureFlightWatchIndex backfills bounded batches once, retaining original
// payloads. Indexed metadata keeps historical mission JSON off the ingest path.
// Corrupt legacy records retain a quarantine reason. An unknowable target blocks
// attribution until repaired; known unrelated targets remain isolated.
func ensureFlightWatchIndex(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS flight_watch_index(
 flight_id TEXT PRIMARY KEY,target TEXT NOT NULL,done INTEGER NOT NULL,
 quarantine_reason TEXT NOT NULL DEFAULT '');
 CREATE INDEX IF NOT EXISTS flight_watch_target_done ON flight_watch_index(target,done);
 CREATE INDEX IF NOT EXISTS flight_watch_quarantined ON flight_watch_index(target) WHERE quarantine_reason<>'';`); err != nil {
		return err
	}
	var versionColumn int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('flight_watch_index') WHERE name='validation_version'`).Scan(&versionColumn); err != nil {
		return err
	}
	if versionColumn == 0 {
		if _, err := db.Exec(`ALTER TABLE flight_watch_index ADD COLUMN validation_version INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	for {
		n, err := backfillFlightWatchIndex(db)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

func backfillFlightWatchIndex(db *sql.DB) (int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.Query(`SELECT w.flight_id,w.start_command_id,w.payload FROM flight_watches w LEFT JOIN flight_watch_index i ON i.flight_id=w.flight_id WHERE i.flight_id IS NULL OR i.validation_version<1 LIMIT 32`)
	if err != nil {
		return 0, err
	}
	type entry struct {
		id, target, reason string
		done               bool
	}
	var entries []entry
	for rows.Next() {
		var id, start string
		var raw []byte
		if err = rows.Scan(&id, &start, &raw); err != nil {
			_ = rows.Close()
			return 0, err
		}
		var watch FlightWatch
		validation := json.Unmarshal(raw, &watch)
		if validation == nil && (watch.Command.GetCommandId() != start || watch.Command.GetContext().GetFlightId() != id || watch.Target == "") {
			validation = errors.New("flight watch authority or target mismatch")
		}
		if validation == nil {
			validation = validateFlightWatchStructure(watch)
		}
		e := entry{id: id, target: watch.Target, done: watch.Done}
		if validation != nil {
			// Partially decoded data cannot establish authority. Recover only routing
			// metadata from independently valid JSON, keeping the payload quarantined.
			var header struct {
				Target string `json:"target"`
				Done   bool   `json:"done"`
			}
			if json.Unmarshal(raw, &header) == nil {
				e.target = header.Target
				e.done = header.Done
			} else {
				e.target = ""
				e.done = false
			}
			e.reason = validation.Error()
		}
		entries = append(entries, e)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if _, err = tx.Exec(`INSERT INTO flight_watch_index(flight_id,target,done,quarantine_reason,validation_version) VALUES(?,?,?,?,1) ON CONFLICT(flight_id) DO UPDATE SET target=excluded.target,done=excluded.done,quarantine_reason=excluded.quarantine_reason,validation_version=1`, e.id, e.target, e.done, e.reason); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	for _, e := range entries {
		if e.reason != "" {
			slog.Error("flight watch quarantined; operator repair required", "flight_id", e.id, "reason", e.reason)
		}
	}
	return len(entries), nil
}

func validateFlightWatchStructure(watch FlightWatch) error {
	m := watch.Command.GetMavlink()
	if m == nil || m.MissionPrecondition == nil || len(m.MissionPrecondition.Items) == 0 {
		return errors.New("flight watch lacks terminal mission structure")
	}
	items := m.MissionPrecondition.Items
	last := items[len(items)-1].GetCommand()
	if last != 20 && last != 21 {
		return errors.New("flight watch mission does not end in RTL or LAND")
	}
	_, err := missiondigest.Digest(m.MissionPrecondition)
	return err
}
