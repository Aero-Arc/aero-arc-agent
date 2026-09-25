package wal

import (
	"context"
	"fmt"

	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"google.golang.org/protobuf/proto"
)

// CommandRecord is an immutable command and its durable execution evidence.
type CommandRecord struct {
	Digest            string
	Payload, Evidence []byte
	EffectStarted     bool
}

// LoadCommand reads command identity and evidence. Missing IDs return sql.ErrNoRows.
//
// Parameters: ctx bounds reads; id selects stable identity.
//
// Returns: The persisted record, sql.ErrNoRows, or a SQLite error.
func (w *WAL) LoadCommand(ctx context.Context, id string) (CommandRecord, error) {
	var r CommandRecord
	err := w.db.QueryRowContext(ctx, `SELECT digest,payload,evidence,effect_started FROM c2_commands WHERE command_id=?`, id).Scan(&r.Digest, &r.Payload, &r.Evidence, &r.EffectStarted)
	return r, err
}

// AdmitCommand persists immutable authority and initial evidence before execution.
// Conflicting IDs fail. Exact duplicates preserve the original evidence and effect fence.
//
// Parameters: ctx bounds persistence; id selects identity; r carries digest, payload, and initial evidence.
//
// Returns: Nil for admission or exact replay; conflicting identity and storage errors fail closed.
func (w *WAL) AdmitCommand(ctx context.Context, id string, r CommandRecord) error {
	_, err := w.db.ExecContext(ctx, `INSERT INTO c2_commands(command_id,digest,payload,evidence) VALUES(?,?,?,?) ON CONFLICT(command_id) DO NOTHING`, id, r.Digest, r.Payload, r.Evidence)
	if err != nil {
		return err
	}
	existing, err := w.LoadCommand(ctx, id)
	if err != nil {
		return err
	}
	if existing.Digest != r.Digest {
		return fmt.Errorf("command identity conflict")
	}
	return nil
}

// SaveCommand atomically records evidence and the irreversible first-effect fence.
// A true effect flag can never revert. The caller serializes execution per aircraft.
//
// Parameters: ctx bounds the transaction; id and digest bind evidence; evidence contains immutable protobuf events; effect preserves the irreversible fence.
//
// Returns: Nil after atomic merge; conflicting evidence, identity mismatch, and storage errors roll back.
func (w *WAL) SaveCommand(ctx context.Context, id, digest string, evidence []byte, effect bool) error {
	incoming := &pb.CommandEvidence{}
	if err := proto.Unmarshal(evidence, incoming); err != nil {
		return err
	}
	if incoming.CommandId != id || incoming.CommandDigest != digest {
		return fmt.Errorf("evidence identity mismatch")
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var previous []byte
	err = tx.QueryRowContext(ctx, `UPDATE c2_commands SET effect_started=MAX(effect_started,?) WHERE command_id=? AND digest=? RETURNING evidence`, effect, id, digest).Scan(&previous)
	if err != nil {
		return err
	}
	merged := &pb.CommandEvidence{}
	if err = proto.Unmarshal(previous, merged); err != nil {
		return err
	}
	for _, event := range incoming.Events {
		found := false
		for _, old := range merged.Events {
			if old.EventId == event.EventId {
				if !proto.Equal(old, event) {
					return fmt.Errorf("immutable command event conflict")
				}
				found = true
				break
			}
		}
		if !found {
			merged.Events = append(merged.Events, event)
		}
	}
	encoded, err := proto.Marshal(merged)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE c2_commands SET evidence=? WHERE command_id=? AND digest=?`, encoded, id, digest); err != nil {
		return err
	}
	return tx.Commit()
}

// CommandIsLatest prevents late observations from being attributed across a newer
// command admission on this single-aircraft Agent journal.
//
// Parameters: ctx bounds reads; id selects an admitted command.
//
// Returns: Whether no newer command has been admitted, or a SQLite error.
func (w *WAL) CommandIsLatest(ctx context.Context, id string) (bool, error) {
	var latest bool
	err := w.db.QueryRowContext(ctx, `SELECT rowid=(SELECT MAX(rowid) FROM c2_commands) FROM c2_commands WHERE command_id=?`, id).Scan(&latest)
	return latest, err
}

// BeginCommandEffect atomically grants the sole first-effect permit across WAL
// users. False means an effect may already have begun and must never be repeated.
//
// Parameters: ctx bounds the atomic update; id and digest select exact authority.
//
// Returns: True only for the first permit, false when consumed or absent, or a SQLite error.
func (w *WAL) BeginCommandEffect(ctx context.Context, id, digest string) (bool, error) {
	result, err := w.db.ExecContext(ctx, `UPDATE c2_commands SET effect_started=1 WHERE command_id=? AND digest=? AND effect_started=0`, id, digest)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}
