package wal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aero-arc/aero-arc-protos/flightcompletion"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// FlightWatch retains flight milestones across restarts; fresh ground samples
// are deliberately process-local so a restart cannot combine stale observations.
type FlightWatch struct {
	// HandoffAt fences observations captured before the successful mission-start write.
	HandoffAt  int64              `json:"handoff_at"`
	Command    *pb.DurableCommand `json:"command"`
	AirborneAt int64              `json:"airborne_at"`
	TerminalAt int64              `json:"terminal_at"`
	Outcome    string             `json:"outcome"`
	Done       bool               `json:"done"`
}

// MarshalJSON preserves the protobuf execution oneof in the persisted watch.
//
// Parameters: the receiver supplies command authority, post-handoff boundary,
// persisted milestones, and completion status; this method takes no arguments.
// Returns: JSON bytes with protobuf-aware command encoding, or a protobuf/JSON
// encoding error. It performs no storage writes and does not change the receiver.
func (v FlightWatch) MarshalJSON() ([]byte, error) {
	command, err := protojson.Marshal(v.Command)
	if err != nil {
		return nil, err
	}
	type plain FlightWatch
	return json.Marshal(struct {
		*plain
		Command json.RawMessage `json:"command"`
	}{plain: (*plain)(&v), Command: command})
}

// UnmarshalJSON restores protobuf execution variants from a persisted watch.
//
// Parameters: raw contains persisted watch JSON, including the nested protobuf
// command encoding. Missing handoff_at decodes as zero and cannot authorize
// automatic completion from a legacy watch.
// Returns: nil after decoding the watch and command oneof, or a JSON/protobuf
// decoding error. The receiver may be partially populated on error and callers
// must discard it; this method never changes the stored payload or repairs data.
func (v *FlightWatch) UnmarshalJSON(raw []byte) error {
	type plain FlightWatch
	data := struct {
		*plain
		Command json.RawMessage `json:"command"`
	}{plain: (*plain)(v)}
	if err := json.Unmarshal(raw, &data); err != nil {
		return err
	}
	v.Command = new(pb.DurableCommand)
	return protojson.Unmarshal(data.Command, v.Command)
}

// BeginFlightWatch binds automatic completion to a verified mission-start command.
// Existing flight authority is immutable, including after process restart.
//
// Parameters: ctx bounds the SQLite transaction; c is the mission-start authority
// containing exact flight context and a verified mission precondition.
// Returns: nil after creating/replaying the watch, or as a no-op when no mission
// precondition or terminal RTL/LAND exists. Different start authority can replace
// only a rejected, never-airborne watch; active/applied/unresolved predecessors,
// malformed persisted state, encoding errors, and SQLite failures return errors.
func (w *WAL) BeginFlightWatch(ctx context.Context, c *pb.DurableCommand) error {
	m := c.GetMavlink()
	if m == nil || m.MissionPrecondition == nil || len(m.MissionPrecondition.Items) == 0 {
		return nil
	}
	items := m.MissionPrecondition.Items
	last := items[len(items)-1].Command
	if last != 20 && last != 21 {
		return nil
	}
	raw, err := json.Marshal(FlightWatch{Command: c})
	if err != nil {
		return err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `INSERT INTO flight_watches(flight_id,start_command_id,payload) VALUES(?,?,?) ON CONFLICT(flight_id) DO NOTHING`, c.Context.FlightId, c.CommandId, raw); err != nil {
		return err
	}
	var previous []byte
	if err = tx.QueryRowContext(ctx, `SELECT payload FROM flight_watches WHERE flight_id=?`, c.Context.FlightId).Scan(&previous); err != nil {
		return err
	}
	var old FlightWatch
	if err = json.Unmarshal(previous, &old); err != nil {
		return err
	}
	if old.Command.CommandId == c.CommandId {
		return tx.Commit()
	}
	if old.AirborneAt != 0 || old.TerminalAt != 0 || old.Done {
		return errors.New("flight already bound to another mission start")
	}
	var evidence []byte
	if err = tx.QueryRowContext(ctx, `SELECT evidence FROM c2_commands WHERE command_id=?`, old.Command.CommandId).Scan(&evidence); err != nil {
		return err
	}
	var outcome pb.CommandEvidence
	if err = proto.Unmarshal(evidence, &outcome); err != nil {
		return err
	}
	rejected := false
	for _, e := range outcome.Events {
		if e.Stage == "rejected" {
			rejected = true
		}
		if e.Stage == "applied" {
			return errors.New("previous mission start was applied")
		}
	}
	if !rejected {
		return errors.New("previous mission start outcome is unresolved")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE flight_watches SET start_command_id=?,payload=? WHERE flight_id=?`, c.CommandId, raw, c.Context.FlightId); err != nil {
		return err
	}
	return tx.Commit()
}

// LoadFlightWatch restores an exact flight's persisted completion milestones.
//
// Parameters: ctx bounds reads; flightID selects the immutable watch binding.
// Returns: the decoded watch, sql.ErrNoRows if absent, or a storage/decoding error.
func (w *WAL) LoadFlightWatch(ctx context.Context, flightID string) (FlightWatch, error) {
	var raw []byte
	var v FlightWatch
	if err := w.db.QueryRowContext(ctx, `SELECT payload FROM flight_watches WHERE flight_id=?`, flightID).Scan(&raw); err != nil {
		return v, err
	}
	err := json.Unmarshal(raw, &v)
	return v, err
}

// SaveFlightWatch atomically persists milestones and, when present, the immutable
// completion delivery obligation. Failed writes never mark a flight done.
//
// Parameters: ctx bounds the transaction; v carries the exact flight/start binding
// and updated milestones; e is optional validated immutable completion evidence.
// Returns: nil after both records commit, or a binding, digest conflict, evidence
// validation, encoding, or storage error with the transaction rolled back.
func (w *WAL) SaveFlightWatch(ctx context.Context, v FlightWatch, e *pb.FlightCompletionEvidence) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if e != nil {
		raw, digest, err := flightcompletion.Encode(e)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO flight_completion_events(event_id,digest,payload) VALUES(?,?,?) ON CONFLICT(event_id) DO NOTHING`, e.EventId, digest, raw); err != nil {
			return err
		}
		var saved string
		if err = tx.QueryRowContext(ctx, `SELECT digest FROM flight_completion_events WHERE event_id=?`, e.EventId).Scan(&saved); err != nil {
			return err
		}
		if saved != digest {
			return fmt.Errorf("immutable completion conflict")
		}
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE flight_watches SET payload=? WHERE flight_id=? AND start_command_id=?`, raw, v.Command.Context.FlightId, v.Command.CommandId)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("flight watch changed")
	}
	return tx.Commit()
}

// PendingFlightCompletions returns bounded unacknowledged events for replay.
//
// Parameters: ctx bounds the read/quarantine transaction.
// Returns: a bounded admission-ordered page of valid events, or a storage error.
// Malformed protobufs, invalid evidence, and identity/digest mismatches are
// durably quarantined with a reason and logged; their original bytes and pending
// delivery status are preserved for operator repair. They do not block later
// events or terminate telemetry streaming. Reading never acknowledges an event.
func (w *WAL) PendingFlightCompletions(ctx context.Context) ([]*pb.FlightCompletionEvidence, error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT event_id,digest,payload FROM flight_completion_events e WHERE delivered=0 AND NOT EXISTS(SELECT 1 FROM flight_completion_quarantine q WHERE q.event_id=e.event_id) ORDER BY rowid LIMIT 32`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	type invalid struct{ id, reason string }
	var corrupt []invalid
	var result []*pb.FlightCompletionEvidence
	for rows.Next() {
		var id, digest string
		var raw []byte
		if err = rows.Scan(&id, &digest, &raw); err != nil {
			return nil, err
		}
		e := new(pb.FlightCompletionEvidence)
		validationErr := proto.Unmarshal(raw, e)
		if validationErr == nil {
			_, actual, encodeErr := flightcompletion.Encode(e)
			validationErr = encodeErr
			if validationErr == nil && (e.EventId != id || actual != digest) {
				validationErr = errors.New("completion identity or digest mismatch")
			}
		}
		if validationErr != nil {
			corrupt = append(corrupt, invalid{id, validationErr.Error()})
			continue
		}
		result = append(result, e)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	for _, v := range corrupt {
		if _, err = tx.ExecContext(ctx, `INSERT INTO flight_completion_quarantine(event_id,reason,quarantined_at) VALUES(?,?,?) ON CONFLICT(event_id) DO NOTHING`, v.id, v.reason, time.Now().UnixNano()); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	for _, v := range corrupt {
		slog.Error("flight completion quarantined; operator repair required", "event_id", v.id, "reason", v.reason)
	}
	return result, nil
}

// RecordFlightWatchHandoff persists the post-write boundary before start is applied.
//
// Parameters: ctx bounds storage; c is the exact mission-start authority; at is
// the local capture-clock time immediately after successful MAVLink handoff.
// Returns: nil for a persisted exact binding, an identical replay, or an absent
// watch (missions without terminal recovery); errors for a changed binding/time,
// invalid timestamp, or storage failure. A missing boundary never enables
// completion observations, including watches written by older Agent versions.
func (w *WAL) RecordFlightWatchHandoff(ctx context.Context, c *pb.DurableCommand, at int64) error {
	if at <= 0 {
		return errors.New("positive handoff time required")
	}
	v, err := w.LoadFlightWatch(ctx, c.Context.FlightId)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if v.Command.CommandId != c.CommandId {
		return errors.New("flight watch handoff binding mismatch")
	}
	if v.HandoffAt != 0 && v.HandoffAt != at {
		return errors.New("flight watch handoff is immutable")
	}
	v.HandoffAt = at
	return w.SaveFlightWatch(ctx, v, nil)
}

// AcknowledgeFlightCompletion retires only a receipt matching persisted content.
//
// Parameters: ctx bounds persistence; r binds the event ID and encoded digest
// acknowledged by Relay only after its durable outbox commit.
// Returns: nil after exact acknowledgement or duplicate receipt; nil, unknown,
// mismatched receipts and storage errors leave the delivery obligation retained.
func (w *WAL) AcknowledgeFlightCompletion(ctx context.Context, r *pb.FlightCompletionReceipt) error {
	if r == nil {
		return errors.New("completion receipt required")
	}
	result, err := w.db.ExecContext(ctx, `UPDATE flight_completion_events SET delivered=1 WHERE event_id=? AND digest=?`, r.EventId, r.PayloadSha256)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return sql.ErrNoRows
	}
	return nil
}
