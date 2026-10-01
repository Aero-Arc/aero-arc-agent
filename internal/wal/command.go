package wal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"google.golang.org/protobuf/proto"
)

// ErrCommandSuperseded means a newer admitted command already began an effect.
var ErrCommandSuperseded = errors.New("command superseded before first effect")

// ErrObservationSuperseded means newer durable or legacy effects prevent attribution.
var ErrObservationSuperseded = errors.New("command observation superseded")

// ErrCommandIdentityConflict identifies reuse of a command ID for different authority.
var ErrCommandIdentityConflict = errors.New("command identity conflict")

// CommandRecord is an immutable command and its durable execution evidence.
type CommandRecord struct {
	Digest            string
	Payload, Evidence []byte
	EffectStarted     bool
	Target            string
}

// LoadCommand reads command identity and evidence. Missing IDs return sql.ErrNoRows.
//
// Parameters: ctx bounds reads; id selects stable identity.
//
// Returns: The persisted record, sql.ErrNoRows, or a SQLite error.
func (w *WAL) LoadCommand(ctx context.Context, id string) (CommandRecord, error) {
	var r CommandRecord
	err := w.db.QueryRowContext(ctx, `SELECT digest,payload,evidence,effect_started,COALESCE((SELECT target FROM c2_command_targets WHERE command_id=c2_commands.command_id),'') FROM c2_commands WHERE command_id=?`, id).Scan(&r.Digest, &r.Payload, &r.Evidence, &r.EffectStarted, &r.Target)
	return r, err
}

// AdmitCommand persists immutable authority and initial evidence before execution.
// Conflicting IDs fail. Exact duplicates preserve the original evidence and effect fence.
//
// Parameters: ctx bounds persistence; id selects identity; r carries digest, payload, and initial evidence.
//
// Returns: Nil for admission or exact replay; conflicting identity and storage errors fail closed.
func (w *WAL) AdmitCommand(ctx context.Context, id string, r CommandRecord) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Start with a conditional write to acquire SQLite's writer lock before
	// reading the namespace; upgrading a deferred read can fail with SQLITE_BUSY.
	if _, err = tx.ExecContext(ctx, `INSERT INTO c2_commands(command_id,digest,payload,evidence) SELECT ?,?,?,? WHERE NOT EXISTS(SELECT 1 FROM operation_context_commands WHERE command_id=?) ON CONFLICT(command_id) DO NOTHING`, id, r.Digest, r.Payload, r.Evidence, id); err != nil {
		return err
	}
	var conflict bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operation_context_commands WHERE command_id=?)`, id).Scan(&conflict); err != nil {
		return err
	}
	if conflict {
		return ErrCommandIdentityConflict
	}
	var missionPayload []byte
	err = tx.QueryRowContext(ctx, `SELECT command_payload FROM mission_deployments WHERE command_id=?`, id).Scan(&missionPayload)
	if err == nil && !pairedMissionCommand(id, r.Payload, missionPayload) {
		return ErrCommandIdentityConflict
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var digest string
	if err = tx.QueryRowContext(ctx, `SELECT digest FROM c2_commands WHERE command_id=?`, id).Scan(&digest); err != nil {
		return err
	}
	if digest != r.Digest {
		return ErrCommandIdentityConflict
	}
	return tx.Commit()
}

// SaveCommand atomically records evidence and the irreversible first-effect fence.
// A true effect flag can never revert. The caller serializes execution per aircraft.
//
// Parameters: ctx bounds the transaction; id and digest bind evidence; evidence contains immutable protobuf events; effect preserves the irreversible fence.
//
// Returns: nil after atomic merge; ErrObservationSuperseded if a new observed
// event crosses a newer durable or legacy effect. Existing observations replay
// unchanged. ErrCommandSuperseded rejects new applied evidence for an effect-free
// command overtaken by newer authority. Conflicting evidence, identity mismatch,
// and storage errors roll back.
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
			// Matching readback is not evidence that a prepared command applied
			// when newer authority installed the same mission. Check atomically
			// with persistence so even another WAL user cannot race attribution.
			if event.Stage == "applied" {
				var superseded bool
				if err = tx.QueryRowContext(ctx, `SELECT effect_started=0 AND (rowid<=COALESCE((SELECT c2_rowid FROM legacy_aircraft_effect WHERE id=1),0) OR EXISTS(SELECT 1 FROM c2_commands newer WHERE newer.rowid>current.rowid AND newer.effect_started=1)) FROM c2_commands current WHERE command_id=?`, id).Scan(&superseded); err != nil {
					return err
				}
				if superseded {
					return ErrCommandSuperseded
				}
			}
			if event.Stage == "observed" {
				var latest bool
				if err = tx.QueryRowContext(ctx, `SELECT rowid>COALESCE((SELECT c2_rowid FROM legacy_aircraft_effect WHERE id=1),0) AND rowid=(SELECT MAX(rowid) FROM c2_commands WHERE effect_started=1) FROM c2_commands WHERE command_id=?`, id).Scan(&latest); err != nil {
					return err
				}
				if !latest {
					return ErrObservationSuperseded
				}
			}
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
// effect-bearing command on this single-aircraft Agent journal.
//
// Parameters: ctx bounds reads; id selects an admitted command.
//
// Returns: Whether no newer command has begun an effect, or a SQLite error.
func (w *WAL) CommandIsLatest(ctx context.Context, id string) (bool, error) {
	var latest bool
	err := w.db.QueryRowContext(ctx, `SELECT rowid>COALESCE((SELECT c2_rowid FROM legacy_aircraft_effect WHERE id=1),0) AND rowid=(SELECT MAX(rowid) FROM c2_commands WHERE effect_started=1) FROM c2_commands WHERE command_id=?`, id).Scan(&latest)
	return latest, err
}

// BeginCommandEffect atomically grants the sole first-effect permit across WAL
// users. False means an effect may already have begun and must never be repeated.
//
// Parameters: ctx bounds the atomic update; id and digest select exact authority.
//
// Returns: True only for the first permit, false when consumed or absent,
// ErrCommandSuperseded when newer authority has begun an effect, or a SQLite error.
func (w *WAL) BeginCommandEffect(ctx context.Context, id, digest string) (bool, error) {
	result, err := w.db.ExecContext(ctx, `UPDATE c2_commands SET effect_started=1 WHERE command_id=? AND digest=? AND effect_started=0 AND rowid>COALESCE((SELECT c2_rowid FROM legacy_aircraft_effect WHERE id=1),0) AND NOT EXISTS(SELECT 1 FROM c2_commands newer WHERE newer.rowid>c2_commands.rowid AND newer.effect_started=1)`, id, digest)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 1 {
		return n == 1, err
	}
	var superseded bool
	if err = w.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM c2_commands current WHERE command_id=? AND digest=? AND effect_started=0 AND (current.rowid<=COALESCE((SELECT c2_rowid FROM legacy_aircraft_effect WHERE id=1),0) OR EXISTS(SELECT 1 FROM c2_commands newer WHERE newer.rowid>current.rowid AND newer.effect_started=1)))`, id, digest).Scan(&superseded); err != nil {
		return false, err
	}
	if superseded {
		return false, ErrCommandSuperseded
	}
	return false, nil
}

// RecordLegacyAircraftEffect fences durable observations before a legacy MAVLink write.
// A failed or uncertain write conservatively retains the fence across restart.
//
// Parameters: ctx bounds persistence; callers serialize aircraft execution.
// Returns: nil after all currently admitted durable commands are superseded,
// or a storage error that must prevent the legacy write. Later admissions remain eligible.
func (w *WAL) RecordLegacyAircraftEffect(ctx context.Context) error {
	_, err := w.db.ExecContext(ctx, `INSERT INTO legacy_aircraft_effect(id,c2_rowid) SELECT 1,COALESCE(MAX(rowid),0) FROM c2_commands WHERE true ON CONFLICT(id) DO UPDATE SET c2_rowid=MAX(c2_rowid,excluded.c2_rowid)`)
	return err
}

// Only an exact embedded deployment may share an ID with its durable envelope.
func pairedMissionCommand(id string, commandRaw, missionRaw []byte) bool {
	var command pb.DurableCommand
	var mission pb.DeployMissionCommand
	if proto.Unmarshal(commandRaw, &command) != nil || proto.Unmarshal(missionRaw, &mission) != nil {
		return false
	}
	return command.CommandId == id && mission.CommandId == id && command.GetMission() != nil && proto.Equal(command.GetMission(), &mission)
}

// BindCommandTarget persists the selected endpoint and autopilot before its effect.
// Parameters: ctx bounds persistence; id selects admitted authority; target must be nonempty.
// Returns nil for the first binding or its exact retry. Changed bindings and already
// effected commands without a binding fail closed; historical effects are never relabeled.
func (w *WAL) BindCommandTarget(ctx context.Context, id, target string) error {
	if target == "" {
		return errors.New("command target identity unavailable")
	}
	_, err := w.db.ExecContext(ctx, `INSERT INTO c2_command_targets(command_id,target) SELECT command_id,? FROM c2_commands WHERE command_id=? AND effect_started=0 ON CONFLICT(command_id) DO NOTHING`, target, id)
	if err != nil {
		return err
	}
	var saved string
	if err = w.db.QueryRowContext(ctx, `SELECT target FROM c2_command_targets WHERE command_id=?`, id).Scan(&saved); err != nil {
		return err
	}
	if saved != target {
		return errors.New("command target changed")
	}
	return nil
}

// BindMissionDeploymentTarget persists the exact autopilot before any mission effect.
// Parameters: ctx bounds storage; id selects reserved deployment authority; target
// identifies the configured endpoint and selected MAVLink system/component/profile.
// Returns: nil for initial prepared binding or exact replay; missing historic
// bindings, changed targets, absent records, and storage errors fail closed.
func (w *WAL) BindMissionDeploymentTarget(ctx context.Context, id, target string) error {
	if target == "" {
		return errors.New("mission target identity unavailable")
	}
	_, err := w.db.ExecContext(ctx, `INSERT INTO mission_deployment_targets(command_id,target) SELECT command_id,? FROM mission_deployments WHERE command_id=? AND state='prepared' ON CONFLICT(command_id) DO NOTHING`, target, id)
	if err != nil {
		return err
	}
	var saved string
	if err = w.db.QueryRowContext(ctx, `SELECT target FROM mission_deployment_targets WHERE command_id=?`, id).Scan(&saved); err != nil {
		return err
	}
	if saved != target {
		return errors.New("mission target changed")
	}
	return nil
}
