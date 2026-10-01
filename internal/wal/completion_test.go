// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package wal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aero-arc/aero-arc-protos/flightcompletion"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"google.golang.org/protobuf/proto"
	"path/filepath"
	"strings"
	"testing"
)

func TestFlightWatchReplacementRequiresRejectedUnflownStart(t *testing.T) {
	ctx := context.Background()
	w, err := New(ctx, filepath.Join(t.TempDir(), "watch.db"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	first := &pb.DurableCommand{CommandId: "first", Context: &pb.OperationContext{FlightId: "flight"}, Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPrecondition: &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 20}}}}}}
	raw, _ := proto.Marshal(&pb.CommandEvidence{CommandId: "first", CommandDigest: "digest"})
	if err = w.AdmitCommand(ctx, "first", CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: raw}); err != nil {
		t.Fatal(err)
	}
	if err = w.BeginFlightWatch(ctx, first, "test-target"); err != nil {
		t.Fatal(err)
	}
	second := proto.Clone(first).(*pb.DurableCommand)
	second.CommandId = "second"
	if err = w.BeginFlightWatch(ctx, second, "test-target"); err == nil {
		t.Fatal("unresolved start was replaced")
	}
	raw, _ = proto.Marshal(&pb.CommandEvidence{CommandId: "first", CommandDigest: "digest", Events: []*pb.CommandEvent{{EventId: "first/rejected", Stage: "rejected"}}})
	if err = w.SaveCommand(ctx, "first", "digest", raw, true); err != nil {
		t.Fatal(err)
	}
	if err = w.BeginFlightWatch(ctx, second, "test-target"); err != nil {
		t.Fatal(err)
	}
	watch, err := w.LoadFlightWatch(ctx, "flight")
	if err != nil || watch.Command.CommandId != "second" {
		t.Fatalf("replacement=%+v %v", watch, err)
	}
	watch.AirborneAt = 1
	if err = w.SaveFlightWatch(ctx, watch, nil); err != nil {
		t.Fatal(err)
	}
	if err = w.BeginFlightWatch(ctx, first, "test-target"); err == nil {
		t.Fatal("airborne watch replaced")
	}
}

func TestPendingCompletionQuarantinesCorruptionAndContinues(t *testing.T) {
	ctx := context.Background()
	w, err := New(ctx, filepath.Join(t.TempDir(), "events.db"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	e := &pb.FlightCompletionEvidence{EventId: "valid", AgentId: "agent", Context: &pb.OperationContext{AircraftId: "aircraft", FlightId: "flight", IntentId: "intent", IntentVersion: 1}, MissionId: "mission", MissionDigest: strings.Repeat("a", 64), StartCommandId: "start", Outcome: "mission_completed", AirborneAtUnixNs: 1, TerminalAtUnixNs: 2, LandedAtUnixNs: 3, DisarmedAtUnixNs: 4, ObservationEpoch: "epoch"}
	raw, digest, err := flightcompletion.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := []byte{0xff}
	for i := 0; i < 33; i++ {
		if _, err = w.db.ExecContext(ctx, `INSERT INTO flight_completion_events(event_id,digest,payload) VALUES(?,?,?)`, fmt.Sprintf("bad-%d", i), "invalid", corrupt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = w.db.ExecContext(ctx, `INSERT INTO flight_completion_events(event_id,digest,payload) VALUES(?,?,?)`, e.EventId, digest, raw); err != nil {
		t.Fatal(err)
	}
	events, err := w.PendingFlightCompletions(ctx)
	if err != nil || len(events) != 0 {
		t.Fatalf("first quarantine page: %v %v", events, err)
	}
	events, err = w.PendingFlightCompletions(ctx)
	if err != nil || len(events) != 1 || events[0].EventId != "valid" {
		t.Fatalf("healthy event blocked: %v %v", events, err)
	}
	var count, delivered int
	var original []byte
	if err = w.db.QueryRowContext(ctx, `SELECT count(*) FROM flight_completion_quarantine`).Scan(&count); err != nil || count != 33 {
		t.Fatalf("quarantine count=%d %v", count, err)
	}
	if err = w.db.QueryRowContext(ctx, `SELECT payload,delivered FROM flight_completion_events WHERE event_id='bad-0'`).Scan(&original, &delivered); err != nil || delivered != 0 || !bytes.Equal(original, corrupt) {
		t.Fatalf("quarantine discarded/acknowledged evidence: %v", err)
	}
}

func TestUnfinishedTargetWatchBlocksOtherFlightsIncludingNoWatchStarts(t *testing.T) {
	ctx := context.Background()
	w, err := New(ctx, filepath.Join(t.TempDir(), "watch.db"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	first := &pb.DurableCommand{CommandId: "first", Context: &pb.OperationContext{FlightId: "first-flight"}, Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPrecondition: &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 21}}}}}}
	raw, _ := proto.Marshal(&pb.CommandEvidence{CommandId: "first", Events: []*pb.CommandEvent{{Stage: "applied"}}})
	if err = w.AdmitCommand(ctx, "first", CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: raw}); err != nil {
		t.Fatal(err)
	}
	if err = w.BeginFlightWatch(ctx, first, "target"); err != nil {
		t.Fatal(err)
	}
	for _, terminal := range []uint32{16, 20, 21} {
		next := proto.Clone(first).(*pb.DurableCommand)
		next.CommandId = "next"
		next.Context.FlightId = "next-flight"
		next.GetMavlink().MissionPrecondition.Items[0].Command = terminal
		if err = w.BeginFlightWatch(ctx, next, "target"); err == nil {
			t.Fatalf("new flight with terminal %d bypassed unresolved target owner", terminal)
		}
	}
	if _, err = w.LoadFlightWatch(ctx, "next-flight"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("blocked admission persisted watch: %v", err)
	}
}

func TestFlightWatchIndexMigratesHistoryAndIsolatesCorruption(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	w, err := New(ctx, path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	command := &pb.DurableCommand{CommandId: "healthy", Context: &pb.OperationContext{FlightId: "healthy-flight"}, Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPrecondition: &pb.MissionPlan{Items: []*pb.MissionItem{{Command: 20}}}}}}
	evidence, _ := proto.Marshal(&pb.CommandEvidence{Events: []*pb.CommandEvent{{Stage: "applied"}}})
	if err = w.AdmitCommand(ctx, command.CommandId, CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: evidence}); err != nil {
		t.Fatal(err)
	}
	if err = w.BeginFlightWatch(ctx, command, "healthy-target"); err != nil {
		t.Fatal(err)
	}
	// Simulate a pre-index database with retained completed missions and a
	// malformed protobuf in an unrelated watch whose routing JSON is intact.
	for i := 0; i < 80; i++ {
		c := proto.Clone(command).(*pb.DurableCommand)
		c.CommandId = fmt.Sprintf("old-%d", i)
		c.Context.FlightId = c.CommandId
		raw, marshalErr := json.Marshal(FlightWatch{Command: c, Target: "healthy-target", Done: true})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if _, err = w.db.Exec(`INSERT INTO flight_watches(flight_id,start_command_id,payload) VALUES(?,?,?)`, c.CommandId, c.CommandId, raw); err != nil {
			t.Fatal(err)
		}
	}
	corrupt := []byte(`{"target":"other-target","done":false,"command":{"invalidProtoField":true}}`)
	if _, err = w.db.Exec(`INSERT INTO flight_watches VALUES('broken','broken',?)`, corrupt); err != nil {
		t.Fatal(err)
	}
	if _, err = w.db.Exec(`DROP TABLE flight_watch_index`); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = New(ctx, path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	watch, err := w.LoadUnresolvedFlightWatch(ctx, "healthy-target")
	if err != nil || watch.Command.GetCommandId() != command.CommandId {
		t.Fatalf("healthy watch=%+v err=%v", watch, err)
	}
	var retained []byte
	var reason string
	if err = w.db.QueryRow(`SELECT w.payload,i.quarantine_reason FROM flight_watches w JOIN flight_watch_index i ON i.flight_id=w.flight_id WHERE w.flight_id='broken'`).Scan(&retained, &reason); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(retained, corrupt) || reason == "" {
		t.Fatal("corrupt watch was not retained and quarantined")
	}
	next := proto.Clone(command).(*pb.DurableCommand)
	next.CommandId = "new"
	next.Context.FlightId = "new"
	if err = w.BeginFlightWatch(ctx, next, "other-target"); err == nil {
		t.Fatal("quarantined ownership allowed a new start")
	}
	// Corruption of already indexed history is irrelevant to the active target:
	// no JSON expression or decoder should touch these completed records.
	if _, err = w.db.Exec(`UPDATE flight_watches SET payload=X'ff' WHERE flight_id LIKE 'old-%' OR flight_id='broken'`); err != nil {
		t.Fatal(err)
	}
	if _, err = w.LoadUnresolvedFlightWatch(ctx, "healthy-target"); err != nil {
		t.Fatal(err)
	}
	watch.Done = true
	if err = w.SaveFlightWatch(ctx, watch, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = w.LoadUnresolvedFlightWatch(ctx, "healthy-target"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("completed watch remains active: %v", err)
	}
}

func TestFlightWatchUnknownCorruptLegacyAuthorityFailsClosed(t *testing.T) {
	ctx := context.Background()
	w, err := New(ctx, filepath.Join(t.TempDir(), "unknown.db"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err = w.db.Exec(`INSERT INTO flight_watches VALUES('broken','broken',X'ff')`); err != nil {
		t.Fatal(err)
	}
	if err = ensureFlightWatchIndex(w.db); err != nil {
		t.Fatal(err)
	}
	if _, err = w.LoadUnresolvedFlightWatch(ctx, "any-target"); err == nil || errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unknown corrupt ownership did not fail closed: %v", err)
	}
}
