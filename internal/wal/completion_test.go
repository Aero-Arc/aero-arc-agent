// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package wal

import (
	"bytes"
	"context"
	"database/sql"
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
