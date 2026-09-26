// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package wal

import (
	"context"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"google.golang.org/protobuf/proto"
	"path/filepath"
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
	if err = w.BeginFlightWatch(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := proto.Clone(first).(*pb.DurableCommand)
	second.CommandId = "second"
	if err = w.BeginFlightWatch(ctx, second); err == nil {
		t.Fatal("unresolved start was replaced")
	}
	raw, _ = proto.Marshal(&pb.CommandEvidence{CommandId: "first", CommandDigest: "digest", Events: []*pb.CommandEvent{{EventId: "first/rejected", Stage: "rejected"}}})
	if err = w.SaveCommand(ctx, "first", "digest", raw, true); err != nil {
		t.Fatal(err)
	}
	if err = w.BeginFlightWatch(ctx, second); err != nil {
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
	if err = w.BeginFlightWatch(ctx, first); err == nil {
		t.Fatal("airborne watch replaced")
	}
}
