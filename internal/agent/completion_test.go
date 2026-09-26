// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package agent

import (
	"context"
	"github.com/aero-arc/aero-arc-protos/flightcompletion"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/protobuf/proto"
	"path/filepath"
	"testing"
	"time"
)

func TestCompletionRequiresAirborneRecoveryAndFreshDisarmedGround(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(map[bool]string{false: "mission", true: "early"}[early], func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "agent.db")
			w, err := wal.New(ctx, path, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := w.Close(); err != nil {
					t.Error(err)
				}
			}()
			at := time.Now().UnixNano()
			binding := &pb.OperationContext{AircraftId: "aircraft", FlightId: "flight", IntentId: "intent", IntentVersion: 1}
			c := &pb.DurableCommand{CommandId: "start", AgentId: "agent", Context: binding, IssuedAtUnixMs: at / int64(time.Millisecond), Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPreconditionId: "mission", MissionPrecondition: &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 21, Autocontinue: true, Param4: 1}}}}}}
			evidence, _ := proto.Marshal(&pb.CommandEvidence{CommandId: c.CommandId, Events: []*pb.CommandEvent{{Stage: "applied"}}})
			if err = w.AdmitCommand(ctx, c.CommandId, wal.CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: evidence}); err != nil {
				t.Fatal(err)
			}
			if err = w.BeginFlightWatch(ctx, c); err != nil {
				t.Fatal(err)
			}
			a := &Agent{wal: w}
			samples := completionSamples{}
			observe := func(o completionObservation) {
				t.Helper()
				o.context = binding
				o.at += at
				if err := a.observeCompletion(ctx, o, "epoch", &samples); err != nil {
					t.Fatal(err)
				}
			}
			pending := func(want int) {
				t.Helper()
				p, err := w.PendingFlightCompletions(ctx)
				if err != nil || len(p) != want {
					t.Fatalf("pending=%v err=%v want=%d", p, err, want)
				}
			}
			observe(completionObservation{kind: "heartbeat", mode: 3})
			observe(completionObservation{kind: "landed", landed: 1})
			pending(0)
			observe(completionObservation{kind: "heartbeat", armed: true, mode: 3, at: int64(time.Second)})
			observe(completionObservation{kind: "landed", landed: 2, at: int64(time.Second)})
			if early {
				observe(completionObservation{kind: "heartbeat", armed: true, mode: 6, at: int64(2 * time.Second)})
			} else {
				observe(completionObservation{kind: "mission", sequence: 1, at: int64(2 * time.Second)})
			}
			// A restart loses cached observations, but retains the airborne/terminal milestones.
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = wal.New(ctx, path, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			a.wal = w
			samples = completionSamples{}
			observe(completionObservation{kind: "landed", landed: 1, at: int64(3 * time.Second)})
			pending(0)
			observe(completionObservation{kind: "heartbeat", at: int64(10 * time.Second)})
			pending(0) // stale landed observation cannot complete
			observe(completionObservation{kind: "landed", landed: 1, at: int64(11 * time.Second)})
			pending(1)
			events, err := w.PendingFlightCompletions(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := "mission_completed"
			if early {
				want = "ended_early"
			}
			if events[0].Outcome != want {
				t.Fatalf("outcome=%s", events[0].Outcome)
			}
			_, digest, err := flightcompletion.Encode(events[0])
			if err != nil {
				t.Fatal(err)
			}
			if err = w.AcknowledgeFlightCompletion(ctx, &pb.FlightCompletionReceipt{EventId: events[0].EventId, PayloadSha256: digest}); err != nil {
				t.Fatal(err)
			}
			observe(completionObservation{kind: "heartbeat", at: int64(12 * time.Second)})
			pending(0)
		})
	}
}
