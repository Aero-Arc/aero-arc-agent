// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package agent

import (
	"context"
	"database/sql"
	"errors"
	"github.com/aero-arc/aero-arc-protos/flightcompletion"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/frame"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/protobuf/proto"
	"path/filepath"
	"strings"
	"sync"
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
			if err = w.BeginFlightWatch(ctx, c, "test-target"); err != nil {
				t.Fatal(err)
			}
			if err = w.RecordFlightWatchHandoff(ctx, c, at); err != nil {
				t.Fatal(err)
			}
			a := &Agent{wal: w}
			samples := completionSamples{}
			observe := func(o completionObservation) {
				t.Helper()
				if o.at < int64(2*time.Second) || o.target != "test-target" {
					o.context = binding
				} else if early {
					o.context = &pb.OperationContext{FlightId: "next-flight", IntentId: "next-intent", IntentVersion: 2, AircraftId: binding.AircraftId}
				}
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
			observe(completionObservation{target: "test-target", kind: "heartbeat", mode: 3})
			observe(completionObservation{target: "test-target", kind: "landed", landed: 1})
			pending(0)
			observe(completionObservation{target: "test-target", kind: "heartbeat", armed: true, mode: 3, at: int64(time.Second)})
			observe(completionObservation{target: "test-target", kind: "landed", landed: 2, at: int64(time.Second)})
			if early {
				observe(completionObservation{target: "test-target", kind: "heartbeat", armed: true, mode: 6, at: int64(2 * time.Second)})
			} else {
				observe(completionObservation{target: "test-target", kind: "mission", sequence: 1, at: int64(2 * time.Second)})
			}
			// Another selected autopilot cannot finish this flight, including
			// observations queued before the selection changes again.
			observe(completionObservation{target: "other-target", kind: "heartbeat", at: int64(3 * time.Second)})
			observe(completionObservation{target: "other-target", kind: "landed", landed: 1, at: int64(3 * time.Second)})
			pending(0)
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
			observe(completionObservation{target: "test-target", kind: "landed", landed: 1, at: int64(3 * time.Second)})
			pending(0)
			observe(completionObservation{target: "test-target", kind: "heartbeat", at: int64(10 * time.Second)})
			pending(0) // stale landed observation cannot complete
			observe(completionObservation{target: "test-target", kind: "landed", landed: 1, at: int64(11 * time.Second)})
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
			observe(completionObservation{target: "test-target", kind: "heartbeat", at: int64(12 * time.Second)})
			pending(0)
		})
	}
}

func TestAckFailureCancelsStreamBeforeWaitingForBlockedCompletionSend(t *testing.T) {
	a, cleanup := testMissionAgent(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testC2Command(t)
	c.CommandId = "start"
	c.Execution = &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPrecondition: &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 21, Autocontinue: true, Param4: 1}}}}}
	if err := a.wal.BeginFlightWatch(ctx, c, "test-target"); err != nil {
		t.Fatal(err)
	}
	watch, err := a.wal.LoadFlightWatch(ctx, c.Context.FlightId)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UnixNano()
	event := &pb.FlightCompletionEvidence{EventId: "event", AgentId: c.AgentId, Context: c.Context, MissionId: "mission", MissionDigest: strings.Repeat("a", 64), StartCommandId: c.CommandId, Outcome: "mission_completed", AirborneAtUnixNs: at, TerminalAtUnixNs: at, LandedAtUnixNs: at, DisarmedAtUnixNs: at, ObservationEpoch: "epoch"}
	if err = a.wal.SaveFlightWatch(ctx, watch, event); err != nil {
		t.Fatal(err)
	}
	a.durableFlightCompletion = true
	sending := make(chan struct{})
	var once sync.Once
	stream := &mockStream{sendFunc: func(*pb.AgentStreamMessage) error { once.Do(func() { close(sending) }); <-ctx.Done(); return ctx.Err() }, recvFunc: func() (*pb.RelayStreamMessage, error) {
		select {
		case <-sending:
			return &pb.RelayStreamMessage{Payload: &pb.RelayStreamMessage_TelemetryAck{TelemetryAck: &pb.TelemetryAck{Seq: 0}}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	done := make(chan error, 1)
	go func() { done <- a.runAckLoop(ctx, stream, cancel) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrInvalidTelemetryAck) {
			t.Fatalf("unexpected failure: %v", err)
		}
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("ACK failure deadlocked behind blocked completion Send")
	}
}

func TestShutdownDrainsAcceptedTerminalObservation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "completion.db")
	w, err := wal.New(context.Background(), path, 1, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	c := testC2Command(t)
	c.GetMavlink().MissionPrecondition = &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 21, Param4: 1, Autocontinue: true}}}
	c.GetMavlink().MissionPreconditionId = "mission"
	raw, _ := proto.Marshal(&pb.CommandEvidence{CommandId: c.CommandId, Events: []*pb.CommandEvent{{Stage: "applied"}}})
	if err = w.AdmitCommand(ctx, c.CommandId, wal.CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: raw}); err != nil {
		t.Fatal(err)
	}
	if err = w.BeginFlightWatch(ctx, c, "test-target"); err != nil {
		t.Fatal(err)
	}
	watch, err := w.LoadFlightWatch(ctx, c.Context.FlightId)
	if err != nil {
		t.Fatal(err)
	}
	watch.Target = "udp-server:0.0.0.0:14550/1/1/2/3"
	watch.HandoffAt = time.Now().Add(-2 * time.Second).UnixNano()
	watch.AirborneAt = time.Now().Add(-time.Second).UnixNano()
	if err = w.SaveFlightWatch(ctx, watch, nil); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	// The independent blocker connection must wait for routine WAL bookkeeping
	// before acquiring its deliberate test lock, just like production connections.
	if _, err = db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		t.Fatal(err)
	}
	blocker, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err = blocker.Exec("UPDATE flight_watches SET payload=payload"); err != nil {
		t.Fatal(err)
	}
	channel := &gomavlib.Channel{}
	accepted := make(chan struct{})
	a := &Agent{wal: w, options: &AgentOptions{Debug: true}, operationContext: &wal.OperationContext{AircraftID: c.AircraftId, FlightID: c.Context.FlightId, IntentID: c.Context.IntentId, IntentVersion: 1}, mavlinkTarget: &mavlinkTarget{channel: channel, systemID: 1, componentID: 1}, appendTelemetryFrame: func(context.Context, *pb.TelemetryFrame) error { close(accepted); return nil }}
	events := make(chan gomavlib.Event)
	done := make(chan error, 1)
	go func() { done <- a.runMAVLinkEvents(ctx, events) }()
	events <- &gomavlib.EventFrame{Channel: channel, Frame: &frame.V2Frame{SystemID: 1, ComponentID: 1, Message: &common.MessageHeartbeat{Type: common.MAV_TYPE_QUADROTOR, Autopilot: common.MAV_AUTOPILOT_ARDUPILOTMEGA, BaseMode: common.MAV_MODE_FLAG_SAFETY_ARMED, CustomMode: 6}}}
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("observation not accepted")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("stopped before draining blocked completion: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err = blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("graceful completion drain stalled")
	}
	saved, err := w.LoadFlightWatch(context.Background(), c.Context.FlightId)
	if err != nil || saved.TerminalAt == 0 || saved.Outcome != "ended_early" {
		t.Fatalf("accepted terminal milestone lost: %+v %v", saved, err)
	}
}

func TestCompletionIgnoresQueuedPreHandoffObservations(t *testing.T) {
	ctx := context.Background()
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "watch.db"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	c := testC2Command(t)
	c.GetMavlink().MissionPrecondition = &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 21, Param4: 1, Autocontinue: true}}}
	raw, _ := proto.Marshal(&pb.CommandEvidence{CommandId: c.CommandId, Events: []*pb.CommandEvent{{Stage: "applied"}}})
	if err = w.AdmitCommand(ctx, c.CommandId, wal.CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: raw}); err != nil {
		t.Fatal(err)
	}
	if err = w.BeginFlightWatch(ctx, c, "test-target"); err != nil {
		t.Fatal(err)
	}
	handoff := time.Now().Add(time.Second).UnixNano()
	if err = w.RecordFlightWatchHandoff(ctx, c, handoff); err != nil {
		t.Fatal(err)
	}
	a := &Agent{wal: w}
	samples := completionSamples{}
	// Simulate queued observations captured after API issue but before the effect.
	for _, o := range []completionObservation{{kind: "heartbeat", armed: true, mode: 6}, {kind: "landed", landed: 2}, {kind: "heartbeat", armed: true, mode: 6}, {kind: "landed", landed: 1}, {kind: "heartbeat", armed: false}} {
		o.target = "test-target"
		o.context = c.Context
		o.at = handoff - 1
		if err = a.observeCompletion(ctx, o, "epoch", &samples); err != nil {
			t.Fatal(err)
		}
	}
	watch, err := w.LoadFlightWatch(ctx, c.Context.FlightId)
	if err != nil || watch.AirborneAt != 0 || watch.TerminalAt != 0 || watch.Done || samples.heartbeatAt != 0 {
		t.Fatalf("pre-handoff evidence accepted: %+v %+v %v", watch, samples, err)
	}
	// Old watches without an actual handoff boundary also fail closed.
	watch.HandoffAt = 0
	if err = w.SaveFlightWatch(ctx, watch, nil); err != nil {
		t.Fatal(err)
	}
	if err = a.observeCompletion(ctx, completionObservation{target: "test-target", context: c.Context, at: handoff + 1, kind: "heartbeat", armed: true, mode: 6}, "epoch", &samples); err != nil {
		t.Fatal(err)
	}
	if samples.heartbeatAt != 0 {
		t.Fatal("missing handoff boundary accepted evidence")
	}
}

func TestCompletionPollingRequiresAppliedNonRejectedStart(t *testing.T) {
	for _, stages := range [][]string{{"acknowledged"}, {"rejected"}, {"applied"}, {"applied", "rejected"}} {
		t.Run(strings.Join(stages, "-"), func(t *testing.T) {
			ctx := context.Background()
			w, err := wal.New(ctx, filepath.Join(t.TempDir(), "wal.db"), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			c := &pb.DurableCommand{CommandId: "start", Context: &pb.OperationContext{FlightId: "flight"}, Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPrecondition: &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 21}}}}}}
			e := &pb.CommandEvidence{CommandId: "start"}
			for _, stage := range stages {
				e.Events = append(e.Events, &pb.CommandEvent{Stage: stage})
			}
			raw, _ := proto.Marshal(e)
			if err := w.AdmitCommand(ctx, "start", wal.CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: raw}); err != nil {
				t.Fatal(err)
			}
			if err := w.BeginFlightWatch(ctx, c, "udp-server:0.0.0.0:14550/0/0/0/0"); err != nil {
				t.Fatal(err)
			}
			writes := 0
			a := &Agent{wal: w, options: &AgentOptions{Debug: true}, operationContext: &wal.OperationContext{FlightID: "flight"}, mavlinkTarget: &mavlinkTarget{channel: &gomavlib.Channel{}, heartbeatAt: time.Now()}, writeMAVLinkCommand: func(_ *gomavlib.Channel, _ *common.MessageCommandLong) error { writes++; return nil }}
			a.requestCompletionObservations(ctx)
			want := 0
			if len(stages) == 1 && stages[0] == "applied" {
				want = 2
			}
			if writes != want {
				t.Fatalf("writes=%d want=%d", writes, want)
			}
			writes = 0
			a.operationContext = nil
			observation, ok := a.completionObservation(&gomavlib.EventFrame{Channel: a.mavlinkTarget.channel, Frame: &frame.V2Frame{Message: &common.MessageHeartbeat{}}})
			if !ok || observation.context != nil {
				t.Fatal("cleared context discarded capture before durable watch lookup")
			}
			a.requestCompletionObservations(ctx)
			if writes != want {
				t.Fatalf("cleared context stopped polling: %d want %d", writes, want)
			}
			writes = 0
			a.operationContext = &wal.OperationContext{FlightID: "next-flight", IntentVersion: 2}
			a.requestCompletionObservations(ctx)
			if writes != want {
				t.Fatalf("replacement context stopped polling: %d want %d", writes, want)
			}
			writes = 0
			a.options.DebugMAVLinkAddress = "127.0.0.1:15550"
			a.requestCompletionObservations(ctx)
			if writes != 0 {
				t.Fatal("polled an unrelated endpoint")
			}

		})
	}
}

func TestCompletionTargetUsesConfiguredEndpoint(t *testing.T) {
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1}
	a := &Agent{options: &AgentOptions{SerialPath: "/dev/serial/by-id/aircraft-a"}}
	first := a.completionTargetIdentity(target)
	a.options.SerialPath = "/dev/serial/by-id/aircraft-b"
	if first == "" || first == a.completionTargetIdentity(target) {
		t.Fatal("distinct serial devices share completion identity")
	}
	a.options = &AgentOptions{Debug: true, DebugMAVLinkAddress: "127.0.0.1:14550"}
	first = a.completionTargetIdentity(target)
	target.channel = &gomavlib.Channel{}
	if first != a.completionTargetIdentity(target) {
		t.Fatal("runtime channel replacement changed configured UDP identity")
	}
	a.options.DebugMAVLinkAddress = "127.0.0.1:14560"
	if first == a.completionTargetIdentity(target) {
		t.Fatal("changed configured UDP endpoint retained identity")
	}
}
