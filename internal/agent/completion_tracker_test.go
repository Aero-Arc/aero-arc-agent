package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/frame"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/protobuf/proto"
)

func TestCompletionDoesNotAttributePreExistingRecoveryToNewStart(t *testing.T) {
	for _, mode := range []uint32{6, 9} {
		watch := wal.FlightWatch{Target: "target", HandoffAt: 1, AppliedAfter: 2, Command: &pb.DurableCommand{Context: &pb.OperationContext{FlightId: "flight"}, Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPrecondition: &pb.MissionPlan{Items: []*pb.MissionItem{{Command: 21}}}}}}}
		samples := completionSamples{}
		observe := func(o completionObservation) {
			t.Helper()
			o.target = "target"
			if _, _, err := reduceCompletion(&watch, o, "epoch", &samples); err != nil {
				t.Fatal(err)
			}
		}
		observe(completionObservation{kind: "heartbeat", armed: true, mode: mode, at: 2})
		observe(completionObservation{kind: "landed", landed: 2, at: 3})
		observe(completionObservation{kind: "heartbeat", armed: true, mode: mode, at: 4})
		if watch.TerminalAt != 0 || watch.MissionActiveAt != 0 {
			t.Fatalf("pre-existing recovery classified as ending: %+v", watch)
		}
		observe(completionObservation{kind: "heartbeat", armed: true, mode: 3, at: 5})
		observe(completionObservation{kind: "mission", missionState: uint32(common.MISSION_STATE_COMPLETE), sequence: 1, at: 5})
		if watch.MissionActiveAt != 0 || watch.TerminalAt != 0 {
			t.Fatal("AUTO with stale completed mission counted as execution")
		}
		observe(completionObservation{kind: "mission", missionState: uint32(common.MISSION_STATE_ACTIVE), missionMode: 1, at: 5})
		if watch.MissionActiveAt != 5 {
			t.Fatalf("AUTO execution not retained: %+v", watch)
		}
		// Milestones survive restart independently of process-local fresh samples.
		raw, err := json.Marshal(watch)
		if err != nil {
			t.Fatal(err)
		}
		watch = wal.FlightWatch{}
		if err = json.Unmarshal(raw, &watch); err != nil {
			t.Fatal(err)
		}
		watch.AppliedAfter = 2 // restored from durable applied command evidence
		samples = completionSamples{}
		observe(completionObservation{kind: "heartbeat", armed: true, mode: mode, at: 6})
		if watch.TerminalAt != 6 || watch.Outcome != "ended_early" {
			t.Fatalf("post-execution recovery not retained: %+v", watch)
		}
	}
}

func TestCompletionPreservesPreHandoffArrival(t *testing.T) {
	a := &Agent{mavlinkTarget: &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1}}
	arrival := time.Now().Add(-time.Second)
	o, ok := a.completionObservationAt(&gomavlib.EventFrame{Channel: a.mavlinkTarget.channel, Frame: &frame.V2Frame{SystemID: 1, ComponentID: 1, Message: &common.MessageHeartbeat{}}}, arrival)
	if !ok || o.at != arrival.UnixNano() {
		t.Fatalf("arrival restamped: %+v", o)
	}
	watch := wal.FlightWatch{Target: o.target, HandoffAt: time.Now().UnixNano()}
	changed, evidence, err := reduceCompletion(&watch, o, "epoch", &completionSamples{})
	if err != nil || changed || evidence != nil {
		t.Fatalf("pre-handoff evidence admitted: %v %v %v", changed, evidence, err)
	}
}

func TestCompletionRejectsPreviousMissionUntilStartACK(t *testing.T) {
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1}
	a := &Agent{mavlinkTarget: target}
	pending := &pendingC2{target: target, command: uint32(common.MAV_CMD_MISSION_START), after: time.Unix(0, 10), completionCommandID: "new-start", frames: make(chan *gomavlib.EventFrame, 64)}
	a.c2Pending = pending
	watch := wal.FlightWatch{Target: "target", HandoffAt: 10, Command: &pb.DurableCommand{CommandId: "new-start", Context: &pb.OperationContext{FlightId: "flight"}, Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPrecondition: &pb.MissionPlan{Items: []*pb.MissionItem{{Command: 21}}}}}}}
	a.trackFlightCompletion(watch)
	observe := func(o completionObservation) { o.target = "target"; a.accumulateCompletion(o) }
	previousMission := func(at int64) {
		observe(completionObservation{kind: "heartbeat", armed: true, mode: 3, at: at})
		observe(completionObservation{kind: "landed", landed: 2, at: at})
		observe(completionObservation{kind: "mission", missionState: uint32(common.MISSION_STATE_ACTIVE), missionMode: 1, at: at})
		observe(completionObservation{kind: "mission", missionState: uint32(common.MISSION_STATE_COMPLETE), sequence: 1, at: at + 1})
	}
	previousMission(11) // arrival after handoff, before the new start ACK
	a.acceptCompletionStart("different-start", 20)
	previousMission(13)
	// The reader sees acceptance and immediate progress while the command
	// goroutine has not consumed even the ACK from its queue.
	a.observeC2FrameAt(&gomavlib.EventFrame{Channel: target.channel, Frame: &frame.V2Frame{SystemID: 1, ComponentID: 1, Message: &common.MessageCommandAck{Command: common.MAV_CMD_MISSION_START, Result: common.MAV_RESULT_ACCEPTED}}}, time.Unix(0, 20))
	previousMission(15) // delayed observer retains its original pre-ACK arrival
	tracker := a.completionTrackers["target"]
	if tracker.watch.AirborneAt != 0 || tracker.watch.MissionActiveAt != 0 || tracker.watch.TerminalAt != 0 || tracker.revision != 0 {
		t.Fatalf("previous mission admitted: %+v", tracker)
	}
	observe(completionObservation{kind: "heartbeat", armed: true, mode: 3, at: 21})
	observe(completionObservation{kind: "landed", landed: 2, at: 22})
	observe(completionObservation{kind: "mission", missionState: uint32(common.MISSION_STATE_COMPLETE), sequence: 1, at: 23})
	if tracker.watch.TerminalAt != 0 {
		t.Fatal("pre-ACK ACTIVE unlocked a post-ACK COMPLETE")
	}
	observe(completionObservation{kind: "mission", missionState: uint32(common.MISSION_STATE_ACTIVE), missionMode: 1, at: 24})
	observe(completionObservation{kind: "mission", missionState: uint32(common.MISSION_STATE_COMPLETE), sequence: 1, at: 25})
	if tracker.watch.MissionActiveAt != 24 || tracker.watch.TerminalAt != 25 {
		t.Fatalf("post-ACK mission milestones missing: %+v", tracker.watch)
	}
	if len(pending.frames) != 1 || tracker.watch.StartACKAt != 20 || tracker.watch.AppliedAfter != 21 {
		t.Fatalf("ACK arrival lost before command consumer ran: %+v", tracker.watch)
	}
}

func TestCompletionMilestonesSurvivePersistenceBackpressure(t *testing.T) {
	for _, test := range []struct {
		name                  string
		early, pendingHandoff bool
	}{{name: "mission"}, {name: "land", early: true}, {name: "before-handoff-commit", pendingHandoff: true}} {
		t.Run(test.name, func(t *testing.T) {
			early := test.early
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "agent.db")
			w, err := wal.New(ctx, path, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			at := time.Now().UnixNano()
			c := &pb.DurableCommand{CommandId: "start", AgentId: "agent", Context: &pb.OperationContext{AircraftId: "aircraft", FlightId: "flight", IntentId: "intent", IntentVersion: 1}, IssuedAtUnixMs: at / int64(time.Millisecond), Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPreconditionId: "mission", MissionPrecondition: &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 21, Autocontinue: true, Param4: 1}}}}}}
			raw, _ := proto.Marshal(&pb.CommandEvidence{CommandId: "start", Events: []*pb.CommandEvent{{Stage: "applied", OccurredAtUnixMs: (at / int64(time.Millisecond)) - 1}}})
			if err = w.AdmitCommand(ctx, "start", wal.CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: raw}); err != nil {
				t.Fatal(err)
			}
			if err = w.BeginFlightWatch(ctx, c, "target"); err != nil {
				t.Fatal(err)
			}
			if !test.pendingHandoff {
				if err = w.RecordFlightWatchHandoff(ctx, c, at); err != nil {
					t.Fatal(err)
				}
				if err = w.RecordFlightWatchACK(ctx, c, at); err != nil {
					t.Fatal(err)
				}
			}
			a := &Agent{wal: w}
			if err = a.restoreCompletionTrackers(ctx); err != nil {
				t.Fatal(err)
			}
			if test.pendingHandoff {
				watch, loadErr := w.LoadFlightWatch(ctx, c.Context.FlightId)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				watch.HandoffAt = at
				watch.StartACKAt = at
				watch.AppliedAfter = at + 1
				a.trackFlightCompletion(watch)
			}
			observe := func(o completionObservation) { o.target = "target"; o.at += at; a.accumulateCompletion(o) }
			observe(completionObservation{kind: "heartbeat", armed: true, mode: 3, at: 1})
			observe(completionObservation{kind: "landed", landed: 2, at: 2})
			observe(completionObservation{kind: "mission", missionState: uint32(common.MISSION_STATE_ACTIVE), missionMode: 1, at: 2})
			// Hold SQLite's writer lock while the worker attempts to persist the
			// airborne milestone. Ingest must continue without waiting on that lock.
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err = tx.Exec(`UPDATE flight_watches SET payload=payload`); err != nil {
				t.Fatal(err)
			}
			flushCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			done := make(chan struct{})
			go func() { a.flushCompletionTrackers(flushCtx); close(done) }()
			ingested := make(chan struct{})
			go func() {
				for i := int64(3); i < 10003; i++ {
					observe(completionObservation{kind: "heartbeat", armed: true, mode: 3, at: i})
				}
				if early {
					observe(completionObservation{kind: "heartbeat", armed: true, mode: 9, at: 10004})
				} else {
					observe(completionObservation{kind: "mission", sequence: 1, at: 10004})
				}
				observe(completionObservation{kind: "landed", landed: 1, at: 10005})
				observe(completionObservation{kind: "heartbeat", armed: false, mode: 9, at: 10006})
				close(ingested)
			}()
			select {
			case <-ingested:
			case <-time.After(time.Second):
				t.Fatal("completion ingest blocked on storage")
			}
			<-done
			cancel()
			if !a.completionWritesPending() {
				t.Fatal("failed persistence discarded pending milestones")
			}
			a.completionMu.Lock()
			count := len(a.completionTrackers)
			a.completionMu.Unlock()
			if count != 1 {
				t.Fatalf("frame rate grew tracking state: %d", count)
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if test.pendingHandoff {
				a.flushCompletionTrackers(ctx)
				events, loadErr := w.PendingFlightCompletions(ctx)
				if loadErr != nil || len(events) != 0 {
					t.Fatalf("uncommitted handoff published: %v %v", events, loadErr)
				}
				if err = w.RecordFlightWatchHandoff(ctx, c, at); err != nil {
					t.Fatal(err)
				}
				if err = w.RecordFlightWatchACK(ctx, c, at); err != nil {
					t.Fatal(err)
				}
			}
			a.flushCompletionTrackers(ctx)
			if a.completionWritesPending() {
				t.Fatal("completion did not drain after storage recovered")
			}
			events, err := w.PendingFlightCompletions(ctx)
			if err != nil || len(events) != 1 {
				t.Fatalf("completion lost: %v, %v", events, err)
			}
			want := "mission_completed"
			if early {
				want = "ended_early"
			}
			if events[0].Outcome != want || events[0].TerminalAtUnixNs != at+10004 {
				t.Fatalf("terminal evidence changed: %+v", events[0])
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := wal.New(ctx, path, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reopened.Close() }()
			replayed, err := reopened.PendingFlightCompletions(ctx)
			if err != nil || len(replayed) != 1 || !proto.Equal(events[0], replayed[0]) {
				t.Fatalf("restart changed completion evidence: %v %v", replayed, err)
			}
		})
	}
}
