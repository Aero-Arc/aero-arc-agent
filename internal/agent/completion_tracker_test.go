package agent

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/protobuf/proto"
)

func TestCompletionMilestonesSurvivePersistenceBackpressure(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(map[bool]string{false: "mission", true: "land"}[early], func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "agent.db")
			w, err := wal.New(ctx, path, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			at := time.Now().UnixNano()
			c := &pb.DurableCommand{CommandId: "start", AgentId: "agent", Context: &pb.OperationContext{AircraftId: "aircraft", FlightId: "flight", IntentId: "intent", IntentVersion: 1}, IssuedAtUnixMs: at / int64(time.Millisecond), Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{MissionPreconditionId: "mission", MissionPrecondition: &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 21, Autocontinue: true, Param4: 1}}}}}}
			raw, _ := proto.Marshal(&pb.CommandEvidence{CommandId: "start", Events: []*pb.CommandEvent{{Stage: "applied"}}})
			if err = w.AdmitCommand(ctx, "start", wal.CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: raw}); err != nil {
				t.Fatal(err)
			}
			if err = w.BeginFlightWatch(ctx, c, "target"); err != nil {
				t.Fatal(err)
			}
			if err = w.RecordFlightWatchHandoff(ctx, c, at); err != nil {
				t.Fatal(err)
			}
			a := &Agent{wal: w}
			if err = a.restoreCompletionTrackers(ctx); err != nil {
				t.Fatal(err)
			}
			observe := func(o completionObservation) { o.target = "target"; o.at += at; a.accumulateCompletion(o) }
			observe(completionObservation{kind: "heartbeat", armed: true, mode: 3, at: 1})
			observe(completionObservation{kind: "landed", landed: 2, at: 2})
			// Hold SQLite's writer lock while the worker attempts to persist the
			// airborne milestone. Ingest must continue without waiting on that lock.
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
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
			defer reopened.Close()
			replayed, err := reopened.PendingFlightCompletions(ctx)
			if err != nil || len(replayed) != 1 || !proto.Equal(events[0], replayed[0]) {
				t.Fatalf("restart changed completion evidence: %v %v", replayed, err)
			}
		})
	}
}
