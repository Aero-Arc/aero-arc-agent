package wal

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"google.golang.org/protobuf/proto"
	"path/filepath"
	"testing"
)

func TestDurableOperationCommandNamespaceAndLegacyFence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "commands.db")
	w, err := New(ctx, path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	record := CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: []byte{}}
	if _, err = w.ClearOperationContext(ctx, "operation-first", ""); err != nil {
		t.Fatal(err)
	}
	if err = w.AdmitCommand(ctx, "operation-first", record); !errors.Is(err, ErrCommandIdentityConflict) {
		t.Fatalf("operation then C2: %v", err)
	}
	if err = w.AdmitCommand(ctx, "c2-first", record); err != nil {
		t.Fatal(err)
	}
	if _, err = w.ClearOperationContext(ctx, "c2-first", ""); !errors.Is(err, ErrOperationCommandConflict) {
		t.Fatalf("C2 then operation: %v", err)
	}
	if ok, err := w.BeginCommandEffect(ctx, "c2-first", "digest"); !ok || err != nil {
		t.Fatalf("effect: %v %v", ok, err)
	}
	if err = w.AdmitCommand(ctx, "queued", record); err != nil {
		t.Fatal(err)
	}
	if err = w.RecordLegacyAircraftEffect(ctx); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = New(ctx, path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if ok, err := w.CommandIsLatest(ctx, "c2-first"); ok || err != nil {
		t.Fatalf("legacy effect lost across restart: %v %v", ok, err)
	}
	if ok, err := w.BeginCommandEffect(ctx, "queued", "digest"); ok || !errors.Is(err, ErrCommandSuperseded) {
		t.Fatalf("old queued authority survived legacy effect: %v %v", ok, err)
	}
	if err = w.AdmitCommand(ctx, "new", record); err != nil {
		t.Fatal(err)
	}
	if ok, err := w.BeginCommandEffect(ctx, "new", "digest"); !ok || err != nil {
		t.Fatalf("new authority blocked: %v %v", ok, err)
	}
}

func TestObservationCommitFencesLegacyEffectAtomically(t *testing.T) {
	ctx := context.Background()
	w, err := New(ctx, filepath.Join(t.TempDir(), "wal.db"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	evidence := &pb.CommandEvidence{CommandId: "c", CommandDigest: "digest"}
	raw, _ := proto.Marshal(evidence)
	if err := w.AdmitCommand(ctx, "c", CommandRecord{Digest: "digest", Payload: []byte{}, Evidence: raw}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.BeginCommandEffect(ctx, "c", "digest"); err != nil {
		t.Fatal(err)
	}
	if latest, err := w.CommandIsLatest(ctx, "c"); err != nil || !latest {
		t.Fatalf("initial eligibility: %v %v", latest, err)
	}
	if err := w.RecordLegacyAircraftEffect(ctx); err != nil {
		t.Fatal(err)
	}
	evidence.Events = []*pb.CommandEvent{{EventId: "observed", Stage: "observed"}}
	raw, _ = proto.Marshal(evidence)
	if err := w.SaveCommand(ctx, "c", "digest", raw, true); !errors.Is(err, ErrObservationSuperseded) {
		t.Fatalf("stale observation committed: %v", err)
	}
	record, err := w.LoadCommand(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(record.Evidence, evidence); err != nil {
		t.Fatal(err)
	}
	if len(evidence.Events) != 0 {
		t.Fatalf("rejected event persisted: %v", evidence)
	}
}

func TestMissionAndDurableCommandNamespace(t *testing.T) {
	for _, missionFirst := range []bool{false, true} {
		for _, paired := range []bool{false, true} {
			t.Run(fmt.Sprintf("missionFirst=%v/paired=%v", missionFirst, paired), func(t *testing.T) {
				ctx := context.Background()
				w, err := New(ctx, filepath.Join(t.TempDir(), "wal.db"), 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				mission := &pb.DeployMissionCommand{CommandId: "shared"}
				missionRaw, _ := proto.Marshal(mission)
				command := &pb.DurableCommand{CommandId: "shared"}
				if paired {
					command.Execution = &pb.DurableCommand_Mission{Mission: mission}
				} else {
					command.Execution = &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{Command: 400}}
				}
				commandRaw, _ := proto.Marshal(command)
				admit := func() error {
					return w.AdmitCommand(ctx, "shared", CommandRecord{Digest: "digest", Payload: commandRaw, Evidence: []byte{}})
				}
				reserve := func() error {
					_, _, err := w.ReserveMissionDeployment(ctx, "shared", "fingerprint", missionRaw)
					return err
				}
				first, second := admit, reserve
				if missionFirst {
					first, second = reserve, admit
				}
				if err := first(); err != nil {
					t.Fatal(err)
				}
				err = second()
				if paired && err != nil {
					t.Fatal(err)
				}
				if !paired && !errors.Is(err, ErrCommandIdentityConflict) && !errors.Is(err, ErrMissionDeploymentConflict) {
					t.Fatalf("cross-kind reuse accepted: %v", err)
				}
				if paired {
					mission.Binding = &pb.MissionBinding{MissionId: "changed"}
					changed, _ := proto.Marshal(mission)
					if _, _, err := w.ReserveMissionDeployment(ctx, "shared", "fingerprint", changed); !errors.Is(err, ErrMissionDeploymentConflict) {
						t.Fatalf("changed inner deployment accepted: %v", err)
					}
				}
			})
		}
	}
}
