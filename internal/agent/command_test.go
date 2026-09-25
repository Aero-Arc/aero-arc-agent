package agent

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-protos/commanddigest"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/frame"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"github.com/makinje/aero-arc-agent/internal/identity"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/protobuf/proto"
)

func testC2Command(t *testing.T) *pb.DurableCommand {
	t.Helper()
	now := time.Now()
	c := &pb.DurableCommand{CommandId: "command-1", OperatorId: "operator", AircraftId: "aircraft", AgentId: identity.Resolve().FinalID, Context: &pb.OperationContext{AircraftId: "aircraft", FlightId: "flight", IntentId: "intent", IntentVersion: 1}, Definition: "ARM", DefinitionVersion: 1, Capability: "mavlink_command_v1", IssuedAtUnixMs: now.UnixMilli(), ExpiresAtUnixMs: now.Add(30 * time.Second).UnixMilli(), RecoveryPolicy: "no_repeat_effect_v1", Execution: &pb.DurableCommand_Mavlink{Mavlink: &pb.MavlinkExecution{Command: 400, Parameters: []float32{1, 0, 0, 0, 0, 0, 0}, Observation: "armed", VehicleProfile: "arducopter_v1"}}}
	var err error
	c.CommandDigest, err = commanddigest.Digest(c)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestCommandRestartNeverRepeatsAnUncertainEffect(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "command.db")
	w, err := wal.New(ctx, path, 1, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	c := testC2Command(t)
	payload, _ := proto.Marshal(c)
	evidence, _ := proto.Marshal(&pb.CommandEvidence{CommandId: c.CommandId, CommandDigest: c.CommandDigest, Events: []*pb.CommandEvent{commandEvent(c, "acknowledged", "admitted", "agent_journal")}})
	if err = w.AdmitCommand(ctx, c.CommandId, wal.CommandRecord{Digest: c.CommandDigest, Payload: payload, Evidence: evidence}); err != nil {
		t.Fatal(err)
	}
	if err = w.SaveCommand(ctx, c.CommandId, c.CommandDigest, evidence, true); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = wal.New(ctx, path, 1, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	a := &Agent{wal: w}
	e, err := a.executeDurableCommand(ctx, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasStage(e, "outcome_unknown") || hasStage(e, "applied") {
		t.Fatalf("uncertain effect misclassified: %v", e)
	}
	again, err := a.executeDurableCommand(ctx, c, nil)
	if err != nil || !proto.Equal(e, again) {
		t.Fatalf("immutable replay changed: %v %v", again, err)
	}
	changed := proto.Clone(c).(*pb.DurableCommand)
	changed.GetMavlink().Parameters[0] = 0
	changed.CommandDigest, _ = commanddigest.Digest(changed)
	if _, err = a.executeDurableCommand(ctx, changed, nil); err == nil {
		t.Fatal("conflicting command ID accepted")
	}
}
func TestExpiredFirstCommandIsDurablyRejected(t *testing.T) {
	ctx := context.Background()
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "command.db"), 1, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	c := testC2Command(t)
	c.IssuedAtUnixMs = time.Now().Add(-time.Minute).UnixMilli()
	c.ExpiresAtUnixMs = time.Now().Add(-time.Second).UnixMilli()
	c.CommandDigest, _ = commanddigest.Digest(c)
	a := &Agent{wal: w}
	e, err := a.executeDurableCommand(ctx, c, nil)
	if err != nil || !hasStage(e, "rejected") {
		t.Fatalf("expired command=%v err=%v", e, err)
	}
	record, err := w.LoadCommand(ctx, c.CommandId)
	if err != nil || record.EffectStarted {
		t.Fatalf("expired command started effect: %+v %v", record, err)
	}
}

func TestCommandEffectPermitIsSingleUse(t *testing.T) {
	ctx := context.Background()
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "permit.db"), 1, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	c := testC2Command(t)
	payload, _ := proto.Marshal(c)
	evidence, _ := proto.Marshal(&pb.CommandEvidence{CommandId: c.CommandId, CommandDigest: c.CommandDigest})
	if err = w.AdmitCommand(ctx, c.CommandId, wal.CommandRecord{Digest: c.CommandDigest, Payload: payload, Evidence: evidence}); err != nil {
		t.Fatal(err)
	}
	owned, err := w.BeginCommandEffect(ctx, c.CommandId, c.CommandDigest)
	if err != nil || !owned {
		t.Fatalf("first permit=%v %v", owned, err)
	}
	owned, err = w.BeginCommandEffect(ctx, c.CommandId, c.CommandDigest)
	if err != nil || owned {
		t.Fatalf("duplicate permit=%v %v", owned, err)
	}
}

func TestLandAppliedRemainsIndependentFromTouchdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	a, closeAgent := testMissionAgent(t)
	defer closeAgent()
	c := testC2Command(t)
	c.Definition = "LAND"
	m := c.GetMavlink()
	m.Command = 21
	m.Parameters[0] = 0
	m.Observation = "landed"
	c.CommandDigest, _ = commanddigest.Digest(c)
	a.operationContext = &wal.OperationContext{AircraftID: c.AircraftId, FlightID: c.Context.FlightId, IntentID: c.Context.IntentId, IntentVersion: 1}
	channel := a.mavlinkTarget.channel
	a.mavlinkTarget.vehicleType = common.MAV_TYPE_QUADROTOR
	a.mavlinkTarget.autopilot = common.MAV_AUTOPILOT_ARDUPILOTMEGA
	emit := func(value message.Message) {
		a.observeMAVLinkFrame(&gomavlib.EventFrame{Channel: channel, Frame: &frame.V2Frame{SystemID: 1, ComponentID: 1, Message: value}})
	}
	var writes atomic.Int32
	var touchdown atomic.Bool
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, value message.Message) error {
		command, ok := value.(*common.MessageCommandLong)
		if !ok || command.Command != common.MAV_CMD_NAV_LAND {
			t.Errorf("unexpected request: %v", value)
		}
		writes.Add(1)
		return nil
	}
	done := make(chan struct{})
	defer func() { cancel(); <-done }()
	go func() {
		defer close(done)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				emit(&common.MessageHeartbeat{Type: common.MAV_TYPE_QUADROTOR, Autopilot: common.MAV_AUTOPILOT_ARDUPILOTMEGA})
				if writes.Load() > 0 {
					emit(&common.MessageCommandAck{Command: common.MAV_CMD_NAV_LAND, Result: common.MAV_RESULT_ACCEPTED})
				}
				if touchdown.Load() {
					emit(&common.MessageExtendedSysState{LandedState: common.MAV_LANDED_STATE_ON_GROUND})
				}
			}
		}
	}()
	result, err := a.executeDurableCommand(ctx, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasStage(result, "applied") || hasStage(result, "observed") {
		t.Fatalf("LAND acceptance collapsed with touchdown: %v", result)
	}
	touchdown.Store(true)
	result, err = a.executeDurableCommand(ctx, c, nil)
	if err != nil || !hasStage(result, "observed") {
		t.Fatalf("touchdown not reconciled: %v %v", result, err)
	}
	if writes.Load() != 1 {
		t.Fatalf("result recovery repeated aircraft effect %d times", writes.Load())
	}
}
