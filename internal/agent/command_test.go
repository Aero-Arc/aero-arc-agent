package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-protos/commanddigest"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/aero-arc/aero-arc-protos/missiondigest"
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
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
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
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
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
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
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

func TestDispatchJournalsIdentityBeforeAsyncExecution(t *testing.T) {
	a, cleanup := testMissionAgent(t)
	defer cleanup()
	c := testC2Command(t)
	stream := &mockStream{}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	// Hold execution before its binding check, allowing an immediate duplicate.
	a.operationContextMu.Lock()
	if err := a.dispatchDurableCommand(context.Background(), stream, c, &wg, errs); err != nil {
		a.operationContextMu.Unlock()
		t.Fatal(err)
	}
	record, err := a.wal.LoadCommand(context.Background(), c.CommandId)
	if err != nil || record.Digest != c.CommandDigest {
		a.operationContextMu.Unlock()
		wg.Wait()
		t.Fatalf("dispatch returned before admission: %+v %v", record, err)
	}
	if err := a.dispatchDurableCommand(context.Background(), stream, c, &wg, errs); err != nil {
		t.Error(err)
	}
	record, err = a.wal.LoadCommand(context.Background(), c.CommandId)
	e := &pb.CommandEvidence{}
	if err == nil {
		err = proto.Unmarshal(record.Evidence, e)
	}
	if err != nil || hasStage(e, "rejected") {
		t.Errorf("duplicate rejected active command: %v %v", e, err)
	}
	a.operationContextMu.Unlock()
	wg.Wait()
}

func TestUncertainRecoveryObservesWithoutAppliedOrAnotherEffect(t *testing.T) {
	a, cleanup := testMissionAgent(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c := testC2Command(t)
	payload, _ := proto.Marshal(c)
	evidence, _ := proto.Marshal(&pb.CommandEvidence{CommandId: c.CommandId, CommandDigest: c.CommandDigest})
	if err := a.wal.AdmitCommand(ctx, c.CommandId, wal.CommandRecord{Digest: c.CommandDigest, Payload: payload, Evidence: evidence}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.wal.BeginCommandEffect(ctx, c.CommandId, c.CommandDigest); err != nil {
		t.Fatal(err)
	}
	// A subsequent effect-free rejection must not supersede this observation.
	if err := a.wal.AdmitCommand(ctx, "rejected-other", wal.CommandRecord{Digest: "other", Payload: payload, Evidence: evidence}); err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32
	a.writeMAVLinkMessage = func(*gomavlib.Channel, message.Message) error { writes.Add(1); return nil }
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.mavlinkMu.Lock()
				p := a.c2Pending
				a.mavlinkMu.Unlock()
				if p != nil {
					a.observeC2Frame(&gomavlib.EventFrame{Channel: p.target.channel, Frame: &frame.V2Frame{SystemID: p.target.systemID, ComponentID: p.target.componentID, Message: &common.MessageHeartbeat{BaseMode: common.MAV_MODE_FLAG_SAFETY_ARMED}}})
				}
			}
		}
	}()
	e, err := a.executeDurableCommand(ctx, c, nil)
	cancel()
	<-done
	if err != nil || !hasStage(e, "observed") || !hasStage(e, "outcome_unknown") || hasStage(e, "applied") || writes.Load() != 0 {
		t.Fatalf("unsafe recovery: %v err=%v writes=%d", e, err, writes.Load())
	}
}

func TestDurableArmRejectsTargetChangedDuringMissionReadback(t *testing.T) {
	a, cleanup := testMissionAgent(t)
	defer cleanup()
	c := testC2Command(t)
	c.GetMavlink().MissionPrecondition = &pb.MissionPlan{SchemaVersion: 1, Items: []*pb.MissionItem{{Command: 21, Autocontinue: true, Param4: 1}}}
	c.GetMavlink().MissionPreconditionId = "mission"
	c.GetMavlink().MissionPreconditionVersion = 1
	var err error
	c.CommandDigest, err = commanddigest.Digest(c)
	if err != nil {
		t.Fatal(err)
	}
	a.operationContext = &wal.OperationContext{AircraftID: c.AircraftId, FlightID: c.Context.FlightId, IntentID: c.Context.IntentId, IntentVersion: 1}
	a.mavlinkTarget.autopilot = common.MAV_AUTOPILOT_ARDUPILOTMEGA
	a.mavlinkTarget.vehicleType = common.MAV_TYPE_QUADROTOR
	a.mavlinkTarget.heartbeatAt = time.Now()
	a.deployMAVLinkMission = func(_ context.Context, _ *mavlinkTarget, plan *pb.MissionPlan, _ bool, _ int64) (string, uint32, *uint32, error) {
		a.mavlinkMu.Lock()
		next := *a.mavlinkTarget
		next.systemID++
		a.mavlinkTarget = &next
		a.mavlinkMu.Unlock()
		digest, err := missiondigest.Digest(plan)
		return digest, 1, nil, err
	}
	var writes atomic.Int32
	a.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error { writes.Add(1); return nil }
	e, err := a.executeDurableCommand(context.Background(), c, nil)
	if err != nil || !hasStage(e, "rejected") || writes.Load() != 0 {
		t.Fatalf("retargeted ARM: %v err=%v writes=%d", e, err, writes.Load())
	}
}

func TestSupersededAdmissionCannotAcquireEffectPermit(t *testing.T) {
	a, cleanup := testMissionAgent(t)
	defer cleanup()
	ctx := context.Background()
	for _, id := range []string{"old", "new"} {
		if err := a.wal.AdmitCommand(ctx, id, wal.CommandRecord{Digest: id, Payload: []byte{}, Evidence: []byte{}}); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := a.wal.BeginCommandEffect(ctx, "new", "new"); err != nil || !ok {
		t.Fatalf("new permit: %v %v", ok, err)
	}
	if ok, err := a.wal.BeginCommandEffect(ctx, "old", "old"); ok || !errors.Is(err, wal.ErrCommandSuperseded) {
		t.Fatalf("superseded effect allowed: %v %v", ok, err)
	}
	record, err := a.wal.LoadCommand(ctx, "old")
	if err != nil || record.EffectStarted {
		t.Fatalf("stale effect fence consumed: %+v %v", record, err)
	}
}

func TestMalformedMavlinkParametersFailBeforeJournalAdmission(t *testing.T) {
	a, cleanup := testMissionAgent(t)
	defer cleanup()
	c := testC2Command(t)
	c.GetMavlink().Command = 21
	c.GetMavlink().Parameters = []float32{0}
	if _, err := a.executeDurableCommand(context.Background(), c, nil); err == nil {
		t.Fatal("malformed envelope accepted")
	}
	if _, err := a.wal.LoadCommand(context.Background(), c.CommandId); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("malformed command journaled: %v", err)
	}
}

func TestMissionStartRejectsDisarmedAutopilotBeforeEffect(t *testing.T) {
	a, cleanup := testMissionAgent(t)
	defer cleanup()
	c := testC2Command(t)
	c.Definition = "MISSION_START"
	c.GetMavlink().Command = 300
	c.GetMavlink().Parameters[0] = 0
	c.GetMavlink().Observation = "mission_running"
	var err error
	c.CommandDigest, err = commanddigest.Digest(c)
	if err != nil {
		t.Fatal(err)
	}
	a.operationContext = &wal.OperationContext{AircraftID: c.AircraftId, FlightID: c.Context.FlightId, IntentID: c.Context.IntentId, IntentVersion: 1}
	a.mavlinkTarget.armed = false
	a.mavlinkTarget.heartbeatAt = time.Now()
	var writes atomic.Int32
	a.writeMAVLinkMessage = func(*gomavlib.Channel, message.Message) error { writes.Add(1); return nil }
	e, err := a.executeDurableCommand(context.Background(), c, nil)
	if err != nil || !hasStage(e, "rejected") || writes.Load() != 0 {
		t.Fatalf("disarmed mission start: %v %v writes=%d", e, err, writes.Load())
	}
	record, err := a.wal.LoadCommand(context.Background(), c.CommandId)
	if err != nil || record.EffectStarted {
		t.Fatalf("precondition consumed effect permit: %+v %v", record, err)
	}
}

func TestMalformedDurableCommandsDoNotEndTelemetry(t *testing.T) {
	for _, busy := range []bool{false, true} {
		for _, kind := range []string{"nil", "digest", "agent"} {
			t.Run(fmt.Sprintf("%t/%s", busy, kind), func(t *testing.T) {
				a := &Agent{}
				if busy {
					a.c2Mu.Lock()
					defer a.c2Mu.Unlock()
				}
				c := testC2Command(t)
				switch kind {
				case "nil":
					c = nil
				case "digest":
					c.CommandDigest = "wrong"
				case "agent":
					c.AgentId = "other"
					c.CommandDigest, _ = commanddigest.Digest(c)
				}
				var wg sync.WaitGroup
				if err := a.dispatchDurableCommand(context.Background(), nil, c, &wg, make(chan error, 1)); err != nil {
					t.Fatalf("invalid command terminated telemetry: %v", err)
				}
				wg.Wait()
			})
		}
	}
}

func TestC2MissionEffectFenceBeginsAtMissionWrite(t *testing.T) {
	for _, scenario := range []string{"busy", "no-target", "no-transport", "write"} {
		t.Run(scenario, func(t *testing.T) {
			a, closeWAL := testMissionAgent(t)
			defer closeWAL()
			ctx := context.Background()
			if err := a.wal.AdmitCommand(ctx, "older", wal.CommandRecord{Digest: "older", Payload: []byte{}, Evidence: []byte{}}); err != nil {
				t.Fatal(err)
			}
			if _, err := a.wal.BeginCommandEffect(ctx, "older", "older"); err != nil {
				t.Fatal(err)
			}
			mission := validMissionCommand(t, "mission-c2")
			c := &pb.DurableCommand{CommandId: mission.CommandId, OperatorId: "operator-1", AircraftId: "aircraft-1", AgentId: identity.Resolve().FinalID, Context: &pb.OperationContext{AircraftId: "aircraft-1", FlightId: "flight-1", IntentId: "intent-1", IntentVersion: 1}, Definition: "MISSION_UPLOAD", DefinitionVersion: 1, Capability: "mission_upload_v1", IssuedAtUnixMs: mission.IssuedAtUnixMs, ExpiresAtUnixMs: mission.ExpiresAtUnixMs, RecoveryPolicy: "mission_readback_v1", Execution: &pb.DurableCommand_Mission{Mission: mission}}
			var err error
			c.CommandDigest, err = commanddigest.Digest(c)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "busy" {
				a.aircraftCommandActive = true
			}
			if scenario == "no-target" {
				a.mavlinkTarget = nil
			}
			writes := 0
			if scenario != "no-transport" {
				a.deployMAVLinkMission = func(context.Context, *mavlinkTarget, *pb.MissionPlan, bool, int64) (string, uint32, *uint32, error) {
					writes++
					record, err := a.wal.LoadCommand(ctx, c.CommandId)
					if err != nil || !record.EffectStarted {
						t.Fatalf("write without paired fence: %+v %v", record, err)
					}
					return mission.Binding.MissionDigest, 1, nil, nil
				}
			}
			e, err := a.executeDurableCommand(ctx, c, nil)
			if err != nil {
				t.Fatal(err)
			}
			record, err := a.wal.LoadCommand(ctx, c.CommandId)
			if err != nil {
				t.Fatal(err)
			}
			latest, err := a.wal.CommandIsLatest(ctx, "older")
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "write" {
				if writes != 1 || !record.EffectStarted || latest || !hasStage(e, "observed") {
					t.Fatalf("write result: %+v latest=%v writes=%d", e, latest, writes)
				}
			} else if writes != 0 || record.EffectStarted || !latest {
				t.Fatalf("effect-free attempt superseded prior command: %+v latest=%v writes=%d", record, latest, writes)
			}
		})
	}
}
