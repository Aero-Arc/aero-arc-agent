package agent

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/protobuf/proto"
)

func TestValidateMissionCommandRejectsNonCanonicalFloat(t *testing.T) {
	command := validMissionCommand(t, "command-1")
	command.Plan.Items[0].AltitudeM = 100.1
	setMissionDigest(t, command)
	_, _, err := validateMissionCommand(command, time.Now())
	if err == nil || !strings.Contains(err.Error(), "float32-canonical") {
		t.Fatalf("validateMissionCommand() error = %v", err)
	}
}

func TestMissionCurrentBitIsRejectedAndReadbackNormalized(t *testing.T) {
	command := validMissionCommand(t, "current-1")
	command.Plan.Items[0].Current = true
	setMissionDigest(t, command)
	if _, _, err := validateMissionCommand(command, time.Now()); err == nil || !strings.Contains(err.Error(), "current must be false") {
		t.Fatalf("current=true validation error = %v", err)
	}
	readback := protoMissionItem(&common.MessageMissionItemInt{Current: 1})
	if readback.Current {
		t.Fatal("dynamic autopilot current bit entered canonical readback")
	}
}

func TestMissionDeploymentRequiresExactAircraftOperationBinding(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	a.operationContext.AircraftID = "another-aircraft"
	result := a.executeMissionDeployment(context.Background(), validMissionCommand(t, "binding-1"))
	if result.Status != agentv1.MissionDeploymentResult_STATUS_BINDING_MISMATCH {
		t.Fatalf("status = %v, want BINDING_MISMATCH", result.Status)
	}
	if a.deployMAVLinkMission != nil {
		t.Fatal("test transport unexpectedly installed")
	}
}

func TestMissionDeploymentDurablyReplaysTerminalResultAndRejectsIDConflict(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	command := validMissionCommand(t, "mission-1")
	command.ExpiresAtUnixMs = time.Now().Add(25 * time.Millisecond).UnixMilli()
	digest := command.Binding.MissionDigest
	calls := 0
	a.deployMAVLinkMission = func(context.Context, *mavlinkTarget, *agentv1.MissionPlan, bool) (string, uint32, *uint32, error) {
		calls++
		ack := uint32(common.MAV_MISSION_ACCEPTED)
		return digest, 1, &ack, nil
	}
	first := a.executeMissionDeployment(context.Background(), command)
	if first.Status != agentv1.MissionDeploymentResult_STATUS_APPLIED || calls != 1 {
		t.Fatalf("first = %v, calls = %d", first.Status, calls)
	}
	time.Sleep(30 * time.Millisecond)
	replayed := a.executeMissionDeployment(context.Background(), proto.Clone(command).(*agentv1.DeployMissionCommand))
	if !proto.Equal(first, replayed) || calls != 1 {
		t.Fatalf("terminal retry was not replayed: first=%v replayed=%v calls=%d", first, replayed, calls)
	}
	conflict := proto.Clone(command).(*agentv1.DeployMissionCommand)
	conflict.Plan.Items[0].AltitudeM = 101
	setMissionDigest(t, conflict)
	conflicted := a.executeMissionDeployment(context.Background(), conflict)
	if conflicted.Status != agentv1.MissionDeploymentResult_STATUS_REJECTED || calls != 1 {
		t.Fatalf("conflict = %v, calls = %d", conflicted.Status, calls)
	}
}

func TestMissionDeploymentUnknownRetryReconcilesBeforeAnyUpload(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	command := validMissionCommand(t, "uncertain-1")
	digest := command.Binding.MissionDigest
	readbackFlags := []bool{}
	a.deployMAVLinkMission = func(_ context.Context, _ *mavlinkTarget, _ *agentv1.MissionPlan, readbackOnly bool) (string, uint32, *uint32, error) {
		readbackFlags = append(readbackFlags, readbackOnly)
		if len(readbackFlags) == 1 {
			return "", 0, nil, errMissionOutcomeUnknown
		}
		return digest, 0, nil, nil
	}
	first := a.executeMissionDeployment(context.Background(), command)
	if first.Status != agentv1.MissionDeploymentResult_STATUS_OUTCOME_UNKNOWN {
		t.Fatalf("first status = %v", first.Status)
	}
	second := a.executeMissionDeployment(context.Background(), command)
	if second.Status != agentv1.MissionDeploymentResult_STATUS_ALREADY_APPLIED {
		t.Fatalf("second status = %v", second.Status)
	}
	if len(readbackFlags) != 2 || readbackFlags[0] || !readbackFlags[1] {
		t.Fatalf("readback flags = %v, want [false true]", readbackFlags)
	}
}

func TestMAVLinkMissionUploadRequiresACKAndCanonicalReadback(t *testing.T) {
	command := validMissionCommand(t, "protocol-1")
	command.Plan.Items = append(command.Plan.Items, &agentv1.MissionItem{Sequence: 1, Frame: 3, Command: 16,
		Autocontinue: true, LatitudeE7: 410001000, LongitudeE7: -870001000, AltitudeM: 110})
	setMissionDigest(t, command)
	now := time.Now()
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, heartbeatAt: now,
		landedState: common.MAV_LANDED_STATE_ON_GROUND, landedStateAt: now}
	a := &Agent{mavlinkTarget: target, options: &AgentOptions{AircraftCommandTimeout: time.Second}}
	home := &agentv1.MissionItem{Frame: 0, Command: 16, LatitudeE7: 409999000, LongitudeE7: -869999000, AltitudeM: 200}
	uploadResponses := 0
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
		a.mavlinkMu.Lock()
		events := a.pendingMissionEvents
		a.mavlinkMu.Unlock()
		switch value := outbound.(type) {
		case *common.MessageMissionCount:
			// MAVLink permits retries and does not promise request order. Resend
			// duplicates, but report only unique uploaded sequences to Relay.
			events <- &common.MessageMissionRequestInt{Seq: 2, MissionType: common.MAV_MISSION_TYPE_MISSION}
			events <- &common.MessageMissionRequestInt{Seq: 0, MissionType: common.MAV_MISSION_TYPE_MISSION}
			events <- &common.MessageMissionRequestInt{Seq: 1, MissionType: common.MAV_MISSION_TYPE_MISSION}
			events <- &common.MessageMissionRequestInt{Seq: 2, MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionItemInt:
			uploadResponses++
			if uploadResponses == 4 {
				events <- &common.MessageMissionAck{Type: common.MAV_MISSION_ACCEPTED, MissionType: common.MAV_MISSION_TYPE_MISSION}
			}
		case *common.MessageMissionRequestList:
			events <- &common.MessageMissionCount{Count: 3, MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionRequestInt:
			item := home
			if value.Seq > 0 {
				item = command.Plan.Items[value.Seq-1]
			}
			itemMessage := missionItemINT(target, item, value.Seq)
			if value.Seq == 1 {
				// ArduPilot reports the execution cursor dynamically; canonical
				// digest normalization must ignore it.
				itemMessage.Current = 1
			}
			events <- itemMessage
		}
		return nil
	}
	digest, uploaded, ack, err := a.executeMAVLinkMissionDeployment(context.Background(), target, command.Plan, false)
	if err != nil || digest != command.Binding.MissionDigest || uploaded != 2 || ack == nil || *ack != uint32(common.MAV_MISSION_ACCEPTED) {
		t.Fatalf("upload/readback = digest %q count %d ack %v err %v", digest, uploaded, ack, err)
	}
}

func TestMissionProtocolCorrelationRejectsMessagesForAnotherGCS(t *testing.T) {
	if missionMessageTargetsAgent(&common.MessageMissionAck{TargetSystem: 42, TargetComponent: mavlinkSourceComponentID}) {
		t.Fatal("mission ACK for another GCS was accepted")
	}
	if !missionMessageTargetsAgent(&common.MessageMissionAck{TargetSystem: mavlinkSourceSystemID, TargetComponent: mavlinkSourceComponentID}) {
		t.Fatal("mission ACK for this Agent was rejected")
	}
}

func TestMAVLinkMissionUploadDoesNotAcceptMissingOrMismatchedACK(t *testing.T) {
	command := validMissionCommand(t, "ack-1")
	now := time.Now()
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, heartbeatAt: now,
		landedState: common.MAV_LANDED_STATE_ON_GROUND, landedStateAt: now}
	a := &Agent{mavlinkTarget: target, options: &AgentOptions{AircraftCommandTimeout: 20 * time.Millisecond}}
	home := &agentv1.MissionItem{Frame: 0, Command: 16, LatitudeE7: 409999000, LongitudeE7: -869999000, AltitudeM: 200}
	uploadResponses := 0
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
		a.mavlinkMu.Lock()
		events := a.pendingMissionEvents
		a.mavlinkMu.Unlock()
		switch value := outbound.(type) {
		case *common.MessageMissionRequestList:
			events <- &common.MessageMissionCount{Count: 2, MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionRequestInt:
			item := home
			if value.Seq == 1 {
				item = command.Plan.Items[0]
			}
			events <- missionItemINT(target, item, value.Seq)
		case *common.MessageMissionCount:
			events <- &common.MessageMissionRequestInt{Seq: 0, MissionType: common.MAV_MISSION_TYPE_MISSION}
			events <- &common.MessageMissionRequestInt{Seq: 1, MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionItemInt:
			uploadResponses++
			if uploadResponses == 2 {
				// Correct source/target but wrong mission type must not complete
				// the standard-mission transaction.
				events <- &common.MessageMissionAck{Type: common.MAV_MISSION_ACCEPTED, MissionType: common.MAV_MISSION_TYPE_FENCE}
			}
		}
		return nil
	}
	_, uploaded, _, err := a.executeMAVLinkMissionDeployment(context.Background(), target, command.Plan, false)
	if !errors.Is(err, errMissionOutcomeUnknown) || uploaded != 1 {
		t.Fatalf("missing/mismatched ACK result = uploaded %d, err %v", uploaded, err)
	}
}

func testMissionAgent(t *testing.T) (*Agent, func()) {
	t.Helper()
	w, err := wal.New(context.Background(), filepath.Join(t.TempDir(), "mission.db"), 1, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	a := &Agent{
		wal: w,
		operationContext: &wal.OperationContext{
			AircraftID: "aircraft-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 1,
		},
		mavlinkTarget: &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1,
			heartbeatAt: now, landedState: common.MAV_LANDED_STATE_ON_GROUND, landedStateAt: now},
	}
	return a, func() {
		if err := w.Close(); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("close WAL: %v", err)
		}
	}
}

func validMissionCommand(t *testing.T, commandID string) *agentv1.DeployMissionCommand {
	t.Helper()
	now := time.Now()
	command := &agentv1.DeployMissionCommand{
		CommandId: commandID, IssuedAtUnixMs: now.Add(-time.Second).UnixMilli(), ExpiresAtUnixMs: now.Add(time.Minute).UnixMilli(),
		Binding: &agentv1.MissionBinding{MissionId: "mission-1", MissionVersion: 1, DeploymentId: "deployment-1",
			OperatorId: "operator-1", AircraftId: "aircraft-1", FlightId: "flight-1", IntentId: "intent-1", IntentVersion: 1},
		Plan: &agentv1.MissionPlan{SchemaVersion: 1, Items: []*agentv1.MissionItem{{Sequence: 0, Frame: 3,
			Command: 16, Autocontinue: true, LatitudeE7: 410000000, LongitudeE7: -870000000, AltitudeM: 100}}},
	}
	setMissionDigest(t, command)
	return command
}

func setMissionDigest(t *testing.T, command *agentv1.DeployMissionCommand) {
	t.Helper()
	digest, err := digestMissionPlan(command.Plan)
	if err != nil {
		t.Fatal(err)
	}
	command.Binding.MissionDigest = digest
}
