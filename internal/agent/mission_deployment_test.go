package agent

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/aero-arc/aero-arc-protos/missiondigest"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/protobuf/proto"
)

func TestValidateMissionCommandModelsSignedCentimeterAltitude(t *testing.T) {
	command := validMissionCommand(t, "command-1")
	command.Plan.Items[0].AltitudeM = float32(math.Copysign(0, -1))
	setMissionDigest(t, command)
	_, _, err := validateMissionCommand(command, time.Now())
	if err == nil || !strings.Contains(err.Error(), "centimeter storage") {
		t.Fatalf("validateMissionCommand() error = %v", err)
	}
	command.Plan.Items[0].AltitudeM = 16.8
	setMissionDigest(t, command)
	if _, _, err := validateMissionCommand(command, time.Now()); err == nil || !strings.Contains(err.Error(), "centimeter storage") {
		t.Fatalf("16.8m truncating-centimeter validation error = %v", err)
	}
	command.Plan.Items[0].AltitudeM = 20.1
	setMissionDigest(t, command)
	if _, _, err := validateMissionCommand(command, time.Now()); err != nil {
		t.Fatalf("20.1m signed-centimeter altitude rejected: %v", err)
	}
}

func TestMissionDigestUsesSharedSchemaOneGoldenVector(t *testing.T) {
	plan := &agentv1.MissionPlan{SchemaVersion: 1, Items: []*agentv1.MissionItem{{
		Sequence: 0, Frame: 0, Command: 16, Autocontinue: true,
		LatitudeE7: -353632620, LongitudeE7: 1491652370, AltitudeM: 20.1,
	}}}
	const want = "6efa96b36af29a800d53ee7d7baf57d4b24f00d9ce2b408327281e74824acf4f"
	if got, err := digestMissionPlan(plan); err != nil || got != want {
		t.Fatalf("digestMissionPlan() = %q, %v; want %q", got, err, want)
	}
	canonical, err := missiondigest.CanonicalBytes(plan)
	if err != nil || len(canonical) == 0 {
		t.Fatalf("CanonicalBytes() = %x, %v", canonical, err)
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

func TestSchemaOneAcceptsOnlyGlobalFrameZero(t *testing.T) {
	command := validMissionCommand(t, "frame-1")
	command.Plan.Items[0].Frame = uint32(common.MAV_FRAME_GLOBAL_RELATIVE_ALT)
	setMissionDigest(t, command)
	if _, _, err := validateMissionCommand(command, time.Now()); err == nil || !strings.Contains(err.Error(), "unsupported frame") {
		t.Fatalf("relative-alt frame validation error = %v", err)
	}
}

func TestCanonicalLandRequiresArduPilotReadbackParam4(t *testing.T) {
	command := validMissionCommand(t, "land-param-1")
	command.Plan.Items[0].Command = uint32(common.MAV_CMD_NAV_LAND)
	setMissionDigest(t, command)
	if _, _, err := validateMissionCommand(command, time.Now()); err == nil || !strings.Contains(err.Error(), "canonical values") {
		t.Fatalf("LAND param4=0 validation error = %v", err)
	}
	command.Plan.Items[0].Param4 = 1
	setMissionDigest(t, command)
	if _, _, err := validateMissionCommand(command, time.Now()); err != nil {
		t.Fatalf("LAND param4=1 rejected: %v", err)
	}
}

func TestCanonicalMissionParametersRejectNegativeZero(t *testing.T) {
	command := validMissionCommand(t, "negative-zero-1")
	command.Plan.Items[0].Param1 = math.Copysign(0, -1)
	setMissionDigest(t, command)
	if _, _, err := validateMissionCommand(command, time.Now()); err == nil || !strings.Contains(err.Error(), "canonical values") {
		t.Fatalf("negative zero validation error = %v", err)
	}
}

func TestLegacyCoordinateRestrictionAppliesOnlyToActualLegacyRequest(t *testing.T) {
	const latitudeE7 int32 = -353632608
	const longitudeE7 int32 = 1491652352
	if !legacyCoordinateRoundTrips(latitudeE7) || !legacyCoordinateRoundTrips(longitudeE7) {
		t.Fatal("known Canberra coordinates did not pass legacy float32 round-trip")
	}
	if legacyCoordinateRoundTrips(latitudeE7 + 1) {
		t.Fatal("non-lossless adjacent latitude passed legacy float32 round-trip")
	}
	command := validMissionCommand(t, "int-coordinate-1")
	command.Plan.Items[0].LatitudeE7 = latitudeE7 + 1
	setMissionDigest(t, command)
	if _, _, err := validateMissionCommand(command, time.Now()); err != nil {
		t.Fatalf("MISSION_ITEM_INT coordinate rejected globally: %v", err)
	}
	target := &mavlinkTarget{systemID: 1, componentID: 1}
	legacy, err := missionItemLegacy(target, &agentv1.MissionItem{LatitudeE7: latitudeE7, LongitudeE7: longitudeE7}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if stored := int32(legacy.X * float32(1e7)); stored != latitudeE7 {
		t.Fatalf("stored latitude = %d, want %d", stored, latitudeE7)
	}
	if _, err := missionItemLegacy(target, &agentv1.MissionItem{LatitudeE7: latitudeE7 + 1, LongitudeE7: longitudeE7}, 1); err == nil {
		t.Fatal("non-lossless legacy coordinate was accepted")
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
	if _, err := a.wal.LoadMissionDeployment(context.Background(), "binding-1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("binding mismatch was admitted durably: %v", err)
	}
}

func TestMissionDeploymentDurablyReplaysTerminalResultAndRejectsIDConflict(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	command := validMissionCommand(t, "mission-1")
	command.ExpiresAtUnixMs = time.Now().Add(500 * time.Millisecond).UnixMilli()
	digest := command.Binding.MissionDigest
	calls := 0
	a.deployMAVLinkMission = func(context.Context, *mavlinkTarget, *agentv1.MissionPlan, bool, int64) (string, uint32, *uint32, error) {
		calls++
		ack := uint32(common.MAV_MISSION_ACCEPTED)
		return digest, 1, &ack, nil
	}
	first := a.executeMissionDeployment(context.Background(), command)
	if first.Status != agentv1.MissionDeploymentResult_STATUS_APPLIED || calls != 1 {
		t.Fatalf("first = %v, calls = %d", first.Status, calls)
	}
	time.Sleep(550 * time.Millisecond)
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

func TestMissionDeploymentCorruptTerminalResultFailsClosed(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	command := validMissionCommand(t, "corrupt-terminal-1")
	payload, fingerprint, err := missionCommandIdentity(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.wal.ReserveMissionDeployment(context.Background(), command.CommandId, fingerprint, payload); err != nil {
		t.Fatal(err)
	}
	if err := a.wal.StoreMissionDeploymentResult(context.Background(), command.CommandId, fingerprint, []byte{0xff}, false); err != nil {
		t.Fatal(err)
	}
	a.deployMAVLinkMission = func(context.Context, *mavlinkTarget, *agentv1.MissionPlan, bool, int64) (string, uint32, *uint32, error) {
		t.Fatal("corrupt terminal result caused another MAVLink effect")
		return "", 0, nil, nil
	}
	result := a.executeMissionDeployment(context.Background(), command)
	if result.Status != agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR || !strings.Contains(result.Message, "corrupt") {
		t.Fatalf("corrupt terminal result = %+v", result)
	}
}

func TestMissionDeploymentRetryableResultHasCompletionTimeWithoutBecomingTerminal(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	a.mavlinkTarget.armed = true
	command := validMissionCommand(t, "retryable-time-1")
	var sent *agentv1.MissionDeploymentResult
	stream := &mockStream{sendFunc: func(message *agentv1.AgentStreamMessage) error {
		sent = message.GetMissionDeploymentResult()
		return nil
	}}
	if err := a.handleMissionDeployment(context.Background(), stream, command); err != nil {
		t.Fatal(err)
	}
	if sent == nil || sent.Status != agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR || sent.CompletedAtUnixMs <= 0 {
		t.Fatalf("retryable wire result = %+v", sent)
	}
	record, err := a.wal.LoadMissionDeployment(context.Background(), command.CommandId)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != "prepared" || len(record.ResultPayload) != 0 {
		t.Fatalf("retryable result became durable terminal state: %+v", record)
	}
}

func TestMissionDeploymentAcquiresExplicitOnGroundEvidenceBeforeUpload(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	a.mavlinkTarget.landedState = common.MAV_LANDED_STATE_UNDEFINED
	a.mavlinkTarget.landedStateAt = time.Time{}
	command := validMissionCommand(t, "acquire-ground-1")
	a.writeMAVLinkCommand = func(channel *gomavlib.Channel, request *common.MessageCommandLong) error {
		if channel != a.mavlinkTarget.channel || request.Command != common.MAV_CMD_REQUEST_MESSAGE ||
			request.TargetSystem != a.mavlinkTarget.systemID || request.TargetComponent != a.mavlinkTarget.componentID || request.Param1 != 245 {
			t.Fatalf("unexpected EXTENDED_SYS_STATE request: channel=%p request=%+v", channel, request)
		}
		a.observeMAVLinkLandedState(channel, request.TargetSystem, request.TargetComponent, common.MAV_LANDED_STATE_ON_GROUND)
		return nil
	}
	a.deployMAVLinkMission = func(context.Context, *mavlinkTarget, *agentv1.MissionPlan, bool, int64) (string, uint32, *uint32, error) {
		ack := uint32(common.MAV_MISSION_ACCEPTED)
		return command.Binding.MissionDigest, 1, &ack, nil
	}
	result := a.executeMissionDeployment(context.Background(), command)
	if result.Status != agentv1.MissionDeploymentResult_STATUS_APPLIED {
		t.Fatalf("explicit landed-state acquisition result = %+v", result)
	}
}

func TestAcquireFreshLandedStateRejectsWrongTargetAndTimeout(t *testing.T) {
	for _, test := range []struct {
		name        string
		wrongTarget bool
	}{
		{name: "wrong target", wrongTarget: true},
		{name: "timeout"},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now()
			target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, heartbeatAt: now}
			a := &Agent{mavlinkTarget: target, options: &AgentOptions{AircraftCommandTimeout: 20 * time.Millisecond}}
			a.writeMAVLinkCommand = func(channel *gomavlib.Channel, request *common.MessageCommandLong) error {
				if request.Command != common.MAV_CMD_REQUEST_MESSAGE || request.Param1 != 245 {
					t.Fatalf("request = %+v", request)
				}
				if test.wrongTarget {
					a.observeMAVLinkLandedState(channel, 2, request.TargetComponent, common.MAV_LANDED_STATE_ON_GROUND)
				}
				return nil
			}
			if _, err := a.acquireFreshLandedState(context.Background(), target); err == nil || !strings.Contains(err.Error(), "timed out") {
				t.Fatalf("acquireFreshLandedState() error = %v", err)
			}
			if a.mavlinkTarget.landedStateSequence != 0 {
				t.Fatal("wrong-target EXTENDED_SYS_STATE updated selected target")
			}
		})
	}
}

func TestMissionDeploymentUnknownRetryReconcilesBeforeAnyUpload(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	command := validMissionCommand(t, "uncertain-1")
	digest := command.Binding.MissionDigest
	readbackFlags := []bool{}
	a.deployMAVLinkMission = func(_ context.Context, _ *mavlinkTarget, _ *agentv1.MissionPlan, readbackOnly bool, _ int64) (string, uint32, *uint32, error) {
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

func TestMissionDeploymentUnknownRetryReplacesDefinitiveMismatchBeforeExpiry(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	command := validMissionCommand(t, "uncertain-empty-replacement-1")
	payload, fingerprint, err := missionCommandIdentity(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := a.wal.ReserveMissionDeployment(context.Background(), command.CommandId, fingerprint, payload); err != nil || !created {
		t.Fatalf("reserve uncertain command = %v, %v", created, err)
	}
	if err := a.wal.MarkMissionDeploymentEffectStarted(context.Background(), command.CommandId, fingerprint); err != nil {
		t.Fatal(err)
	}
	readbackFlags := []bool{}
	a.deployMAVLinkMission = func(_ context.Context, _ *mavlinkTarget, _ *agentv1.MissionPlan, readbackOnly bool, _ int64) (string, uint32, *uint32, error) {
		readbackFlags = append(readbackFlags, readbackOnly)
		if readbackOnly {
			// A zero MISSION_COUNT is translated to this definitive sentinel by
			// the MAVLink adapter, allowing safe pre-expiry replacement.
			return "", 0, nil, errOnboardMismatch
		}
		ack := uint32(common.MAV_MISSION_ACCEPTED)
		return command.Binding.MissionDigest, 1, &ack, nil
	}
	result := a.executeMissionDeployment(context.Background(), command)
	if result.Status != agentv1.MissionDeploymentResult_STATUS_APPLIED || !slices.Equal(readbackFlags, []bool{true, false}) {
		t.Fatalf("pre-expiry mismatch recovery result = %+v, readback flags = %v", result, readbackFlags)
	}
}

func TestConcurrentExactMissionRetryReloadsTerminalStateAfterLock(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	command := validMissionCommand(t, "concurrent-exact-1")
	deploymentStarted := make(chan struct{}, 2)
	releaseDeployment := make(chan struct{})
	var calls atomic.Int32
	a.deployMAVLinkMission = func(context.Context, *mavlinkTarget, *agentv1.MissionPlan, bool, int64) (string, uint32, *uint32, error) {
		calls.Add(1)
		deploymentStarted <- struct{}{}
		<-releaseDeployment
		ack := uint32(common.MAV_MISSION_ACCEPTED)
		return command.Binding.MissionDigest, 1, &ack, nil
	}
	firstResult := make(chan *agentv1.MissionDeploymentResult, 1)
	go func() { firstResult <- a.executeMissionDeployment(context.Background(), command) }()
	select {
	case <-deploymentStarted:
	case <-time.After(time.Second):
		t.Fatal("first deployment did not reach MAVLink")
	}
	secondResult := make(chan *agentv1.MissionDeploymentResult, 1)
	go func() {
		secondResult <- a.executeMissionDeployment(context.Background(), proto.Clone(command).(*agentv1.DeployMissionCommand))
	}()
	// The first invocation owns operationContextMu while blocked in MAVLink. Give
	// the exact retry time to load effect_started and wait on that lock.
	time.Sleep(20 * time.Millisecond)
	close(releaseDeployment)
	var first, second *agentv1.MissionDeploymentResult
	select {
	case first = <-firstResult:
	case <-time.After(time.Second):
		t.Fatal("first deployment did not finish")
	}
	select {
	case second = <-secondResult:
	case <-time.After(time.Second):
		t.Fatal("exact concurrent retry did not finish")
	}
	if first.GetStatus() != agentv1.MissionDeploymentResult_STATUS_APPLIED || !proto.Equal(first, second) || calls.Load() != 1 {
		t.Fatalf("concurrent results first=%+v second=%+v MAVLink calls=%d", first, second, calls.Load())
	}
}

func TestMissionDeploymentFirstSeenExpiredIsRejectedWithoutAdmissionOrEffect(t *testing.T) {
	a, closeWAL := testMissionAgent(t)
	defer closeWAL()
	command := validMissionCommand(t, "expired-first-seen-1")
	command.IssuedAtUnixMs = time.Now().Add(-2 * time.Minute).UnixMilli()
	command.ExpiresAtUnixMs = time.Now().Add(-time.Minute).UnixMilli()
	a.deployMAVLinkMission = func(context.Context, *mavlinkTarget, *agentv1.MissionPlan, bool, int64) (string, uint32, *uint32, error) {
		t.Fatal("expired first-seen command reached MAVLink")
		return "", 0, nil, nil
	}
	result := a.executeMissionDeployment(context.Background(), command)
	if result.Status != agentv1.MissionDeploymentResult_STATUS_REJECTED {
		t.Fatalf("expired first-seen result = %+v", result)
	}
	if _, err := a.wal.LoadMissionDeployment(context.Background(), command.CommandId); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expired first-seen command was admitted: %v", err)
	}
}

func TestExpiredUncertainDeploymentIsReadbackOnly(t *testing.T) {
	for _, test := range []struct {
		name        string
		readback    string
		readbackErr error
		wantStatus  agentv1.MissionDeploymentResult_Status
	}{
		{name: "matching", readback: "requested", wantStatus: agentv1.MissionDeploymentResult_STATUS_ALREADY_APPLIED},
		{name: "mismatching", readback: "different", wantStatus: agentv1.MissionDeploymentResult_STATUS_ONBOARD_MISSION_MISMATCH},
		{name: "invalid onboard count", readbackErr: errOnboardMismatch, wantStatus: agentv1.MissionDeploymentResult_STATUS_ONBOARD_MISSION_MISMATCH},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, closeWAL := testMissionAgent(t)
			defer closeWAL()
			command := validMissionCommand(t, "expired-recovery-"+test.name)
			command.IssuedAtUnixMs = time.Now().Add(-2 * time.Minute).UnixMilli()
			command.ExpiresAtUnixMs = time.Now().Add(-time.Minute).UnixMilli()
			payload, fingerprint, err := missionCommandIdentity(command)
			if err != nil {
				t.Fatal(err)
			}
			if _, created, err := a.wal.ReserveMissionDeployment(context.Background(), command.CommandId, fingerprint, payload); err != nil || !created {
				t.Fatalf("reserve uncertain command = %v, %v", created, err)
			}
			if err := a.wal.MarkMissionDeploymentEffectStarted(context.Background(), command.CommandId, fingerprint); err != nil {
				t.Fatal(err)
			}
			// Recovery readback is effect-free and remains permitted after the
			// operation context has moved on.
			a.operationContext = &wal.OperationContext{AircraftID: "other-aircraft", FlightID: "other-flight", IntentID: "other-intent", IntentVersion: 2}
			calls := 0
			a.deployMAVLinkMission = func(_ context.Context, _ *mavlinkTarget, _ *agentv1.MissionPlan, readbackOnly bool, _ int64) (string, uint32, *uint32, error) {
				calls++
				if !readbackOnly {
					t.Fatal("expired uncertain command attempted a replacement upload")
				}
				if test.readbackErr != nil {
					return "", 0, nil, test.readbackErr
				}
				if test.readback == "requested" {
					return command.Binding.MissionDigest, 0, nil, nil
				}
				return strings.Repeat("0", 64), 0, nil, nil
			}
			result := a.executeMissionDeployment(context.Background(), command)
			if result.Status != test.wantStatus || calls != 1 {
				t.Fatalf("expired recovery result = %+v, calls=%d", result, calls)
			}
			replayed := a.executeMissionDeployment(context.Background(), command)
			if !proto.Equal(result, replayed) || calls != 1 {
				t.Fatalf("terminal expired recovery was not replayed: first=%+v replay=%+v calls=%d", result, replayed, calls)
			}
		})
	}
}

func TestMAVLinkMissionUploadRequiresACKAndCanonicalReadback(t *testing.T) {
	command := validMissionCommand(t, "protocol-1")
	command.Plan.Items = append(command.Plan.Items, &agentv1.MissionItem{Sequence: 1, Frame: 0, Command: 16,
		Autocontinue: true, LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 110})
	setMissionDigest(t, command)
	now := time.Now()
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, heartbeatAt: now,
		landedState: common.MAV_LANDED_STATE_ON_GROUND, landedStateAt: now}
	a := &Agent{mavlinkTarget: target, options: &AgentOptions{AircraftCommandTimeout: time.Second}}
	home := &agentv1.MissionItem{Frame: 0, Command: 16, LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 200}
	uploadResponses := 0
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
		a.mavlinkMu.Lock()
		events := a.pendingMissionEvents
		a.mavlinkMu.Unlock()
		switch value := outbound.(type) {
		case *common.MessageMissionCount:
			// A late accepted ACK from an older upload must not end this
			// transaction before its own full wire list was handed off.
			events <- &common.MessageMissionAck{Type: common.MAV_MISSION_ACCEPTED, MissionType: common.MAV_MISSION_TYPE_MISSION}
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
	digest, uploaded, ack, err := a.executeMAVLinkMissionDeployment(context.Background(), target, command.Plan, false, command.ExpiresAtUnixMs)
	if err != nil || digest != command.Binding.MissionDigest || uploaded != 2 || ack == nil || *ack != uint32(common.MAV_MISSION_ACCEPTED) {
		t.Fatalf("upload/readback = digest %q count %d ack %v err %v", digest, uploaded, ack, err)
	}
}

func TestMAVLinkLargeMissionUsesIdleResponseTimeout(t *testing.T) {
	command := validMissionCommand(t, "slow-large-protocol-1")
	command.Plan.Items = make([]*agentv1.MissionItem, maxMissionItems)
	for sequence := range command.Plan.Items {
		command.Plan.Items[sequence] = &agentv1.MissionItem{Sequence: uint32(sequence), Frame: 0, Command: 16,
			Autocontinue: true, LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 100}
	}
	setMissionDigest(t, command)
	now := time.Now()
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, heartbeatAt: now,
		landedState: common.MAV_LANDED_STATE_ON_GROUND, landedStateAt: now}
	const responseTimeout = 15 * time.Millisecond
	const responseDelay = 2 * time.Millisecond
	a := &Agent{mavlinkTarget: target, options: &AgentOptions{AircraftCommandTimeout: responseTimeout}}
	home := &agentv1.MissionItem{Frame: 0, Command: 16, Autocontinue: true,
		LatitudeE7: -353632508, LongitudeE7: 1491652252, AltitudeM: 200}
	listRequests := 0
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
		a.mavlinkMu.Lock()
		events := a.pendingMissionEvents
		a.mavlinkMu.Unlock()
		switch value := outbound.(type) {
		case *common.MessageMissionRequestList:
			listRequests++
			count := uint16(1)
			if listRequests == 2 {
				count = uint16(maxWireMissionItems)
			}
			events <- &common.MessageMissionCount{Count: count, MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionCount:
			events <- &common.MessageMissionRequestInt{Seq: 0, MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionRequestInt:
			time.Sleep(responseDelay)
			item := home
			if value.Seq > 0 {
				item = command.Plan.Items[value.Seq-1]
			}
			events <- missionItemINT(target, item, value.Seq)
		case *common.MessageMissionItemInt:
			time.Sleep(responseDelay)
			if int(value.Seq) < maxMissionItems {
				events <- &common.MessageMissionRequestInt{Seq: value.Seq + 1, MissionType: common.MAV_MISSION_TYPE_MISSION}
			} else {
				events <- &common.MessageMissionAck{Type: common.MAV_MISSION_ACCEPTED, MissionType: common.MAV_MISSION_TYPE_MISSION}
			}
		}
		return nil
	}
	started := time.Now()
	digest, uploaded, ack, err := a.executeMAVLinkMissionDeployment(context.Background(), target, command.Plan, false, command.ExpiresAtUnixMs)
	if err != nil || digest != command.Binding.MissionDigest || uploaded != maxMissionItems || ack == nil || *ack != uint32(common.MAV_MISSION_ACCEPTED) {
		t.Fatalf("slow large upload/readback = digest %q count %d ack %v err %v", digest, uploaded, ack, err)
	}
	if elapsed := time.Since(started); elapsed <= responseTimeout*2 {
		t.Fatalf("large mission completed in %v; test did not exceed former one-shot timeout", elapsed)
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
	home := &agentv1.MissionItem{Frame: 0, Command: 16, LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 200}
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
	_, uploaded, _, err := a.executeMAVLinkMissionDeployment(context.Background(), target, command.Plan, false, command.ExpiresAtUnixMs)
	if !errors.Is(err, errMissionOutcomeUnknown) || uploaded != 1 {
		t.Fatalf("missing/mismatched ACK result = uploaded %d, err %v", uploaded, err)
	}
}

func TestMAVLinkMissionUploadRechecksExpiryAtMissionCountBoundary(t *testing.T) {
	command := validMissionCommand(t, "expired-boundary-1")
	now := time.Now()
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, heartbeatAt: now,
		landedState: common.MAV_LANDED_STATE_ON_GROUND, landedStateAt: now}
	a := &Agent{mavlinkTarget: target, options: &AgentOptions{AircraftCommandTimeout: time.Second}}
	home := &agentv1.MissionItem{Frame: 0, Command: 16, LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 200}
	missionCountSent := false
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
		a.mavlinkMu.Lock()
		events := a.pendingMissionEvents
		a.mavlinkMu.Unlock()
		switch value := outbound.(type) {
		case *common.MessageMissionRequestList:
			events <- &common.MessageMissionCount{Count: 1, MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionRequestInt:
			events <- missionItemINT(target, home, value.Seq)
		case *common.MessageMissionCount:
			missionCountSent = true
		}
		return nil
	}
	_, _, _, err := a.executeMAVLinkMissionDeployment(context.Background(), target, command.Plan, false, now.Add(-time.Millisecond).UnixMilli())
	if err == nil || !strings.Contains(err.Error(), "effect deadline expired") {
		t.Fatalf("expired upload boundary error = %v", err)
	}
	if missionCountSent {
		t.Fatal("expired mission reached MISSION_COUNT effect boundary")
	}
}

func TestMAVLinkMissionUploadReadsHomeFromLargerExistingMission(t *testing.T) {
	command := validMissionCommand(t, "large-existing-mission-1")
	now := time.Now()
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, heartbeatAt: now,
		landedState: common.MAV_LANDED_STATE_ON_GROUND, landedStateAt: now}
	a := &Agent{mavlinkTarget: target, options: &AgentOptions{AircraftCommandTimeout: time.Second}}
	home := &agentv1.MissionItem{Frame: 0, Command: 16, LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 200}
	cancelledHomeRead := false
	replacementStarted := false
	errReplacementObserved := errors.New("replacement MISSION_COUNT observed")
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
		a.mavlinkMu.Lock()
		events := a.pendingMissionEvents
		a.mavlinkMu.Unlock()
		switch value := outbound.(type) {
		case *common.MessageMissionRequestList:
			events <- &common.MessageMissionCount{Count: uint16(maxWireMissionItems + 10), MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionRequestInt:
			if value.Seq != 0 {
				t.Fatalf("HOME-only readback requested sequence %d", value.Seq)
			}
			events <- missionItemINT(target, home, 0)
		case *common.MessageMissionAck:
			if value.Type == common.MAV_MISSION_OPERATION_CANCELLED {
				cancelledHomeRead = true
			}
		case *common.MessageMissionCount:
			replacementStarted = true
			return errReplacementObserved
		}
		return nil
	}
	_, _, _, err := a.executeMAVLinkMissionDeployment(context.Background(), target, command.Plan, false, command.ExpiresAtUnixMs)
	if !errors.Is(err, errMissionOutcomeUnknown) || !strings.Contains(err.Error(), errReplacementObserved.Error()) {
		t.Fatalf("oversized existing mission replacement error = %v", err)
	}
	if !cancelledHomeRead || !replacementStarted {
		t.Fatalf("HOME-only read cancelled=%v replacement started=%v, want both true", cancelledHomeRead, replacementStarted)
	}
}

func TestMAVLinkMissionUploadBootstrapsArduPilotHomeFromEmptyMission(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "first attempt"
		if recovery {
			name = "pre-expiry recovery"
		}
		t.Run(name, func(t *testing.T) {
			a, closeWAL := testMissionAgent(t)
			defer closeWAL()
			a.options = &AgentOptions{AircraftCommandTimeout: 20 * time.Millisecond}
			command := validMissionCommand(t, "empty-home-"+strings.ReplaceAll(name, " ", "-"))
			if recovery {
				payload, fingerprint, err := missionCommandIdentity(command)
				if err != nil {
					t.Fatal(err)
				}
				if _, created, err := a.wal.ReserveMissionDeployment(context.Background(), command.CommandId, fingerprint, payload); err != nil || !created {
					t.Fatalf("reserve uncertain command = %v, %v", created, err)
				}
				if err := a.wal.MarkMissionDeploymentEffectStarted(context.Background(), command.CommandId, fingerprint); err != nil {
					t.Fatal(err)
				}
			}
			a.deployMAVLinkMission = a.executeMAVLinkMissionDeployment
			target := a.mavlinkTarget
			home := &agentv1.MissionItem{Frame: 0, Command: 16, Autocontinue: true,
				LatitudeE7: -353632508, LongitudeE7: 1491652252, AltitudeM: 200}
			emptyReadbacks := 1
			if recovery {
				emptyReadbacks = 2
			}
			listRequests := 0
			uploadedItems := make([]*common.MessageMissionItemInt, 0, 2)
			a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
				a.mavlinkMu.Lock()
				events := a.pendingMissionEvents
				a.mavlinkMu.Unlock()
				switch value := outbound.(type) {
				case *common.MessageMissionRequestList:
					listRequests++
					count := uint16(0)
					if listRequests > emptyReadbacks {
						count = 2
					}
					events <- &common.MessageMissionCount{Count: count, MissionType: common.MAV_MISSION_TYPE_MISSION}
				case *common.MessageMissionCount:
					if value.Count != 2 {
						t.Fatalf("replacement wire count = %d, want 2", value.Count)
					}
					events <- &common.MessageMissionRequestInt{Seq: 0, MissionType: common.MAV_MISSION_TYPE_MISSION}
					events <- &common.MessageMissionRequestInt{Seq: 1, MissionType: common.MAV_MISSION_TYPE_MISSION}
				case *common.MessageMissionItemInt:
					copyValue := *value
					uploadedItems = append(uploadedItems, &copyValue)
					if len(uploadedItems) == 2 {
						events <- &common.MessageMissionAck{Type: common.MAV_MISSION_ACCEPTED, MissionType: common.MAV_MISSION_TYPE_MISSION}
					}
				case *common.MessageMissionRequestInt:
					item := home
					if value.Seq == 1 {
						item = command.Plan.Items[0]
					}
					events <- missionItemINT(target, item, value.Seq)
				}
				return nil
			}
			result := a.executeMissionDeployment(context.Background(), command)
			if result.Status != agentv1.MissionDeploymentResult_STATUS_APPLIED || result.OnboardMissionDigest != command.Binding.MissionDigest {
				t.Fatalf("empty HOME replacement result = %+v", result)
			}
			if len(uploadedItems) != 2 || uploadedItems[0].Seq != 0 || uploadedItems[1].Seq != 1 ||
				uploadedItems[0].X != command.Plan.Items[0].LatitudeE7 || uploadedItems[1].X != command.Plan.Items[0].LatitudeE7 {
				t.Fatalf("HOME bootstrap upload items = %+v", uploadedItems)
			}
		})
	}
}

func TestMAVLinkRecoveryTreatsInvalidMissionCountsAsDefinitiveMismatch(t *testing.T) {
	for _, test := range []struct {
		name  string
		count uint16
	}{
		{name: "empty", count: 0},
		{name: "oversized", count: uint16(maxWireMissionItems + 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := validMissionCommand(t, test.name+"-recovery-1")
			target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1}
			a := &Agent{options: &AgentOptions{AircraftCommandTimeout: time.Second}}
			cancelledRead := false
			a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
				a.mavlinkMu.Lock()
				events := a.pendingMissionEvents
				a.mavlinkMu.Unlock()
				switch value := outbound.(type) {
				case *common.MessageMissionRequestList:
					events <- &common.MessageMissionCount{Count: test.count, MissionType: common.MAV_MISSION_TYPE_MISSION}
				case *common.MessageMissionRequestInt:
					t.Fatalf("invalid-count recovery requested mission sequence %d", value.Seq)
				case *common.MessageMissionAck:
					if value.Type == common.MAV_MISSION_OPERATION_CANCELLED {
						cancelledRead = true
					}
				}
				return nil
			}
			_, _, _, err := a.executeMAVLinkMissionDeployment(context.Background(), target, command.Plan, true, command.ExpiresAtUnixMs)
			if !errors.Is(err, errOnboardMismatch) || errors.Is(err, errMissionOutcomeUnknown) {
				t.Fatalf("invalid-count recovery readback error = %v, want definitive mismatch", err)
			}
			if !cancelledRead {
				t.Fatal("invalid-count recovery did not cancel the mission readback")
			}
		})
	}
}

func TestMAVLinkRecoveryDrainsStaleCountBeforeNewReadbackEpoch(t *testing.T) {
	command := validMissionCommand(t, "stale-count-recovery-1")
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1}
	a := &Agent{options: &AgentOptions{AircraftCommandTimeout: 20 * time.Millisecond}}
	cancelCount := 0
	requestListCount := 0
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
		a.mavlinkMu.Lock()
		events := a.pendingMissionEvents
		a.mavlinkMu.Unlock()
		switch value := outbound.(type) {
		case *common.MessageMissionAck:
			if value.Type == common.MAV_MISSION_OPERATION_CANCELLED {
				cancelCount++
				if cancelCount == 1 {
					// This belongs to the prior timed-out request. It arrives only
					// after the retry has installed its new event channel.
					events <- &common.MessageMissionCount{Count: 0, MissionType: common.MAV_MISSION_TYPE_MISSION}
				}
			}
		case *common.MessageMissionRequestList:
			requestListCount++
			events <- &common.MessageMissionCount{Count: 2, MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionRequestInt:
			item := &agentv1.MissionItem{Frame: 0, Command: 16, Autocontinue: true,
				LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 200}
			if value.Seq == 1 {
				item = command.Plan.Items[0]
			}
			events <- missionItemINT(target, item, value.Seq)
		}
		return nil
	}
	digest, _, _, err := a.executeMAVLinkMissionDeployment(context.Background(), target, command.Plan, true, command.ExpiresAtUnixMs)
	if err != nil || digest != command.Binding.MissionDigest {
		t.Fatalf("readback after stale count = digest %q, err %v; want %q", digest, err, command.Binding.MissionDigest)
	}
	if cancelCount != 1 || requestListCount != 1 {
		t.Fatalf("readback epoch cancel count = %d, request-list count = %d; want 1, 1", cancelCount, requestListCount)
	}
}

func TestMAVLinkReadbackAcceptsMaximumCanonicalPlanPlusHome(t *testing.T) {
	plan := &agentv1.MissionPlan{SchemaVersion: missionSchemaVersion, Items: make([]*agentv1.MissionItem, maxMissionItems)}
	for sequence := range plan.Items {
		plan.Items[sequence] = &agentv1.MissionItem{Sequence: uint32(sequence), Frame: 0, Command: 16,
			Autocontinue: true, LatitudeE7: 410000000 + int32(sequence), LongitudeE7: -870000000, AltitudeM: 100}
	}
	wantDigest, err := digestMissionPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1}
	events := make(chan message.Message, maxWireMissionItems+1)
	home := &agentv1.MissionItem{Frame: 0, Command: 16, LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 200}
	a := &Agent{options: &AgentOptions{AircraftCommandTimeout: time.Second}}
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
		switch value := outbound.(type) {
		case *common.MessageMissionRequestList:
			events <- &common.MessageMissionCount{Count: uint16(maxWireMissionItems), MissionType: common.MAV_MISSION_TYPE_MISSION}
		case *common.MessageMissionRequestInt:
			item := home
			if value.Seq > 0 {
				item = plan.Items[value.Seq-1]
			}
			events <- missionItemINT(target, item, value.Seq)
		}
		return nil
	}
	gotDigest, err := a.readbackMAVLinkMission(context.Background(), target, events)
	if err != nil || gotDigest != wantDigest {
		t.Fatalf("maximum readback digest = %q, %v; want %q", gotDigest, err, wantDigest)
	}
}

func TestMAVLinkReadbackRestartsAfterRepeatedMissionCount(t *testing.T) {
	plan := &agentv1.MissionPlan{SchemaVersion: missionSchemaVersion, Items: []*agentv1.MissionItem{
		{Sequence: 0, Frame: 0, Command: 16, Autocontinue: true, LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 110},
		{Sequence: 1, Frame: 0, Command: 16, Autocontinue: true, LatitudeE7: -353632708, LongitudeE7: 1491652452, AltitudeM: 120},
	}}
	wantDigest, err := digestMissionPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1}
	home := &agentv1.MissionItem{Frame: 0, Command: 16, LatitudeE7: -353632508, LongitudeE7: 1491652252, AltitudeM: 200}
	events := make(chan message.Message, 8)

	requested := make([]uint16, 0, 5)
	a := &Agent{options: &AgentOptions{AircraftCommandTimeout: 20 * time.Millisecond}}
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, outbound message.Message) error {
		switch request := outbound.(type) {
		case *common.MessageMissionRequestList:
			events <- &common.MessageMissionCount{Count: 3, MissionType: common.MAV_MISSION_TYPE_MISSION}
			events <- missionItemINT(target, home, 0)
			// ArduPilot may restart the transfer after partial progress. An item
			// queued for the old epoch must not advance the restarted transfer or
			// leave holes.
			events <- &common.MessageMissionCount{Count: 3, MissionType: common.MAV_MISSION_TYPE_MISSION}
			events <- missionItemINT(target, plan.Items[0], 1)
			events <- missionItemINT(target, home, 0)
			events <- missionItemINT(target, plan.Items[0], 1)
			events <- missionItemINT(target, plan.Items[1], 2)
		case *common.MessageMissionRequestInt:
			requested = append(requested, request.Seq)
		}
		return nil
	}
	gotDigest, err := a.readbackMAVLinkMission(context.Background(), target, events)
	if err != nil || gotDigest != wantDigest {
		t.Fatalf("restarted readback digest = %q, %v; want %q", gotDigest, err, wantDigest)
	}
	wantRequested := []uint16{0, 1, 0, 1, 2}
	if !slices.Equal(requested, wantRequested) {
		t.Fatalf("requested sequences = %v, want %v", requested, wantRequested)
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
		Plan: &agentv1.MissionPlan{SchemaVersion: 1, Items: []*agentv1.MissionItem{{Sequence: 0, Frame: 0,
			Command: 16, Autocontinue: true, LatitudeE7: -353632608, LongitudeE7: 1491652352, AltitudeM: 100}}},
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
