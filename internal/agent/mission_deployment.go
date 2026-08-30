package agent

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/aero-arc/aero-arc-protos/missiondigest"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	missionSchemaVersion = 1
	maxMissionItems      = 200
	maxWireMissionItems  = maxMissionItems + 1 // ArduPilot HOME plus canonical operational items.
	missionEvidenceTTL   = 3 * time.Second
)

var (
	errMissionOutcomeUnknown = errors.New("MAVLink mission outcome is unknown")
	errOnboardMismatch       = errors.New("onboard mission does not match requested mission")
)

func (a *Agent) handleMissionDeployment(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], command *agentv1.DeployMissionCommand) error {
	result := a.executeMissionDeployment(ctx, command)
	if result.CompletedAtUnixMs == 0 {
		// Retryable outcomes intentionally remain non-terminal in the durable
		// journal, but every wire result is a completed Agent attempt and must
		// carry its own observation time for Relay admission.
		result.CompletedAtUnixMs = time.Now().UnixMilli()
	}
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	return stream.Send(&agentv1.AgentStreamMessage{Payload: &agentv1.AgentStreamMessage_MissionDeploymentResult{MissionDeploymentResult: result}})
}

func (a *Agent) dispatchMissionDeployment(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], command *agentv1.DeployMissionCommand, wg *sync.WaitGroup, errorC chan<- error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.handleMissionDeployment(ctx, stream, command); err != nil {
			select {
			case errorC <- err:
			default:
			}
		}
	}()
}

func (a *Agent) executeMissionDeployment(ctx context.Context, command *agentv1.DeployMissionCommand) *agentv1.MissionDeploymentResult {
	result := newMissionResult(command)
	payload, fingerprint, identityErr := missionCommandIdentity(command)
	if identityErr != nil {
		result.Status = agentv1.MissionDeploymentResult_STATUS_REJECTED
		result.Message = identityErr.Error()
		return result
	}

	// Durable lookup precedes expiry and all other mutable admission checks so
	// exact terminal replay and uncertain readback recovery remain available
	// after the command's effect deadline.
	record, loadErr := a.wal.LoadMissionDeployment(ctx, command.CommandId)
	found := loadErr == nil
	if loadErr != nil && !errors.Is(loadErr, sql.ErrNoRows) {
		result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		result.Message = loadErr.Error()
		return result
	}
	if found && record.PayloadFingerprint != fingerprint {
		result.Status = agentv1.MissionDeploymentResult_STATUS_REJECTED
		result.Message = wal.ErrMissionDeploymentConflict.Error()
		return result
	}
	if found && record.State == "terminal" {
		persisted := &agentv1.MissionDeploymentResult{}
		if err := proto.Unmarshal(record.ResultPayload, persisted); err != nil {
			result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
			result.Message = "durable terminal mission result is corrupt; refusing another MAVLink effect: " + err.Error()
			return result
		}
		return persisted
	}

	recovery := found && (record.State == "effect_started" || record.State == "outcome_unknown")
	_, _, validationErr := validateMissionCommandAt(command, time.Now(), recovery)
	if validationErr != nil {
		result.Status = agentv1.MissionDeploymentResult_STATUS_REJECTED
		result.Message = validationErr.Error()
		if found {
			return a.persistMissionResult(ctx, fingerprint, result, false)
		}
		return result
	}

	// Holding the operation-context lock makes the exact binding fence stable
	// from first admission through any new aircraft effect and verified success.
	// Terminal replay and recovery readback above remain effect-free exemptions.
	a.operationContextMu.Lock()
	defer a.operationContextMu.Unlock()
	a.stateMu.RLock()
	active := a.operationContext
	var contextCopy wal.OperationContext
	if active != nil {
		contextCopy = *active
	}
	a.stateMu.RUnlock()
	bindingMatches := active != nil && contextCopy.AircraftID != "" && contextCopy.AircraftID == command.Binding.AircraftId &&
		contextCopy.FlightID == command.Binding.FlightId && contextCopy.IntentID == command.Binding.IntentId &&
		contextCopy.IntentVersion == command.Binding.IntentVersion
	if !recovery && !bindingMatches {
		result.Status = agentv1.MissionDeploymentResult_STATUS_BINDING_MISMATCH
		result.Message = "mission binding does not exactly match the active aircraft/flight/intent/version context"
		if found {
			return a.persistMissionResult(ctx, fingerprint, result, false)
		}
		return result
	}

	if !found {
		var created bool
		var err error
		record, created, err = a.wal.ReserveMissionDeployment(ctx, command.CommandId, fingerprint, payload)
		if err != nil {
			if errors.Is(err, wal.ErrMissionDeploymentConflict) {
				result.Status = agentv1.MissionDeploymentResult_STATUS_REJECTED
			} else {
				result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
			}
			result.Message = err.Error()
			return result
		}
		if !created {
			// Another process admitted this command after our lookup. Refuse to
			// infer its in-flight state in this process; an exact retry will load
			// and reconcile the durable record from the beginning.
			result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
			result.Message = "mission command was concurrently admitted; retry exact command"
			return result
		}
		found = true
	}

	if !a.tryBeginAircraftCommand() {
		result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		result.Message = "another aircraft-affecting command is in progress"
		return result
	}
	defer a.endAircraftCommand()
	a.mavlinkMu.Lock()
	target := a.mavlinkTarget
	if target != nil {
		copyTarget := *target
		target = &copyTarget
	}
	a.mavlinkMu.Unlock()
	now := time.Now()
	if target == nil || target.channel == nil || target.heartbeatAt.IsZero() || now.Sub(target.heartbeatAt) > missionEvidenceTTL {
		result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		result.Message = "fresh authoritative MAVLink heartbeat evidence is required"
		return result
	}
	if a.deployMAVLinkMission == nil {
		result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		result.Message = "MAVLink mission transport is unavailable"
		return result
	}

	if recovery {
		digest, _, _, readbackErr := a.deployMAVLinkMission(ctx, target, command.Plan, true)
		result.OnboardMissionDigest = digest
		if readbackErr != nil {
			result.Status = agentv1.MissionDeploymentResult_STATUS_OUTCOME_UNKNOWN
			result.Message = readbackErr.Error()
			return a.persistMissionResult(ctx, fingerprint, result, true)
		}
		if digest == command.Binding.MissionDigest {
			result.Status = agentv1.MissionDeploymentResult_STATUS_ALREADY_APPLIED
			result.Message = "uncertain deployment reconciled by verified onboard mission readback"
			return a.persistMissionResult(ctx, fingerprint, result, false)
		}
		if time.Now().UnixMilli() > command.ExpiresAtUnixMs {
			result.Status = agentv1.MissionDeploymentResult_STATUS_ONBOARD_MISSION_MISMATCH
			result.Message = "expired uncertain deployment does not match onboard mission; replacement upload is forbidden"
			return a.persistMissionResult(ctx, fingerprint, result, false)
		}
		// A complete mismatch proves the prior attempt did not install the
		// desired mission. Before expiry, replacement is allowed only if its
		// binding remains active and the aircraft is still safe below.
		if !bindingMatches {
			result.Status = agentv1.MissionDeploymentResult_STATUS_BINDING_MISMATCH
			result.Message = "uncertain deployment was absent on readback, but its operation binding is no longer active"
			return a.persistMissionResult(ctx, fingerprint, result, false)
		}
	}

	now = time.Now()
	if target.armed {
		result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		result.Message = "fresh authoritative MAVLink evidence must show the aircraft disarmed"
		return result
	}
	if target.landedStateAt.IsZero() || now.Sub(target.landedStateAt) > missionEvidenceTTL {
		var acquisitionErr error
		target, acquisitionErr = a.acquireFreshLandedState(ctx, target)
		if acquisitionErr != nil {
			result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
			result.Message = "acquire authoritative on-ground evidence: " + acquisitionErr.Error()
			return result
		}
	}
	now = time.Now()
	if target.armed || target.heartbeatAt.IsZero() || now.Sub(target.heartbeatAt) > missionEvidenceTTL ||
		target.landedState != common.MAV_LANDED_STATE_ON_GROUND || target.landedStateAt.IsZero() || now.Sub(target.landedStateAt) > missionEvidenceTTL {
		result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		result.Message = "fresh authoritative MAVLink evidence must show the aircraft disarmed and on ground"
		return result
	}
	if !recovery {
		if err := a.wal.MarkMissionDeploymentEffectStarted(ctx, command.CommandId, fingerprint); err != nil {
			result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
			result.Message = err.Error()
			return result
		}
	}
	digest, count, ack, err := a.deployMAVLinkMission(ctx, target, command.Plan, false)
	result.OnboardMissionDigest = digest
	result.UploadedItemCount = count
	result.MavlinkMissionAckType = ack
	switch {
	case err == nil && digest == command.Binding.MissionDigest:
		result.Status = agentv1.MissionDeploymentResult_STATUS_APPLIED
		result.Message = "onboard mission readback digest verified"
		return a.persistMissionResult(ctx, fingerprint, result, false)
	case errors.Is(err, errMissionOutcomeUnknown):
		result.Status = agentv1.MissionDeploymentResult_STATUS_OUTCOME_UNKNOWN
		result.Message = err.Error()
		return a.persistMissionResult(ctx, fingerprint, result, true)
	case errors.Is(err, errOnboardMismatch) || (err == nil && digest != command.Binding.MissionDigest):
		result.Status = agentv1.MissionDeploymentResult_STATUS_ONBOARD_MISSION_MISMATCH
		result.Message = "onboard mission readback digest does not match requested mission"
		return a.persistMissionResult(ctx, fingerprint, result, false)
	default:
		result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		result.Message = err.Error()
		return result
	}
}

func (a *Agent) acquireFreshLandedState(ctx context.Context, expected *mavlinkTarget) (*mavlinkTarget, error) {
	if expected == nil || expected.channel == nil || a.writeMAVLinkCommand == nil {
		return nil, errors.New("MAVLink request-message transport is unavailable")
	}
	baseline := expected.landedStateSequence
	request := &common.MessageCommandLong{
		TargetSystem: expected.systemID, TargetComponent: expected.componentID,
		Command: common.MAV_CMD_REQUEST_MESSAGE, Param1: float32((&common.MessageExtendedSysState{}).GetID()),
	}
	if err := a.writeMAVLinkCommand(expected.channel, request); err != nil {
		return nil, fmt.Errorf("request EXTENDED_SYS_STATE: %w", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, a.aircraftCommandTimeout())
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		a.mavlinkMu.Lock()
		current := a.mavlinkTarget
		if current == nil || current.channel != expected.channel || current.systemID != expected.systemID || current.componentID != expected.componentID {
			a.mavlinkMu.Unlock()
			return nil, errors.New("selected autopilot target changed while awaiting EXTENDED_SYS_STATE")
		}
		if current.landedStateSequence > baseline {
			result := *current
			a.mavlinkMu.Unlock()
			return &result, nil
		}
		a.mavlinkMu.Unlock()
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("timed out awaiting matching EXTENDED_SYS_STATE: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (a *Agent) persistMissionResult(ctx context.Context, fingerprint string, result *agentv1.MissionDeploymentResult, uncertain bool) *agentv1.MissionDeploymentResult {
	result.CompletedAtUnixMs = time.Now().UnixMilli()
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		result.Message = "marshal durable mission result: " + err.Error()
		return result
	}
	if err := a.wal.StoreMissionDeploymentResult(ctx, result.CommandId, fingerprint, payload, uncertain); err != nil {
		result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		result.Message = "persist mission result: " + err.Error()
	}
	return result
}

func newMissionResult(command *agentv1.DeployMissionCommand) *agentv1.MissionDeploymentResult {
	result := &agentv1.MissionDeploymentResult{}
	if command != nil {
		result.CommandId = command.CommandId
		if command.Binding != nil {
			result.Binding = proto.Clone(command.Binding).(*agentv1.MissionBinding)
		}
	}
	return result
}

func validateMissionCommand(command *agentv1.DeployMissionCommand, now time.Time) ([]byte, string, error) {
	return validateMissionCommandAt(command, now, false)
}

func validateMissionCommandAt(command *agentv1.DeployMissionCommand, now time.Time, allowExpired bool) ([]byte, string, error) {
	if command == nil || strings.TrimSpace(command.CommandId) == "" || command.Binding == nil || command.Plan == nil {
		return nil, "", errors.New("command_id, binding, and plan are required")
	}
	b := command.Binding
	if b.MissionId == "" || b.MissionVersion == 0 || b.DeploymentId == "" || b.OperatorId == "" || b.AircraftId == "" || b.FlightId == "" || b.IntentId == "" || b.IntentVersion == 0 {
		return nil, "", errors.New("all mission binding identifiers and positive versions are required")
	}
	if command.ExpiresAtUnixMs <= 0 || (!allowExpired && now.UnixMilli() > command.ExpiresAtUnixMs) || command.IssuedAtUnixMs <= 0 || command.IssuedAtUnixMs > command.ExpiresAtUnixMs {
		return nil, "", errors.New("mission command timing is invalid or expired")
	}
	if command.Plan.SchemaVersion != missionSchemaVersion || len(command.Plan.Items) == 0 || len(command.Plan.Items) > maxMissionItems {
		return nil, "", fmt.Errorf("mission plan must use schema version 1 and contain 1..%d items", maxMissionItems)
	}
	for i, item := range command.Plan.Items {
		if item == nil || item.Sequence != uint32(i) {
			return nil, "", errors.New("mission item sequences must be contiguous from zero")
		}
		if !supportedMissionFrame(item.Frame) || !supportedMissionCommand(item.Command) {
			return nil, "", fmt.Errorf("mission item %d has unsupported frame or command", i)
		}
		if item.Current {
			return nil, "", fmt.Errorf("mission item %d current must be false; execution position is dynamic state, not canonical mission content", i)
		}
		if !item.Autocontinue {
			return nil, "", fmt.Errorf("mission item %d autocontinue must be true for the ArduPilot canonical adapter", i)
		}
		if !canonicalMissionParameters(item) {
			return nil, "", fmt.Errorf("mission item %d parameters do not match the ArduPilot canonical values for command %d", i, item.Command)
		}
		for _, value := range []float64{item.Param1, item.Param2, item.Param3, item.Param4, float64(item.AltitudeM)} {
			if math.IsNaN(value) || math.IsInf(value, 0) || float64(float32(value)) != value {
				return nil, "", fmt.Errorf("mission item %d floating values must be finite and exactly float32-canonical", i)
			}
		}
		if item.LatitudeE7 < -900000000 || item.LatitudeE7 > 900000000 || item.LongitudeE7 < -1800000000 || item.LongitudeE7 > 1800000000 {
			return nil, "", fmt.Errorf("mission item %d coordinates are out of range", i)
		}
		altitudeCMValue := math.Round(float64(item.AltitudeM) * 100)
		if altitudeCMValue < math.MinInt32 || altitudeCMValue > math.MaxInt32 {
			return nil, "", fmt.Errorf("mission item %d altitude must round-trip through ArduPilot centimeter storage", i)
		}
		altitudeCM := int32(altitudeCMValue)
		if math.Float32bits(float32(altitudeCM)/100) != math.Float32bits(item.AltitudeM) {
			return nil, "", fmt.Errorf("mission item %d altitude must round-trip through ArduPilot centimeter storage", i)
		}
	}
	planDigest, err := missiondigest.Digest(command.Plan)
	if err != nil {
		return nil, "", fmt.Errorf("encode canonical mission: %w", err)
	}
	if b.MissionDigest != planDigest {
		return nil, "", errors.New("mission_digest does not match the canonical mission plan")
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		return nil, "", fmt.Errorf("marshal mission command: %w", err)
	}
	fingerprint := sha256.Sum256(payload)
	return payload, hex.EncodeToString(fingerprint[:]), nil
}

func canonicalMissionParameters(item *agentv1.MissionItem) bool {
	if item == nil || !positiveZero(item.Param1) || !positiveZero(item.Param2) || !positiveZero(item.Param3) {
		return false
	}
	if item.Command == uint32(common.MAV_CMD_NAV_LAND) {
		return item.Param4 == 1
	}
	return positiveZero(item.Param4)
}

func positiveZero(value float64) bool {
	return value == 0 && !math.Signbit(value)
}

func missionCommandIdentity(command *agentv1.DeployMissionCommand) ([]byte, string, error) {
	if command == nil || strings.TrimSpace(command.CommandId) == "" || command.Binding == nil || command.Plan == nil {
		return nil, "", errors.New("command_id, binding, and plan are required")
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		return nil, "", fmt.Errorf("marshal mission command: %w", err)
	}
	fingerprint := sha256.Sum256(payload)
	return payload, hex.EncodeToString(fingerprint[:]), nil
}

func supportedMissionFrame(frame uint32) bool {
	return frame == uint32(common.MAV_FRAME_GLOBAL)
}

func supportedMissionCommand(command uint32) bool {
	switch command {
	case 16, 21, 22:
		return true
	default:
		return false
	}
}
