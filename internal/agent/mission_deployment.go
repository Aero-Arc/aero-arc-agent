package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
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
	record, created, err := a.wal.ReserveMissionDeployment(ctx, command.CommandId, fingerprint, payload)
	if err != nil {
		if errors.Is(err, wal.ErrMissionDeploymentConflict) {
			result.Status = agentv1.MissionDeploymentResult_STATUS_REJECTED
		} else {
			result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
		}
		result.Message = err.Error()
		return result
	}
	if record.State == "terminal" {
		persisted := &agentv1.MissionDeploymentResult{}
		if err := proto.Unmarshal(record.ResultPayload, persisted); err != nil {
			result.Status = agentv1.MissionDeploymentResult_STATUS_TEMPORARY_ERROR
			result.Message = "durable terminal mission result is corrupt; refusing another MAVLink effect: " + err.Error()
			return result
		}
		return persisted
	}
	recovery := !created && (record.State == "effect_started" || record.State == "outcome_unknown")
	validationTime := time.Now()
	if recovery && command.ExpiresAtUnixMs > 0 {
		// Expiration fences the first effect, not reconciliation of an effect
		// already durably marked uncertain.
		validationTime = time.UnixMilli(command.ExpiresAtUnixMs - 1)
	}
	_, _, validationErr := validateMissionCommand(command, validationTime)
	if validationErr != nil {
		result.Status = agentv1.MissionDeploymentResult_STATUS_REJECTED
		result.Message = validationErr.Error()
		return a.persistMissionResult(ctx, fingerprint, result, false)
	}

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
		return a.persistMissionResult(ctx, fingerprint, result, false)
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
		// A complete mismatch proves the prior attempt did not install the
		// desired mission. A replacement is allowed only if its binding remains
		// active and the aircraft is still safe below.
		if !bindingMatches {
			result.Status = agentv1.MissionDeploymentResult_STATUS_BINDING_MISMATCH
			result.Message = "uncertain deployment was absent on readback, but its operation binding is no longer active"
			return a.persistMissionResult(ctx, fingerprint, result, false)
		}
	}

	now = time.Now()
	if target.armed || target.landedState != 1 || target.landedStateAt.IsZero() || now.Sub(target.landedStateAt) > missionEvidenceTTL {
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
	if command == nil || strings.TrimSpace(command.CommandId) == "" || command.Binding == nil || command.Plan == nil {
		return nil, "", errors.New("command_id, binding, and plan are required")
	}
	b := command.Binding
	if b.MissionId == "" || b.MissionVersion == 0 || b.DeploymentId == "" || b.OperatorId == "" || b.AircraftId == "" || b.FlightId == "" || b.IntentId == "" || b.IntentVersion == 0 {
		return nil, "", errors.New("all mission binding identifiers and positive versions are required")
	}
	if command.ExpiresAtUnixMs <= 0 || now.UnixMilli() > command.ExpiresAtUnixMs || command.IssuedAtUnixMs <= 0 || command.IssuedAtUnixMs > command.ExpiresAtUnixMs {
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
		if item.Param1 != 0 || item.Param2 != 0 || item.Param3 != 0 || item.Param4 != 0 {
			return nil, "", fmt.Errorf("mission item %d parameters must be zero in the first ArduPilot canonical slice", i)
		}
		for _, value := range []float64{item.Param1, item.Param2, item.Param3, item.Param4, item.AltitudeM} {
			if math.IsNaN(value) || math.IsInf(value, 0) || float64(float32(value)) != value {
				return nil, "", fmt.Errorf("mission item %d floating values must be finite and exactly float32-canonical", i)
			}
		}
		if item.LatitudeE7 < -900000000 || item.LatitudeE7 > 900000000 || item.LongitudeE7 < -1800000000 || item.LongitudeE7 > 1800000000 {
			return nil, "", fmt.Errorf("mission item %d coordinates are out of range", i)
		}
		altitudeCM := math.Round(item.AltitudeM * 100)
		if altitudeCM < -8388608 || altitudeCM > 8388607 || float32(altitudeCM/100) != float32(item.AltitudeM) {
			return nil, "", fmt.Errorf("mission item %d altitude must round-trip through ArduPilot centimeter storage", i)
		}
	}
	planPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(command.Plan)
	if err != nil {
		return nil, "", fmt.Errorf("marshal canonical mission: %w", err)
	}
	planDigest := sha256.Sum256(planPayload)
	if b.MissionDigest != hex.EncodeToString(planDigest[:]) {
		return nil, "", errors.New("mission_digest does not match the canonical mission plan")
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		return nil, "", fmt.Errorf("marshal mission command: %w", err)
	}
	fingerprint := sha256.Sum256(payload)
	return payload, hex.EncodeToString(fingerprint[:]), nil
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
	switch frame {
	case 0, 3, 10:
		return true
	default:
		return false
	}
}

func supportedMissionCommand(command uint32) bool {
	switch command {
	case 16, 21, 22:
		return true
	default:
		return false
	}
}
