package agent

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"google.golang.org/grpc"
)

const (
	defaultAircraftCommandTimeout = 4 * time.Second
	mavlinkSourceSystemID         = 254
	mavlinkSourceComponentID      = uint8(common.MAV_COMP_ID_ONBOARD_COMPUTER)
)

type mavlinkTarget struct {
	channel             *gomavlib.Channel
	systemID            uint8
	componentID         uint8
	heartbeatSequence   uint64
	armed               bool
	heartbeatAt         time.Time
	landedState         common.MAV_LANDED_STATE
	landedStateAt       time.Time
	landedStateSequence uint64
}

type mavlinkCommandAck struct {
	systemID    uint8
	componentID uint8
	result      common.MAV_RESULT
}

type mavlinkArmedStateEvidence struct {
	armed            bool
	oppositeObserved bool
}

type pendingMAVLinkCommand struct {
	channel            *gomavlib.Channel
	systemID           uint8
	componentID        uint8
	command            common.MAV_CMD
	desiredArmed       bool
	armedAtEnqueue     bool
	heartbeatAtEnqueue uint64
	enqueueComplete    bool
	// stateVerificationRequired can be raised by stale ACK activity observed
	// before the gomavlib handoff boundary.
	stateVerificationRequired bool
	oppositeStateObserved     bool
	acks                      chan mavlinkCommandAck
	armedStateChange          chan mavlinkArmedStateEvidence
}

type preparedAircraftCommand struct {
	command      *agentv1.AircraftCommand
	result       *agentv1.AircraftCommandResult
	param1       float32
	desiredArmed bool
}

func (a *Agent) observeMAVLinkFrame(frame *gomavlib.EventFrame) {
	if frame == nil {
		return
	}
	a.observeMissionProtocolMessage(frame)
	switch message := frame.Message().(type) {
	case *common.MessageHeartbeat:
		if frame.ComponentID() == uint8(common.MAV_COMP_ID_AUTOPILOT1) && message.Type != common.MAV_TYPE_GCS {
			a.observeMAVLinkHeartbeat(frame.Channel, frame.SystemID(), frame.ComponentID(), message.BaseMode&common.MAV_MODE_FLAG_SAFETY_ARMED != 0)
		} else {
			a.observeMAVLinkEventProgress(frame.Channel)
		}
	case *common.MessageCommandAck:
		// ACK classification and its fence reset share one critical section.
		// Generic progress must not expose a quiet epoch before this same event
		// is consumed as potentially stale command evidence.
		a.observeMAVLinkCommandAck(frame.Channel, frame.SystemID(), frame.ComponentID(), message)
	case *common.MessageExtendedSysState:
		a.observeMAVLinkLandedState(frame.Channel, frame.SystemID(), frame.ComponentID(), message.LandedState)
	default:
		a.observeMAVLinkEventProgress(frame.Channel)
	}
}

func (a *Agent) observeMAVLinkLandedState(channel *gomavlib.Channel, systemID, componentID uint8, state common.MAV_LANDED_STATE) {
	a.mavlinkMu.Lock()
	defer a.mavlinkMu.Unlock()
	if target := a.mavlinkTarget; target != nil && target.channel == channel && target.systemID == systemID && target.componentID == componentID {
		a.mavlinkLandedStateSeq++
		target.landedState = state
		target.landedStateAt = time.Now()
		target.landedStateSequence = a.mavlinkLandedStateSeq
	}
}

func (a *Agent) observeMAVLinkEventProgress(channel *gomavlib.Channel) {
	a.mavlinkMu.Lock()
	defer a.mavlinkMu.Unlock()
	if a.mavlinkTarget == nil || a.mavlinkTarget.channel != channel || !a.aircraftAckAmbiguous {
		return
	}
	a.advanceAircraftACKQuiescenceLocked(time.Now())
}

func (a *Agent) advanceAircraftACKQuiescenceLocked(now time.Time) {
	if !a.aircraftAckAmbiguous {
		return
	}
	timeout := a.aircraftCommandTimeout()
	maxProgressGap := timeout / 2
	if a.aircraftAckAmbiguousSince.IsZero() || a.aircraftAckLastProgressAt.IsZero() ||
		now.Sub(a.aircraftAckLastProgressAt) > maxProgressGap {
		a.aircraftAckAmbiguousSince = now
	}
	a.aircraftAckLastProgressAt = now
	if now.Sub(a.aircraftAckAmbiguousSince) >= timeout {
		a.aircraftAckAmbiguous = false
		a.aircraftAckAmbiguousSince = time.Time{}
		a.aircraftAckLastProgressAt = time.Time{}
	}
}

func (a *Agent) rearmAircraftACKFenceLocked() {
	a.aircraftAckAmbiguous = true
	a.aircraftAckAmbiguousSince = time.Time{}
	a.aircraftAckLastProgressAt = time.Time{}
}

func (a *Agent) observeMAVLinkHeartbeat(channel *gomavlib.Channel, systemID, componentID uint8, armed bool) {
	a.mavlinkMu.Lock()
	previous := a.mavlinkTarget
	targetChanged := previous == nil || previous.channel != channel || previous.systemID != systemID || previous.componentID != componentID
	if targetChanged {
		// A different transport target starts a new ACK-correlation domain. Even
		// if the old target completed a quiet epoch, delayed ARM/DISARM evidence
		// from before this selection cannot be correlated directly.
		a.rearmAircraftACKFenceLocked()
	}
	if a.aircraftAckAmbiguous {
		a.advanceAircraftACKQuiescenceLocked(time.Now())
	}
	a.mavlinkHeartbeatSeq++
	sequence := a.mavlinkHeartbeatSeq
	now := time.Now()
	updated := &mavlinkTarget{
		channel: channel, systemID: systemID, componentID: componentID,
		heartbeatSequence: sequence, armed: armed, heartbeatAt: now,
	}
	if !targetChanged {
		updated.landedState = previous.landedState
		updated.landedStateAt = previous.landedStateAt
		updated.landedStateSequence = previous.landedStateSequence
	}
	a.mavlinkTarget = updated
	pending := a.pendingMAVLinkCommand
	var stateChanges chan mavlinkArmedStateEvidence
	var stateEvidence mavlinkArmedStateEvidence
	if pending != nil && pending.channel == channel && pending.systemID == systemID && pending.componentID == componentID {
		if !pending.enqueueComplete {
			pending.armedAtEnqueue = armed
		} else if sequence > pending.heartbeatAtEnqueue {
			if armed != pending.desiredArmed {
				pending.oppositeStateObserved = true
			}
			stateChanges = pending.armedStateChange
			stateEvidence = mavlinkArmedStateEvidence{
				armed: armed, oppositeObserved: pending.oppositeStateObserved,
			}
		}
	}
	a.mavlinkMu.Unlock()
	if stateChanges == nil {
		return
	}
	select {
	case stateChanges <- stateEvidence:
	default:
		// Coalesce to the latest vehicle state without losing the fact that an
		// opposite state occurred earlier in this command epoch.
		select {
		case <-stateChanges:
		default:
		}
		select {
		case stateChanges <- stateEvidence:
		default:
		}
	}
}

func (a *Agent) observeMAVLinkCommandAck(channel *gomavlib.Channel, systemID, componentID uint8, ack *common.MessageCommandAck) {
	if ack == nil {
		return
	}
	a.mavlinkMu.Lock()
	pending := a.pendingMAVLinkCommand
	matchesPending := pending != nil && pending.channel == channel && pending.command == ack.Command &&
		pending.systemID == systemID && pending.componentID == componentID &&
		(ack.TargetSystem == 0 || ack.TargetSystem == mavlinkSourceSystemID) &&
		(ack.TargetComponent == 0 || ack.TargetComponent == mavlinkSourceComponentID)
	if matchesPending && !pending.enqueueComplete {
		// A COMMAND_ACK observed before WriteMessageTo returns predates the
		// command's gomavlib handoff boundary. It cannot be correlated to this
		// attempt. Discard it and require state verification for any later ACK;
		// its presence proves the shared ARM/DISARM ACK domain was not quiescent.
		a.rearmAircraftACKFenceLocked()
		a.advanceAircraftACKQuiescenceLocked(time.Now())
		pending.stateVerificationRequired = true
		a.mavlinkMu.Unlock()
		return
	}
	if ack.Command == common.MAV_CMD_COMPONENT_ARM_DISARM &&
		a.mavlinkTarget != nil && a.mavlinkTarget.channel == channel &&
		a.mavlinkTarget.systemID == systemID && a.mavlinkTarget.componentID == componentID &&
		(ack.TargetSystem == 0 || ack.TargetSystem == mavlinkSourceSystemID) &&
		(ack.TargetComponent == 0 || ack.TargetComponent == mavlinkSourceComponentID) {
		a.rearmAircraftACKFenceLocked()
		a.advanceAircraftACKQuiescenceLocked(time.Now())
	}
	if !matchesPending {
		a.mavlinkMu.Unlock()
		return
	}
	result := mavlinkCommandAck{systemID: systemID, componentID: componentID, result: ack.Result}
	if ack.Result == common.MAV_RESULT_IN_PROGRESS {
		a.mavlinkMu.Unlock()
		return
	}
	// Progress ACKs do not complete a command and are never queued. Retain only
	// the latest terminal evidence so a burst cannot hide the one-shot final ACK.
	select {
	case pending.acks <- result:
	default:
		select {
		case <-pending.acks:
		default:
		}
		select {
		case pending.acks <- result:
		default:
		}
	}
	a.mavlinkMu.Unlock()
}

func (a *Agent) clearMAVLinkTarget(channel *gomavlib.Channel) {
	a.mavlinkMu.Lock()
	defer a.mavlinkMu.Unlock()
	if a.mavlinkTarget != nil && a.mavlinkTarget.channel == channel {
		a.mavlinkTarget = nil
	}
}

func (a *Agent) handleAircraftCommand(
	ctx context.Context,
	stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage],
	command *agentv1.AircraftCommand,
) error {
	startedAt := time.Now()
	result := a.executeAircraftCommand(ctx, command)
	return a.sendAircraftCommandResult(ctx, stream, command, result, startedAt)
}

func (a *Agent) dispatchAircraftCommand(
	ctx context.Context,
	stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage],
	command *agentv1.AircraftCommand,
	wg *sync.WaitGroup,
	errors chan<- error,
) error {
	startedAt := time.Now()
	prepared, immediate := prepareAircraftCommand(command)
	if immediate != nil {
		return a.sendAircraftCommandResult(ctx, stream, command, immediate, startedAt)
	}
	if !a.tryBeginAircraftCommand() {
		return a.sendAircraftCommandResult(ctx, stream, command, aircraftCommandBusyResult(prepared.result), startedAt)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		result := a.executePreparedAircraftCommand(ctx, prepared)
		a.endAircraftCommand()
		if err := a.sendAircraftCommandResult(ctx, stream, command, result, startedAt); err != nil {
			select {
			case errors <- err:
			default:
			}
		}
	}()
	return nil
}

func (a *Agent) sendAircraftCommandResult(
	ctx context.Context,
	stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage],
	command *agentv1.AircraftCommand,
	result *agentv1.AircraftCommandResult,
	startedAt time.Time,
) error {
	slog.LogAttrs(ctx, slog.LevelInfo, "command_completed",
		slog.String("command_id", result.GetCommandId()),
		slog.String("aircraft_id", result.GetAircraftId()),
		slog.String("command_type", command.GetType().String()),
		slog.String("result", result.GetStatus().String()),
		slog.Duration("duration", time.Since(startedAt)),
	)
	message := &agentv1.AgentStreamMessage{
		Payload: &agentv1.AgentStreamMessage_AircraftCommandResult{AircraftCommandResult: result},
	}
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	return stream.Send(message)
}

func (a *Agent) executeAircraftCommand(ctx context.Context, command *agentv1.AircraftCommand) *agentv1.AircraftCommandResult {
	prepared, immediate := prepareAircraftCommand(command)
	if immediate != nil {
		return immediate
	}
	if !a.tryBeginAircraftCommand() {
		return aircraftCommandBusyResult(prepared.result)
	}
	defer a.endAircraftCommand()
	return a.executePreparedAircraftCommand(ctx, prepared)
}

func prepareAircraftCommand(command *agentv1.AircraftCommand) (*preparedAircraftCommand, *agentv1.AircraftCommandResult) {
	result := &agentv1.AircraftCommandResult{}
	if command != nil {
		result.CommandId = command.GetCommandId()
		result.AircraftId = command.GetAircraftId()
	}
	if command == nil || strings.TrimSpace(command.GetCommandId()) == "" || strings.TrimSpace(command.GetAircraftId()) == "" {
		result.Status = agentv1.AircraftCommandResult_STATUS_REJECTED
		result.Message = "command_id and aircraft_id are required"
		return nil, result
	}

	param1 := float32(0)
	desiredArmed := false
	switch command.GetType() {
	case agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM:
		param1 = 1
		desiredArmed = true
	case agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM:
	case agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_UNSPECIFIED:
		result.Status = agentv1.AircraftCommandResult_STATUS_REJECTED
		result.Message = "command type must be ARM or DISARM"
		return nil, result
	default:
		result.Status = agentv1.AircraftCommandResult_STATUS_REJECTED
		result.Message = fmt.Sprintf("unsupported aircraft command type %s", command.GetType())
		return nil, result
	}
	return &preparedAircraftCommand{command: command, result: result, param1: param1, desiredArmed: desiredArmed}, nil
}

func (a *Agent) tryBeginAircraftCommand() bool {
	a.aircraftCommandMu.Lock()
	defer a.aircraftCommandMu.Unlock()
	if a.aircraftCommandActive {
		return false
	}
	a.aircraftCommandActive = true
	return true
}

func (a *Agent) endAircraftCommand() {
	a.aircraftCommandMu.Lock()
	a.aircraftCommandActive = false
	a.aircraftCommandMu.Unlock()
}

func aircraftCommandBusyResult(result *agentv1.AircraftCommandResult) *agentv1.AircraftCommandResult {
	result.Status = agentv1.AircraftCommandResult_STATUS_DELIVERY_FAILED
	result.Message = "another ARM or DISARM command is already in progress"
	return result
}

func (a *Agent) waitForAircraftACKQuiescenceOrTransition(ctx context.Context, desiredArmed bool) bool {
	pollInterval := aircraftACKQuiescencePollInterval(a.aircraftCommandTimeout())
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		a.mavlinkMu.Lock()
		target := a.mavlinkTarget
		ready := target == nil || !a.aircraftAckAmbiguous || target.armed != desiredArmed
		a.mavlinkMu.Unlock()
		if ready {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func aircraftACKQuiescencePollInterval(timeout time.Duration) time.Duration {
	pollInterval := timeout / 20
	if pollInterval < time.Millisecond {
		pollInterval = time.Millisecond
	} else if pollInterval > 50*time.Millisecond {
		pollInterval = 50 * time.Millisecond
	}
	return pollInterval
}

func (a *Agent) executePreparedAircraftCommand(ctx context.Context, prepared *preparedAircraftCommand) *agentv1.AircraftCommandResult {
	command := prepared.command
	result := prepared.result
	param1 := prepared.param1
	desiredArmed := prepared.desiredArmed
	timeout := a.aircraftCommandTimeout()
	// A newly re-armed fence has no epoch until the next selected-channel event.
	// Bound admission by one interval to observe that first progress, one full
	// interval to prove continuous quiescence, and one polling interval of
	// scheduling slack.
	admissionTimeout := 2*timeout + aircraftACKQuiescencePollInterval(timeout)
	admissionCtx, cancelAdmission := context.WithTimeout(ctx, admissionTimeout)
	defer cancelAdmission()

	var target *mavlinkTarget
	var pending *pendingMAVLinkCommand
	for {
		a.mavlinkMu.Lock()
		target = a.mavlinkTarget
		if target == nil || target.channel == nil {
			a.mavlinkMu.Unlock()
			result.Status = agentv1.AircraftCommandResult_STATUS_DELIVERY_FAILED
			result.Message = "autopilot MAVLink channel is unavailable"
			return result
		}
		if a.aircraftAckAmbiguous && target.armed == desiredArmed {
			a.mavlinkMu.Unlock()
			if !a.waitForAircraftACKQuiescenceOrTransition(admissionCtx, desiredArmed) {
				result.Status = agentv1.AircraftCommandResult_STATUS_TIMEOUT
				result.Message = "timed out waiting for ACK quiescence before an already-satisfied aircraft state command"
				return result
			}
			// Recheck under the same lock used to install the pending matcher. ACK
			// activity may have re-armed the fence after the waiter woke.
			continue
		}
		pending = &pendingMAVLinkCommand{
			channel: target.channel, systemID: target.systemID, componentID: target.componentID,
			command:            common.MAV_CMD_COMPONENT_ARM_DISARM,
			desiredArmed:       desiredArmed,
			armedAtEnqueue:     target.armed,
			heartbeatAtEnqueue: target.heartbeatSequence,
			acks:               make(chan mavlinkCommandAck, 1),
			armedStateChange:   make(chan mavlinkArmedStateEvidence, 1),
		}
		// Process startup and any uncertain write or timeout fence COMMAND_ACK for
		// this MAV_CMD: an ACK buffered across either boundary cannot be identified
		// as stale. Keep this Agent lifecycle in combined ACK/state-verification mode:
		// positive completion requires both an accepted acknowledgement and a fresh
		// armed-state transition, while an ambiguous negative acknowledgement cannot
		// terminate the current command.
		pending.stateVerificationRequired = a.aircraftAckAmbiguous
		a.pendingMAVLinkCommand = pending
		a.mavlinkMu.Unlock()
		break
	}
	// Waiting out an ambiguity fence is an admission phase, not part of the
	// autopilot's response budget. Once the matcher is installed safely, give
	// stream handoff and COMMAND_ACK processing their own bounded interval.
	cancelAdmission()
	commandCtx, cancelCommand := context.WithTimeout(ctx, timeout)
	defer cancelCommand()
	defer func() {
		a.mavlinkMu.Lock()
		if a.pendingMAVLinkCommand == pending {
			a.pendingMAVLinkCommand = nil
		}
		a.mavlinkMu.Unlock()
	}()

	mavlinkCommand := &common.MessageCommandLong{
		TargetSystem: target.systemID, TargetComponent: target.componentID,
		Command: common.MAV_CMD_COMPONENT_ARM_DISARM, Param1: param1,
	}
	slog.LogAttrs(ctx, slog.LevelInfo, "mavlink_command_enqueue_started",
		slog.String("command_id", command.GetCommandId()),
		slog.String("aircraft_id", command.GetAircraftId()),
		slog.String("command_type", command.GetType().String()),
		slog.Int("target_system", int(target.systemID)),
		slog.Int("target_component", int(target.componentID)),
	)
	if a.writeMAVLinkCommand == nil {
		result.Status = agentv1.AircraftCommandResult_STATUS_DELIVERY_FAILED
		result.Message = "MAVLink command writer is unavailable"
		return result
	}
	if err := a.writeMAVLinkCommand(target.channel, mavlinkCommand); err != nil {
		a.mavlinkMu.Lock()
		a.rearmAircraftACKFenceLocked()
		a.mavlinkMu.Unlock()
		result.Status = agentv1.AircraftCommandResult_STATUS_DELIVERY_FAILED
		result.Message = "send MAVLink command: " + err.Error()
		return result
	}
	a.mavlinkMu.Lock()
	if a.pendingMAVLinkCommand == pending {
		pending.heartbeatAtEnqueue = a.mavlinkHeartbeatSeq
		if current := a.mavlinkTarget; current != nil && current.channel == target.channel &&
			current.systemID == pending.systemID && current.componentID == pending.componentID {
			pending.armedAtEnqueue = current.armed
		}
		// gomavlib WriteMessageTo returning confirms handoff to the node, not
		// physical channel I/O. COMMAND_ACK remains the transmission evidence.
		pending.enqueueComplete = true
	}
	stateVerificationRequired := pending.stateVerificationRequired
	a.mavlinkMu.Unlock()
	// ARM and DISARM share one MAV_CMD and carry no request nonce. Once this
	// command is handed to gomavlib, any duplicate or delayed terminal ACK can
	// collide with the next command, so every outcome starts a fresh quiet epoch.
	defer a.rearmAircraftACKFence()

	oppositeStateObserved := pending.armedAtEnqueue != pending.desiredArmed
	acceptedAckObserved := false
	stateTransitionObserved := false
	for {
		select {
		case ack := <-pending.acks:
			slog.LogAttrs(ctx, slog.LevelInfo, "mavlink_ack_received",
				slog.String("command_id", command.GetCommandId()),
				slog.String("aircraft_id", command.GetAircraftId()),
				slog.String("mavlink_result", ack.result.String()),
			)
			if ack.result == common.MAV_RESULT_IN_PROGRESS {
				continue
			}
			if ack.result != common.MAV_RESULT_ACCEPTED {
				if stateVerificationRequired {
					// The protocol does not echo a per-command nonce. Behind the
					// ambiguity fence, this may be a delayed rejection for an older
					// ARM/DISARM and cannot terminate the current command safely.
					continue
				}
				result.Status = agentv1.AircraftCommandResult_STATUS_REJECTED
				result.Message = "autopilot rejected command: " + ack.result.String()
				return result
			}
			if !stateVerificationRequired {
				result.Status = agentv1.AircraftCommandResult_STATUS_ACCEPTED
				result.Message = "autopilot acknowledged command"
				return result
			}
			acceptedAckObserved = true
			if stateTransitionObserved {
				result.Status = agentv1.AircraftCommandResult_STATUS_ACCEPTED
				result.Message = "accepted acknowledgement and fresh aircraft state transition confirmed command"
				return result
			}
		case state := <-pending.armedStateChange:
			if !stateVerificationRequired {
				continue
			}
			if state.oppositeObserved {
				oppositeStateObserved = true
			}
			if state.armed != pending.desiredArmed {
				continue
			}
			if oppositeStateObserved {
				stateTransitionObserved = true
				if acceptedAckObserved {
					result.Status = agentv1.AircraftCommandResult_STATUS_ACCEPTED
					result.Message = "accepted acknowledgement and fresh aircraft state transition confirmed command"
					return result
				}
			}
		case <-commandCtx.Done():
			a.mavlinkMu.Lock()
			a.rearmAircraftACKFenceLocked()
			a.mavlinkMu.Unlock()
			result.Status = agentv1.AircraftCommandResult_STATUS_TIMEOUT
			if stateVerificationRequired {
				result.Message = "timed out waiting for accepted acknowledgement and fresh aircraft state transition"
			} else {
				result.Message = "timed out waiting for autopilot COMMAND_ACK"
			}
			return result
		}
	}
}

func (a *Agent) rearmAircraftACKFence() {
	a.mavlinkMu.Lock()
	a.rearmAircraftACKFenceLocked()
	a.mavlinkMu.Unlock()
}

func (a *Agent) aircraftCommandTimeout() time.Duration {
	if a.options != nil && a.options.AircraftCommandTimeout > 0 {
		return a.options.AircraftCommandTimeout
	}
	return defaultAircraftCommandTimeout
}
