package agent

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"google.golang.org/grpc"
)

const defaultAircraftCommandTimeout = 4 * time.Second

type mavlinkTarget struct {
	channel     *gomavlib.Channel
	systemID    uint8
	componentID uint8
}

type mavlinkCommandAck struct {
	systemID    uint8
	componentID uint8
	result      common.MAV_RESULT
}

type pendingMAVLinkCommand struct {
	systemID    uint8
	componentID uint8
	command     common.MAV_CMD
	acks        chan mavlinkCommandAck
}

func (a *Agent) observeMAVLinkFrame(frame *gomavlib.EventFrame) {
	if frame == nil {
		return
	}
	switch message := frame.Message().(type) {
	case *common.MessageHeartbeat:
		if frame.ComponentID() == uint8(common.MAV_COMP_ID_AUTOPILOT1) && message.Type != common.MAV_TYPE_GCS {
			a.mavlinkMu.Lock()
			a.mavlinkTarget = &mavlinkTarget{channel: frame.Channel, systemID: frame.SystemID(), componentID: frame.ComponentID()}
			a.mavlinkMu.Unlock()
		}
	case *common.MessageCommandAck:
		a.observeMAVLinkCommandAck(frame.SystemID(), frame.ComponentID(), message)
	}
}

func (a *Agent) observeMAVLinkCommandAck(systemID, componentID uint8, ack *common.MessageCommandAck) {
	if ack == nil {
		return
	}
	a.mavlinkMu.Lock()
	pending := a.pendingMAVLinkCommand
	if pending == nil || pending.command != ack.Command || pending.systemID != systemID || pending.componentID != componentID {
		a.mavlinkMu.Unlock()
		return
	}
	result := mavlinkCommandAck{systemID: systemID, componentID: componentID, result: ack.Result}
	a.mavlinkMu.Unlock()
	select {
	case pending.acks <- result:
	default:
	}
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
	result := &agentv1.AircraftCommandResult{}
	if command != nil {
		result.CommandId = command.GetCommandId()
		result.AircraftId = command.GetAircraftId()
	}
	if command == nil || strings.TrimSpace(command.GetCommandId()) == "" || strings.TrimSpace(command.GetAircraftId()) == "" {
		result.Status = agentv1.AircraftCommandResult_STATUS_REJECTED
		result.Message = "command_id and aircraft_id are required"
		return result
	}

	param1 := float32(0)
	switch command.GetType() {
	case agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM:
		param1 = 1
	case agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM:
	case agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_UNSPECIFIED:
		result.Status = agentv1.AircraftCommandResult_STATUS_REJECTED
		result.Message = "command type must be ARM or DISARM"
		return result
	default:
		result.Status = agentv1.AircraftCommandResult_STATUS_REJECTED
		result.Message = fmt.Sprintf("unsupported aircraft command type %s", command.GetType())
		return result
	}

	a.aircraftCommandMu.Lock()
	if a.aircraftCommandActive {
		a.aircraftCommandMu.Unlock()
		result.Status = agentv1.AircraftCommandResult_STATUS_DELIVERY_FAILED
		result.Message = "another ARM or DISARM command is already in progress"
		return result
	}
	a.aircraftCommandActive = true
	a.aircraftCommandMu.Unlock()
	defer func() {
		a.aircraftCommandMu.Lock()
		a.aircraftCommandActive = false
		a.aircraftCommandMu.Unlock()
	}()

	a.mavlinkMu.Lock()
	target := a.mavlinkTarget
	if target == nil || target.channel == nil {
		a.mavlinkMu.Unlock()
		result.Status = agentv1.AircraftCommandResult_STATUS_DELIVERY_FAILED
		result.Message = "autopilot MAVLink channel is unavailable"
		return result
	}
	pending := &pendingMAVLinkCommand{
		systemID: target.systemID, componentID: target.componentID,
		command: common.MAV_CMD_COMPONENT_ARM_DISARM,
		acks:    make(chan mavlinkCommandAck, 4),
	}
	a.pendingMAVLinkCommand = pending
	a.mavlinkMu.Unlock()
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
	slog.LogAttrs(ctx, slog.LevelInfo, "mavlink_command_sent",
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
		result.Status = agentv1.AircraftCommandResult_STATUS_DELIVERY_FAILED
		result.Message = "send MAVLink command: " + err.Error()
		return result
	}

	timeout := defaultAircraftCommandTimeout
	if a.options != nil && a.options.AircraftCommandTimeout > 0 {
		timeout = a.options.AircraftCommandTimeout
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
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
			if ack.result == common.MAV_RESULT_ACCEPTED {
				result.Status = agentv1.AircraftCommandResult_STATUS_ACCEPTED
				result.Message = "autopilot acknowledged command"
				return result
			}
			result.Status = agentv1.AircraftCommandResult_STATUS_REJECTED
			result.Message = "autopilot rejected command: " + ack.result.String()
			return result
		case <-waitCtx.Done():
			result.Status = agentv1.AircraftCommandResult_STATUS_TIMEOUT
			result.Message = "timed out waiting for autopilot COMMAND_ACK"
			return result
		}
	}
}
