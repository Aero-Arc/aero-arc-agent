package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
)

func TestAircraftCommandTranslatesArmAndDisarmToMAVLink(t *testing.T) {
	tests := []struct {
		name        string
		commandType agentv1.AircraftCommandType
		wantParam1  float32
	}{
		{name: "arm", commandType: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM, wantParam1: 1},
		{name: "disarm", commandType: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM, wantParam1: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			channel := &gomavlib.Channel{}
			agent := &Agent{
				options:       &AgentOptions{AircraftCommandTimeout: time.Second},
				mavlinkTarget: &mavlinkTarget{channel: channel, systemID: 42, componentID: 1},
			}
			agent.writeMAVLinkCommand = func(gotChannel *gomavlib.Channel, command *common.MessageCommandLong) error {
				if gotChannel != channel {
					t.Fatal("command sent on a different MAVLink channel")
				}
				if command.Command != common.MAV_CMD_COMPONENT_ARM_DISARM || command.TargetSystem != 42 || command.TargetComponent != 1 || command.Param1 != test.wantParam1 {
					t.Fatalf("MAVLink command = %+v", command)
				}
				agent.observeMAVLinkCommandAck(42, 1, &common.MessageCommandAck{
					Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
					Result:  common.MAV_RESULT_ACCEPTED,
				})
				return nil
			}

			result := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
				CommandId: "command-1", AircraftId: "aircraft-1", Type: test.commandType,
			})
			if result.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestAircraftCommandMapsMAVLinkRejection(t *testing.T) {
	agent := commandTestAgent(t, common.MAV_RESULT_DENIED)
	result := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "command-1", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
	})
	if result.GetStatus() != agentv1.AircraftCommandResult_STATUS_REJECTED {
		t.Fatalf("status = %v", result.GetStatus())
	}
	if !strings.Contains(result.GetMessage(), common.MAV_RESULT_DENIED.String()) {
		t.Fatalf("message = %q", result.GetMessage())
	}
}

func TestAircraftCommandIgnoresAckAddressedToAnotherMAVLinkNode(t *testing.T) {
	agent := &Agent{
		options:       &AgentOptions{AircraftCommandTimeout: 100 * time.Millisecond},
		mavlinkTarget: &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1},
	}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		agent.observeMAVLinkCommandAck(1, 1, &common.MessageCommandAck{
			Command:         common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:          common.MAV_RESULT_ACCEPTED,
			TargetSystem:    mavlinkSourceSystemID,
			TargetComponent: uint8(common.MAV_COMP_ID_MISSIONPLANNER),
		})
		agent.observeMAVLinkCommandAck(1, 1, &common.MessageCommandAck{
			Command:         common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:          common.MAV_RESULT_DENIED,
			TargetSystem:    mavlinkSourceSystemID,
			TargetComponent: mavlinkSourceComponentID,
		})
		return nil
	}
	result := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "command-recipient", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
	})
	if result.GetStatus() != agentv1.AircraftCommandResult_STATUS_REJECTED || !strings.Contains(result.GetMessage(), common.MAV_RESULT_DENIED.String()) {
		t.Fatalf("result = %+v, want denial addressed to Agent MAVLink identity", result)
	}
}

func TestAircraftCommandTimesOutWithoutAck(t *testing.T) {
	agent := &Agent{
		options:       &AgentOptions{AircraftCommandTimeout: 10 * time.Millisecond},
		mavlinkTarget: &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1},
		writeMAVLinkCommand: func(*gomavlib.Channel, *common.MessageCommandLong) error {
			return nil
		},
	}
	result := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "command-1", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
	})
	if result.GetStatus() != agentv1.AircraftCommandResult_STATUS_TIMEOUT {
		t.Fatalf("result = %+v", result)
	}
}

func TestAircraftCommandDoesNotCorrelateLateAckAfterTimeout(t *testing.T) {
	channel := &gomavlib.Channel{}
	secondWrite := make(chan struct{})
	writes := 0
	agent := &Agent{
		options:             &AgentOptions{AircraftCommandTimeout: 200 * time.Millisecond},
		mavlinkHeartbeatSeq: 1,
		mavlinkTarget: &mavlinkTarget{
			channel: channel, systemID: 1, componentID: 1, heartbeatSequence: 1,
		},
	}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		writes++
		if writes == 2 {
			// This ACK belongs to the timed-out ARM, but the wire protocol cannot
			// distinguish it from the DISARM now in progress.
			agent.observeMAVLinkCommandAck(1, 1, &common.MessageCommandAck{
				Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
				Result:  common.MAV_RESULT_ACCEPTED,
			})
			close(secondWrite)
		}
		return nil
	}

	agent.options.AircraftCommandTimeout = 10 * time.Millisecond
	first := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "arm-1", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
	})
	if first.GetStatus() != agentv1.AircraftCommandResult_STATUS_TIMEOUT {
		t.Fatalf("first result = %+v, want timeout", first)
	}

	agent.options.AircraftCommandTimeout = 200 * time.Millisecond
	result := make(chan *agentv1.AircraftCommandResult, 1)
	go func() {
		result <- agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
			CommandId: "disarm-2", AircraftId: "aircraft-1",
			Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM,
		})
	}()
	<-secondWrite
	select {
	case got := <-result:
		t.Fatalf("late ARM ACK completed DISARM: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}

	agent.observeMAVLinkHeartbeat(channel, 1, 1, true)
	select {
	case got := <-result:
		t.Fatalf("opposite armed state completed DISARM: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	agent.observeMAVLinkHeartbeat(channel, 1, 1, false)
	got := <-result
	if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED || !strings.Contains(got.GetMessage(), "fresh heartbeat") {
		t.Fatalf("state-confirmed DISARM result = %+v", got)
	}
}

func TestAircraftCommandFailsWhenMAVLinkUnavailable(t *testing.T) {
	agent := &Agent{}
	result := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "command-1", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
	})
	if result.GetStatus() != agentv1.AircraftCommandResult_STATUS_DELIVERY_FAILED {
		t.Fatalf("result = %+v", result)
	}
}

func TestHandleRelayMessageReturnsCorrelatedAircraftCommandResult(t *testing.T) {
	agent := commandTestAgent(t, common.MAV_RESULT_ACCEPTED)
	var sent *agentv1.AgentStreamMessage
	stream := &mockStream{sendFunc: func(message *agentv1.AgentStreamMessage) error {
		sent = message
		return nil
	}}
	command := &agentv1.AircraftCommand{
		CommandId: "command-1", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM,
	}
	err := agent.handleRelayMessage(context.Background(), stream, &agentv1.RelayStreamMessage{
		Payload: &agentv1.RelayStreamMessage_AircraftCommand{AircraftCommand: command},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := sent.GetAircraftCommandResult()
	if result.GetCommandId() != command.GetCommandId() || result.GetAircraftId() != command.GetAircraftId() || result.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED {
		t.Fatalf("result = %+v", result)
	}
}

func commandTestAgent(t *testing.T, mavlinkResult common.MAV_RESULT) *Agent {
	t.Helper()
	agent := &Agent{
		options:       &AgentOptions{AircraftCommandTimeout: time.Second},
		mavlinkTarget: &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1},
	}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		agent.observeMAVLinkCommandAck(1, 1, &common.MessageCommandAck{
			Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:  mavlinkResult,
		})
		return nil
	}
	return agent
}
