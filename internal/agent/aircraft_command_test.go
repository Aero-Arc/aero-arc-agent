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
				agent.observeMAVLinkCommandAck(channel, 42, 1, &common.MessageCommandAck{
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
	channel := &gomavlib.Channel{}
	agent := &Agent{
		options:       &AgentOptions{AircraftCommandTimeout: 100 * time.Millisecond},
		mavlinkTarget: &mavlinkTarget{channel: channel, systemID: 1, componentID: 1},
	}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
			Command:         common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:          common.MAV_RESULT_ACCEPTED,
			TargetSystem:    mavlinkSourceSystemID,
			TargetComponent: uint8(common.MAV_COMP_ID_MISSIONPLANNER),
		})
		agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
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
			agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
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
	agent.observeMAVLinkHeartbeat(channel, 1, 1, true)

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
	if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED || !strings.Contains(got.GetMessage(), "fresh aircraft state") {
		t.Fatalf("state-confirmed DISARM result = %+v", got)
	}
}

func TestFirstAircraftCommandAfterStartDoesNotCorrelateBufferedAck(t *testing.T) {
	agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	channel := &gomavlib.Channel{}
	agent.mavlinkHeartbeatSeq = 1
	agent.mavlinkTarget = &mavlinkTarget{
		channel: channel, systemID: 1, componentID: 1, heartbeatSequence: 1, armed: true,
	}
	written := make(chan struct{})
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		// Model an accepted ARM ACK left in the transport buffer by the Agent
		// process that ran before this one. It cannot satisfy the new DISARM.
		agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
			Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:  common.MAV_RESULT_ACCEPTED,
		})
		close(written)
		return nil
	}

	result := make(chan *agentv1.AircraftCommandResult, 1)
	go func() {
		result <- agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
			CommandId: "disarm-after-restart", AircraftId: "aircraft-1",
			Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM,
		})
	}()
	<-written
	select {
	case got := <-result:
		t.Fatalf("buffered pre-restart ACK completed DISARM: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}

	agent.observeMAVLinkHeartbeat(channel, 1, 1, false)
	got := <-result
	if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED || !strings.Contains(got.GetMessage(), "fresh aircraft state") {
		t.Fatalf("state-confirmed post-restart DISARM result = %+v", got)
	}
}

func TestAircraftCommandFenceDoesNotLetAmbiguousRejectionTerminateCommand(t *testing.T) {
	agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	channel := &gomavlib.Channel{}
	agent.mavlinkTarget = &mavlinkTarget{channel: channel, systemID: 1, componentID: 1}
	written := make(chan struct{})
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		close(written)
		return nil
	}

	result := make(chan *agentv1.AircraftCommandResult, 1)
	go func() {
		result <- agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
			CommandId: "denied-disarm-after-restart", AircraftId: "aircraft-1",
			Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM,
		})
	}()
	<-written
	agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
		Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
		Result:  common.MAV_RESULT_DENIED,
	})
	agent.observeMAVLinkHeartbeat(channel, 1, 1, false)
	got := <-result
	if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_TIMEOUT {
		t.Fatalf("ambiguous denial result = %+v, want timeout", got)
	}
}

func TestAircraftCommandFenceIgnoresLateDenialForPreviousCommand(t *testing.T) {
	agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	channel := &gomavlib.Channel{}
	agent.mavlinkTarget = &mavlinkTarget{channel: channel, systemID: 1, componentID: 1, armed: true}
	written := make(chan struct{})
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		close(written)
		return nil
	}

	result := make(chan *agentv1.AircraftCommandResult, 1)
	go func() {
		result <- agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
			CommandId: "disarm-after-denied-arm", AircraftId: "aircraft-1",
			Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM,
		})
	}()
	<-written
	// This could be the delayed denial of an ARM sent before this DISARM.
	agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
		Command: common.MAV_CMD_COMPONENT_ARM_DISARM, Result: common.MAV_RESULT_DENIED,
	})
	select {
	case got := <-result:
		t.Fatalf("ambiguous late denial terminated DISARM: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}

	agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
		Command: common.MAV_CMD_COMPONENT_ARM_DISARM, Result: common.MAV_RESULT_ACCEPTED,
	})
	agent.observeMAVLinkHeartbeat(channel, 1, 1, false)
	got := <-result
	if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED {
		t.Fatalf("current DISARM evidence result = %+v, want accepted", got)
	}
}

func TestAircraftCommandFenceRequiresObservedTransitionFromMatchingState(t *testing.T) {
	agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 60 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	channel := &gomavlib.Channel{}
	agent.mavlinkTarget = &mavlinkTarget{channel: channel, systemID: 1, componentID: 1, armed: false}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
			Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:  common.MAV_RESULT_ACCEPTED,
		})
		return nil
	}

	result := make(chan *agentv1.AircraftCommandResult, 1)
	go func() {
		result <- agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
			CommandId: "already-disarmed-after-restart", AircraftId: "aircraft-1",
			Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM,
		})
	}()
	time.Sleep(10 * time.Millisecond)
	agent.observeMAVLinkHeartbeat(channel, 1, 1, false)
	got := <-result
	if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_TIMEOUT {
		t.Fatalf("state-matching command without an observed transition = %+v, want timeout", got)
	}
}

func TestAircraftCommandFenceRefreshesArmedStateAtEnqueueBoundary(t *testing.T) {
	agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 60 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	channel := &gomavlib.Channel{}
	agent.mavlinkTarget = &mavlinkTarget{channel: channel, systemID: 1, componentID: 1, armed: false}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		// The aircraft reaches the requested state while the write is in
		// progress. That is now the enqueue-boundary baseline, not evidence that
		// can complete the command.
		agent.observeMAVLinkHeartbeat(channel, 1, 1, true)
		agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
			Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:  common.MAV_RESULT_ACCEPTED,
		})
		return nil
	}

	result := make(chan *agentv1.AircraftCommandResult, 1)
	go func() {
		result <- agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
			CommandId: "arm-at-send-boundary", AircraftId: "aircraft-1",
			Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
		})
	}()
	time.Sleep(10 * time.Millisecond)
	agent.observeMAVLinkHeartbeat(channel, 1, 1, true)
	got := <-result
	if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_TIMEOUT {
		t.Fatalf("state reached before send boundary completed command = %+v, want timeout", got)
	}
}

func TestAircraftCommandFenceDoesNotAcceptStateTransitionWithoutAck(t *testing.T) {
	agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 60 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	channel := &gomavlib.Channel{}
	agent.mavlinkTarget = &mavlinkTarget{channel: channel, systemID: 1, componentID: 1, armed: false}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		// gomavlib can accept this handoff and still drop it at a full channel
		// queue. No ACK means there is no proof it reached the autopilot.
		return nil
	}

	result := make(chan *agentv1.AircraftCommandResult, 1)
	go func() {
		result <- agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
			CommandId: "arm-with-dropped-enqueue", AircraftId: "aircraft-1",
			Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
		})
	}()
	time.Sleep(10 * time.Millisecond)
	agent.observeMAVLinkHeartbeat(channel, 1, 1, true)
	got := <-result
	if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_TIMEOUT {
		t.Fatalf("state transition without ACK result = %+v, want timeout", got)
	}
}

func TestAircraftCommandEvidenceMustMatchSelectedMAVLinkChannel(t *testing.T) {
	for _, test := range []struct {
		name         string
		ackOnOther   bool
		stateOnOther bool
	}{
		{name: "acknowledgement", ackOnOther: true},
		{name: "heartbeat", stateOnOther: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 200 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			selected := &gomavlib.Channel{}
			other := &gomavlib.Channel{}
			agent.mavlinkTarget = &mavlinkTarget{channel: selected, systemID: 1, componentID: 1, armed: false}
			written := make(chan struct{})
			agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
				close(written)
				return nil
			}

			result := make(chan *agentv1.AircraftCommandResult, 1)
			go func() {
				result <- agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
					CommandId: "arm-channel-bound", AircraftId: "aircraft-1",
					Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
				})
			}()
			<-written
			ackChannel := selected
			if test.ackOnOther {
				ackChannel = other
			}
			stateChannel := selected
			if test.stateOnOther {
				stateChannel = other
			}
			agent.observeMAVLinkCommandAck(ackChannel, 1, 1, &common.MessageCommandAck{
				Command: common.MAV_CMD_COMPONENT_ARM_DISARM, Result: common.MAV_RESULT_ACCEPTED,
			})
			agent.observeMAVLinkHeartbeat(stateChannel, 1, 1, true)
			select {
			case got := <-result:
				t.Fatalf("foreign-channel %s completed command: %+v", test.name, got)
			case <-time.After(20 * time.Millisecond):
			}

			if test.ackOnOther {
				agent.observeMAVLinkCommandAck(selected, 1, 1, &common.MessageCommandAck{
					Command: common.MAV_CMD_COMPONENT_ARM_DISARM, Result: common.MAV_RESULT_ACCEPTED,
				})
			} else {
				agent.observeMAVLinkHeartbeat(selected, 1, 1, true)
			}
			got := <-result
			if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED {
				t.Fatalf("selected MAVLink channel result = %+v, want accepted", got)
			}
		})
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
	channel := &gomavlib.Channel{}
	agent := &Agent{
		options:       &AgentOptions{AircraftCommandTimeout: time.Second},
		mavlinkTarget: &mavlinkTarget{channel: channel, systemID: 1, componentID: 1},
	}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
			Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:  mavlinkResult,
		})
		return nil
	}
	return agent
}
