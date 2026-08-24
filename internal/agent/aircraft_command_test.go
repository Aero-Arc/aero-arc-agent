package agent

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
)

func TestMAVLinkTargetChangeRearmsAircraftACKFence(t *testing.T) {
	oldChannel := &gomavlib.Channel{}
	newChannel := &gomavlib.Channel{}
	agent := &Agent{
		mavlinkTarget:        &mavlinkTarget{channel: oldChannel, systemID: 1, componentID: 1},
		aircraftAckAmbiguous: false,
	}
	agent.observeMAVLinkHeartbeat(newChannel, 1, 1, false)
	if !agent.aircraftAckAmbiguous || agent.aircraftAckAmbiguousSince.IsZero() {
		t.Fatal("new MAVLink channel did not restart the ACK-ambiguity epoch")
	}

	agent.aircraftAckAmbiguous = false
	agent.aircraftAckAmbiguousSince = time.Time{}
	agent.observeMAVLinkHeartbeat(newChannel, 1, 1, false)
	if agent.aircraftAckAmbiguous || !agent.aircraftAckAmbiguousSince.IsZero() {
		t.Fatal("heartbeat from the unchanged target re-armed a quiet ACK fence")
	}

	agent.observeMAVLinkHeartbeat(newChannel, 2, 1, false)
	if !agent.aircraftAckAmbiguous || agent.aircraftAckAmbiguousSince.IsZero() {
		t.Fatal("new MAVLink system ID did not restart the ACK-ambiguity epoch")
	}
}

func TestAircraftCommandTerminalACKRearmsAmbiguityFence(t *testing.T) {
	for _, mavlinkResult := range []common.MAV_RESULT{common.MAV_RESULT_ACCEPTED, common.MAV_RESULT_DENIED} {
		t.Run(mavlinkResult.String(), func(t *testing.T) {
			agent := commandTestAgent(t, mavlinkResult)
			result := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
				CommandId: "terminal-ack", AircraftId: "aircraft-1",
				Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
			})
			if result.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED &&
				result.GetStatus() != agentv1.AircraftCommandResult_STATUS_REJECTED {
				t.Fatalf("terminal result = %+v", result)
			}
			if !agent.aircraftAckAmbiguous || !agent.aircraftAckAmbiguousSince.IsZero() || !agent.aircraftAckLastProgressAt.IsZero() {
				t.Fatal("terminal command did not require a fresh event-progress epoch")
			}
		})
	}
}

func TestAircraftCommandProgressBurstPreservesTerminalACK(t *testing.T) {
	channel := &gomavlib.Channel{}
	agent := &Agent{
		options:       &AgentOptions{AircraftCommandTimeout: time.Second},
		mavlinkTarget: &mavlinkTarget{channel: channel, systemID: 1, componentID: 1},
	}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		afterAircraftCommandEnqueue(agent, func() {
			for range 8 {
				agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
					Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
					Result:  common.MAV_RESULT_IN_PROGRESS,
				})
			}
			agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
				Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
				Result:  common.MAV_RESULT_ACCEPTED,
			})
		})
		return nil
	}
	result := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "progress-then-accepted", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
	})
	if result.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED {
		t.Fatalf("progress burst hid terminal ACK: %+v", result)
	}
}

func TestAircraftCommandStateEvidenceCoalescesLatestTransition(t *testing.T) {
	channel := &gomavlib.Channel{}
	pending := &pendingMAVLinkCommand{
		channel: channel, systemID: 1, componentID: 1,
		desiredArmed:     true,
		enqueueComplete:  true,
		armedStateChange: make(chan mavlinkArmedStateEvidence, 1),
	}
	agent := &Agent{
		mavlinkTarget:         &mavlinkTarget{channel: channel, systemID: 1, componentID: 1},
		pendingMAVLinkCommand: pending,
	}
	for range 4 {
		agent.observeMAVLinkHeartbeat(channel, 1, 1, false)
	}
	agent.observeMAVLinkHeartbeat(channel, 1, 1, true)

	select {
	case state := <-pending.armedStateChange:
		if !state.armed || !state.oppositeObserved {
			t.Fatalf("coalesced state = %+v, want latest armed state with prior opposite evidence", state)
		}
	default:
		t.Fatal("state coalescing lost the decisive transition")
	}
}

func TestAircraftCommandRejectsACKBeforeEnqueueBoundary(t *testing.T) {
	channel := &gomavlib.Channel{}
	agent := &Agent{
		options:                   &AgentOptions{AircraftCommandTimeout: 20 * time.Millisecond},
		mavlinkTarget:             &mavlinkTarget{channel: channel, systemID: 1, componentID: 1},
		aircraftAckAmbiguous:      false,
		aircraftAckAmbiguousSince: time.Time{},
	}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		// This is delayed evidence for an older ARM/DISARM. The pending matcher
		// exists, but the new command has not crossed the gomavlib handoff yet.
		agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
			Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:  common.MAV_RESULT_ACCEPTED,
		})
		// Even a fresh terminal ACK after handoff must not directly complete the
		// command: the pre-boundary activity re-established ambiguity.
		afterAircraftCommandEnqueue(agent, func() {
			agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
				Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
				Result:  common.MAV_RESULT_ACCEPTED,
			})
		})
		return nil
	}
	result := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "pre-enqueue-ack", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
	})
	if result.GetStatus() != agentv1.AircraftCommandResult_STATUS_TIMEOUT {
		t.Fatalf("pre-enqueue ACK completed command: %+v", result)
	}
}

func TestDelayedDuplicateTerminalACKCannotCompleteNextCommand(t *testing.T) {
	channel := &gomavlib.Channel{}
	agent := &Agent{
		options: &AgentOptions{AircraftCommandTimeout: 20 * time.Millisecond},
		mavlinkTarget: &mavlinkTarget{
			channel: channel, systemID: 1, componentID: 1,
		},
	}
	writes := 0
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		writes++
		emitACK := func() {
			agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
				Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
				Result:  common.MAV_RESULT_ACCEPTED,
			})
		}
		if writes == 1 {
			afterAircraftCommandEnqueue(agent, emitACK)
		} else {
			// The second delivery represents a delayed duplicate of the first
			// terminal ACK arriving before the new handoff boundary.
			emitACK()
		}
		return nil
	}
	first := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "arm-first", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
	})
	if first.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED {
		t.Fatalf("first result = %+v", first)
	}
	second := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "disarm-second", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM,
	})
	if second.GetStatus() != agentv1.AircraftCommandResult_STATUS_TIMEOUT {
		t.Fatalf("duplicate ACK completed second command: %+v", second)
	}
}

func TestRunAckLoopRejectsConcurrentAircraftCommandWithoutDelayingReceive(t *testing.T) {
	channel := &gomavlib.Channel{}
	commandStarted := make(chan struct{})
	allResultsSent := make(chan struct{})
	results := make(chan *agentv1.AircraftCommandResult, 2)
	var writes atomic.Int32
	var sends atomic.Int32
	agent := &Agent{
		options:       &AgentOptions{AircraftCommandTimeout: time.Second},
		mavlinkTarget: &mavlinkTarget{channel: channel, systemID: 1, componentID: 1},
	}
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		if writes.Add(1) == 1 {
			close(commandStarted)
		}
		return nil
	}

	commands := []*agentv1.RelayStreamMessage{
		{Payload: &agentv1.RelayStreamMessage_AircraftCommand{AircraftCommand: &agentv1.AircraftCommand{
			CommandId: "arm-1", AircraftId: "aircraft-1",
			Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
		}}},
		{Payload: &agentv1.RelayStreamMessage_AircraftCommand{AircraftCommand: &agentv1.AircraftCommand{
			CommandId: "disarm-2", AircraftId: "aircraft-1",
			Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM,
		}}},
	}
	var receives atomic.Int32
	stream := &mockStream{
		recvFunc: func() (*agentv1.RelayStreamMessage, error) {
			call := receives.Add(1)
			if call <= int32(len(commands)) {
				if call == 2 {
					<-commandStarted
				}
				return commands[call-1], nil
			}
			<-allResultsSent
			return nil, io.EOF
		},
		sendFunc: func(message *agentv1.AgentStreamMessage) error {
			results <- message.GetAircraftCommandResult()
			if sends.Add(1) == 2 {
				close(allResultsSent)
			}
			return nil
		},
	}
	loopDone := make(chan error, 1)
	go func() { loopDone <- agent.runAckLoop(context.Background(), stream) }()

	select {
	case result := <-results:
		if result.GetCommandId() != "disarm-2" || result.GetStatus() != agentv1.AircraftCommandResult_STATUS_DELIVERY_FAILED ||
			!strings.Contains(result.GetMessage(), "already in progress") {
			t.Fatalf("second command result = %+v", result)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second command was not received and rejected while the first was pending")
	}
	if writes.Load() != 1 {
		t.Fatalf("MAVLink writes before first completion = %d, want 1", writes.Load())
	}
	agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
		Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
		Result:  common.MAV_RESULT_ACCEPTED,
	})

	select {
	case result := <-results:
		if result.GetCommandId() != "arm-1" || result.GetStatus() != agentv1.AircraftCommandResult_STATUS_ACCEPTED {
			t.Fatalf("first command result = %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("first command did not complete after ACK")
	}
	select {
	case err := <-loopDone:
		if err != io.EOF {
			t.Fatalf("runAckLoop error = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runAckLoop did not stop")
	}
	if writes.Load() != 1 {
		t.Fatalf("MAVLink writes = %d, want only the first command", writes.Load())
	}
}

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
				afterAircraftCommandEnqueue(agent, func() {
					agent.observeMAVLinkCommandAck(channel, 42, 1, &common.MessageCommandAck{
						Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
						Result:  common.MAV_RESULT_ACCEPTED,
					})
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
		afterAircraftCommandEnqueue(agent, func() {
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
	waitForAircraftCommandEnqueue(t, agent)
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
	agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
		Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
		Result:  common.MAV_RESULT_ACCEPTED,
	})
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
	waitForAircraftCommandEnqueue(t, agent)
	select {
	case got := <-result:
		t.Fatalf("buffered pre-restart ACK completed DISARM: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}

	agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
		Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
		Result:  common.MAV_RESULT_ACCEPTED,
	})
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
	waitForAircraftCommandEnqueue(t, agent)
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
	waitForAircraftCommandEnqueue(t, agent)
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

func TestAircraftCommandRearmsAmbiguityAfterTerminalACKFollowingQuietEpoch(t *testing.T) {
	for _, test := range []struct {
		name       string
		armed      bool
		command    agentv1.AircraftCommandType
		ack        common.MAV_RESULT
		wantStatus agentv1.AircraftCommandResult_Status
	}{
		{name: "definitive denial", armed: false, command: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM, ack: common.MAV_RESULT_DENIED, wantStatus: agentv1.AircraftCommandResult_STATUS_REJECTED},
		{name: "accepted no-op", armed: true, command: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM, ack: common.MAV_RESULT_ACCEPTED, wantStatus: agentv1.AircraftCommandResult_STATUS_ACCEPTED},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 50 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			channel := &gomavlib.Channel{}
			agent.observeMAVLinkHeartbeat(channel, 1, 1, test.armed)
			agent.mavlinkMu.Lock()
			agent.aircraftAckAmbiguousSince = time.Now().Add(-agent.aircraftCommandTimeout())
			agent.aircraftAckLastProgressAt = time.Now()
			agent.mavlinkMu.Unlock()
			agent.observeMAVLinkHeartbeat(channel, 1, 1, test.armed)
			agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
				afterAircraftCommandEnqueue(agent, func() {
					agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
						Command: common.MAV_CMD_COMPONENT_ARM_DISARM, Result: test.ack,
					})
				})
				return nil
			}

			got := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
				CommandId: "after-quiet-epoch", AircraftId: "aircraft-1", Type: test.command,
			})
			if got.GetStatus() != test.wantStatus {
				t.Fatalf("post-quiescence result = %+v, want %s", got, test.wantStatus)
			}
			agent.mavlinkMu.Lock()
			rearmed := agent.aircraftAckAmbiguous && agent.aircraftAckAmbiguousSince.IsZero() && agent.aircraftAckLastProgressAt.IsZero()
			agent.mavlinkMu.Unlock()
			if !rearmed {
				t.Fatal("terminal ACK after a quiet epoch did not re-arm ambiguity")
			}
		})
	}
}

func TestAircraftCommandAckActivityRestartsQuietEpoch(t *testing.T) {
	agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	channel := &gomavlib.Channel{}
	agent.observeMAVLinkHeartbeat(channel, 1, 1, false)
	agent.mavlinkMu.Lock()
	agent.aircraftAckAmbiguousSince = time.Now().Add(-agent.aircraftCommandTimeout())
	agent.aircraftAckLastProgressAt = time.Now()
	agent.mavlinkMu.Unlock()
	// An ACK observed before the next send proves the transport is not yet
	// quiescent and restarts the epoch even when no command is pending.
	agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
		Command: common.MAV_CMD_COMPONENT_ARM_DISARM, Result: common.MAV_RESULT_DENIED,
	})
	agent.writeMAVLinkCommand = func(*gomavlib.Channel, *common.MessageCommandLong) error {
		agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
			Command: common.MAV_CMD_COMPONENT_ARM_DISARM, Result: common.MAV_RESULT_DENIED,
		})
		return nil
	}

	got := agent.executeAircraftCommand(context.Background(), &agentv1.AircraftCommand{
		CommandId: "before-renewed-quiet-epoch", AircraftId: "aircraft-1",
		Type: agentv1.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM,
	})
	if got.GetStatus() != agentv1.AircraftCommandResult_STATUS_TIMEOUT {
		t.Fatalf("result after pre-send ACK activity = %+v, want fenced timeout", got)
	}
}

func TestAircraftCommandPausedReaderDoesNotExpireQuietEpoch(t *testing.T) {
	agent, err := NewAgent(&AgentOptions{AircraftCommandTimeout: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	channel := &gomavlib.Channel{}
	agent.observeMAVLinkHeartbeat(channel, 1, 1, false)
	agent.mavlinkMu.Lock()
	agent.aircraftAckAmbiguousSince = time.Now().Add(-agent.aircraftCommandTimeout())
	agent.aircraftAckLastProgressAt = time.Now().Add(-agent.aircraftCommandTimeout())
	agent.mavlinkMu.Unlock()

	// The first event after a reader pause starts a new progress epoch. Wall
	// time spent suspended cannot make an ACK already queued behind it safe.
	agent.observeMAVLinkHeartbeat(channel, 1, 1, false)
	agent.mavlinkMu.Lock()
	ambiguous := agent.aircraftAckAmbiguous
	quietSince := agent.aircraftAckAmbiguousSince
	lastProgress := agent.aircraftAckLastProgressAt
	agent.mavlinkMu.Unlock()
	if !ambiguous || quietSince.IsZero() || lastProgress.IsZero() || lastProgress.Sub(quietSince) >= agent.aircraftCommandTimeout() {
		t.Fatalf("paused reader expired fence: ambiguous=%t since=%v progress=%v", ambiguous, quietSince, lastProgress)
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
		afterAircraftCommandEnqueue(agent, func() {
			agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
				Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
				Result:  common.MAV_RESULT_ACCEPTED,
			})
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
		afterAircraftCommandEnqueue(agent, func() {
			agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
				Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
				Result:  common.MAV_RESULT_ACCEPTED,
			})
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
			waitForAircraftCommandEnqueue(t, agent)
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
		afterAircraftCommandEnqueue(agent, func() {
			agent.observeMAVLinkCommandAck(channel, 1, 1, &common.MessageCommandAck{
				Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
				Result:  mavlinkResult,
			})
		})
		return nil
	}
	return agent
}

func afterAircraftCommandEnqueue(agent *Agent, observe func()) {
	go func() {
		for {
			agent.mavlinkMu.Lock()
			pending := agent.pendingMAVLinkCommand
			ready := pending != nil && pending.enqueueComplete
			agent.mavlinkMu.Unlock()
			if ready {
				observe()
				return
			}
			time.Sleep(time.Microsecond)
		}
	}()
}

func waitForAircraftCommandEnqueue(t *testing.T, agent *Agent) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		agent.mavlinkMu.Lock()
		pending := agent.pendingMAVLinkCommand
		ready := pending != nil && pending.enqueueComplete
		agent.mavlinkMu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Microsecond)
	}
	t.Fatal("aircraft command did not cross the enqueue boundary")
}
