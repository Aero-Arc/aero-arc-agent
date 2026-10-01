package agent

import (
	"testing"
	"time"

	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/frame"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
)

func TestC2AdmissionRequiresArrivalAndFullProfile(t *testing.T) {
	for _, kind := range []string{"ack", "heartbeat"} {
		for _, change := range []string{"none", "arrival", "selected-autopilot", "selected-vehicle", "heartbeat-autopilot", "heartbeat-vehicle"} {
			if kind == "ack" && (change == "heartbeat-autopilot" || change == "heartbeat-vehicle") {
				continue
			}
			t.Run(kind+"/"+change, func(t *testing.T) {
				now := time.Now()
				target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, heartbeatAt: now, autopilot: common.MAV_AUTOPILOT_ARDUPILOTMEGA, vehicleType: common.MAV_TYPE_QUADROTOR}
				current := *target
				p := &pendingC2{target: target, after: now, frames: make(chan *gomavlib.EventFrame, 1)}
				a := &Agent{mavlinkTarget: &current, c2Pending: p}
				heartbeat := &common.MessageHeartbeat{Type: target.vehicleType, Autopilot: target.autopilot, BaseMode: common.MAV_MODE_FLAG_SAFETY_ARMED}
				arrival := now.Add(time.Millisecond)
				switch change {
				case "arrival":
					arrival = now.Add(-time.Millisecond)
				case "selected-autopilot":
					current.autopilot++
				case "selected-vehicle":
					current.vehicleType++
				case "heartbeat-autopilot":
					heartbeat.Autopilot++
				case "heartbeat-vehicle":
					heartbeat.Type++
				}
				var payload message.Message = heartbeat
				if kind == "ack" {
					payload = &common.MessageCommandAck{Command: common.MAV_CMD_NAV_LAND, Result: common.MAV_RESULT_ACCEPTED}
				}
				a.observeC2FrameAt(&gomavlib.EventFrame{Channel: target.channel, Frame: &frame.V2Frame{SystemID: 1, ComponentID: 1, Message: payload}}, arrival)
				want := 0
				if change == "none" {
					want = 1
				}
				if len(p.frames) != want {
					t.Fatalf("admitted=%d want=%d", len(p.frames), want)
				}
			})
		}
	}
}

func TestArmEvidenceRejectsDelayedArrivalAndChangedProfile(t *testing.T) {
	now := time.Now()
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, heartbeatAt: now, autopilot: common.MAV_AUTOPILOT_ARDUPILOTMEGA, vehicleType: common.MAV_TYPE_QUADROTOR}
	p := &pendingMAVLinkCommand{target: target, channel: target.channel, systemID: 1, componentID: 1, command: common.MAV_CMD_COMPONENT_ARM_DISARM, desiredArmed: true, after: now, enqueueComplete: true, acks: make(chan mavlinkCommandAck, 1), armedStateChange: make(chan mavlinkArmedStateEvidence, 1)}
	a := &Agent{mavlinkTarget: target, pendingMAVLinkCommand: p}
	ack := &common.MessageCommandAck{Command: p.command, Result: common.MAV_RESULT_ACCEPTED}
	a.observeMAVLinkCommandAckAt(now.Add(-time.Millisecond), target.channel, 1, 1, ack)
	a.observeMAVLinkHeartbeatAt(now.Add(-time.Millisecond), target.channel, 1, 1, true, uint32(target.vehicleType), uint32(target.autopilot))
	if len(p.acks) != 0 || len(p.armedStateChange) != 0 {
		t.Fatal("pre-handoff evidence admitted")
	}
	a.observeMAVLinkHeartbeatAt(now.Add(time.Millisecond), target.channel, 1, 1, true, uint32(common.MAV_TYPE_FIXED_WING), uint32(target.autopilot))
	a.observeMAVLinkCommandAckAt(now.Add(time.Millisecond), target.channel, 1, 1, ack)
	if len(p.acks) != 0 || len(p.armedStateChange) != 0 {
		t.Fatal("changed-profile evidence admitted")
	}
	a.observeMAVLinkHeartbeatAt(now.Add(2*time.Millisecond), target.channel, 1, 1, true, uint32(target.vehicleType), uint32(target.autopilot))
	a.observeMAVLinkCommandAckAt(now.Add(2*time.Millisecond), target.channel, 1, 1, ack)
	if len(p.acks) != 1 || len(p.armedStateChange) != 1 {
		t.Fatal("fresh matching evidence not admitted")
	}
}

func TestMissionResponseRejectsChangedSelectedProfile(t *testing.T) {
	target := &mavlinkTarget{channel: &gomavlib.Channel{}, systemID: 1, componentID: 1, autopilot: common.MAV_AUTOPILOT_ARDUPILOTMEGA, vehicleType: common.MAV_TYPE_QUADROTOR}
	p := &missionTransactionTarget{channel: target.channel, systemID: 1, componentID: 1, autopilot: target.autopilot, vehicleType: target.vehicleType}
	queue := make(chan message.Message, 1)
	a := &Agent{mavlinkTarget: target, pendingMissionTarget: p, pendingMissionEvents: queue}
	f := &gomavlib.EventFrame{Channel: target.channel, Frame: &frame.V2Frame{SystemID: 1, ComponentID: 1, Message: &common.MessageMissionCount{Count: 2}}}
	target.vehicleType = common.MAV_TYPE_FIXED_WING
	a.observeMissionProtocolMessage(f)
	if len(queue) != 0 {
		t.Fatal("changed-profile mission response admitted")
	}
	target.vehicleType = p.vehicleType
	a.observeMissionProtocolMessage(f)
	if len(queue) != 1 {
		t.Fatal("matching mission response not admitted")
	}
}
