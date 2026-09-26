package agent

import (
	"context"
	"errors"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/frame"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"testing"
	"time"
)

func quietFrame(channel *gomavlib.Channel, msg message.Message) *gomavlib.EventFrame {
	return &gomavlib.EventFrame{Channel: channel, Frame: &frame.V2Frame{SystemID: 1, ComponentID: 1, Message: msg}}
}

func TestContinuousQuietCreditsIdleAndRejectsGapsTargetsAndDelayedACK(t *testing.T) {
	channel := &gomavlib.Channel{}
	a := &Agent{mavlinkTarget: &mavlinkTarget{channel: channel, systemID: 1, componentID: 1}}
	now := time.Now()
	for i := 0; i <= 15; i++ {
		a.trackProtocolQuietLocked(quietFrame(channel, &common.MessageHeartbeat{}), now.Add(time.Duration(i-15)*time.Second))
	}
	if got := a.protocolQuietForLocked(a.mavlinkTarget, 300, false, now); got != 15*time.Second {
		t.Fatalf("idle credit=%s", got)
	}
	a.trackProtocolQuietLocked(quietFrame(channel, &common.MessageCommandAck{Command: 300}), now)
	if got := a.protocolQuietForLocked(a.mavlinkTarget, 300, false, now); got != 0 {
		t.Fatalf("delayed ACK retained credit=%s", got)
	}
	if got := a.protocolQuietForLocked(a.mavlinkTarget, 193, false, now); got != 15*time.Second {
		t.Fatal("unrelated ACK reset domain")
	}
	a.trackProtocolQuietLocked(quietFrame(channel, &common.MessageHeartbeat{}), now.Add(4*time.Second))
	if got := a.protocolQuietForLocked(a.mavlinkTarget, 193, false, now.Add(4*time.Second)); got != 0 {
		t.Fatal("traffic gap retained credit")
	}
	other := *a.mavlinkTarget
	other.systemID = 2
	if got := a.protocolQuietForLocked(&other, 193, false, now); got != 0 {
		t.Fatal("new target inherited quiet epoch")
	}
}

func TestIdleMissionReadbackSkipsWaitingButFailureResetsEpoch(t *testing.T) {
	channel := &gomavlib.Channel{}
	a := &Agent{options: &AgentOptions{AircraftCommandTimeout: time.Second}, mavlinkTarget: &mavlinkTarget{channel: channel, systemID: 1, componentID: 1}}
	now := time.Now()
	a.protocolQuiet = protocolQuiet{channel: channel, system: 1, component: 1, since: now.Add(-20 * time.Second), last: now, mission: now.Add(-20 * time.Second)}
	writes := 0
	a.writeMAVLinkMessage = func(_ *gomavlib.Channel, m message.Message) error { writes++; return errors.New("lost write") }
	_, err := a.readbackMAVLinkWireMissionWithin(context.Background(), a.mavlinkTarget, make(chan message.Message), time.Second, time.Second)
	if err == nil || writes != 1 {
		t.Fatalf("expected immediate request and failed write: writes=%d err=%v", writes, err)
	}
	if time.Since(a.protocolQuiet.mission) > time.Second {
		t.Fatal("failed transfer kept old idle credit")
	}
}

func TestGenericCommandUsesPreviouslyObservedQuietTime(t *testing.T) {
	channel := &gomavlib.Channel{}
	now := time.Now()
	target := &mavlinkTarget{channel: channel, systemID: 1, componentID: 1}
	a := &Agent{mavlinkTarget: target, protocolQuiet: protocolQuiet{channel: channel, system: 1, component: 1, since: now.Add(-20 * time.Second), last: now, acks: map[uint32]time.Time{}}}
	p := &pendingC2{target: target, command: 300, frames: make(chan *gomavlib.EventFrame, 2)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.waitC2Quiet(ctx, p, now.Add(time.Second).UnixMilli()); err != nil {
		t.Fatalf("idle target waited a fresh four seconds: %v", err)
	}
	a.protocolQuiet.acks[300] = time.Now()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel2()
	if err := a.waitC2Quiet(ctx2, p, time.Now().Add(time.Second).UnixMilli()); err == nil {
		t.Fatal("late ACK did not rearm fence")
	}
}

func TestMissionSilenceBudgetIsIndependentAndDefaultsRemainCompatible(t *testing.T) {
	a := &Agent{options: &AgentOptions{AircraftCommandTimeout: 30 * time.Second}}
	if a.missionReadbackQuietPeriod() != 30*time.Second {
		t.Fatal("existing guard shortened")
	}
	a.options.MissionProtocolQuietPeriod = 12 * time.Second
	if a.missionReadbackQuietPeriod() != 12*time.Second || a.aircraftCommandTimeout() != 30*time.Second {
		t.Fatal("silence and response budgets coupled")
	}
}
