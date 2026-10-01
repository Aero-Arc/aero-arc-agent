package agent

import (
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"time"
)

// protocolQuiet is scoped to the selected MAVLink transport and only credits
// continuously observed traffic. All access is protected by mavlinkMu.
type protocolQuiet struct {
	channel              *gomavlib.Channel
	system, component    uint8
	since, last, mission time.Time
	acks                 map[uint32]time.Time
}

func (a *Agent) trackProtocolQuietLocked(frame *gomavlib.EventFrame, now time.Time) {
	t := a.mavlinkTarget
	if t == nil || frame.Channel != t.channel || frame.SystemID() != t.systemID || frame.ComponentID() != t.componentID {
		return
	}
	q := &a.protocolQuiet
	if q.channel != t.channel || q.system != t.systemID || q.component != t.componentID || q.last.IsZero() || now.Sub(q.last) > 2*time.Second {
		*q = protocolQuiet{channel: t.channel, system: t.systemID, component: t.componentID, since: now, mission: now, acks: map[uint32]time.Time{}}
	}
	q.last = now
	if ack, ok := frame.Message().(*common.MessageCommandAck); ok {
		q.acks[uint32(ack.Command)] = now
	}
}

func (a *Agent) protocolQuietForLocked(target *mavlinkTarget, command uint32, mission bool, now time.Time) time.Duration {
	q := &a.protocolQuiet
	if target == nil || q.channel != target.channel || q.system != target.systemID || q.component != target.componentID || q.last.IsZero() || now.Sub(q.last) > 2*time.Second {
		return 0
	}
	since := q.since
	if mission {
		if q.mission.After(since) {
			since = q.mission
		}
	} else if q.acks[command].After(since) {
		since = q.acks[command]
	}
	return now.Sub(since)
}

func (a *Agent) resetMissionQuiet() {
	a.mavlinkMu.Lock()
	a.protocolQuiet.mission = time.Now()
	a.mavlinkMu.Unlock()
}
