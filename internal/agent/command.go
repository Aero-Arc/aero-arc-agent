package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aero-arc/aero-arc-protos/commanddigest"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/aero-arc/aero-arc-protos/missiondigest"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"github.com/makinje/aero-arc-agent/internal/identity"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type pendingC2 struct {
	target  *mavlinkTarget
	command uint32
	after   time.Time
	frames  chan *gomavlib.EventFrame
}

func (a *Agent) observeC2Frame(frame *gomavlib.EventFrame) {
	a.mavlinkMu.Lock()
	defer a.mavlinkMu.Unlock()
	p := a.c2Pending
	current := a.mavlinkTarget
	if current == nil || p == nil || current.channel != p.target.channel || current.systemID != p.target.systemID || current.componentID != p.target.componentID {
		return
	}
	if p.after.IsZero() || frame.Channel != p.target.channel || frame.SystemID() != p.target.systemID || frame.ComponentID() != p.target.componentID {
		return
	}
	select {
	case p.frames <- frame:
	default:
	}
}

func (a *Agent) dispatchDurableCommand(ctx context.Context, stream grpc.BidiStreamingClient[pb.AgentStreamMessage, pb.RelayStreamMessage], c *pb.DurableCommand, wg *sync.WaitGroup, errs chan<- error) error {
	// Bound concurrent submissions without blocking telemetry ingest.
	if !a.c2Mu.TryLock() {
		digest, err := commanddigest.Digest(c)
		if err != nil || digest != c.GetCommandDigest() || c.GetAgentId() != identity.Resolve().FinalID {
			return fmt.Errorf("invalid busy command")
		}
		record, err := a.wal.LoadCommand(ctx, c.GetCommandId())
		if errors.Is(err, sql.ErrNoRows) {
			e := &pb.CommandEvidence{CommandId: c.CommandId, CommandDigest: digest, Events: []*pb.CommandEvent{commandEvent(c, "rejected", "another aircraft command is active; no effect was admitted", "agent_journal")}}
			payload, marshalErr := proto.Marshal(c)
			if marshalErr != nil {
				return marshalErr
			}
			evidence, marshalErr := proto.Marshal(e)
			if marshalErr != nil {
				return marshalErr
			}
			if err = a.wal.AdmitCommand(ctx, c.CommandId, wal.CommandRecord{Digest: digest, Payload: payload, Evidence: evidence}); err != nil {
				return err
			}
			record, err = a.wal.LoadCommand(ctx, c.CommandId)
		}
		if err != nil {
			return err
		}
		if record.Digest != c.GetCommandDigest() {
			return fmt.Errorf("conflicting command identity")
		}
		e := &pb.CommandEvidence{}
		if err = proto.Unmarshal(record.Evidence, e); err != nil {
			return err
		}
		a.sendMu.Lock()
		defer a.sendMu.Unlock()
		return stream.Send(&pb.AgentStreamMessage{Payload: &pb.AgentStreamMessage_CommandEvidence{CommandEvidence: e}})
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer a.c2Mu.Unlock()
		e, err := a.executeDurableCommand(ctx, c, func(e *pb.CommandEvidence) {
			a.sendMu.Lock()
			defer a.sendMu.Unlock()
			_ = stream.Send(&pb.AgentStreamMessage{Payload: &pb.AgentStreamMessage_CommandEvidence{CommandEvidence: e}})
		})
		if err == nil {
			a.sendMu.Lock()
			err = stream.Send(&pb.AgentStreamMessage{Payload: &pb.AgentStreamMessage_CommandEvidence{CommandEvidence: e}})
			a.sendMu.Unlock()
		}
		if err != nil {
			select {
			case errs <- err:
			default:
			}
		}
	}()
	return nil
}

func commandEvent(c *pb.DurableCommand, stage, message, source string) *pb.CommandEvent {
	return &pb.CommandEvent{EventId: c.CommandId + "/" + stage, Stage: stage, OccurredAtUnixMs: time.Now().UnixMilli(), Message: message, EvidenceSource: source}
}
func hasStage(e *pb.CommandEvidence, stage string) bool {
	for _, v := range e.Events {
		if v.Stage == stage {
			return true
		}
	}
	return false
}

func (a *Agent) executeDurableCommand(ctx context.Context, c *pb.DurableCommand, emit func(*pb.CommandEvidence)) (*pb.CommandEvidence, error) {
	digest, err := commanddigest.Digest(c)
	if err != nil || digest != c.GetCommandDigest() || c.GetCommandId() == "" {
		return nil, fmt.Errorf("invalid durable command identity")
	}
	if c.AgentId != identity.Resolve().FinalID {
		return nil, fmt.Errorf("wrong Agent destination")
	}
	e := &pb.CommandEvidence{CommandId: c.CommandId, CommandDigest: digest}
	effectOwned := false
	save := func(stage, msg, source string, effect bool) error {
		if stage != "" && !hasStage(e, stage) {
			e.Events = append(e.Events, commandEvent(c, stage, msg, source))
		}
		if effect && !effectOwned {
			owned, err := a.wal.BeginCommandEffect(ctx, c.CommandId, digest)
			if err != nil {
				return err
			}
			if !owned {
				return fmt.Errorf("effect permit already consumed; reconcile existing command")
			}
			effectOwned = true
		}
		b, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		err = a.wal.SaveCommand(ctx, c.CommandId, digest, b, effect)
		if err == nil && stage != "" && emit != nil {
			emit(proto.Clone(e).(*pb.CommandEvidence))
		}
		return err
	}
	record, err := a.wal.LoadCommand(ctx, c.CommandId)
	if err == nil {
		effectOwned = record.EffectStarted
		if record.Digest != digest {
			return nil, fmt.Errorf("command identity conflict")
		}
		if err = proto.Unmarshal(record.Evidence, e); err != nil {
			return nil, err
		}
		if record.EffectStarted && !hasStage(e, "applied") && !hasStage(e, "rejected") && !hasStage(e, "outcome_unknown") {
			err = save("outcome_unknown", "Agent restarted or disconnected after effect fence; automatic re-execution prohibited", "agent_journal", true)
		}
		if err != nil {
			return nil, err
		}
		if hasStage(e, "rejected") || hasStage(e, "observed") {
			return e, nil
		}
		if record.EffectStarted {
			if c.GetMission() != nil {
				return a.executeC2Mission(ctx, c, e, save)
			}
			// Recovery only listens for new observations. It never emits another effect.
			if hasStage(e, "applied") {
				return a.observeDurableCommand(ctx, c, e, save)
			}
			return e, nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	payload, err := proto.Marshal(c)
	if err != nil {
		return nil, err
	}
	evidence, err := proto.Marshal(e)
	if err != nil {
		return nil, err
	}
	if err = a.wal.AdmitCommand(ctx, c.CommandId, wal.CommandRecord{Digest: digest, Payload: payload, Evidence: evidence}); err != nil {
		return nil, err
	}
	reject := func(reason string) (*pb.CommandEvidence, error) { return e, save("rejected", reason, "agent", false) }
	if time.Now().UnixMilli() >= c.ExpiresAtUnixMs {
		return reject("first-effect authorization expired")
	}
	a.operationContextMu.Lock()
	a.stateMu.RLock()
	active := a.operationContext
	a.stateMu.RUnlock()
	matches := active != nil && active.AircraftID == c.AircraftId && active.FlightID == c.Context.FlightId && active.IntentID == c.Context.IntentId && active.IntentVersion == c.Context.IntentVersion
	a.operationContextMu.Unlock()
	if !matches {
		return reject("operation binding mismatch")
	}
	if err = save("acknowledged", "Agent durably admitted command", "agent_journal", false); err != nil {
		return nil, err
	}
	if c.GetMission() != nil {
		return a.executeC2Mission(ctx, c, e, save)
	}
	m := c.GetMavlink()
	switch m.Observation {
	case "armed", "disarmed", "custom_mode", "landed", "mission_running", "unavailable":
	default:
		return reject("unsupported observation capability")
	}
	if !a.tryBeginAircraftCommand() {
		return reject("aircraft execution is busy")
	}
	defer a.endAircraftCommand()
	a.operationContextMu.Lock()
	defer a.operationContextMu.Unlock()
	a.stateMu.RLock()
	active = a.operationContext
	a.stateMu.RUnlock()
	if active == nil || active.AircraftID != c.AircraftId || active.FlightID != c.Context.FlightId || active.IntentID != c.Context.IntentId || active.IntentVersion != c.Context.IntentVersion {
		return reject("operation binding changed")
	}
	if time.Now().UnixMilli() >= c.ExpiresAtUnixMs {
		return reject("first-effect authorization expired")
	}
	a.mavlinkMu.Lock()
	target := a.mavlinkTarget
	if target != nil {
		snapshot := *target
		target = &snapshot
	}
	if target == nil || target.channel == nil || time.Since(target.heartbeatAt) > 3*time.Second {
		a.mavlinkMu.Unlock()
		return reject("fresh autopilot target unavailable")
	}
	pending := &pendingC2{target: target, command: m.Command, after: time.Now(), frames: make(chan *gomavlib.EventFrame, 64)}
	a.c2Pending = pending
	a.mavlinkMu.Unlock()
	defer func() { a.mavlinkMu.Lock(); a.c2Pending = nil; a.mavlinkMu.Unlock() }()
	if m.VehicleProfile != "arducopter_v1" || target.autopilot != common.MAV_AUTOPILOT_ARDUPILOTMEGA {
		return reject("unsupported vehicle profile")
	}
	switch target.vehicleType {
	case common.MAV_TYPE_QUADROTOR, common.MAV_TYPE_HEXAROTOR, common.MAV_TYPE_OCTOROTOR, common.MAV_TYPE_TRICOPTER, common.MAV_TYPE_HELICOPTER, common.MAV_TYPE_COAXIAL:
	default:
		return reject("command profile requires an ArduCopter vehicle")
	}
	if m.MissionPrecondition != nil {
		if a.deployMAVLinkMission == nil {
			return reject("mission readback unavailable")
		}
		want, er := missiondigest.Digest(m.MissionPrecondition)
		if er != nil {
			return reject(er.Error())
		}
		got, _, _, er := a.deployMAVLinkMission(ctx, target, m.MissionPrecondition, true, c.ExpiresAtUnixMs)
		if er != nil || got != want {
			return reject("current onboard mission does not match command precondition")
		}
	}
	if time.Now().UnixMilli() >= c.ExpiresAtUnixMs {
		return reject("authorization expired during precondition checks")
	}
	if m.Command == 400 {
		if m.UseCommandInt || (m.Parameters[0] != 0 && m.Parameters[0] != 1) {
			return reject("unsupported arm/disarm encoding")
		}
		for _, value := range m.Parameters[1:] {
			if value != 0 {
				return reject("arm/disarm optional parameters are not supported")
			}
		}
		kind := pb.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_DISARM
		if m.Parameters[0] == 1 {
			kind = pb.AircraftCommandType_AIRCRAFT_COMMAND_TYPE_ARM
		}
		prepared, immediate := prepareAircraftCommand(&pb.AircraftCommand{CommandId: c.CommandId, AircraftId: c.AircraftId, Type: kind, IssuedAtUnixMs: c.IssuedAtUnixMs})
		if immediate != nil {
			return reject(immediate.Message)
		}
		if m.Parameters[1] != 0 {
			return reject("force arm/disarm is not supported")
		}
		if err = save("", "", "", true); err != nil {
			return nil, err
		}
		effectCtx, cancelEffect := context.WithDeadline(ctx, time.UnixMilli(c.ExpiresAtUnixMs))
		result := a.executePreparedAircraftCommand(effectCtx, prepared)
		cancelEffect()
		switch result.Status {
		case pb.AircraftCommandResult_STATUS_ACCEPTED:
			if err = save("applied", result.Message, "mavlink_command_ack", true); err != nil {
				return nil, err
			}
			return a.observeDurableCommand(ctx, c, e, save)
		case pb.AircraftCommandResult_STATUS_REJECTED:
			return e, save("rejected", result.Message, "mavlink_command_ack", true)
		default:
			return e, save("outcome_unknown", result.Message, "mavlink_transport", true)
		}
	}
	if err = a.waitC2Quiet(ctx, pending, c.ExpiresAtUnixMs); err != nil {
		return reject(err.Error())
	}
	a.mavlinkMu.Lock()
	current := a.mavlinkTarget
	same := current != nil && current.channel == target.channel && current.systemID == target.systemID && current.componentID == target.componentID && time.Since(current.heartbeatAt) <= 3*time.Second
	pending.after = time.Time{}
	a.mavlinkMu.Unlock()
	if !same {
		return reject("autopilot target changed before effect")
	}
	if time.Now().UnixMilli() >= c.ExpiresAtUnixMs {
		return reject("authorization expired before effect")
	}
	// Discard evidence from the pre-send epoch; callbacks during handoff are
	// ignored until the new post-send observation boundary is installed.
	for len(pending.frames) > 0 {
		<-pending.frames
	}
	if err = save("", "", "", true); err != nil {
		return nil, err
	}
	if a.writeMAVLinkMessage == nil {
		return e, save("outcome_unknown", "MAVLink writer unavailable after effect fence", "agent", true)
	}
	p := m.Parameters
	var request message.Message = &common.MessageCommandLong{TargetSystem: target.systemID, TargetComponent: target.componentID, Command: common.MAV_CMD(m.Command), Param1: p[0], Param2: p[1], Param3: p[2], Param4: p[3], Param5: p[4], Param6: p[5], Param7: p[6]}
	if m.UseCommandInt {
		request = &common.MessageCommandInt{TargetSystem: target.systemID, TargetComponent: target.componentID, Command: common.MAV_CMD(m.Command), Frame: common.MAV_FRAME(m.Frame), Param1: p[0], Param2: p[1], Param3: p[2], Param4: p[3], X: m.X, Y: m.Y, Z: m.Z}
	}
	if ctx.Err() != nil || time.Now().UnixMilli() >= c.ExpiresAtUnixMs {
		return reject("authorization expired or execution canceled before effect")
	}
	if err = a.writeMAVLinkMessage(target.channel, request); err != nil {
		return e, save("outcome_unknown", err.Error(), "mavlink_transport", true)
	}
	a.mavlinkMu.Lock()
	pending.after = time.Now()
	a.mavlinkMu.Unlock()
	execution, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	acked, observed := false, false
	// MAVLink does not echo our identity. Require fresh matching vehicle evidence
	// as well as an ACK before treating generic execution as applied.
	for {
		select {
		case <-execution.Done():
			if acked {
				return e, nil
			}
			return e, save("outcome_unknown", "no correlated acceptance before timeout; no automatic effect retry", "agent", true)
		case f := <-pending.frames:
			switch v := f.Message().(type) {
			case *common.MessageCommandAck:
				if uint32(v.Command) != m.Command || (v.TargetSystem != 0 && v.TargetSystem != mavlinkSourceSystemID) || (v.TargetComponent != 0 && v.TargetComponent != mavlinkSourceComponentID) {
					continue
				}
				if v.Result == common.MAV_RESULT_ACCEPTED && !acked {
					acked = true
					if err = save("applied", "autopilot accepted command; observation pending", "mavlink_command_ack", true); err != nil {
						return nil, err
					}
					return a.observeDurableCommand(ctx, c, e, save)
				} else if v.Result != common.MAV_RESULT_IN_PROGRESS && v.Result != common.MAV_RESULT_ACCEPTED {
					return e, save("rejected", fmt.Sprintf("autopilot rejected command: %s", v.Result.String()), "mavlink_command_ack", true)
				}
			case *common.MessageHeartbeat:
				armed := v.BaseMode&common.MAV_MODE_FLAG_SAFETY_ARMED != 0
				observed = observed || (m.Observation == "armed" && armed) || (m.Observation == "disarmed" && !armed) || (m.Observation == "custom_mode" && v.CustomMode == m.ExpectedCustomMode)
			case *common.MessageMissionCurrent:
				observed = observed || (m.Observation == "mission_running" && v.MissionState == common.MISSION_STATE_ACTIVE && v.MissionMode == 1)
			case *common.MessageExtendedSysState:
				observed = observed || (m.Observation == "landed" && v.LandedState == common.MAV_LANDED_STATE_ON_GROUND)
			}
			if acked && observed {
				return e, save("observed", "fresh "+m.Observation+" matched command predicate", "mavlink_vehicle_state", true)
			}
		}
	}
}

// observeDurableCommand consumes only fresh messages from the currently selected
// autopilot. It is safe after expiry and across restart because it issues no effect.
func (a *Agent) observeDurableCommand(ctx context.Context, c *pb.DurableCommand, e *pb.CommandEvidence, save func(string, string, string, bool) error) (*pb.CommandEvidence, error) {
	m := c.GetMavlink()
	if m == nil {
		return e, nil
	}
	if m.Observation == "unavailable" {
		return e, save("observation_unavailable", "vehicle protocol does not expose an authoritative predicate for this operation", "agent", true)
	}
	latest, err := a.wal.CommandIsLatest(ctx, c.CommandId)
	if err != nil {
		return nil, err
	}
	if !latest {
		return e, save("observation_superseded", "newer aircraft command prevents attributing observations to this command", "agent_journal", true)
	}
	a.mavlinkMu.Lock()
	target := a.mavlinkTarget
	if target != nil {
		snapshot := *target
		target = &snapshot
	}
	if target == nil || target.channel == nil {
		a.mavlinkMu.Unlock()
		return e, nil
	}
	p := &pendingC2{target: target, command: m.Command, after: time.Now(), frames: make(chan *gomavlib.EventFrame, 64)}
	a.c2Pending = p
	a.mavlinkMu.Unlock()
	defer func() {
		a.mavlinkMu.Lock()
		if a.c2Pending == p {
			a.c2Pending = nil
		}
		a.mavlinkMu.Unlock()
	}()
	observation, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		select {
		case <-observation.Done():
			return e, nil
		case f := <-p.frames:
			matched := false
			switch v := f.Message().(type) {
			case *common.MessageHeartbeat:
				armed := v.BaseMode&common.MAV_MODE_FLAG_SAFETY_ARMED != 0
				matched = (m.Observation == "armed" && armed) || (m.Observation == "disarmed" && !armed) || (m.Observation == "custom_mode" && v.CustomMode == m.ExpectedCustomMode)
			case *common.MessageMissionCurrent:
				matched = m.Observation == "mission_running" && v.MissionState == common.MISSION_STATE_ACTIVE && v.MissionMode == 1
			case *common.MessageExtendedSysState:
				matched = m.Observation == "landed" && v.LandedState == common.MAV_LANDED_STATE_ON_GROUND
			}
			if matched {
				return e, save("observed", fmt.Sprintf("%s matched at system=%d component=%d", m.Observation, f.SystemID(), f.ComponentID()), "mavlink_vehicle_state", true)
			}
		}
	}
}

// waitC2Quiet requires continuous target traffic and a quiet ACK domain before
// sending a fresh generic command. Any matching ACK restarts the quiet interval.
func (a *Agent) waitC2Quiet(ctx context.Context, p *pendingC2, expires int64) error {
	started, last := time.Now(), time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f := <-p.frames:
			now := time.Now()
			if now.Sub(last) > 2*time.Second {
				started = now
			}
			last = now
			if ack, ok := f.Message().(*common.MessageCommandAck); ok && uint32(ack.Command) == p.command {
				started = now
			}
		case now := <-ticker.C:
			if now.UnixMilli() >= expires {
				return fmt.Errorf("authorization expired before quiet ACK epoch")
			}
			if now.Sub(last) > 2*time.Second {
				started = now
			}
			if now.Sub(started) >= 4*time.Second {
				return nil
			}
		}
	}
}

func (a *Agent) executeC2Mission(ctx context.Context, c *pb.DurableCommand, e *pb.CommandEvidence, save func(string, string, string, bool) error) (*pb.CommandEvidence, error) {
	mission := c.GetMission()
	var err error
	if err = save("", "", "", true); err != nil {
		return nil, err
	}
	result := a.executeMissionDeployment(ctx, mission)
	switch result.Status {
	case pb.MissionDeploymentResult_STATUS_APPLIED, pb.MissionDeploymentResult_STATUS_ALREADY_APPLIED:
		if err = save("applied", "mission accepted and verified onboard", "mavlink_mission_protocol", true); err == nil {
			err = save("observed", "onboard digest "+result.OnboardMissionDigest, "mavlink_mission_readback", true)
		}
	case pb.MissionDeploymentResult_STATUS_REJECTED, pb.MissionDeploymentResult_STATUS_BINDING_MISMATCH:
		err = save("rejected", result.Message, "agent", true)
	default:
		err = save("outcome_unknown", result.Message, "mavlink_mission_protocol", true)
	}
	return e, err
}
