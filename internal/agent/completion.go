package agent

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/aero-arc/aero-arc-protos/missiondigest"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type completionObservation struct {
	target       string
	channel      *gomavlib.Channel
	context      *pb.OperationContext
	at           int64
	kind         string
	armed        bool
	mode         uint32
	landed       uint32
	sequence     uint32
	missionState uint32
}

type completionSamples struct {
	target      string
	channel     *gomavlib.Channel
	flight      string
	armed       bool
	mode        uint32
	heartbeatAt int64
	landed      uint32
	landedAt    int64
}

func (a *Agent) completionObservation(frame *gomavlib.EventFrame) (completionObservation, bool) {
	a.mavlinkMu.Lock()
	target := a.mavlinkTarget
	if target != nil {
		snapshot := *target
		target = &snapshot
	}
	matches := target != nil && target.channel == frame.Channel && target.systemID == frame.SystemID() && target.componentID == frame.ComponentID()
	a.mavlinkMu.Unlock()
	if !matches {
		return completionObservation{}, false
	}
	a.stateMu.RLock()
	current := a.operationContext
	o := completionObservation{target: a.completionTargetIdentity(target), channel: frame.Channel, at: time.Now().UnixNano()}
	if current != nil {
		o.context = &pb.OperationContext{AircraftId: current.AircraftID, FlightId: current.FlightID, IntentId: current.IntentID, IntentVersion: current.IntentVersion}
	}
	a.stateMu.RUnlock()
	switch m := frame.Message().(type) {
	case *common.MessageHeartbeat:
		o.kind = "heartbeat"
		o.armed = m.BaseMode&common.MAV_MODE_FLAG_SAFETY_ARMED != 0
		o.mode = m.CustomMode
	case *common.MessageExtendedSysState:
		o.kind = "landed"
		o.landed = uint32(m.LandedState)
	case *common.MessageMissionCurrent:
		o.kind = "mission"
		o.sequence = uint32(m.Seq)
		o.missionState = uint32(m.MissionState)
	default:
		return completionObservation{}, false
	}
	return o, true
}

func (a *Agent) runCompletionObservations(ctx context.Context, queue <-chan completionObservation) {
	epoch := uuid.NewString()
	samples := completionSamples{}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.requestCompletionObservations(ctx)
		case o, ok := <-queue:
			if !ok {
				return
			}
			if a.wal == nil {
				continue
			}
			if err := a.observeCompletion(ctx, o, epoch, &samples); err != nil && ctx.Err() == nil && !errors.Is(err, sql.ErrNoRows) {
				slog.Error("flight completion evidence persistence failed", "error", err)
			}
		}
	}
}

func (a *Agent) observeCompletion(ctx context.Context, o completionObservation, epoch string, samples *completionSamples) error {
	// Active planning context can be cleared or replaced independently of this
	// aircraft's unfinished flight. Only persisted start authority owns completion.
	watch, err := a.wal.LoadUnresolvedFlightWatch(ctx, o.target)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	o.context = watch.Command.Context
	if watch.Done || watch.Target == "" || watch.Target != o.target {
		return nil
	}
	c := watch.Command
	record, err := a.wal.LoadCommand(ctx, c.CommandId)
	if err != nil {
		return err
	}
	var evidence pb.CommandEvidence
	if err = proto.Unmarshal(record.Evidence, &evidence); err != nil {
		return err
	}
	applied := false
	for _, event := range evidence.Events {
		if event.Stage == "applied" {
			applied = true
		}
	}
	if !applied {
		return nil
	}
	if c.Context.AircraftId != o.context.AircraftId || c.Context.IntentId != o.context.IntentId || c.Context.IntentVersion != o.context.IntentVersion || watch.HandoffAt == 0 || o.at < watch.HandoffAt {
		return nil
	}
	if samples.flight != o.context.FlightId || samples.target != o.target || samples.channel != o.channel {
		*samples = completionSamples{flight: o.context.FlightId, target: o.target, channel: o.channel}
	}
	switch o.kind {
	case "heartbeat":
		samples.armed = o.armed
		samples.mode = o.mode
		samples.heartbeatAt = o.at
	case "landed":
		samples.landed = o.landed
		samples.landedAt = o.at
	}
	fresh := func(at int64) bool { return at > 0 && o.at >= at && o.at-at <= int64(5*time.Second) }
	changed := false
	if watch.AirborneAt == 0 && samples.armed && samples.landed == uint32(common.MAV_LANDED_STATE_IN_AIR) && fresh(samples.heartbeatAt) && fresh(samples.landedAt) {
		watch.AirborneAt = o.at
		changed = true
	}
	if watch.AirborneAt == 0 {
		return nil
	}
	if watch.TerminalAt == 0 {
		if o.kind == "mission" && samples.mode == 3 && samples.armed && fresh(samples.heartbeatAt) && (o.sequence == uint32(len(c.GetMavlink().MissionPrecondition.Items)) || o.missionState == uint32(common.MISSION_STATE_COMPLETE)) {
			watch.TerminalAt = o.at
			watch.Outcome = "mission_completed"
			changed = true
		} else if o.kind == "heartbeat" && o.armed && (o.mode == 6 || o.mode == 9) {
			watch.TerminalAt = o.at
			watch.Outcome = "ended_early"
			changed = true
		}
	}
	if watch.TerminalAt > 0 && !samples.armed && samples.landed == uint32(common.MAV_LANDED_STATE_ON_GROUND) && fresh(samples.heartbeatAt) && fresh(samples.landedAt) && samples.heartbeatAt >= watch.TerminalAt && samples.landedAt >= watch.TerminalAt {
		digest, err := missiondigest.Digest(c.GetMavlink().MissionPrecondition)
		if err != nil {
			return err
		}
		evidence := &pb.FlightCompletionEvidence{EventId: uuid.NewSHA1(uuid.NameSpaceOID, []byte(c.CommandId+"/flight-completion")).String(), AgentId: c.AgentId, Context: c.Context, MissionId: c.GetMavlink().MissionPreconditionId, MissionDigest: digest, StartCommandId: c.CommandId, Outcome: watch.Outcome, AirborneAtUnixNs: watch.AirborneAt, TerminalAtUnixNs: watch.TerminalAt, LandedAtUnixNs: samples.landedAt, DisarmedAtUnixNs: samples.heartbeatAt, ObservationEpoch: epoch}
		watch.Done = true
		return a.wal.SaveFlightWatch(ctx, watch, evidence)
	}
	if changed {
		return a.wal.SaveFlightWatch(ctx, watch, nil)
	}
	return nil
}

func (a *Agent) runCompletionDelivery(ctx context.Context, stream grpc.BidiStreamingClient[pb.AgentStreamMessage, pb.RelayStreamMessage]) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if a.wal != nil {
			pending, err := a.wal.PendingFlightCompletions(ctx)
			if err != nil {
				return err
			}
			for _, event := range pending {
				a.sendMu.Lock()
				err = stream.Send(&pb.AgentStreamMessage{Payload: &pb.AgentStreamMessage_FlightCompletionEvidence{FlightCompletionEvidence: event}})
				a.sendMu.Unlock()
				if err != nil {
					return err
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Completion must not depend on a ground station configuring optional MAVLink
// telemetry streams. Request the two independent observations while a durable
// flight watch is active; these requests never command a vehicle action.
func (a *Agent) requestCompletionObservations(ctx context.Context) {
	if a.wal == nil || a.writeMAVLinkCommand == nil {
		return
	}
	a.mavlinkMu.Lock()
	var target mavlinkTarget
	if a.mavlinkTarget != nil {
		target = *a.mavlinkTarget
	}
	a.mavlinkMu.Unlock()
	watch, err := a.wal.LoadUnresolvedFlightWatch(ctx, a.completionTargetIdentity(&target))
	if err != nil || watch.Done {
		return
	}
	record, err := a.wal.LoadCommand(ctx, watch.Command.CommandId)
	if err != nil {
		return
	}
	var evidence pb.CommandEvidence
	if proto.Unmarshal(record.Evidence, &evidence) != nil || !hasStage(&evidence, "applied") || hasStage(&evidence, "rejected") {
		return
	}
	if target.channel == nil || time.Since(target.heartbeatAt) > 3*time.Second || watch.Target == "" || a.completionTargetIdentity(&target) != watch.Target {
		return
	}
	for _, id := range []uint32{245, 42} {
		if ctx.Err() != nil {
			return
		}
		if err := a.writeMAVLinkCommand(target.channel, &common.MessageCommandLong{TargetSystem: target.systemID, TargetComponent: target.componentID, Command: common.MAV_CMD_REQUEST_MESSAGE, Param1: float32(id)}); err != nil {
			return
		}
	}
}

// The endpoint and MAVLink identity/profile survive process restart. A changed
// endpoint or autopilot identity requires explicit recovery, never automatic
// rebinding of a flight watch. MAVLink IDs are not cryptographic hardware IDs.
func (a *Agent) completionTargetIdentity(target *mavlinkTarget) string {
	return a.commandTargetIdentity(target)
}
