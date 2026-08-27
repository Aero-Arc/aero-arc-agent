package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"google.golang.org/protobuf/proto"
)

func (a *Agent) observeMissionProtocolMessage(frame *gomavlib.EventFrame) {
	if frame == nil {
		return
	}
	switch frame.Message().(type) {
	case *common.MessageMissionRequestInt, *common.MessageMissionRequest, *common.MessageMissionAck,
		*common.MessageMissionCount, *common.MessageMissionItemInt, *common.MessageMissionItem:
	default:
		return
	}
	if !missionMessageTargetsAgent(frame.Message()) {
		return
	}
	a.mavlinkMu.Lock()
	target := a.mavlinkTarget
	events := a.pendingMissionEvents
	matched := target != nil && target.channel == frame.Channel && target.systemID == frame.SystemID() && target.componentID == frame.ComponentID()
	a.mavlinkMu.Unlock()
	if !matched || events == nil {
		return
	}
	select {
	case events <- frame.Message():
	default:
		// A full protocol queue makes correlation ambiguous; the transaction will
		// time out and reconcile by readback rather than guessing.
	}
}

func missionMessageTargetsAgent(value message.Message) bool {
	var systemID, componentID uint8
	switch value := value.(type) {
	case *common.MessageMissionRequestInt:
		systemID, componentID = value.TargetSystem, value.TargetComponent
	case *common.MessageMissionRequest:
		systemID, componentID = value.TargetSystem, value.TargetComponent
	case *common.MessageMissionAck:
		systemID, componentID = value.TargetSystem, value.TargetComponent
	case *common.MessageMissionCount:
		systemID, componentID = value.TargetSystem, value.TargetComponent
	case *common.MessageMissionItemInt:
		systemID, componentID = value.TargetSystem, value.TargetComponent
	case *common.MessageMissionItem:
		systemID, componentID = value.TargetSystem, value.TargetComponent
	default:
		return false
	}
	return (systemID == 0 || systemID == mavlinkSourceSystemID) &&
		(componentID == 0 || componentID == mavlinkSourceComponentID)
}

func (a *Agent) executeMAVLinkMissionDeployment(ctx context.Context, target *mavlinkTarget, plan *agentv1.MissionPlan, readbackOnly bool) (string, uint32, *uint32, error) {
	if target == nil || target.channel == nil || a.writeMAVLinkMessage == nil {
		return "", 0, nil, errors.New("MAVLink mission channel is unavailable")
	}
	events := make(chan message.Message, maxMissionItems+8)
	a.mavlinkMu.Lock()
	if a.pendingMissionEvents != nil {
		a.mavlinkMu.Unlock()
		return "", 0, nil, errors.New("another MAVLink mission transaction is active")
	}
	a.pendingMissionEvents = events
	a.mavlinkMu.Unlock()
	defer func() {
		a.mavlinkMu.Lock()
		if a.pendingMissionEvents == events {
			a.pendingMissionEvents = nil
		}
		a.mavlinkMu.Unlock()
	}()

	if readbackOnly {
		digest, err := a.readbackMAVLinkMission(ctx, target, events)
		if err != nil {
			return digest, 0, nil, fmt.Errorf("%w: reconcile uncertain deployment: %v", errMissionOutcomeUnknown, err)
		}
		return digest, 0, nil, nil
	}
	wireItems, err := a.readbackMAVLinkWireMission(ctx, target, events)
	if err != nil {
		return "", 0, nil, fmt.Errorf("pre-upload HOME readback: %w", err)
	}
	if len(wireItems) == 0 {
		return "", 0, nil, errors.New("pre-upload readback omitted ArduPilot HOME at wire sequence 0")
	}
	home := wireItems[0]

	if err := a.ensureMissionUploadSafe(target); err != nil {
		return "", 0, nil, err
	}
	if err := a.writeMAVLinkMessage(target.channel, &common.MessageMissionCount{
		TargetSystem: target.systemID, TargetComponent: target.componentID,
		Count: uint16(len(plan.Items) + 1), MissionType: common.MAV_MISSION_TYPE_MISSION,
	}); err != nil {
		return "", 0, nil, fmt.Errorf("%w: hand off MISSION_COUNT: %v", errMissionOutcomeUnknown, err)
	}
	uploaded := uint32(0)
	uploadedSequences := make([]bool, len(plan.Items))
	var ackType *uint32
	deadline := time.NewTimer(a.aircraftCommandTimeout())
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return "", uploaded, ackType, fmt.Errorf("%w: %v", errMissionOutcomeUnknown, ctx.Err())
		case <-deadline.C:
			return "", uploaded, ackType, fmt.Errorf("%w: upload timed out", errMissionOutcomeUnknown)
		case event := <-events:
			switch request := event.(type) {
			case *common.MessageMissionRequestInt:
				if request.MissionType != common.MAV_MISSION_TYPE_MISSION || int(request.Seq) > len(plan.Items) {
					continue
				}
				if err := a.ensureMissionUploadSafe(target); err != nil {
					return "", uploaded, ackType, fmt.Errorf("%w: %v", errMissionOutcomeUnknown, err)
				}
				item := home
				if request.Seq > 0 {
					item = plan.Items[request.Seq-1]
				}
				if err := a.writeMAVLinkMessage(target.channel, missionItemINT(target, item, request.Seq)); err != nil {
					return "", uploaded, ackType, fmt.Errorf("%w: hand off MISSION_ITEM_INT: %v", errMissionOutcomeUnknown, err)
				}
				if request.Seq > 0 && !uploadedSequences[request.Seq-1] {
					uploadedSequences[request.Seq-1] = true
					uploaded++
				}
			case *common.MessageMissionRequest:
				if request.MissionType != common.MAV_MISSION_TYPE_MISSION || int(request.Seq) > len(plan.Items) {
					continue
				}
				if err := a.ensureMissionUploadSafe(target); err != nil {
					return "", uploaded, ackType, fmt.Errorf("%w: %v", errMissionOutcomeUnknown, err)
				}
				item := home
				if request.Seq > 0 {
					item = plan.Items[request.Seq-1]
				}
				legacyItem, err := missionItemLegacy(target, item, request.Seq)
				if err != nil {
					return "", uploaded, ackType, fmt.Errorf("%w: %v", errMissionOutcomeUnknown, err)
				}
				if err := a.writeMAVLinkMessage(target.channel, legacyItem); err != nil {
					return "", uploaded, ackType, fmt.Errorf("%w: hand off legacy MISSION_ITEM: %v", errMissionOutcomeUnknown, err)
				}
				if request.Seq > 0 && !uploadedSequences[request.Seq-1] {
					uploadedSequences[request.Seq-1] = true
					uploaded++
				}
			case *common.MessageMissionAck:
				if request.MissionType != common.MAV_MISSION_TYPE_MISSION {
					continue
				}
				value := uint32(request.Type)
				ackType = &value
				if request.Type != common.MAV_MISSION_ACCEPTED {
					return "", uploaded, ackType, fmt.Errorf("autopilot rejected mission upload: %s", request.Type)
				}
				digest, err := a.readbackMAVLinkMission(ctx, target, events)
				if err != nil {
					return digest, uploaded, ackType, fmt.Errorf("%w: post-upload readback: %v", errMissionOutcomeUnknown, err)
				}
				return digest, uploaded, ackType, nil
			}
		}
	}
}

func (a *Agent) ensureMissionUploadSafe(expected *mavlinkTarget) error {
	a.mavlinkMu.Lock()
	defer a.mavlinkMu.Unlock()
	current := a.mavlinkTarget
	now := time.Now()
	if current == nil || current.channel != expected.channel || current.systemID != expected.systemID || current.componentID != expected.componentID ||
		current.heartbeatAt.IsZero() || now.Sub(current.heartbeatAt) > missionEvidenceTTL || current.armed ||
		current.landedState != common.MAV_LANDED_STATE_ON_GROUND || current.landedStateAt.IsZero() || now.Sub(current.landedStateAt) > missionEvidenceTTL {
		return errors.New("fresh MAVLink evidence no longer shows the selected aircraft disarmed and on ground")
	}
	return nil
}

func (a *Agent) readbackMAVLinkMission(ctx context.Context, target *mavlinkTarget, events <-chan message.Message) (string, error) {
	wireItems, err := a.readbackMAVLinkWireMission(ctx, target, events)
	if err != nil {
		return "", err
	}
	if len(wireItems) == 0 {
		return "", errors.New("ArduPilot mission readback omitted HOME at wire sequence 0")
	}
	canonical := make([]*agentv1.MissionItem, 0, len(wireItems)-1)
	for sequence, item := range wireItems[1:] {
		item.Sequence = uint32(sequence)
		item.Current = false
		canonical = append(canonical, item)
	}
	return digestMissionPlan(&agentv1.MissionPlan{SchemaVersion: missionSchemaVersion, Items: canonical})
}

func (a *Agent) readbackMAVLinkWireMission(ctx context.Context, target *mavlinkTarget, events <-chan message.Message) ([]*agentv1.MissionItem, error) {
	if err := a.writeMAVLinkMessage(target.channel, &common.MessageMissionRequestList{
		TargetSystem: target.systemID, TargetComponent: target.componentID, MissionType: common.MAV_MISSION_TYPE_MISSION,
	}); err != nil {
		return nil, err
	}
	timeout := time.NewTimer(a.aircraftCommandTimeout())
	defer timeout.Stop()
	count := -1
	var nextSequence uint16
	items := make([]*agentv1.MissionItem, 0)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout.C:
			return nil, errors.New("mission readback timed out")
		case event := <-events:
			switch value := event.(type) {
			case *common.MessageMissionCount:
				if value.MissionType != common.MAV_MISSION_TYPE_MISSION || value.Count > maxWireMissionItems {
					continue
				}
				count = int(value.Count)
				items = make([]*agentv1.MissionItem, count)
				if count == 0 {
					return items, nil
				}
				if err := a.requestMissionItem(target, 0); err != nil {
					return nil, err
				}
			case *common.MessageMissionItemInt:
				if count < 0 || value.MissionType != common.MAV_MISSION_TYPE_MISSION || int(value.Seq) >= count || value.Seq != nextSequence || items[value.Seq] != nil {
					continue
				}
				items[value.Seq] = protoMissionItem(value)
				nextSequence++
				if int(value.Seq)+1 < count {
					if err := a.requestMissionItem(target, value.Seq+1); err != nil {
						return nil, err
					}
					continue
				}
				if err := a.writeMAVLinkMessage(target.channel, &common.MessageMissionAck{TargetSystem: target.systemID, TargetComponent: target.componentID, Type: common.MAV_MISSION_ACCEPTED, MissionType: common.MAV_MISSION_TYPE_MISSION}); err != nil {
					return nil, err
				}
				return items, nil
			}
		}
	}
}

func (a *Agent) requestMissionItem(target *mavlinkTarget, sequence uint16) error {
	return a.writeMAVLinkMessage(target.channel, &common.MessageMissionRequestInt{
		TargetSystem: target.systemID, TargetComponent: target.componentID, Seq: sequence, MissionType: common.MAV_MISSION_TYPE_MISSION,
	})
}

func missionItemINT(target *mavlinkTarget, item *agentv1.MissionItem, wireSequence uint16) *common.MessageMissionItemInt {
	return &common.MessageMissionItemInt{TargetSystem: target.systemID, TargetComponent: target.componentID,
		Seq: wireSequence, Frame: common.MAV_FRAME(item.Frame), Command: common.MAV_CMD(item.Command),
		Current: 0, Autocontinue: boolByte(item.Autocontinue), Param1: float32(item.Param1),
		Param2: float32(item.Param2), Param3: float32(item.Param3), Param4: float32(item.Param4),
		X: item.LatitudeE7, Y: item.LongitudeE7, Z: float32(item.AltitudeM), MissionType: common.MAV_MISSION_TYPE_MISSION}
}

func missionItemLegacy(target *mavlinkTarget, item *agentv1.MissionItem, wireSequence uint16) (*common.MessageMissionItem, error) {
	latitude := float32(item.LatitudeE7) / float32(1e7)
	longitude := float32(item.LongitudeE7) / float32(1e7)
	if int32(latitude*float32(1e7)) != item.LatitudeE7 || int32(longitude*float32(1e7)) != item.LongitudeE7 {
		return nil, errors.New("legacy MISSION_ITEM request cannot preserve canonical coordinates; MISSION_REQUEST_INT is required")
	}
	return &common.MessageMissionItem{TargetSystem: target.systemID, TargetComponent: target.componentID,
		Seq: wireSequence, Frame: common.MAV_FRAME(item.Frame), Command: common.MAV_CMD(item.Command),
		Current: 0, Autocontinue: boolByte(item.Autocontinue), Param1: float32(item.Param1),
		Param2: float32(item.Param2), Param3: float32(item.Param3), Param4: float32(item.Param4),
		X: latitude, Y: longitude, Z: float32(item.AltitudeM), MissionType: common.MAV_MISSION_TYPE_MISSION}, nil
}

func legacyCoordinateRoundTrips(coordinateE7 int32) bool {
	degrees := float32(coordinateE7) / float32(1e7)
	return int32(degrees*float32(1e7)) == coordinateE7
}

func protoMissionItem(item *common.MessageMissionItemInt) *agentv1.MissionItem {
	return &agentv1.MissionItem{Sequence: uint32(item.Seq), Frame: uint32(item.Frame), Command: uint32(item.Command),
		Current: false, Autocontinue: item.Autocontinue != 0, Param1: float64(item.Param1), Param2: float64(item.Param2),
		Param3: float64(item.Param3), Param4: float64(item.Param4), LatitudeE7: item.X, LongitudeE7: item.Y, AltitudeM: float64(item.Z)}
}

func digestMissionPlan(plan *agentv1.MissionPlan) (string, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func boolByte(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}
