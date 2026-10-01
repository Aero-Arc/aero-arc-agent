//go:build sitl

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. See https://mozilla.org/MPL/2.0/.
package agent

import (
	"context"
	"fmt"
	"github.com/aero-arc/aero-arc-protos/commanddigest"
	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// This opt-in test owns its simulator process and temporary storage. It never
// connects to a user-supplied aircraft endpoint or the running observer demo.
func TestSITLOnboardRTLProducesDurableCompletion(t *testing.T)  { testSITLRecoveryCompletion(t, 20) }
func TestSITLOnboardLandProducesDurableCompletion(t *testing.T) { testSITLRecoveryCompletion(t, 21) }

func testSITLRecoveryCompletion(t *testing.T, ending uint32) {
	binary := os.Getenv("AERO_AGENT_TEST_SITL_BINARY")
	if binary == "" {
		t.Skip("set AERO_AGENT_TEST_SITL_BINARY to the local arducopter simulator")
	}
	if filepath.Base(binary) != "arducopter" {
		t.Fatal("expected local arducopter simulator binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	defaults := filepath.Join(dir, "defaults.parm")
	if err = os.WriteFile(defaults, []byte("FRAME_CLASS 1\nFRAME_TYPE 1\nAUTO_OPTIONS 3\nRTL_ALT_FINAL 0\nRTL_ALT 1000\nDISARM_DELAY 30\nSIM_SPEEDUP 5\n"), 0600); err != nil {
		t.Fatal(err)
	}
	logfile, err := os.Create(filepath.Join(dir, "sitl.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logfile.Close() }()
	copterDefaults := filepath.Clean(filepath.Join(filepath.Dir(binary), "../../../Tools/autotest/default_params/copter.parm"))
	if _, err = os.Stat(copterDefaults); err != nil {
		t.Fatalf("standard SITL defaults: %v", err)
	}
	process := exec.CommandContext(ctx, binary, "--model", "quad", "--home", "35,-97,100,0", "--speedup", "5", "--instance", strconv.Itoa(port/10), "--serial0", fmt.Sprintf("tcp:%d", port), "--serial1", "none", "--serial2", "none", "--defaults", copterDefaults+","+defaults)
	process.Dir = dir
	process.Stdout = logfile
	process.Stderr = logfile
	if err = process.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = process.Wait()
		if t.Failed() {
			raw, _ := os.ReadFile(logfile.Name())
			t.Logf("SITL: %s", raw)
		}
	}()
	poll := func(label string, condition func() bool) {
		t.Helper()
		delay := 50 * time.Millisecond
		for !condition() {
			select {
			case <-ctx.Done():
				t.Fatalf("%s: %v", label, ctx.Err())
			case <-time.After(delay):
			}
			delay = min(delay*2, time.Second)
		}
	}
	address := fmt.Sprintf("127.0.0.1:%d", port)
	poll("simulator listening", func() bool {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	})
	node := &gomavlib.Node{Endpoints: []gomavlib.EndpointConf{gomavlib.EndpointTCPClient{Address: address}}, OutVersion: gomavlib.V2, OutSystemID: mavlinkSourceSystemID, OutComponentID: mavlinkSourceComponentID, Dialect: common.Dialect}
	if err = node.Initialize(); err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	w, err := wal.New(ctx, filepath.Join(dir, "agent.db"), 100, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	a := &Agent{wal: w, operationContext: &wal.OperationContext{AircraftID: "aircraft-1", FlightID: "flight-1", IntentID: "intent-1", IntentVersion: 1}}
	a.mavlinkTarget = nil
	a.options = &AgentOptions{AircraftCommandTimeout: 5 * time.Second, MissionProtocolQuietPeriod: time.Second}
	a.deployMAVLinkMission = a.executeMAVLinkMissionDeployment
	a.writeMAVLinkMessage = func(ch *gomavlib.Channel, m message.Message) error { return node.WriteMessageTo(ch, m) }
	a.writeMAVLinkCommand = func(ch *gomavlib.Channel, m *common.MessageCommandLong) error { return node.WriteMessageTo(ch, m) }
	eventCtx, stopEvents := context.WithCancel(ctx)
	eventsDone := make(chan struct{})
	forwarded := make(chan gomavlib.Event)
	go func() {
		defer close(forwarded)
		for {
			select {
			case <-eventCtx.Done():
				return
			case e, ok := <-node.Events():
				if !ok {
					return
				}
				if f, ok := e.(*gomavlib.EventFrame); ok {
					if status, ok := f.Message().(*common.MessageStatustext); ok {
						t.Logf("autopilot: %s", status.Text)
					}
				}
				select {
				case forwarded <- e:
				case <-eventCtx.Done():
					return
				}
			}
		}
	}()
	go func() { defer close(eventsDone); _ = a.runMAVLinkEvents(eventCtx, forwarded) }()
	defer func() { stopEvents(); <-eventsDone }()
	poll("authoritative heartbeat", func() bool { a.mavlinkMu.Lock(); defer a.mavlinkMu.Unlock(); return a.mavlinkTarget != nil })
	// Stream EXTENDED_SYS_STATE independently from heartbeat, as in the real demo.
	if err = node.WriteMessageAll(&common.MessageRequestDataStream{TargetSystem: 1, TargetComponent: 1, ReqStreamId: uint8(common.MAV_DATA_STREAM_ALL), ReqMessageRate: 4, StartStop: 1}); err != nil {
		t.Fatal(err)
	}
	command := validMissionCommand(t, "sitl-upload")
	command.Plan.Items = []*pb.MissionItem{{Sequence: 0, Command: 22, Autocontinue: true, LatitudeE7: 350000000, LongitudeE7: -970000000, AltitudeM: 110}, {Sequence: 1, Command: 16, Autocontinue: true, LatitudeE7: 350001024, LongitudeE7: -970000000, AltitudeM: 110}, {Sequence: 2, Command: 20, Autocontinue: true}}
	if ending == 21 {
		command.Plan.Items[2] = &pb.MissionItem{Sequence: 2, Command: 21, Param4: 1, Autocontinue: true, LatitudeE7: 350001024, LongitudeE7: -970000000, AltitudeM: 100}
	}
	setMissionDigest(t, command)
	var result *pb.MissionDeploymentResult
	poll("mission upload readiness", func() bool {
		result = a.executeMissionDeployment(ctx, command)
		if result.GetStatus() == pb.MissionDeploymentResult_STATUS_TEMPORARY_ERROR {
			t.Logf("upload readiness: %s", result.Message)
			return false
		}
		return true
	})
	if result.GetStatus() != pb.MissionDeploymentResult_STATUS_APPLIED && result.GetStatus() != pb.MissionDeploymentResult_STATUS_ALREADY_APPLIED {
		t.Fatalf("upload: %v", result)
	}
	issue := func(c *pb.DurableCommand) *pb.CommandEvidence {
		t.Helper()
		c.Context = &pb.OperationContext{AircraftId: "aircraft-1", FlightId: "flight-1", IntentId: "intent-1", IntentVersion: 1}
		c.AircraftId = "aircraft-1"
		c.CommandDigest, err = commanddigest.Digest(c)
		if err != nil {
			t.Fatal(err)
		}
		e, err := a.executeDurableCommand(ctx, c, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %v", c.Definition, e)
		return e
	}
	// Readiness can take several seconds; rejected arming has no effect and a new
	// request identity is used for each bounded attempt.
	attempts := 0
	poll("normal arming checks", func() bool {
		attempts++
		c := testC2Command(t)
		c.CommandId = fmt.Sprintf("sitl-arm-%d", attempts)
		e := issue(c)
		if hasStage(e, "outcome_unknown") {
			t.Fatalf("arming uncertain; refusing another effect: %v", e)
		}
		return hasStage(e, "applied")
	})
	start := testC2Command(t)
	start.CommandId = "sitl-start"
	start.Definition = "MISSION_START"
	m := start.GetMavlink()
	m.Command = 300
	m.Parameters = []float32{0, 0, 0, 0, 0, 0, 0}
	m.Observation = "mission_running"
	m.MissionPrecondition = command.Plan
	m.MissionPreconditionId = command.Binding.MissionId
	m.MissionPreconditionVersion = 1
	if e := issue(start); !hasStage(e, "applied") {
		t.Fatalf("mission start not applied: %v", e)
	}
	poll("durable completion", func() bool {
		events, err := a.wal.PendingFlightCompletions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) == 0 {
			return false
		}
		if events[0].Outcome != "mission_completed" {
			t.Fatalf("completion: %v", events[0])
		}
		t.Logf("completion: %v", events[0])
		return true
	})
}
