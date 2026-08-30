package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/frame"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestMAVLinkControlEvidenceBypassesBlockedTelemetryPersistence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	channel := &gomavlib.Channel{}
	pending := &pendingMAVLinkCommand{
		channel: channel, systemID: 1, componentID: 1,
		command:         common.MAV_CMD_COMPONENT_ARM_DISARM,
		enqueueComplete: true,
		acks:            make(chan mavlinkCommandAck, 1),
	}
	persistStarted := make(chan struct{})
	persistRelease := make(chan struct{})
	a := &Agent{
		options:               &AgentOptions{EventQueueSize: 1},
		pendingMAVLinkCommand: pending,
		aircraftAckAmbiguous:  false,
		appendTelemetryFrame: func(ctx context.Context, _ *agentv1.TelemetryFrame) error {
			select {
			case <-persistStarted:
			default:
				close(persistStarted)
			}
			select {
			case <-persistRelease:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	events := make(chan gomavlib.Event, 4)
	loopDone := make(chan error, 1)
	go func() { loopDone <- a.runMAVLinkEvents(ctx, events) }()

	heartbeat := func() gomavlib.Event {
		return &gomavlib.EventFrame{Channel: channel, Frame: &frame.V2Frame{
			SystemID: 1, ComponentID: 1,
			Message: &common.MessageHeartbeat{Type: common.MAV_TYPE_QUADROTOR},
		}}
	}
	events <- heartbeat()
	select {
	case <-persistStarted:
	case <-time.After(time.Second):
		t.Fatal("telemetry persistence did not block")
	}
	// Fill the bounded persistence queue and force an observable overload.
	events <- heartbeat()
	events <- heartbeat()
	events <- &gomavlib.EventFrame{Channel: channel, Frame: &frame.V2Frame{
		SystemID: 1, ComponentID: 1,
		Message: &common.MessageCommandAck{
			Command: common.MAV_CMD_COMPONENT_ARM_DISARM,
			Result:  common.MAV_RESULT_ACCEPTED,
		},
	}}

	select {
	case ack := <-pending.acks:
		if ack.result != common.MAV_RESULT_ACCEPTED {
			t.Fatalf("ACK result = %v", ack.result)
		}
	case <-time.After(time.Second):
		t.Fatal("COMMAND_ACK was blocked behind telemetry persistence")
	}
	if a.telemetryDropCount.Load() == 0 {
		t.Fatal("bounded persistence overload was not accounted")
	}
	close(persistRelease)
	cancel()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("MAVLink event loop did not stop")
	}
}

func TestMAVLinkShutdownDrainsPreWALTelemetryQueue(t *testing.T) {
	persistStarted := make(chan struct{})
	persistRelease := make(chan struct{})
	persisted := 0
	a := &Agent{
		options: &AgentOptions{EventQueueSize: 2},
		appendTelemetryFrame: func(ctx context.Context, _ *agentv1.TelemetryFrame) error {
			if persisted == 0 {
				select {
				case <-persistStarted:
				default:
					close(persistStarted)
				}
			}
			select {
			case <-persistRelease:
				persisted++
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	events := make(chan gomavlib.Event, 2)
	loopDone := make(chan error, 1)
	go func() { loopDone <- a.runMAVLinkEvents(context.Background(), events) }()
	heartbeat := func() gomavlib.Event {
		return &gomavlib.EventFrame{Frame: &frame.V2Frame{
			SystemID: 1, ComponentID: 1,
			Message: &common.MessageHeartbeat{Type: common.MAV_TYPE_QUADROTOR},
		}}
	}
	events <- heartbeat()
	select {
	case <-persistStarted:
	case <-time.After(time.Second):
		t.Fatal("telemetry persistence did not start")
	}
	events <- heartbeat()
	close(events)
	select {
	case err := <-loopDone:
		t.Fatalf("event loop returned before draining telemetry: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(persistRelease)
	select {
	case err := <-loopDone:
		if err != nil {
			t.Fatalf("event loop error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("event loop did not finish after persistence resumed")
	}
	if persisted != 2 {
		t.Fatalf("persisted frames = %d, want 2", persisted)
	}
	if dropped := a.telemetryDropCount.Load(); dropped != 0 {
		t.Fatalf("graceful drain dropped %d frames", dropped)
	}
}

func TestMAVLinkShutdownAccountsForExpiredPreWALDrain(t *testing.T) {
	persistStarted := make(chan struct{})
	a := &Agent{
		options:               &AgentOptions{EventQueueSize: 2},
		telemetryDrainTimeout: 10 * time.Millisecond,
		appendTelemetryFrame: func(ctx context.Context, _ *agentv1.TelemetryFrame) error {
			select {
			case <-persistStarted:
			default:
				close(persistStarted)
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}
	events := make(chan gomavlib.Event, 2)
	loopDone := make(chan error, 1)
	go func() { loopDone <- a.runMAVLinkEvents(context.Background(), events) }()
	heartbeat := func() gomavlib.Event {
		return &gomavlib.EventFrame{Frame: &frame.V2Frame{
			SystemID: 1, ComponentID: 1,
			Message: &common.MessageHeartbeat{Type: common.MAV_TYPE_QUADROTOR},
		}}
	}
	events <- heartbeat()
	select {
	case <-persistStarted:
	case <-time.After(time.Second):
		t.Fatal("telemetry persistence did not start")
	}
	events <- heartbeat()
	close(events)
	select {
	case err := <-loopDone:
		if err != nil {
			t.Fatalf("event loop error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expired persistence drain did not stop")
	}
	if dropped := a.telemetryDropCount.Load(); dropped != 2 {
		t.Fatalf("expired drain accounted %d dropped frames, want 2", dropped)
	}
}

func TestNextBackoff(t *testing.T) {
	tests := []struct {
		current, max time.Duration
		want         time.Duration
	}{
		{current: time.Second, max: 10 * time.Second, want: 2 * time.Second},
		{current: 8 * time.Second, max: 10 * time.Second, want: 10 * time.Second},
		{current: 20 * time.Second, max: 10 * time.Second, want: 10 * time.Second},
	}

	for _, tc := range tests {
		if got := nextBackoff(tc.current, tc.max); got != tc.want {
			t.Fatalf("nextBackoff(%v, %v) = %v, want %v", tc.current, tc.max, got, tc.want)
		}
	}
}

func TestAgentShutdownPassesDeadlineToWALClose(t *testing.T) {
	a := &Agent{}
	closeCalled := make(chan struct{})
	a.closeWALFn = func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("WAL close context has no deadline")
		}
		close(closeCalled)
		<-ctx.Done()
		return ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := a.shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown() error = %v, want deadline exceeded", err)
	}
	select {
	case <-closeCalled:
	default:
		t.Fatal("shutdown did not invoke context-aware WAL close")
	}
	if time.Since(started) > time.Second {
		t.Fatalf("shutdown ignored WAL deadline for %v", time.Since(started))
	}
}

func TestAgentShutdownFlushesWALBeforeMAVLinkDeadline(t *testing.T) {
	a := &Agent{}
	walClosed := make(chan struct{})
	a.closeWALFn = func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			t.Fatalf("WAL close started without reserved shutdown time: %v", err)
		}
		close(walClosed)
		return nil
	}
	a.closeMAVLinkFn = func(ctx context.Context) {
		select {
		case <-walClosed:
		case <-ctx.Done():
			t.Error("MAVLink close started before WAL close")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAgentWALLifecycleOutlivesRunContextForTelemetryDrain(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	a := &Agent{options: &AgentOptions{
		WALPath: filepath.Join(t.TempDir(), "agent.db"), WALBatchSize: 1,
		WALFlushTimeout: time.Millisecond,
	}}
	if err := a.initializeWAL(runCtx); err != nil {
		t.Fatal(err)
	}
	cancelRun()
	// A WAL constructed with runCtx would begin closing asynchronously here.
	// The production Agent owns an independent lifecycle until explicit shutdown.
	time.Sleep(20 * time.Millisecond)
	if err := a.persistTelemetryFrame(context.Background(), &agentv1.TelemetryFrame{
		RawMavlink: []byte("drained-after-run-cancel"),
	}); err != nil {
		t.Fatalf("persist after run cancellation: %v", err)
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	if err := a.shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
}

func TestRunWithReconnect_DialFailureHonorsContextAndBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &Agent{
		options:        &AgentOptions{RelayTarget: "test:1234"},
		backoffInitial: 5 * time.Millisecond,
		backoffMax:     10 * time.Millisecond,
	}

	var dialCalls, sleepCalls int

	a.dialFn = func(ctx context.Context) (*grpc.ClientConn, error) {
		dialCalls++
		return nil, errors.New("dial failed")
	}
	a.registerFn = func(ctx context.Context) error {
		t.Fatalf("register should not be called on dial failure")
		return nil
	}
	a.openStreamFn = func(ctx context.Context) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error) {
		t.Fatalf("openStreamFn should not be called on dial failure")
		return nil, nil
	}
	a.ackLoopFn = func(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error {
		t.Fatalf("ackLoopFn should not be called on dial failure")
		return nil
	}
	a.sleepWithBack = func(c context.Context, d time.Duration) bool {
		sleepCalls++
		// Simulate shutdown after first backoff.
		cancel()
		return false
	}

	err := a.runWithReconnect(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if dialCalls == 0 {
		t.Fatalf("expected at least one dial attempt")
	}
	if sleepCalls != 1 {
		t.Fatalf("expected exactly one sleep/backoff, got %d", sleepCalls)
	}
}

// Mock Stream Implementation
type mockStream struct {
	grpc.ClientStream
	recvFunc func() (*agentv1.RelayStreamMessage, error)
	sendFunc func(*agentv1.AgentStreamMessage) error
}

func (m *mockStream) Recv() (*agentv1.RelayStreamMessage, error) {
	if m.recvFunc != nil {
		return m.recvFunc()
	}
	return nil, io.EOF
}

func (m *mockStream) Send(f *agentv1.AgentStreamMessage) error {
	if m.sendFunc != nil {
		return m.sendFunc(f)
	}
	return nil
}

func (m *mockStream) CloseSend() error { return nil }

// Stub implementations for grpc.ClientStream
func (m *mockStream) Header() (metadata.MD, error)  { return nil, nil }
func (m *mockStream) Trailer() metadata.MD          { return nil }
func (m *mockStream) Context() context.Context      { return context.Background() }
func (m *mockStream) SendMsg(msg interface{}) error { return nil }
func (m *mockStream) RecvMsg(msg interface{}) error { return nil }

func TestRunWithReconnect_StreamFailureTriggersReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_agent.db")
	w, err := wal.New(context.Background(), dbPath, 0, 0)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	a := &Agent{
		options:        &AgentOptions{RelayTarget: "test:1234"},
		backoffInitial: time.Millisecond,
		backoffMax:     10 * time.Millisecond,
		wal:            w,
	}

	dialCount := 0
	registerCount := 0
	streamOpenCount := 0
	sleepCount := 0
	sleepCountAtReconnect := 0

	// We want to simulate:
	// 1. Successful connection
	// 2. Successful stream open
	// 3. Loop runs, then fails
	// 4. Reconnect attempts (dial called again)
	// 5. Shutdown

	// Channel to signal when we have reconnected so we can cancel
	reconnected := make(chan struct{})

	a.dialFn = func(ctx context.Context) (*grpc.ClientConn, error) {
		dialCount++
		if dialCount > 1 {
			sleepCountAtReconnect = sleepCount
			// Signal that we attempted a reconnect
			select {
			case reconnected <- struct{}{}:
			default:
			}
			// Just return error or hang to avoid spinning
			return nil, errors.New("simulated dial fail on reconnect")
		}
		return grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	a.registerFn = func(ctx context.Context) error {
		registerCount++
		return nil
	}

	a.openStreamFn = func(ctx context.Context) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error) {
		streamOpenCount++
		return &mockStream{
			recvFunc: func() (*agentv1.RelayStreamMessage, error) {
				// Block slightly then return error to simulate disconnect
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(10 * time.Millisecond):
					return nil, errors.New("simulated stream error")
				}
			},
		}, nil
	}

	a.ackLoopFn = func(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error {
		// Just call Recv until error
		for {
			_, err := stream.Recv()
			if err != nil {
				return err
			}
		}
	}

	a.sleepWithBack = func(c context.Context, d time.Duration) bool {
		// Don't actually sleep in test, just check context
		sleepCount++
		return c.Err() == nil
	}

	// Run in background
	errCh := make(chan error)
	go func() {
		errCh <- a.runWithReconnect(ctx)
	}()

	// Wait for reconnect signal
	select {
	case <-reconnected:
		// Success: it tried to reconnect
		cancel() // Stop the loop
	case <-time.After(2 * time.Second): // Increased timeout
		t.Fatal("timed out waiting for reconnect attempt")
	}

	// Wait for runWithReconnect to exit
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context canceled, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for runWithReconnect to exit")
	}

	if dialCount < 2 {
		t.Errorf("expected at least 2 dial attempts (initial + reconnect), got %d", dialCount)
	}
	if sleepCountAtReconnect == 0 {
		t.Error("stream failure reconnected without backoff")
	}
}

func TestRunWithReconnectRequeuesUnacknowledgedBatchPeersBeforeReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := wal.New(context.Background(), filepath.Join(t.TempDir(), "teardown-requeue.db"), 3, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	ids := make([]uint64, 3)
	for index := range ids {
		id, err := w.Append(context.Background(), &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: int64(index + 1)})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = uint64(id)
	}

	a := &Agent{
		options:        &AgentOptions{RelayTarget: "test:1234", WALBatchSize: 3},
		backoffInitial: time.Millisecond,
		backoffMax:     time.Millisecond,
		wal:            w,
	}
	dialCount := 0
	a.dialFn = func(context.Context) (*grpc.ClientConn, error) {
		dialCount++
		if dialCount > 1 {
			cancel()
			return nil, errors.New("stop after observing reconnect")
		}
		return grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	a.registerFn = func(context.Context) error { return nil }
	allSent := make(chan struct{})
	sendCount := 0
	stream := &mockStream{sendFunc: func(message *agentv1.AgentStreamMessage) error {
		if message.GetTelemetryFrame() == nil {
			t.Fatalf("unexpected non-telemetry send: %+v", message)
		}
		sendCount++
		if sendCount == len(ids) {
			close(allSent)
		}
		return nil
	}}
	a.openStreamFn = func(context.Context) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error) {
		return stream, nil
	}
	a.ackLoopFn = func(ackCtx context.Context, _ grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error {
		select {
		case <-allSent:
		case <-ackCtx.Done():
			return ackCtx.Err()
		}
		return a.handleTelemetryAck(ackCtx, &agentv1.TelemetryAck{
			Seq: ids[1], Status: agentv1.TelemetryAck_STATUS_PERMANENT_ERROR, Error: "relay rejected peer",
		})
	}
	a.sleepWithBack = func(context.Context, time.Duration) bool { return true }

	if err := a.runWithReconnect(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("runWithReconnect() error = %v, want cancellation", err)
	}
	entries, err := w.ReadUndelivered(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || uint64(entries[0].ID) != ids[0] || uint64(entries[1].ID) != ids[2] {
		t.Fatalf("retry queue after reconnect = %#v, want peers %v and %v", entries, ids[0], ids[2])
	}
	rejected, err := w.ApplyTelemetryAck(context.Background(), ids[1], "", wal.TelemetryAckPermanentReject, "relay rejected peer")
	if err != nil || rejected.Changed || rejected.PreviousStatus != wal.DeliveryStatusQuarantined {
		t.Fatalf("rejected peer replay = %#v, %v", rejected, err)
	}
}

func TestRunWithReconnectRemainsSupervisedAfterWorkerTeardownTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := wal.New(context.Background(), filepath.Join(t.TempDir(), "teardown-timeout.db"), 1, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	if _, err := w.Append(context.Background(), &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 1}); err != nil {
		t.Fatal(err)
	}

	a := &Agent{
		options:        &AgentOptions{RelayTarget: "test:1234", WALBatchSize: 1},
		backoffInitial: time.Millisecond,
		backoffMax:     time.Millisecond,
		wal:            w,
	}
	reconnected := make(chan struct{})
	dialCount := 0
	a.dialFn = func(context.Context) (*grpc.ClientConn, error) {
		dialCount++
		if dialCount > 1 {
			close(reconnected)
			cancel()
			return nil, errors.New("stop after supervised reconnect")
		}
		return grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	a.registerFn = func(context.Context) error { return nil }
	a.openStreamFn = func(context.Context) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error) {
		return &mockStream{sendFunc: func(*agentv1.AgentStreamMessage) error {
			return errors.New("end sender while ACK worker is stuck")
		}}, nil
	}
	releaseACKWorker := make(chan struct{})
	a.ackLoopFn = func(context.Context, grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error {
		<-releaseACKWorker // Deliberately ignore stream cancellation past the teardown deadline.
		return errors.New("released stale ACK worker")
	}
	a.sleepWithBack = func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil }

	errC := make(chan error, 1)
	go func() { errC <- a.runWithReconnect(ctx) }()
	select {
	case <-reconnected:
		close(releaseACKWorker)
	case <-time.After(3 * time.Second):
		close(releaseACKWorker)
		t.Fatal("worker teardown timeout ended reconnect supervision")
	}
	select {
	case err := <-errC:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runWithReconnect() error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runWithReconnect did not stop after supervised reconnect cancellation")
	}
}

func TestTelemetryBatchReservesPerSendAndCleanupPreservesActiveOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w, err := wal.New(context.Background(), filepath.Join(t.TempDir(), "per-send-pending.db"), 2, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = w.Close()
	})
	ids := make([]uint64, 2)
	for index := range ids {
		id, err := w.Append(context.Background(), &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: int64(index + 1)})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = uint64(id)
	}

	a := &Agent{options: &AgentOptions{WALBatchSize: 2}, wal: w}
	firstSendStarted := make(chan struct{})
	releaseFirstSend := make(chan struct{})
	var sendCount atomic.Int32
	stream := &mockStream{sendFunc: func(*agentv1.AgentStreamMessage) error {
		if sendCount.Add(1) == 1 {
			close(firstSendStarted)
			<-releaseFirstSend
		}
		return nil
	}}
	errC := make(chan error, 1)
	go func() { errC <- a.handleTelemetryFrames(ctx, stream) }()
	select {
	case <-firstSendStarted:
	case <-time.After(time.Second):
		t.Fatal("first telemetry Send did not start")
	}
	entries, err := w.ReadUndelivered(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || uint64(entries[0].ID) != ids[1] {
		t.Fatalf("queue while first Send is blocked = %#v, want only unsent sequence %d", entries, ids[1])
	}
	if rows, err := a.resetStuckPending(context.Background(), time.Nanosecond); err != nil || rows != 0 {
		t.Fatalf("cleanup during active Send = %d, %v", rows, err)
	}
	if err := a.handleTelemetryAck(context.Background(), &agentv1.TelemetryAck{Seq: ids[0], Status: agentv1.TelemetryAck_STATUS_OK}); err != nil {
		t.Fatalf("fast exact ACK during Send = %v", err)
	}
	close(releaseFirstSend)
	deadline := time.Now().Add(time.Second)
	for a.telemetryBatchActive.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if a.telemetryBatchActive.Load() != 0 || sendCount.Load() != 2 {
		t.Fatalf("batch did not finish: active=%d sends=%d", a.telemetryBatchActive.Load(), sendCount.Load())
	}
	if err := a.handleTelemetryAck(context.Background(), &agentv1.TelemetryAck{Seq: ids[1], Status: agentv1.TelemetryAck_STATUS_OK}); err != nil {
		t.Fatalf("second exact ACK after batch refresh = %v", err)
	}
	cancel()
	select {
	case err := <-errC:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handleTelemetryFrames() error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("telemetry sender did not stop")
	}
}

func TestHandleTelemetryAckAppliesStatusSpecificDurabilityPolicy(t *testing.T) {
	tests := []struct {
		name           string
		status         agentv1.TelemetryAck_Status
		wantError      error
		wantWritten    bool
		wantQuarantine bool
	}{
		{name: "ok is delivered", status: agentv1.TelemetryAck_STATUS_OK},
		{name: "temporary error retries", status: agentv1.TelemetryAck_STATUS_TEMPORARY_ERROR, wantError: ErrTelemetryRetry, wantWritten: true},
		{name: "backoff retries", status: agentv1.TelemetryAck_STATUS_RETRY_WITH_BACKOFF, wantError: ErrTelemetryRetry, wantWritten: true},
		{name: "permanent error quarantines", status: agentv1.TelemetryAck_STATUS_PERMANENT_ERROR, wantError: ErrTelemetryRejected, wantQuarantine: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			w, err := wal.New(ctx, filepath.Join(t.TempDir(), "ack.db"), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Close() })
			frame := &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 1234, RawMavlink: []byte("test")}
			id, err := w.Append(ctx, frame)
			if err != nil {
				t.Fatal(err)
			}
			if rows, err := w.MarkPendingBatch(ctx, []uint64{uint64(id)}); err != nil || rows != 1 {
				t.Fatalf("mark pending = %d, %v", rows, err)
			}
			a := &Agent{wal: w}
			frameID := fmt.Sprintf("%d:%s:%d:%d", len(frame.AgentId), frame.AgentId, frame.SentAtUnixNs, id)
			err = a.handleTelemetryAck(ctx, &agentv1.TelemetryAck{Seq: uint64(id), FrameId: frameID, Status: test.status, Error: "relay diagnostic"})
			if !errors.Is(err, test.wantError) {
				t.Fatalf("handleTelemetryAck() error = %v, want %v", err, test.wantError)
			}
			if test.wantError != nil {
				err = a.handleTelemetryAck(ctx, &agentv1.TelemetryAck{Seq: uint64(id), FrameId: frameID, Status: test.status, Error: "relay diagnostic"})
				if !errors.Is(err, test.wantError) {
					t.Fatalf("duplicate status %s error = %v, want %v", test.status, err, test.wantError)
				}
			}
			entries, readErr := w.ReadUndelivered(ctx, 10)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if test.wantWritten != (len(entries) == 1 && entries[0].ID == id) {
				t.Fatalf("written entries = %#v, wantWritten=%v", entries, test.wantWritten)
			}
			if test.wantQuarantine {
				if deleted, cleanupErr := w.CleanupQuarantined(ctx, 0); cleanupErr != nil || deleted != 1 {
					t.Fatalf("quarantine cleanup = %d, %v", deleted, cleanupErr)
				}
			}
		})
	}
}

func TestHandleTelemetryAckRejectsMissingMismatchedAndConflictingACKs(t *testing.T) {
	ctx := context.Background()
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "ack-correlation.db"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	first := &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 1234}
	firstID, err := w.Append(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 5678})
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := w.MarkPendingBatch(ctx, []uint64{uint64(firstID), uint64(secondID)}); err != nil || rows != 2 {
		t.Fatalf("mark pending = %d, %v", rows, err)
	}
	a := &Agent{wal: w}
	for _, ack := range []*agentv1.TelemetryAck{nil, {}, {Seq: 0, Status: agentv1.TelemetryAck_STATUS_OK}} {
		if err := a.handleTelemetryAck(ctx, ack); !errors.Is(err, ErrInvalidTelemetryAck) {
			t.Fatalf("missing ACK error = %v", err)
		}
	}
	if err := a.handleTelemetryAck(ctx, &agentv1.TelemetryAck{Seq: uint64(firstID), FrameId: "wrong", Status: agentv1.TelemetryAck_STATUS_OK}); !errors.Is(err, wal.ErrTelemetryFrameIdentityMismatch) {
		t.Fatalf("mismatched frame error = %v", err)
	}
	if err := a.handleTelemetryAck(ctx, &agentv1.TelemetryAck{Seq: uint64(secondID + 100), Status: agentv1.TelemetryAck_STATUS_OK}); !errors.Is(err, wal.ErrTelemetryFrameNotFound) {
		t.Fatalf("unknown sequence error = %v", err)
	}
	if err := a.handleTelemetryAck(ctx, &agentv1.TelemetryAck{Seq: uint64(secondID), Status: agentv1.TelemetryAck_Status(99)}); !errors.Is(err, ErrInvalidTelemetryAck) {
		t.Fatalf("unknown status error = %v", err)
	}
	correctFrameID := fmt.Sprintf("%d:%s:%d:%d", len(first.AgentId), first.AgentId, first.SentAtUnixNs, firstID)
	ok := &agentv1.TelemetryAck{Seq: uint64(firstID), FrameId: correctFrameID, Status: agentv1.TelemetryAck_STATUS_OK}
	if err := a.handleTelemetryAck(ctx, ok); err != nil {
		t.Fatal(err)
	}
	if err := a.handleTelemetryAck(ctx, ok); err != nil {
		t.Fatalf("duplicate OK ACK was not idempotent: %v", err)
	}
	if err := a.handleTelemetryAck(ctx, &agentv1.TelemetryAck{Seq: uint64(firstID), FrameId: correctFrameID, Status: agentv1.TelemetryAck_STATUS_RETRY_WITH_BACKOFF}); !errors.Is(err, wal.ErrTelemetryAckConflict) {
		t.Fatalf("contradictory late ACK error = %v", err)
	}
	if rows, err := w.MarkWrittenBatch(ctx, []uint64{uint64(firstID), uint64(secondID)}); err != nil || rows != 1 {
		t.Fatalf("pending rows after invalid ACKs = %d, %v; wrong row may have mutated", rows, err)
	}
	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil || len(entries) != 1 || entries[0].ID != secondID {
		t.Fatalf("wrong-row mutation check entries = %#v, %v", entries, err)
	}
}

// Mock Gateway
type mockGateway struct {
	agentv1.AgentGatewayClient
	registerFunc func(ctx context.Context, in *agentv1.RegisterRequest, opts ...grpc.CallOption) (*agentv1.RegisterResponse, error)
	streamFunc   func(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error)
}

func (m *mockGateway) TelemetryStream(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error) {
	if m.streamFunc != nil {
		return m.streamFunc(ctx, opts...)
	}
	return nil, errors.New("unexpected telemetry stream")
}

func (m *mockGateway) Register(ctx context.Context, in *agentv1.RegisterRequest, opts ...grpc.CallOption) (*agentv1.RegisterResponse, error) {
	if m.registerFunc != nil {
		return m.registerFunc(ctx, in, opts...)
	}
	return &agentv1.RegisterResponse{}, nil
}

func TestRegister(t *testing.T) {
	ctx := context.Background()
	mockGw := &mockGateway{
		registerFunc: func(ctx context.Context, in *agentv1.RegisterRequest, opts ...grpc.CallOption) (*agentv1.RegisterResponse, error) {
			if in.AgentId == "" {
				return nil, errors.New("empty agent id")
			}
			return &agentv1.RegisterResponse{SessionId: "session-1"}, nil
		},
	}

	a := &Agent{
		gateway: mockGw,
		options: &AgentOptions{RelayTarget: "test"},
	}

	if err := a.register(ctx); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if a.sessionID != "session-1" {
		t.Fatalf("sessionID = %q, want session-1", a.sessionID)
	}

	// Test failure case
	a.gateway = nil
	if err := a.register(ctx); err != ErrGatewayNotInitialized {
		t.Errorf("expected ErrGatewayNotInitialized, got %v", err)
	}
}

func TestOperationContextLifecycleAndFrameSnapshot(t *testing.T) {
	ctx := context.Background()
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "wal.db"), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	var sent []*agentv1.AgentStreamMessage
	stream := &mockStream{sendFunc: func(message *agentv1.AgentStreamMessage) error {
		sent = append(sent, message)
		return nil
	}}
	a := &Agent{wal: w, sessionID: "session-1"}
	set := &agentv1.SetOperationContextCommand{
		CommandId: "set-1",
		Context:   &agentv1.OperationContext{AircraftId: "aircraft-1", FlightId: "flight-1", IntentId: "intent-1", IntentVersion: 2},
	}
	if err := a.handleSetOperationContext(ctx, stream, set); err != nil {
		t.Fatal(err)
	}
	if got := sent[len(sent)-1].GetOperationContextCommandAck().GetStatus(); got != agentv1.OperationContextCommandAck_STATUS_APPLIED {
		t.Fatalf("set status = %v", got)
	}
	if got := sent[len(sent)-1].GetOperationContextCommandAck().GetActiveContext().GetAircraftId(); got != "aircraft-1" {
		t.Fatalf("active aircraft ID = %q", got)
	}

	frame := &agentv1.TelemetryFrame{}
	a.stampFrameContext(frame)
	if frame.SessionId != "" || frame.FlightId != "flight-1" || frame.IntentId != "intent-1" || frame.IntentVersion != 2 {
		t.Fatalf("stamped frame = %+v", frame)
	}
	walID, err := w.Append(ctx, frame)
	if err != nil {
		t.Fatal(err)
	}

	// Reusing a durable command ID with a different payload is rejected and
	// cannot replace the original context.
	conflict := proto.Clone(set).(*agentv1.SetOperationContextCommand)
	conflict.Context.FlightId = "incorrect-retry-value"
	if err := a.handleSetOperationContext(ctx, stream, conflict); err != nil {
		t.Fatal(err)
	}
	ack := sent[len(sent)-1].GetOperationContextCommandAck()
	if ack.GetStatus() != agentv1.OperationContextCommandAck_STATUS_REJECTED || ack.GetActiveContext().GetFlightId() != "flight-1" {
		t.Fatalf("conflicting ack = %+v", ack)
	}

	// An exact retry remains a successful no-op.
	if err := a.handleSetOperationContext(ctx, stream, set); err != nil {
		t.Fatal(err)
	}
	ack = sent[len(sent)-1].GetOperationContextCommandAck()
	if ack.GetStatus() != agentv1.OperationContextCommandAck_STATUS_ALREADY_APPLIED || ack.GetActiveContext().GetFlightId() != "flight-1" {
		t.Fatalf("idempotent ack = %+v", ack)
	}

	// Legacy empty clears and contradictory authoritative clears are rejected;
	// omission must never gain unconditional-clear semantics on the wire.
	if err := a.handleClearOperationContext(ctx, stream, &agentv1.ClearOperationContextCommand{CommandId: "legacy-empty"}); err != nil {
		t.Fatal(err)
	}
	ack = sent[len(sent)-1].GetOperationContextCommandAck()
	if ack.GetStatus() != agentv1.OperationContextCommandAck_STATUS_REJECTED || ack.GetActiveContext().GetFlightId() != "flight-1" {
		t.Fatalf("legacy empty-clear ack = %+v", ack)
	}
	if err := a.handleClearOperationContext(ctx, stream, &agentv1.ClearOperationContextCommand{
		CommandId: "invalid-authoritative", FlightId: "flight-1", Authoritative: true,
	}); err != nil {
		t.Fatal(err)
	}
	ack = sent[len(sent)-1].GetOperationContextCommandAck()
	if ack.GetStatus() != agentv1.OperationContextCommandAck_STATUS_REJECTED || ack.GetActiveContext().GetFlightId() != "flight-1" {
		t.Fatalf("contradictory authoritative-clear ack = %+v", ack)
	}

	// An authoritative empty clear supports control-plane reconciliation when
	// the API has no active flight ID to replay.
	if err := a.handleClearOperationContext(ctx, stream, &agentv1.ClearOperationContextCommand{CommandId: "clear-empty", Authoritative: true}); err != nil {
		t.Fatal(err)
	}
	ack = sent[len(sent)-1].GetOperationContextCommandAck()
	if ack.GetCommandId() != "clear-empty" || ack.GetStatus() != agentv1.OperationContextCommandAck_STATUS_APPLIED || ack.GetActiveContext() != nil {
		t.Fatalf("empty-flight clear ack = %+v", ack)
	}
	frame = &agentv1.TelemetryFrame{}
	a.stampFrameContext(frame)
	if frame.FlightId != "" || frame.IntentId != "" {
		t.Fatalf("empty-flight clear retained context: %+v", frame)
	}

	setAfterReconciliation := proto.Clone(set).(*agentv1.SetOperationContextCommand)
	setAfterReconciliation.CommandId = "set-2"
	if err := a.handleSetOperationContext(ctx, stream, setAfterReconciliation); err != nil {
		t.Fatal(err)
	}
	if err := a.handleClearOperationContext(ctx, stream, &agentv1.ClearOperationContextCommand{CommandId: "clear-empty", Authoritative: true}); err != nil {
		t.Fatal(err)
	}
	ack = sent[len(sent)-1].GetOperationContextCommandAck()
	if ack.GetStatus() != agentv1.OperationContextCommandAck_STATUS_ALREADY_APPLIED || ack.GetActiveContext().GetFlightId() != "flight-1" {
		t.Fatalf("late empty-clear retry = %+v", ack)
	}

	// A stale clear is recorded but cannot clear a newer/different flight.
	if err := a.handleClearOperationContext(ctx, stream, &agentv1.ClearOperationContextCommand{CommandId: "clear-old", FlightId: "flight-old"}); err != nil {
		t.Fatal(err)
	}
	frame = &agentv1.TelemetryFrame{}
	a.stampFrameContext(frame)
	if frame.FlightId != "flight-1" {
		t.Fatalf("stale clear changed flight to %q", frame.FlightId)
	}

	if err := a.handleClearOperationContext(ctx, stream, &agentv1.ClearOperationContextCommand{CommandId: "clear-1", FlightId: "flight-1"}); err != nil {
		t.Fatal(err)
	}
	frame = &agentv1.TelemetryFrame{}
	a.stampFrameContext(frame)
	if frame.FlightId != "" || frame.IntentId != "" {
		t.Fatalf("frame retained cleared context: %+v", frame)
	}
	if err := a.handleSetOperationContext(ctx, stream, set); err != nil {
		t.Fatal(err)
	}
	frame = &agentv1.TelemetryFrame{}
	a.stampFrameContext(frame)
	if frame.FlightId != "" {
		t.Fatalf("retry of old set resurrected cleared context: %+v", frame)
	}
	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	var stored agentv1.TelemetryFrame
	for _, entry := range entries {
		if entry.ID == walID {
			if err := proto.Unmarshal(entry.Payload, &stored); err != nil {
				t.Fatal(err)
			}
		}
	}
	if stored.FlightId != "flight-1" || stored.IntentId != "intent-1" || stored.IntentVersion != 2 {
		t.Fatalf("WAL frame context changed after clear: %+v", &stored)
	}
}

func TestSessionIsStampedAtSendTimeAcrossOfflineCaptureAndReconnect(t *testing.T) {
	a := &Agent{}

	// A frame captured offline has no session, because sessions describe relay
	// connections rather than the capture event.
	offline := &agentv1.TelemetryFrame{}
	a.stampFrameContext(offline)
	if offline.SessionId != "" {
		t.Fatalf("offline frame session = %q", offline.SessionId)
	}

	// The first successful registration supplies the session used when replaying.
	a.gateway = &mockGateway{registerFunc: func(context.Context, *agentv1.RegisterRequest, ...grpc.CallOption) (*agentv1.RegisterResponse, error) {
		return &agentv1.RegisterResponse{SessionId: "session-1"}, nil
	}}
	a.options = &AgentOptions{RelayTarget: "relay"}
	if err := a.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	a.stampCurrentSession(offline)
	if offline.SessionId != "session-1" {
		t.Fatalf("first replay session = %q", offline.SessionId)
	}

	// A WAL frame containing a prior connection's session is overwritten after
	// reconnect rather than rejected as stale by the relay.
	oldWALFrame := &agentv1.TelemetryFrame{SessionId: "session-old"}
	a.stateMu.Lock()
	a.sessionID = "session-2"
	a.stateMu.Unlock()
	a.stampCurrentSession(oldWALFrame)
	if oldWALFrame.SessionId != "session-2" {
		t.Fatalf("reconnected replay session = %q", oldWALFrame.SessionId)
	}
}

func TestDebugTransportCanExplicitlySkipCertificateVerification(t *testing.T) {
	creds, err := relayTransportCredentials(&AgentOptions{Debug: true, SkipTLSVerification: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := creds.Info().SecurityProtocol; got != "tls" {
		t.Fatalf("security protocol = %q, want TLS", got)
	}
}

func TestOpenTelemetryStreamRequiresRegisteredSession(t *testing.T) {
	a := &Agent{gateway: &mockGateway{}, sessionID: "", options: &AgentOptions{RelayTarget: "relay"}}
	if _, err := a.openTelemetryStream(context.Background()); err == nil {
		t.Fatal("openTelemetryStream() accepted an empty Relay session ID")
	}
}

func TestOpenTelemetryStreamBindsRegisteredSessionMetadata(t *testing.T) {
	var outgoing metadata.MD
	gateway := &mockGateway{streamFunc: func(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error) {
		outgoing, _ = metadata.FromOutgoingContext(ctx)
		return &mockStream{}, nil
	}}
	a := &Agent{gateway: gateway, sessionID: "session-1", options: &AgentOptions{RelayTarget: "relay"}}
	if _, err := a.openTelemetryStream(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := outgoing.Get("aero-arc-session-id"); len(got) != 1 || got[0] != "session-1" {
		t.Fatalf("session metadata = %#v", got)
	}
	if got := outgoing.Get("aero-arc-agent-id"); len(got) != 1 || got[0] == "" {
		t.Fatalf("agent metadata = %#v", got)
	}
}

func TestWALGenerationIsStableForLegacyReplay(t *testing.T) {
	ctx := context.Background()
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "wal.db"), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	a := &Agent{wal: w}

	legacy := &agentv1.TelemetryFrame{}
	a.stampWALGeneration(legacy)
	if legacy.GetWalId() != w.GenerationID() {
		t.Fatalf("legacy WAL ID = %q, want %q", legacy.GetWalId(), w.GenerationID())
	}
	original := &agentv1.TelemetryFrame{WalId: "original-generation"}
	a.stampWALGeneration(original)
	if original.GetWalId() != "original-generation" {
		t.Fatalf("persisted WAL ID changed to %q", original.GetWalId())
	}
}

func TestHandleRelayMessageDispatchesTelemetryAck(t *testing.T) {
	ctx := context.Background()
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "wal.db"), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	id, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1"})
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := w.MarkPendingBatch(ctx, []uint64{uint64(id)}); err != nil || rows != 1 {
		t.Fatalf("mark pending = %d, %v", rows, err)
	}
	a := &Agent{wal: w}
	message := &agentv1.RelayStreamMessage{Payload: &agentv1.RelayStreamMessage_TelemetryAck{TelemetryAck: &agentv1.TelemetryAck{Seq: uint64(id)}}}
	if err := a.handleRelayMessage(ctx, &mockStream{}, message); err != nil {
		t.Fatal(err)
	}
	if entries, err := w.ReadUndelivered(ctx, 10); err != nil || len(entries) != 0 {
		t.Fatalf("undelivered after ack = %d, %v", len(entries), err)
	}
}

func TestHandleTelemetryFramesMarksPendingBeforeFastACK(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := wal.New(context.Background(), filepath.Join(t.TempDir(), "fast-ack.db"), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	id, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 1234, MsgName: "heartbeat"})
	if err != nil {
		t.Fatal(err)
	}
	a := &Agent{wal: w, options: &AgentOptions{WALBatchSize: 10}}
	stream := &mockStream{sendFunc: func(message *agentv1.AgentStreamMessage) error {
		frame := message.GetTelemetryFrame()
		if frame == nil || frame.GetSeq() != uint64(id) {
			t.Fatalf("sent frame = %+v", frame)
		}
		if err := a.handleTelemetryAck(ctx, &agentv1.TelemetryAck{Seq: frame.GetSeq(), Status: agentv1.TelemetryAck_STATUS_OK}); err != nil {
			t.Fatalf("fast ACK failed before Send returned: %v", err)
		}
		cancel()
		return nil
	}}
	if err := a.handleTelemetryFrames(ctx, stream); !errors.Is(err, context.Canceled) {
		t.Fatalf("handleTelemetryFrames() error = %v, want cancellation", err)
	}
	if rows, err := w.MarkWrittenBatch(context.Background(), []uint64{uint64(id)}); err != nil || rows != 0 {
		t.Fatalf("delivered frame regressed to retry: rows=%d err=%v", rows, err)
	}
	if entries, err := w.ReadUndelivered(context.Background(), 10); err != nil || len(entries) != 0 {
		t.Fatalf("fast-ACK frame remained undelivered: %#v, %v", entries, err)
	}
}

func TestHandleTelemetryFramesSendFailureReturnsOnlyPendingRowsToRetry(t *testing.T) {
	ctx := context.Background()
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "send-failure.db"), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	id, err := w.Append(ctx, &agentv1.TelemetryFrame{AgentId: "agent-1", SentAtUnixNs: 1234, MsgName: "heartbeat"})
	if err != nil {
		t.Fatal(err)
	}
	a := &Agent{wal: w, options: &AgentOptions{WALBatchSize: 10}}
	sendErr := errors.New("stream send failed")
	stream := &mockStream{sendFunc: func(message *agentv1.AgentStreamMessage) error {
		if message.GetTelemetryFrame().GetSeq() != uint64(id) {
			t.Fatalf("sent frame = %+v", message.GetTelemetryFrame())
		}
		return sendErr
	}}
	if err := a.handleTelemetryFrames(ctx, stream); !errors.Is(err, sendErr) {
		t.Fatalf("handleTelemetryFrames() error = %v, want send failure", err)
	}
	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil || len(entries) != 1 || entries[0].ID != id {
		t.Fatalf("failed-send retry entries = %#v, %v", entries, err)
	}
}

func TestHandleRelayMessageRejectsUnsupportedPayload(t *testing.T) {
	var sent *agentv1.AgentStreamMessage
	stream := &mockStream{sendFunc: func(message *agentv1.AgentStreamMessage) error {
		sent = message
		return nil
	}}

	a := &Agent{}
	if err := a.handleRelayMessage(context.Background(), stream, &agentv1.RelayStreamMessage{}); err != nil {
		t.Fatal(err)
	}

	ack := sent.GetOperationContextCommandAck()
	if ack.GetStatus() != agentv1.OperationContextCommandAck_STATUS_REJECTED {
		t.Fatalf("unsupported payload status = %v, want %v", ack.GetStatus(), agentv1.OperationContextCommandAck_STATUS_REJECTED)
	}
	if ack.GetCommandId() != "" || ack.GetError() == "" {
		t.Fatalf("unsupported payload ack = %+v", ack)
	}
}

func TestNewAgent(t *testing.T) {
	opts := &AgentOptions{
		RelayTarget: "localhost:9090",
		WALPath:     filepath.Join(t.TempDir(), "wal.db"),
	}
	a, err := NewAgent(opts)
	if err != nil {
		t.Fatalf("NewAgent failed: %v", err)
	}
	if a == nil {
		t.Fatal("NewAgent returned nil")
	}
	if a.backoffInitial == 0 {
		t.Error("backoffInitial not set")
	}
	if !a.aircraftAckAmbiguous {
		t.Error("new Agent must fence acknowledgements buffered across process restart")
	}
}

func TestStart_ImmediateCancel(t *testing.T) {
	opts := &AgentOptions{
		RelayTarget: "localhost:9090",
		WALPath:     filepath.Join(t.TempDir(), "wal.db"),
	}
	a, err := NewAgent(opts)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Mock runMAVLink and runWithReconnect to avoid side effects
	a.dialFn = func(ctx context.Context) (*grpc.ClientConn, error) {
		return nil, context.Canceled
	}

	a.node.OutVersion = gomavlib.V2
	a.node.OutSystemID = 10
	a.node.OutComponentID = 1

	if err := a.node.Initialize(); err != nil {
		t.Fatalf("Failed to initialize node: %v", err)
	}
	shutdownErr := errors.New("forced shutdown diagnostic")
	a.closeWALFn = func(ctx context.Context) error {
		if a.wal == nil {
			return shutdownErr
		}
		return errors.Join(a.wal.CloseContext(ctx), shutdownErr)
	}

	err = a.Start(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
	if !errors.Is(err, shutdownErr) {
		t.Errorf("Start did not surface shutdown error: %v", err)
	}
}
