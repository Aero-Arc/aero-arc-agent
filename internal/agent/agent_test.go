package agent

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

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
}

func TestHandleTelemetryAck(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_ack.db")
	w, err := wal.New(context.Background(), dbPath, 0, 0)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	a := &Agent{
		wal: w,
	}

	// Add an entry to WAL
	id, err := w.Append(ctx, &agentv1.TelemetryFrame{RawMavlink: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	// Ack it
	ack := &agentv1.TelemetryAck{
		Seq: uint64(id),
	}

	if err := a.handleTelemetryAck(ctx, ack); err != nil {
		t.Fatalf("handleTelemetryAck failed: %v", err)
	}

	// Verify it is delivered
	entries, err := w.ReadUndelivered(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 undelivered entries, got %d", len(entries))
	}
}

// Mock Gateway
type mockGateway struct {
	agentv1.AgentGatewayClient
	registerFunc func(ctx context.Context, in *agentv1.RegisterRequest, opts ...grpc.CallOption) (*agentv1.RegisterResponse, error)
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
		Context:   &agentv1.OperationContext{FlightId: "flight-1", IntentId: "intent-1", IntentVersion: 2},
	}
	if err := a.handleSetOperationContext(ctx, stream, set); err != nil {
		t.Fatal(err)
	}
	if got := sent[len(sent)-1].GetOperationContextCommandAck().GetStatus(); got != agentv1.OperationContextCommandAck_STATUS_APPLIED {
		t.Fatalf("set status = %v", got)
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

	// The same durable command is acknowledged without replacing its original value.
	set.Context.FlightId = "incorrect-retry-value"
	if err := a.handleSetOperationContext(ctx, stream, set); err != nil {
		t.Fatal(err)
	}
	ack := sent[len(sent)-1].GetOperationContextCommandAck()
	if ack.GetStatus() != agentv1.OperationContextCommandAck_STATUS_ALREADY_APPLIED || ack.GetActiveContext().GetFlightId() != "flight-1" {
		t.Fatalf("duplicate ack = %+v", ack)
	}

	// A malformed clear is rejected, correlated to its command, and leaves the active context intact.
	if err := a.handleClearOperationContext(ctx, stream, &agentv1.ClearOperationContextCommand{CommandId: "clear-empty"}); err != nil {
		t.Fatal(err)
	}
	ack = sent[len(sent)-1].GetOperationContextCommandAck()
	if ack.GetCommandId() != "clear-empty" || ack.GetStatus() != agentv1.OperationContextCommandAck_STATUS_REJECTED {
		t.Fatalf("empty-flight clear ack = %+v", ack)
	}
	frame = &agentv1.TelemetryFrame{}
	a.stampFrameContext(frame)
	if frame.FlightId != "flight-1" {
		t.Fatalf("empty-flight clear changed flight to %q", frame.FlightId)
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

func TestWALGenerationIsStableForLegacyReplay(t *testing.T) {
	ctx := context.Background()
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "wal.db"), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
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
	a := &Agent{wal: w}
	message := &agentv1.RelayStreamMessage{Payload: &agentv1.RelayStreamMessage_TelemetryAck{TelemetryAck: &agentv1.TelemetryAck{Seq: uint64(id)}}}
	if err := a.handleRelayMessage(ctx, &mockStream{}, message); err != nil {
		t.Fatal(err)
	}
	if entries, err := w.ReadUndelivered(ctx, 10); err != nil || len(entries) != 0 {
		t.Fatalf("undelivered after ack = %d, %v", len(entries), err)
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

	err = a.Start(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}
