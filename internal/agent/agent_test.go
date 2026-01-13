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
	a.openStreamFn = func(ctx context.Context) (grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck], error) {
		t.Fatalf("openStreamFn should not be called on dial failure")
		return nil, nil
	}
	a.ackLoopFn = func(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck]) error {
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
	recvFunc func() (*agentv1.TelemetryAck, error)
	sendFunc func(*agentv1.TelemetryFrame) error
}

func (m *mockStream) Recv() (*agentv1.TelemetryAck, error) {
	if m.recvFunc != nil {
		return m.recvFunc()
	}
	return nil, io.EOF
}

func (m *mockStream) Send(f *agentv1.TelemetryFrame) error {
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

	a.openStreamFn = func(ctx context.Context) (grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck], error) {
		streamOpenCount++
		return &mockStream{
			recvFunc: func() (*agentv1.TelemetryAck, error) {
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

	a.ackLoopFn = func(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck]) error {
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
			return &agentv1.RegisterResponse{}, nil
		},
	}

	a := &Agent{
		gateway: mockGw,
		options: &AgentOptions{RelayTarget: "test"},
	}

	if err := a.register(ctx); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Test failure case
	a.gateway = nil
	if err := a.register(ctx); err != ErrGatewayNotInitialized {
		t.Errorf("expected ErrGatewayNotInitialized, got %v", err)
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
