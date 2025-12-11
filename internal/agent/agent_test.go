package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"google.golang.org/grpc"
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
	a.streamLoopFn = func(ctx context.Context, _ grpc.BidiStreamingClient[agentv1.TelemetryFrame, agentv1.TelemetryAck]) error {
		t.Fatalf("streamLoopFn should not be called on dial failure")
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


