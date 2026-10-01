package agent

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/protobuf/proto"
)

func TestTelemetryRetryPreservesControlStreamAndFrameIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = withTelemetryStreamWindow(withTelemetryStreamOwner(ctx, "retry-stream"), 1)
	w, err := wal.New(ctx, filepath.Join(t.TempDir(), "retry.db"), 1, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if _, err = w.Append(ctx, &pb.TelemetryFrame{AgentId: "agent", SentAtUnixNs: 123, MsgName: "heartbeat"}); err != nil {
		t.Fatal(err)
	}
	a := &Agent{wal: w, options: &AgentOptions{WALBatchSize: 1}}
	messages := make(chan *pb.RelayStreamMessage, 4)
	control := make(chan struct{}, 1)
	retried := make(chan time.Duration, 1)
	var first *pb.TelemetryFrame
	var sentAt time.Time
	stream := &mockStream{
		recvFunc: func() (*pb.RelayStreamMessage, error) {
			select {
			case m := <-messages:
				return m, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		sendFunc: func(m *pb.AgentStreamMessage) error {
			if ack := m.GetOperationContextCommandAck(); ack != nil {
				if ack.Status != pb.OperationContextCommandAck_STATUS_APPLIED {
					t.Errorf("control rejected: %v", ack)
				}
				control <- struct{}{}
				return nil
			}
			f := m.GetTelemetryFrame()
			if first == nil {
				first = proto.Clone(f).(*pb.TelemetryFrame)
				sentAt = time.Now()
				messages <- &pb.RelayStreamMessage{Payload: &pb.RelayStreamMessage_TelemetryAck{TelemetryAck: &pb.TelemetryAck{Seq: f.Seq, Status: pb.TelemetryAck_STATUS_RETRY_WITH_BACKOFF}}}
				messages <- &pb.RelayStreamMessage{Payload: &pb.RelayStreamMessage_SetOperationContext{SetOperationContext: &pb.SetOperationContextCommand{CommandId: "context", Context: &pb.OperationContext{AircraftId: "aircraft", FlightId: "flight", IntentId: "intent", IntentVersion: 1}}}}
			} else {
				if !proto.Equal(first, f) {
					t.Errorf("retry restamped frame: before=%v after=%v", first, f)
				}
				retried <- time.Since(sentAt)
				messages <- &pb.RelayStreamMessage{Payload: &pb.RelayStreamMessage_TelemetryAck{TelemetryAck: &pb.TelemetryAck{Seq: f.Seq, Status: pb.TelemetryAck_STATUS_OK}}}
			}
			return nil
		},
	}
	done := make(chan error, 2)
	go func() { done <- a.handleTelemetryFrames(ctx, stream) }()
	go func() { done <- a.runAckLoop(ctx, stream, cancel) }()
	select {
	case <-control:
	case err := <-done:
		t.Fatalf("stream ended before context reconciliation: %v", err)
	case <-time.After(time.Second):
		t.Fatal("control blocked by telemetry retry")
	}
	select {
	case elapsed := <-retried:
		if elapsed < 900*time.Millisecond {
			t.Fatalf("retry did not back off: %v", elapsed)
		}
	case err := <-done:
		t.Fatalf("stream ended before retry: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("telemetry did not retry on the same stream")
	}
	deadline := time.Now().Add(time.Second)
	for {
		count, err := w.CountOutstanding(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retried frame was not acknowledged durably")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("shutdown: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("stream did not stop")
		}
	}
}
