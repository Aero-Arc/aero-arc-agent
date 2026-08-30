package agent

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
	"github.com/bluenviron/gomavlib/v3"
	"github.com/bluenviron/gomavlib/v3/pkg/dialects/common"
	"github.com/bluenviron/gomavlib/v3/pkg/message"
	"github.com/google/uuid"
	"github.com/makinje/aero-arc-agent/internal/identity"
	"github.com/makinje/aero-arc-agent/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	defaultTelemetryPersistenceDrainTimeout = 5 * time.Second
	walShutdownReserve                      = 10 * time.Second
	agentShutdownTimeout                    = defaultTelemetryPersistenceDrainTimeout + walShutdownReserve
	streamTeardownTimeout                   = 2 * time.Second
	defaultTelemetryMaxInflight             = int64(100)
	maximumTelemetryMaxInflight             = int64(10_000)
	telemetryACKFlushInterval               = 10 * time.Millisecond
	telemetryACKQueueCapacity               = 1024
	defaultTelemetryACKProgressTimeout      = 15 * time.Second
)

var errTelemetryACKProgressTimeout = errors.New("telemetry ACK progress timed out with a full in-flight window")

type telemetryStreamOwnerContextKey struct{}

func withTelemetryStreamOwner(ctx context.Context, owner string) context.Context {
	return context.WithValue(ctx, telemetryStreamOwnerContextKey{}, owner)
}

func telemetryStreamOwner(ctx context.Context) string {
	owner, _ := ctx.Value(telemetryStreamOwnerContextKey{}).(string)
	return owner
}

type telemetryStreamWindowContextKey struct{}

type telemetryStreamWindow struct {
	permits         chan struct{}
	progress        chan struct{}
	progressTimeout time.Duration
}

func withTelemetryStreamWindow(ctx context.Context, maximum int64) context.Context {
	if maximum <= 0 {
		maximum = defaultTelemetryMaxInflight
	}
	if maximum > maximumTelemetryMaxInflight {
		maximum = maximumTelemetryMaxInflight
	}
	return context.WithValue(ctx, telemetryStreamWindowContextKey{}, &telemetryStreamWindow{
		permits:         make(chan struct{}, int(maximum)),
		progress:        make(chan struct{}, 1),
		progressTimeout: defaultTelemetryACKProgressTimeout,
	})
}

func telemetryWindow(ctx context.Context) *telemetryStreamWindow {
	window, _ := ctx.Value(telemetryStreamWindowContextKey{}).(*telemetryStreamWindow)
	return window
}

func acquireTelemetryPermit(ctx context.Context) error {
	window := telemetryWindow(ctx)
	if window == nil {
		return nil
	}
	for {
		select {
		case window.permits <- struct{}{}:
			if len(window.permits) == cap(window.permits) {
				// Progress observed before this acquisition cannot satisfy the new
				// full-window epoch.
				select {
				case <-window.progress:
				default:
				}
			}
			return nil
		default:
		}

		timeout := window.progressTimeout
		if timeout <= 0 {
			timeout = defaultTelemetryACKProgressTimeout
		}
		timer := time.NewTimer(timeout)
		select {
		case <-window.progress:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			// A durable ACK released capacity; retry the acquisition.
			continue
		case <-timer.C:
			return errTelemetryACKProgressTimeout
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
}

func releaseTelemetryPermit(ctx context.Context) {
	window := telemetryWindow(ctx)
	if window == nil {
		return
	}
	select {
	case <-window.permits:
		select {
		case window.progress <- struct{}{}:
		default:
		}
	default:
		// A duplicate terminal ACK has no corresponding live permit.
	}
}

func waitForFullTelemetryWindowProgress(ctx context.Context) error {
	window := telemetryWindow(ctx)
	if window == nil || len(window.permits) < cap(window.permits) {
		return nil
	}
	timeout := window.progressTimeout
	if timeout <= 0 {
		timeout = defaultTelemetryACKProgressTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-window.progress:
		return nil
	case <-timer.C:
		return errTelemetryACKProgressTimeout
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Agent struct {
	node *gomavlib.Node
	wal  *wal.WAL

	conn    *grpc.ClientConn
	gateway agentv1.AgentGatewayClient

	options *AgentOptions

	// goroutine waitgroup
	wg sync.WaitGroup

	// reconnection/backoff settings – wired from AgentOptions.
	backoffInitial time.Duration
	backoffMax     time.Duration

	// Internal hooks primarily for testing; in production these are wired to
	// the concrete implementations below.
	dialFn         func(ctx context.Context) (*grpc.ClientConn, error)
	registerFn     func(ctx context.Context) error
	openStreamFn   func(ctx context.Context) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error)
	ackLoopFn      func(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error
	sleepWithBack  func(ctx context.Context, d time.Duration) bool
	closeWALFn     func(ctx context.Context) error
	closeMAVLinkFn func(ctx context.Context)
	mavlinkDone    chan struct{}
	cancelWAL      context.CancelFunc

	stateMu            sync.RWMutex
	operationContextMu sync.Mutex
	sessionID          string
	operationContext   *wal.OperationContext
	sendMu             sync.Mutex

	mavlinkMu             sync.Mutex
	mavlinkTarget         *mavlinkTarget
	mavlinkHeartbeatSeq   uint64
	mavlinkLandedStateSeq uint64
	pendingMAVLinkCommand *pendingMAVLinkCommand
	pendingMissionEvents  chan message.Message
	pendingMissionTarget  *missionTransactionTarget
	aircraftAckAmbiguous  bool
	// aircraftAckAmbiguousSince starts a transport-quiescence epoch at the
	// first continuously processed target-channel event after matching ACK
	// activity or an uncertain command outcome. A full command-timeout interval
	// of event-reader progress safely returns later commands to direct ACK
	// correlation; elapsed wall time while the reader is paused does not count.
	aircraftAckAmbiguousSince time.Time
	aircraftAckLastProgressAt time.Time
	aircraftCommandMu         sync.Mutex
	aircraftCommandActive     bool
	missionDeploymentMu       sync.Mutex
	missionDeploymentActive   bool
	writeMAVLinkCommand       func(*gomavlib.Channel, *common.MessageCommandLong) error
	writeMAVLinkMessage       func(*gomavlib.Channel, message.Message) error
	deployMAVLinkMission      func(context.Context, *mavlinkTarget, *agentv1.MissionPlan, bool, int64) (string, uint32, *uint32, error)
	appendTelemetryFrame      func(context.Context, *agentv1.TelemetryFrame) error
	telemetryDrainTimeout     time.Duration

	ingestCount          atomic.Uint64
	sendCount            atomic.Uint64
	telemetryDropCount   atomic.Uint64
	telemetryRejectCount atomic.Uint64
	telemetryMaxInflight atomic.Int64
	telemetryBatchActive atomic.Int32
	telemetryBatchMu     sync.Mutex
	telemetryBatchOwners map[string]int
	telemetryBatchLegacy int
}

// NewAgent constructs an Agent and its MAVLink endpoint from runtime options.
// It also installs the default Relay connection and stream lifecycle hooks;
// durable WAL resources are opened later by Start.
//
// Parameters:
//   - options: provides the configuration values used to initialize or execute the operation.
//
// Returns:
//   - agent: is configured but does not validate/open the MAVLink endpoint,
//     network connection, or WAL until Start.
//   - error: is currently always nil; deferred resource failures surface from Start.
func NewAgent(options *AgentOptions) (*Agent, error) {
	if options.BackoffInitial <= 0 {
		options.BackoffInitial = time.Second
	}
	if options.BackoffMax <= 0 {
		options.BackoffMax = 30 * time.Second
	}

	a := &Agent{
		node: &gomavlib.Node{
			Endpoints: []gomavlib.EndpointConf{
				gomavlib.EndpointSerial{
					Device: options.SerialPath,
					Baud:   options.SerialBaud,
				},
			},
			OutVersion:     gomavlib.V2,
			OutSystemID:    mavlinkSourceSystemID,
			OutComponentID: mavlinkSourceComponentID,
			Dialect:        common.Dialect,
		},
		options:        options,
		backoffInitial: options.BackoffInitial,
		backoffMax:     options.BackoffMax,
		// A COMMAND_ACK buffered before a process restart is indistinguishable
		// from the first new ARM/DISARM acknowledgement. Start fenced; the first
		// target heartbeat begins a full ACK-quiescence epoch before direct
		// correlation is enabled again.
		aircraftAckAmbiguous: true,
	}
	a.writeMAVLinkCommand = func(channel *gomavlib.Channel, command *common.MessageCommandLong) error {
		return a.node.WriteMessageTo(channel, command)
	}
	a.writeMAVLinkMessage = func(channel *gomavlib.Channel, mavlinkMessage message.Message) error {
		return a.node.WriteMessageTo(channel, mavlinkMessage)
	}

	if options.Debug {
		slog.LogAttrs(context.Background(), slog.LevelInfo, "debug mode enabled, using UDP mavlinkserver")
		a.node = &gomavlib.Node{
			Endpoints: []gomavlib.EndpointConf{
				gomavlib.EndpointUDPServer{
					Address: "0.0.0.0:14550",
				},
			},
			OutVersion:     gomavlib.V2,
			OutSystemID:    mavlinkSourceSystemID,
			OutComponentID: mavlinkSourceComponentID,
			Dialect:        common.Dialect,
		}
	}
	a.deployMAVLinkMission = a.executeMAVLinkMissionDeployment

	// Wire default implementations for lifecycle hooks.
	a.dialFn = a.establishRelayConnection
	a.registerFn = a.register
	a.openStreamFn = a.openTelemetryStream
	a.ackLoopFn = a.runAckLoop
	a.sleepWithBack = sleepWithContext

	return a, nil
}

// Start runs the MAVLink ingest loop and the gRPC reconnect/stream lifecycle
// until the provided context is cancelled or a fatal error occurs.
func (a *Agent) Start(ctx context.Context) (startErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Ensure resources are cleaned up on exit.
	defer func() {
		// Use a fresh phase budget since 'ctx' is cancelled. The bounded pre-WAL
		// drain cannot consume the time reserved for WAL spooling and closure.
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), agentShutdownTimeout)
		defer cancelShutdown()
		if err := a.shutdown(shutdownCtx); err != nil {
			startErr = errors.Join(startErr, fmt.Errorf("agent shutdown: %w", err))
		}
	}()

	// Resolve Identity
	identity := identity.Resolve()
	slog.LogAttrs(ctx, slog.LevelInfo, "agent_identity", slog.String("identity", identity.FinalID))

	if err := a.initializeWAL(ctx); err != nil {
		return err
	}

	a.wg.Add(1)
	go func(ctx context.Context) {
		defer a.wg.Done()
		a.runTelemetryStats(ctx, 10*time.Second)
	}(ctx)

	// Run WAL cleanup loop
	a.wg.Add(1)
	go func(ctx context.Context) {
		defer a.wg.Done()
		timer := time.NewTicker(10 * time.Second)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				// Cleanup delivered frames
				if err := a.wal.CleanupDelivered(ctx, 10000); err != nil {
					slog.LogAttrs(ctx, slog.LevelError, "wal_cleanup_failed", slog.String("error", err.Error()))
				}

				// Reset stuck pending frames (e.g. 5 minute TTL)
				// This handles cases where a frame was marked pending but we never got an ACK or crashed.
				if _, err := a.resetStuckPending(ctx, 5*time.Minute); err != nil {
					slog.LogAttrs(ctx, slog.LevelError, "wal_reset_pending_failed", slog.String("error", err.Error()))
				}
			}
		}
	}(ctx)

	// Run MAVLink loop
	a.mavlinkDone = make(chan struct{})
	a.wg.Add(1)
	go func(ctx context.Context) {
		defer a.wg.Done()
		defer close(a.mavlinkDone)
		a.runMAVLink(ctx)
	}(ctx)

	a.wg.Add(1)
	go func(ctx context.Context) {
		defer a.wg.Done()
		a.runWithReconnect(ctx)
	}(ctx)

	<-ctx.Done()

	slog.LogAttrs(ctx, slog.LevelInfo, "agent received shutdown signal", slog.String("signal", ctx.Err().Error()))

	return ctx.Err()
}

func (a *Agent) shutdown(ctx context.Context) error {
	var shutdownErr error
	if a.cancelWAL != nil {
		defer a.cancelWAL()
	}
	if a.conn != nil {
		slog.Info("shutting down grpc connection")
		if err := a.conn.Close(); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close gRPC connection: %w", err))
		}
	}

	// Let the MAVLink reader close and drain its bounded pre-WAL queue before
	// closing the WAL. Parent cancellation stops new ingest; runMAVLinkEvents
	// bounds the actual drain and accounts for anything it cannot append.
	if a.mavlinkDone != nil {
		select {
		case <-a.mavlinkDone:
		case <-ctx.Done():
		}
	}

	if a.closeWALFn != nil {
		slog.Info("shutting down write-ahead log connection")
		if err := a.closeWALFn(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close write-ahead log: %w", err))
		}
	} else if a.wal != nil {
		slog.Info("shutting down write-ahead log connection")
		if err := a.wal.CloseContext(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close write-ahead log: %w", err))
		}
	}

	slog.Info("shutting down mavlink node (best effort)")
	mavlinkCloseCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if a.closeMAVLinkFn != nil {
		a.closeMAVLinkFn(mavlinkCloseCtx)
	} else {
		a.closeMAVLinkBestEffort(mavlinkCloseCtx)
	}

	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("shutdown timed out waiting for goroutines to finish")
		shutdownErr = errors.Join(shutdownErr, ctx.Err())
	}

	a.gateway = nil
	a.conn = nil

	return shutdownErr
}

// initializeWAL gives the durable writer a lifecycle independent of the Agent
// run context. Agent cancellation first stops MAVLink ingest and drains the
// bounded pre-WAL queue; shutdown then closes the WAL explicitly. Passing the
// run context directly to wal.New would start WAL closure before that drain.
func (a *Agent) initializeWAL(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	walCtx, cancelWAL := context.WithCancel(context.Background())
	w, err := wal.NewWithLifecycle(ctx, walCtx, a.options.WALPath, a.options.WALBatchSize, a.options.WALFlushTimeout)
	if err != nil {
		cancelWAL()
		return fmt.Errorf("failed to initialize WAL: %w", err)
	}
	a.wal = w
	a.cancelWAL = cancelWAL
	if operationContext, ok, err := w.LoadOperationContext(ctx); err != nil {
		return err
	} else if ok {
		a.operationContext = &operationContext
	}
	slog.LogAttrs(ctx, slog.LevelInfo, "wal_initialized", slog.String("path", a.options.WALPath))
	return nil
}

func (a *Agent) closeMAVLinkBestEffort(ctx context.Context) {
	if a.node == nil {
		slog.Info("mavlink node already shutdown or closed")
		return
	}

	done := make(chan struct{})
	go func() {
		a.node.Close()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("mavlink node closed successfully")
	case <-ctx.Done():
		slog.Warn("mavlink node close timed out; continuing shutdown")
	}
}

// runMAVLink owns the lifecycle of the gomavlib node.
func (a *Agent) runMAVLink(ctx context.Context) error {
	slog.LogAttrs(ctx, slog.LevelInfo, "mavlink_node_initializing")

	if err := a.node.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize node: %v", err)
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "mavlink_node_initialized")
	return a.runMAVLinkEvents(ctx, a.node.Events())
}

// runMAVLinkEvents keeps command evidence on the event-reader path while a
// separate worker handles potentially blocking telemetry persistence. The
// queue is deliberately bounded: under sustained disk overload, newly
// observed telemetry is counted and dropped rather than allowing an unbounded
// heap or preventing COMMAND_ACK and heartbeat state from being observed.
func (a *Agent) runMAVLinkEvents(ctx context.Context, events <-chan gomavlib.Event) error {
	queueSize := 1000
	if a.options != nil && a.options.EventQueueSize > 0 {
		queueSize = a.options.EventQueueSize
	}
	telemetryQueue := make(chan *agentv1.TelemetryFrame, queueSize)
	// Persistence owns a bounded graceful-drain context independent of the
	// event-reader context. Start.shutdown waits for this worker before closing
	// the WAL, allowing already-queued telemetry to reach its durable queue.
	persistCtx, cancelPersist := context.WithCancel(context.Background())
	persistDone := make(chan struct{})
	go func() {
		defer close(persistDone)
		a.runTelemetryPersistence(persistCtx, telemetryQueue)
	}()
	defer func() {
		close(telemetryQueue)
		drainTimeout := defaultTelemetryPersistenceDrainTimeout
		if a.telemetryDrainTimeout > 0 {
			drainTimeout = a.telemetryDrainTimeout
		}
		forceStop := time.AfterFunc(drainTimeout, cancelPersist)
		<-persistDone
		forceStop.Stop()
		cancelPersist()
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt, ok := <-events:
			if !ok {
				slog.LogAttrs(ctx, slog.LevelInfo, "mavlink eventstream closed")
				return nil
			}

			if frameEvt, ok := evt.(*gomavlib.EventFrame); ok {
				// Control evidence must be observed before any telemetry work. WAL
				// backpressure must never make a valid aircraft ACK time out.
				a.observeMAVLinkFrame(frameEvt)
				slog.LogAttrs(
					ctx, slog.LevelDebug,
					"mavlink_frame_received",
					slog.String("frame-message", fmt.Sprintf("%+v", frameEvt.Message())),
				)

				tFrame, err := a.buildTelemetryFrame(frameEvt)
				if err != nil {
					a.rejectTelemetryFrame(ctx, frameEvt, err)
					continue
				}
				select {
				case telemetryQueue <- tFrame:
				default:
					dropped := a.telemetryDropCount.Add(1)
					// Log the first drop and then exponentially to keep an
					// overload from becoming a second source of backpressure.
					if shouldLogExponential(dropped) {
						slog.LogAttrs(ctx, slog.LevelError, "telemetry_persistence_queue_full",
							slog.Uint64("dropped_total", dropped),
							slog.Int("queue_capacity", queueSize),
						)
					}
				}
			}

			if _, ok := evt.(*gomavlib.EventChannelOpen); ok {
				slog.LogAttrs(
					ctx, slog.LevelInfo,
					"mavlink_channel_open",
					slog.String("relay-address", a.options.ServerAddress),
					slog.Int("relay-port", a.options.ServerPort),
				)
				continue
			}

			if closeEvent, ok := evt.(*gomavlib.EventChannelClose); ok {
				a.clearMAVLinkTarget(closeEvent.Channel)
				slog.LogAttrs(
					ctx, slog.LevelInfo,
					"mavlink_channel_close",
					slog.String("relay-address", a.options.ServerAddress),
					slog.Int("relay-port", a.options.ServerPort),
				)
				continue
			}
		}
	}
}

func (a *Agent) rejectTelemetryFrame(ctx context.Context, frame *gomavlib.EventFrame, err error) {
	rejected := a.telemetryRejectCount.Add(1)
	dropped := a.telemetryDropCount.Add(1)
	if !shouldLogExponential(rejected) {
		return
	}
	attrs := []slog.Attr{
		slog.String("error", err.Error()),
		slog.Uint64("rejected_total", rejected),
		slog.Uint64("dropped_total", dropped),
	}
	if frame != nil && frame.Message() != nil {
		attrs = append(attrs,
			slog.Uint64("msg_id", uint64(frame.Message().GetID())),
			slog.String("msg_name", fmt.Sprintf("%T", frame.Message())),
		)
	}
	if isNonFiniteJSONError(err) {
		attrs = append(attrs, slog.String("reason", "non_finite_json_value"))
	}
	slog.LogAttrs(ctx, slog.LevelError, "telemetry_frame_rejected", attrs...)
}

func shouldLogExponential(count uint64) bool {
	return count > 0 && (count == 1 || count&(count-1) == 0)
}

func isNonFiniteJSONError(err error) bool {
	var unsupported *json.UnsupportedValueError
	if !errors.As(err, &unsupported) || !unsupported.Value.IsValid() {
		return false
	}
	switch unsupported.Value.Kind() {
	case reflect.Float32, reflect.Float64:
		value := unsupported.Value.Float()
		return math.IsNaN(value) || math.IsInf(value, 0)
	default:
		return false
	}
}

func (a *Agent) runTelemetryPersistence(ctx context.Context, frames <-chan *agentv1.TelemetryFrame) {
	for frame := range frames {
		if err := a.persistTelemetryFrame(ctx, frame); err != nil {
			if ctx.Err() != nil {
				droppedNow := uint64(1)
				for range frames {
					droppedNow++
				}
				dropped := a.telemetryDropCount.Add(droppedNow)
				slog.LogAttrs(context.Background(), slog.LevelError, "telemetry_persistence_drain_expired",
					slog.String("error", err.Error()),
					slog.Uint64("dropped_count", droppedNow),
					slog.Uint64("dropped_total", dropped),
				)
				return
			}
			dropped := a.telemetryDropCount.Add(1)
			slog.LogAttrs(context.Background(), slog.LevelError, "failed_to_persist_frame",
				slog.String("error", err.Error()),
				slog.Uint64("dropped_total", dropped),
			)
		}
	}
}

func (a *Agent) persistTelemetryFrame(ctx context.Context, frame *agentv1.TelemetryFrame) error {
	var err error
	if a.appendTelemetryFrame != nil {
		err = a.appendTelemetryFrame(ctx, frame)
	} else if a.wal == nil {
		err = errors.New("WAL is unavailable")
	} else {
		err = a.wal.AppendAsync(ctx, frame)
	}
	if err != nil {
		return fmt.Errorf("wal append async failed: %w", err)
	}
	a.ingestCount.Add(1)
	return nil
}

// buildTelemetryFrame copies one MAVLink event into its durable wire model.
// It intentionally performs no I/O so the event-reader cannot stall on disk.
func (a *Agent) buildTelemetryFrame(frame *gomavlib.EventFrame) (*agentv1.TelemetryFrame, error) {
	msg := frame.Message()
	payload, err := json.Marshal(msg)
	if err != nil {
		if isNonFiniteJSONError(err) {
			return nil, fmt.Errorf("telemetry message %T contains a non-finite JSON value: %w", msg, err)
		}
		return nil, fmt.Errorf("failed to marshal frame message: %w", err)
	}

	msgName := fmt.Sprintf("%T", msg)
	fields, _ := commonFields(msg)

	// Construct the TelemetryFrame to return
	tFrame := &agentv1.TelemetryFrame{
		RawMavlink:   payload,
		SentAtUnixNs: time.Now().UnixNano(),
		Dialect:      "common",
		MsgId:        msg.GetID(),
		MsgName:      msgName,
		Fields:       fields,
		AgentId:      identity.Resolve().FinalID,
	}
	a.stampFrameContext(tFrame)
	return tFrame, nil
}

func (a *Agent) stampFrameContext(frame *agentv1.TelemetryFrame) {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	if active := a.operationContext; active != nil {
		frame.FlightId = active.FlightID
		frame.IntentId = active.IntentID
		frame.IntentVersion = active.IntentVersion
	}
}

func (a *Agent) stampCurrentSession(frame *agentv1.TelemetryFrame) {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	frame.SessionId = a.sessionID
}

// dialRelay establishes a gRPC connection to the relay using the configured target.
func (a *Agent) establishRelayConnection(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	creds, err := relayTransportCredentials(a.options)
	if err != nil {
		if errors.Is(err, ErrGettingHomeDir) {
			slog.LogAttrs(ctx, slog.LevelError, ErrGettingHomeDir.Error(), slog.String("error", err.Error()))
		}
		return nil, err
	}

	slog.LogAttrs(
		dialCtx, slog.LevelInfo,
		"agent_connecting",
		slog.String("target", a.options.RelayTarget),
	)

	conn, err := grpc.NewClient(
		a.options.RelayTarget,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(TokenAuth{
			Token:  a.options.APIKey,
			Secure: !a.options.Debug,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFailedToConnectToServer, err)
	}

	return conn, nil
}

func relayTransportCredentials(options *AgentOptions) (credentials.TransportCredentials, error) {
	if options.SkipTLSVerification {
		return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}), nil //nolint:gosec // Explicit development-only CLI option.
	}
	if options.Debug {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrGettingHomeDir, err)
		}
		certPath := fmt.Sprintf("%s/%s", homeDir, DebugTLSCertPath)
		creds, err := credentials.NewClientTLSFromFile(certPath, "localhost")
		if err != nil {
			return nil, err
		}
		return creds, nil
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}), nil
}

// register performs the Register RPC with the relay.
func (a *Agent) register(ctx context.Context) error {
	if a.gateway == nil {
		return ErrGatewayNotInitialized
	}

	agentID := identity.Resolve().FinalID
	req := &agentv1.RegisterRequest{
		AgentId: agentID,
	}

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_registering",
		slog.String("target", a.options.RelayTarget),
	)

	regCtx := metadata.AppendToOutgoingContext(ctx, "aero-arc-agent-id", agentID)
	response, err := a.gateway.Register(regCtx, req)
	if err != nil {
		return err
	}
	if response.GetSessionId() == "" {
		return errors.New("relay registration returned an empty session ID")
	}
	maximum := response.GetMaxInflight()
	if maximum <= 0 {
		maximum = defaultTelemetryMaxInflight
	}
	if maximum > maximumTelemetryMaxInflight {
		maximum = maximumTelemetryMaxInflight
	}
	a.telemetryMaxInflight.Store(maximum)
	a.stateMu.Lock()
	a.sessionID = response.GetSessionId()
	a.stateMu.Unlock()

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_registered",
		slog.String("target", a.options.RelayTarget),
	)

	return nil
}

func (a *Agent) configuredTelemetryMaxInflight() int64 {
	maximum := a.telemetryMaxInflight.Load()
	if maximum <= 0 {
		return defaultTelemetryMaxInflight
	}
	if maximum > maximumTelemetryMaxInflight {
		return maximumTelemetryMaxInflight
	}
	return maximum
}

// openTelemetryStream opens the bidi telemetry stream.
func (a *Agent) openTelemetryStream(ctx context.Context) (grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], error) {
	if a.gateway == nil {
		return nil, ErrGatewayNotInitialized
	}

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_stream_opening",
		slog.String("target", a.options.RelayTarget),
	)

	agentID := identity.Resolve().FinalID
	a.stateMu.RLock()
	sessionID := a.sessionID
	a.stateMu.RUnlock()
	if sessionID == "" {
		return nil, errors.New("relay session ID is unavailable")
	}
	streamCtx := metadata.AppendToOutgoingContext(ctx,
		"aero-arc-agent-id", agentID,
		"aero-arc-session-id", sessionID,
	)

	stream, err := a.gateway.TelemetryStream(streamCtx)
	if err != nil {
		return nil, err
	}

	slog.LogAttrs(
		ctx, slog.LevelInfo,
		"agent_stream_open",
		slog.String("target", a.options.RelayTarget),
	)

	return stream, nil
}

type relayStreamReceive struct {
	message *agentv1.RelayStreamMessage
	err     error
}

// runAckLoop drains Relay messages independently from bounded, durable ACK
// commits. This prevents a burst of successful telemetry ACKs from placing
// operation-context or aircraft control messages behind one SQLite FULL commit
// per frame.
func (a *Agent) runAckLoop(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error {
	commandCtx, cancelCommands := context.WithCancel(ctx)
	ackCtx, cancelACKs := context.WithCancel(ctx)
	var commandWG sync.WaitGroup
	commandErrors := make(chan error, 1)
	ackQueue := make(chan *agentv1.TelemetryAck, telemetryACKQueueCapacity)
	ackDone := make(chan error, 1)
	var ackWG sync.WaitGroup
	ackWG.Add(1)
	go func() {
		defer ackWG.Done()
		ackDone <- a.runTelemetryACKWorker(ackCtx, ackQueue)
	}()
	received := make(chan relayStreamReceive, 64)
	go func() {
		for {
			message, err := stream.Recv()
			select {
			case received <- relayStreamReceive{message: message, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		cancelCommands()
		cancelACKs()
		commandWG.Wait()
		ackWG.Wait()
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-commandErrors:
			return err
		case err := <-ackDone:
			return err
		case incoming := <-received:
			if incoming.err != nil {
				close(ackQueue)
				if ackErr := <-ackDone; ackErr != nil {
					return errors.Join(incoming.err, ackErr)
				}
				return incoming.err
			}
			message := incoming.message
			if ack := message.GetTelemetryAck(); ack != nil {
				select {
				case ackQueue <- ack:
				case err := <-ackDone:
					return err
				case <-ctx.Done():
					return ctx.Err()
				}
				continue
			}
			var err error
			if command := message.GetAircraftCommand(); command != nil {
				err = a.dispatchAircraftCommand(commandCtx, stream, command, &commandWG, commandErrors)
			} else if mission := message.GetDeployMission(); mission != nil {
				err = a.dispatchMissionDeployment(commandCtx, stream, mission, &commandWG, commandErrors)
			} else {
				err = a.handleRelayMessage(ctx, stream, message)
			}
			if err != nil {
				return err
			}
		}
	}
}

func (a *Agent) runTelemetryACKWorker(ctx context.Context, acknowledgments <-chan *agentv1.TelemetryAck) error {
	owner := telemetryStreamOwner(ctx)
	batchLimit := int(a.configuredTelemetryMaxInflight())
	if batchLimit <= 0 || batchLimit > telemetryACKQueueCapacity {
		batchLimit = int(defaultTelemetryMaxInflight)
	}
	ticker := time.NewTicker(telemetryACKFlushInterval)
	defer ticker.Stop()
	batch := make([]wal.TelemetryDeliveredAck, 0, batchLimit)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if owner == "" {
			for _, ack := range batch {
				if err := a.handleTelemetryAck(ctx, &agentv1.TelemetryAck{Seq: ack.Sequence, FrameId: ack.FrameID, Status: agentv1.TelemetryAck_STATUS_OK}); err != nil {
					return err
				}
			}
			batch = batch[:0]
			return nil
		}
		results, err := a.wal.ApplyDeliveredTelemetryAckBatchOwned(ctx, batch, owner)
		if err != nil {
			return fmt.Errorf("apply delivered telemetry ACK batch: %w", err)
		}
		for index, result := range results {
			if !result.CorrelatedByFrameID {
				slog.LogAttrs(ctx, slog.LevelDebug, "telemetry_ack_seq_only", slog.Uint64("seq", batch[index].Sequence))
			}
			if result.Changed {
				releaseTelemetryPermit(ctx)
			}
		}
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ack, ok := <-acknowledgments:
			if !ok {
				return flush()
			}
			if ack == nil || ack.GetSeq() == 0 {
				return fmt.Errorf("%w: ACK and non-zero sequence are required", ErrInvalidTelemetryAck)
			}
			if ack.GetStatus() == agentv1.TelemetryAck_STATUS_OK {
				batch = append(batch, wal.TelemetryDeliveredAck{Sequence: ack.GetSeq(), FrameID: ack.GetFrameId()})
				if len(batch) >= batchLimit {
					if err := flush(); err != nil {
						return err
					}
				}
				continue
			}
			if err := flush(); err != nil {
				return err
			}
			if err := a.handleTelemetryAck(ctx, ack); err != nil {
				return err
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func (a *Agent) handleRelayMessage(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], message *agentv1.RelayStreamMessage) error {
	switch payload := message.GetPayload().(type) {
	case *agentv1.RelayStreamMessage_TelemetryAck:
		return a.handleTelemetryAck(ctx, payload.TelemetryAck)
	case *agentv1.RelayStreamMessage_SetOperationContext:
		return a.handleSetOperationContext(ctx, stream, payload.SetOperationContext)
	case *agentv1.RelayStreamMessage_ClearOperationContext:
		return a.handleClearOperationContext(ctx, stream, payload.ClearOperationContext)
	case *agentv1.RelayStreamMessage_AircraftCommand:
		return a.handleAircraftCommand(ctx, stream, payload.AircraftCommand)
	case *agentv1.RelayStreamMessage_DeployMission:
		return a.handleMissionDeployment(ctx, stream, payload.DeployMission)
	default:
		return a.sendOperationContextAck(stream, "", agentv1.OperationContextCommandAck_STATUS_REJECTED, "relay stream message has no supported payload")
	}
}

func (a *Agent) handleSetOperationContext(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], command *agentv1.SetOperationContextCommand) error {
	a.operationContextMu.Lock()
	defer a.operationContextMu.Unlock()
	if command.GetCommandId() == "" || command.GetContext() == nil || command.GetContext().GetFlightId() == "" {
		return a.sendOperationContextAck(stream, command.GetCommandId(), agentv1.OperationContextCommandAck_STATUS_REJECTED, "command_id and flight_id are required")
	}
	value := wal.OperationContext{AircraftID: command.Context.AircraftId, FlightID: command.Context.FlightId, IntentID: command.Context.IntentId, IntentVersion: command.Context.IntentVersion}
	applied, err := a.wal.SetOperationContext(ctx, command.CommandId, value)
	if err != nil {
		if errors.Is(err, wal.ErrOperationCommandConflict) {
			return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_REJECTED, err.Error())
		}
		return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_TEMPORARY_ERROR, err.Error())
	}
	status := agentv1.OperationContextCommandAck_STATUS_APPLIED
	if !applied {
		status = agentv1.OperationContextCommandAck_STATUS_ALREADY_APPLIED
		persisted, ok, loadErr := a.wal.LoadOperationContext(ctx)
		if loadErr != nil {
			return loadErr
		}
		a.stateMu.Lock()
		if ok {
			a.operationContext = &persisted
		} else {
			a.operationContext = nil
		}
		a.stateMu.Unlock()
		return a.sendOperationContextAck(stream, command.CommandId, status, "")
	}
	a.stateMu.Lock()
	a.operationContext = &value
	a.stateMu.Unlock()
	return a.sendOperationContextAck(stream, command.CommandId, status, "")
}

func (a *Agent) handleClearOperationContext(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], command *agentv1.ClearOperationContextCommand) error {
	a.operationContextMu.Lock()
	defer a.operationContextMu.Unlock()
	if command.GetCommandId() == "" {
		return a.sendOperationContextAck(stream, "", agentv1.OperationContextCommandAck_STATUS_REJECTED, "command_id is required")
	}
	if command.GetAuthoritative() {
		if command.GetFlightId() != "" {
			return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_REJECTED, "authoritative clear requires an empty flight_id")
		}
	} else if command.GetFlightId() == "" {
		return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_REJECTED, "conditional clear requires a flight_id")
	}
	applied, err := a.wal.ClearOperationContext(ctx, command.CommandId, command.FlightId)
	if err != nil {
		if errors.Is(err, wal.ErrOperationCommandConflict) {
			return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_REJECTED, err.Error())
		}
		return a.sendOperationContextAck(stream, command.CommandId, agentv1.OperationContextCommandAck_STATUS_TEMPORARY_ERROR, err.Error())
	}
	active, ok, err := a.wal.LoadOperationContext(ctx)
	if err != nil {
		return err
	}
	a.stateMu.Lock()
	if ok {
		a.operationContext = &active
	} else {
		a.operationContext = nil
	}
	a.stateMu.Unlock()
	status := agentv1.OperationContextCommandAck_STATUS_APPLIED
	if !applied {
		status = agentv1.OperationContextCommandAck_STATUS_ALREADY_APPLIED
	}
	return a.sendOperationContextAck(stream, command.CommandId, status, "")
}

func (a *Agent) sendOperationContextAck(stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage], commandID string, status agentv1.OperationContextCommandAck_Status, errorMessage string) error {
	a.stateMu.RLock()
	var active *agentv1.OperationContext
	if value := a.operationContext; value != nil {
		active = &agentv1.OperationContext{AircraftId: value.AircraftID, FlightId: value.FlightID, IntentId: value.IntentID, IntentVersion: value.IntentVersion}
	}
	a.stateMu.RUnlock()
	message := &agentv1.AgentStreamMessage{Payload: &agentv1.AgentStreamMessage_OperationContextCommandAck{OperationContextCommandAck: &agentv1.OperationContextCommandAck{CommandId: commandID, Status: status, Error: errorMessage, ActiveContext: active}}}
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	return stream.Send(message)
}

func (a *Agent) handleTelemetryAck(ctx context.Context, ack *agentv1.TelemetryAck) error {
	if ack == nil || ack.GetSeq() == 0 {
		return fmt.Errorf("%w: ACK and non-zero sequence are required", ErrInvalidTelemetryAck)
	}
	slog.LogAttrs(
		ctx, slog.LevelDebug,
		"telemetry_ack_received",
		slog.String("ack", fmt.Sprintf("%+v", ack)),
	)

	var disposition wal.TelemetryAckDisposition
	switch ack.GetStatus() {
	case agentv1.TelemetryAck_STATUS_OK:
		disposition = wal.TelemetryAckDelivered
	case agentv1.TelemetryAck_STATUS_TEMPORARY_ERROR, agentv1.TelemetryAck_STATUS_RETRY_WITH_BACKOFF:
		disposition = wal.TelemetryAckRetry
	case agentv1.TelemetryAck_STATUS_PERMANENT_ERROR:
		disposition = wal.TelemetryAckPermanentReject
	default:
		return fmt.Errorf("%w: unsupported status %d for sequence %d", ErrInvalidTelemetryAck, ack.GetStatus(), ack.GetSeq())
	}
	var result wal.TelemetryAckResult
	var err error
	if owner := telemetryStreamOwner(ctx); owner != "" {
		result, err = a.wal.ApplyTelemetryAckOwned(ctx, ack.GetSeq(), ack.GetFrameId(), disposition, ack.GetError(), owner)
	} else {
		result, err = a.wal.ApplyTelemetryAck(ctx, ack.GetSeq(), ack.GetFrameId(), disposition, ack.GetError())
	}
	if err != nil {
		return fmt.Errorf("apply telemetry ACK for sequence %d: %w", ack.GetSeq(), err)
	}
	if !result.CorrelatedByFrameID {
		// The current Relay does not populate frame_id and TelemetryAck has no
		// wal_id, so deployed peers can only correlate by stream-scoped WAL seq.
		slog.LogAttrs(ctx, slog.LevelDebug, "telemetry_ack_seq_only", slog.Uint64("seq", ack.GetSeq()))
	}
	if result.Changed {
		releaseTelemetryPermit(ctx)
	}
	switch ack.GetStatus() {
	case agentv1.TelemetryAck_STATUS_TEMPORARY_ERROR, agentv1.TelemetryAck_STATUS_RETRY_WITH_BACKOFF:
		return fmt.Errorf("%w: sequence %d: %s", ErrTelemetryRetry, ack.GetSeq(), ack.GetError())
	case agentv1.TelemetryAck_STATUS_PERMANENT_ERROR:
		return fmt.Errorf("%w: sequence %d quarantined: %s", ErrTelemetryRejected, ack.GetSeq(), ack.GetError())
	}

	return nil
}

func (a *Agent) handleTelemetryFrames(ctx context.Context, stream grpc.BidiStreamingClient[agentv1.AgentStreamMessage, agentv1.RelayStreamMessage]) error {
	// The new architecture unifies "Replay" and "Live" into a single loop.
	// 1. We poll the WAL for undelivered frames.
	// 2. We send them.
	// 3. If there are no frames, we wait for a signal from the WAL writer (WaitForData).

	slog.LogAttrs(ctx, slog.LevelInfo, "telemetry_stream_sender_starting")

	owner := telemetryStreamOwner(ctx)
	for {
		// 1. Read undelivered frames
		entries, err := a.wal.ReadUndelivered(ctx, int(a.options.WALBatchSize))
		if err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "wal_read_error", slog.String("error", err.Error()))
			return err
		}

		entriesLen := len(entries)

		if entriesLen == 0 {
			if err := a.wal.WaitForData(ctx); err != nil {
				return err
			}
			continue
		}

		type outboundFrame struct {
			message *agentv1.AgentStreamMessage
		}
		outbound := make([]outboundFrame, 0, entriesLen)
		ids := make([]uint64, 0, entriesLen)

		// 2. Decode a bounded batch before changing its delivery state.
		for i := 0; i < entriesLen; i++ {
			tFrame := &agentv1.TelemetryFrame{}
			if err := proto.Unmarshal(entries[i].Payload, tFrame); err != nil {
				slog.LogAttrs(ctx, slog.LevelError, "wal_frame_unmarshal_error", slog.String("error", err.Error()))
				continue
			}
			tFrame.Seq = uint64(entries[i].ID)
			a.stampWALGeneration(tFrame)
			a.stampCurrentSession(tFrame)

			message := &agentv1.AgentStreamMessage{Payload: &agentv1.AgentStreamMessage_TelemetryFrame{TelemetryFrame: tFrame}}
			outbound = append(outbound, outboundFrame{message: message})
			ids = append(ids, tFrame.Seq)
		}
		if len(outbound) == 0 {
			return errors.New("WAL batch contained no decodable telemetry frames")
		}
		// 3. Reserve the ordered batch in one FULL-synchronous SQLite transaction.
		// Durable owner tracking keeps cleanup from stealing any row while Send is
		// active, so one fsync can preserve the ACK-before-send fence for the whole
		// batch without letting historical replay throttle live state indefinitely.
		a.beginTelemetryBatch(owner)
		var rows int64
		var markErr error
		if owner != "" {
			rows, markErr = a.wal.MarkPendingBatchOwned(ctx, ids, owner)
		} else {
			rows, markErr = a.wal.MarkPendingBatch(ctx, ids)
		}
		if markErr != nil || rows != int64(len(ids)) {
			a.endTelemetryBatch(owner)
			if markErr == nil {
				markErr = fmt.Errorf("changed %d of %d entries", rows, len(ids))
			}
			return fmt.Errorf("reserve telemetry batch pending: %w", markErr)
		}
		sentIDs := make([]uint64, 0, len(ids))
		var batchErr error
		acquiredPermits := 0
		for index, frame := range outbound {
			if err := acquireTelemetryPermit(ctx); err != nil {
				batchErr = err
				break
			}
			acquiredPermits++
			sentIDs = append(sentIDs, ids[index])
			a.sendMu.Lock()
			err := stream.Send(frame.message)
			a.sendMu.Unlock()
			if err != nil {
				releaseTelemetryPermit(ctx)
				acquiredPermits--
				slog.LogAttrs(ctx, slog.LevelError, "telemetry_frame_send_error", slog.String("error", err.Error()))
				batchErr = err
				break
			}
			a.sendCount.Add(1)
		}
		if batchErr == nil {
			// Give every unacknowledged row a full post-batch ACK window. Terminal
			// ACKs that raced the refresh are excluded by the pending predicate.
			var refreshErr error
			if owner != "" {
				_, refreshErr = a.wal.RefreshPendingBatchOwned(ctx, sentIDs, owner)
			} else {
				_, refreshErr = a.wal.RefreshPendingBatch(ctx, sentIDs)
			}
			if refreshErr != nil {
				batchErr = fmt.Errorf("refresh telemetry batch pending epochs: %w", refreshErr)
			}
		}
		if batchErr == nil {
			// A full advertised window must observe a durably committed ACK before
			// the stream is allowed to remain idle. Otherwise a connected but silent
			// Relay would keep every owner-fenced row pending forever.
			batchErr = waitForFullTelemetryWindowProgress(ctx)
		}
		if batchErr != nil {
			// Return the entire reserved batch, including peers not yet handed to
			// gRPC. Concurrent terminal ACK states for sent rows still win.
			var resetErr error
			var resetRows int64
			if owner != "" {
				resetRows, resetErr = a.wal.MarkWrittenBatchOwned(ctx, ids, owner)
			} else {
				resetRows, resetErr = a.wal.MarkWrittenBatch(ctx, ids)
			}
			if resetErr != nil {
				batchErr = errors.Join(batchErr, fmt.Errorf("return failed telemetry batch to retry: %w", resetErr))
			} else {
				for released := int64(0); released < resetRows && acquiredPermits > 0; released++ {
					releaseTelemetryPermit(ctx)
					acquiredPermits--
				}
			}
		}
		a.endTelemetryBatch(owner)
		if batchErr != nil {
			return batchErr
		}

		slog.LogAttrs(ctx, slog.LevelInfo, "mark_batch_succeed", slog.Int("batch_size", len(outbound)))
	}
}

func (a *Agent) resetStuckPending(ctx context.Context, ttl time.Duration) (int64, error) {
	a.telemetryBatchMu.Lock()
	defer a.telemetryBatchMu.Unlock()
	if a.telemetryBatchLegacy > 0 {
		return 0, nil
	}
	owners := make([]string, 0, len(a.telemetryBatchOwners))
	for owner := range a.telemetryBatchOwners {
		owners = append(owners, owner)
	}
	return a.wal.ResetPendingExcludingOwners(ctx, ttl, owners)
}

func (a *Agent) beginTelemetryBatch(owner string) {
	a.telemetryBatchMu.Lock()
	defer a.telemetryBatchMu.Unlock()
	if owner == "" {
		a.telemetryBatchLegacy++
	} else {
		if a.telemetryBatchOwners == nil {
			a.telemetryBatchOwners = make(map[string]int)
		}
		a.telemetryBatchOwners[owner]++
	}
	a.telemetryBatchActive.Add(1)
}

func (a *Agent) endTelemetryBatch(owner string) {
	a.telemetryBatchMu.Lock()
	defer a.telemetryBatchMu.Unlock()
	if owner == "" {
		a.telemetryBatchLegacy--
	} else if active := a.telemetryBatchOwners[owner]; active <= 1 {
		delete(a.telemetryBatchOwners, owner)
	} else {
		a.telemetryBatchOwners[owner] = active - 1
	}
	a.telemetryBatchActive.Add(-1)
}

func (a *Agent) stampWALGeneration(frame *agentv1.TelemetryFrame) {
	if frame.WalId == "" {
		frame.WalId = a.wal.GenerationID()
	}
}

// runWithReconnect orchestrates dial → register → stream with exponential
// backoff and context-aware cancellation. It owns the full lifecycle of the
// gRPC connection and telemetry stream.
func (a *Agent) runWithReconnect(ctx context.Context) error {
	backoff := a.backoffInitial
	if backoff <= 0 {
		backoff = time.Second
	}
	maxBackoff := a.backoffMax
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// 1. Establish connection.
		conn, err := a.dialFn(ctx)
		if err != nil {
			slog.LogAttrs(
				ctx, slog.LevelError,
				"agent_connect_failed",
				slog.String("target", a.options.RelayTarget),
				slog.String("error", err.Error()),
				slog.Int64("backoff_ms", backoff.Milliseconds()),
			)

			if !a.sleepWithBack(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		connCtx, cancelConn := context.WithCancel(ctx)
		connCtx = withTelemetryStreamOwner(connCtx, uuid.NewString())

		a.conn = conn
		a.gateway = agentv1.NewAgentGatewayClient(conn)

		// 2. Register with the relay.
		regCtx, cancelReg := context.WithTimeout(ctx, 10*time.Second)
		err = a.registerFn(regCtx)
		cancelReg()
		if err != nil {
			slog.LogAttrs(
				ctx, slog.LevelError,
				"agent_register_failed",
				slog.String("target", a.options.RelayTarget),
				slog.String("error", err.Error()),
				slog.Int64("backoff_ms", backoff.Milliseconds()),
			)

			cancelConn()
			_ = conn.Close()
			a.conn = nil
			a.gateway = nil

			if !a.sleepWithBack(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		connCtx = withTelemetryStreamWindow(connCtx, a.configuredTelemetryMaxInflight())

		// 3. Open telemetry stream.
		stream, err := a.openStreamFn(connCtx)
		if err != nil {
			slog.LogAttrs(
				ctx, slog.LevelError,
				"agent_stream_open_failed",
				slog.String("target", a.options.RelayTarget),
				slog.String("error", err.Error()),
				slog.Int64("backoff_ms", backoff.Milliseconds()),
			)

			cancelConn()
			_ = conn.Close()
			a.conn = nil
			a.gateway = nil

			if !a.sleepWithBack(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		errChan := make(chan error, 2)
		streamStopped := make(chan struct{}, 2)

		// 4. Handle telemetry frames.
		go func() {
			defer func() { streamStopped <- struct{}{} }()
			errChan <- a.handleTelemetryFrames(connCtx, stream)
		}()

		// 5. Run the ack loop until it ends or context is cancelled.
		go func() {
			defer func() { streamStopped <- struct{}{} }()
			errChan <- a.ackLoopFn(connCtx, stream)
		}()

		select {
		case <-ctx.Done():
			err = ctx.Err()
		case err = <-errChan:
			slog.LogAttrs(ctx, slog.LevelInfo, "stream_ended", slog.String("error", fmt.Sprint(err)))
		}

		slog.LogAttrs(
			ctx, slog.LevelInfo,
			"agent_stream_closed",
			slog.String("target", a.options.RelayTarget),
			slog.String("error", fmt.Sprintf("%v", err)),
		)

		// Cleanup and Reconnect
		cancelConn()
		_ = stream.CloseSend()
		_ = conn.Close()
		// Wait until no ACK handler can still start or commit before returning
		// stream-owned pending rows to the retry queue. This ordering lets exact
		// terminal ACKs win while avoiding a TTL-sized stall for their peers.
		stopTimer := time.NewTimer(streamTeardownTimeout)
		streamQuiesced := true
	streamStopWait:
		for stopped := 0; stopped < 2; stopped++ {
			select {
			case <-streamStopped:
			case <-stopTimer.C:
				streamQuiesced = false
				break streamStopWait
			}
		}
		if !stopTimer.Stop() {
			select {
			case <-stopTimer.C:
			default:
			}
		}
		if !streamQuiesced {
			// The old stream may still own pending rows, so keep them fenced. The
			// reconnect supervisor must nevertheless remain alive: Start does not
			// consume this goroutine's return value, and returning here would leave
			// an otherwise healthy process permanently disconnected from Relay.
			err = errors.Join(err, errors.New("telemetry stream workers did not stop within teardown deadline; pending rows left fenced"))
		} else {
			teardownCtx, cancelTeardown := context.WithTimeout(context.Background(), streamTeardownTimeout)
			if _, requeueErr := a.wal.RequeuePendingOwner(teardownCtx, telemetryStreamOwner(connCtx)); requeueErr != nil {
				err = errors.Join(err, fmt.Errorf("requeue telemetry after stream teardown: %w", requeueErr))
			}
			cancelTeardown()
		}
		a.conn = nil
		a.gateway = nil

		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Reset backoff after a successful connection cycle (even if the
		// stream eventually ended with an error).
		backoff = a.backoffInitial
		if backoff <= 0 {
			backoff = time.Second
		}

		if err != nil {
			slog.LogAttrs(
				ctx, slog.LevelError,
				"agent_stream_error",
				slog.String("target", a.options.RelayTarget),
				slog.String("error", err.Error()),
				slog.Int64("backoff_ms", backoff.Milliseconds()),
			)

			if !a.sleepWithBack(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
