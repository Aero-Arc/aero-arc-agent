package wal

type DeliveryStatus int

const (
	DeliveryStatusWritten     DeliveryStatus = 0
	DeliveryStatusPending     DeliveryStatus = 1
	DeliveryStatusDelivered   DeliveryStatus = 2
	DeliveryStatusQuarantined DeliveryStatus = 3
)

// TelemetryAckDisposition is the durable action authorized by a Relay ACK.
type TelemetryAckDisposition int

const (
	// TelemetryAckDelivered confirms durable Relay acceptance.
	TelemetryAckDelivered TelemetryAckDisposition = iota
	// TelemetryAckRetry returns the frame to the written retry queue.
	TelemetryAckRetry
	// TelemetryAckPermanentReject preserves the frame in durable quarantine.
	TelemetryAckPermanentReject
)

// TelemetryAckResult describes the atomic WAL transition applied for one ACK.
type TelemetryAckResult struct {
	Changed             bool
	CorrelatedByFrameID bool
	PreviousStatus      DeliveryStatus
}

// TelemetryDeliveredAck identifies one successful Relay telemetry ACK for a
// batch transition to delivered.
type TelemetryDeliveredAck struct {
	Sequence uint64
	FrameID  string
}
