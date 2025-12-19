package agent

import (
	agentv1 "github.com/aero-arc/aero-arc-protos/gen/go/aeroarc/agent/v1"
)

// QueuedFrame wraps a TelemetryFrame with its associated WAL ID (if any).
type QueuedFrame struct {
	Frame *agentv1.TelemetryFrame
	WALID int64
}


