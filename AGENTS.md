# Aero Arc Agent contributor context

## Service purpose

- Agent is the edge process installed on a UAV companion computer.
- It reads MAVLink from the configured serial device (or the debug UDP
  endpoint), converts supported messages into typed protobuf telemetry frames,
  and sends them to Relay over authenticated gRPC.
- `internal/wal` is the durable SQLite write-ahead log between MAVLink ingest
  and network delivery. A transient Relay outage must not require the aircraft
  to remain connected or cause queued frames to be silently discarded.
- Relay owns server-side admission and routing; Agent does not write directly
  to InfluxDB or discover aircraft business records from API.

## Delivery invariants

- Persist a telemetry frame before attempting network delivery. WAL sequence
  numbers are scoped by the `wal_id` append generation stamped into the frame.
  A successful WAL open rotates to a new generation for newly captured frames;
  frames already persisted retain their original generation across replay.
- Keep the composite `(wal_id, seq)` cursor and capture time stable across
  retries. Never restamp a persisted frame with the current open generation.
- `AppendAsync` acceptance transfers an immutable frame copy to the in-memory
  writer queue; it is crash-durable only after the batch reaches SQLite or a
  synced spool file. Graceful close attempts to spool every accepted frame.
- Mark a WAL entry delivered only after Relay acknowledges that exact frame.
- Preserve operation context with each captured frame so a later retry does
  not inherit a different mission assignment.
- Treat registration and telemetry streaming as one reconnecting lifecycle.
  Cancellation must stop serial ingest, stream work, and pending WAL activity.
- Bearer credentials belong in outgoing gRPC metadata and must require TLS
  unless an explicit development-only option disables verification.

## Repository layout

- `cmd/aero-arc-agent` defines CLI flags and process startup.
- `internal/agent` owns MAVLink ingest, Relay registration, streaming, ACK
  handling, reconnection, and operation-context commands.
- `internal/wal` owns durable telemetry and operation-context persistence.
- `internal/identity` resolves the stable Agent installation identity.
- Public cross-service contracts come from `aero-arc-protos`; never edit
  generated protobuf code in this repository.

## Go documentation

- Every exported handwritten Go function or method must have a lint-valid Go
  doc comment beginning with its exact identifier.
- Make hover documentation useful: explain observable behavior and important
  invariants, identify each parameter, and document return values and expected
  error conditions. Avoid comments that merely restate the Go signature.
- Use `Parameters:` and `Returns:` sections for multi-argument, lifecycle,
  persistence, authentication, or concurrency-sensitive APIs. A concise
  identifier-led sentence is sufficient for a genuinely trivial accessor.
- Do not hand-edit comments in generated Go files.

## Validation

Before handoff, run `gofmt` on changed Go files, `go test ./...`,
`go test -race ./...`, `go vet ./...`, and `git diff --check`.
