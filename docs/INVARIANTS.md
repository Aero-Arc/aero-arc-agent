
## Aero Arc Agent — System Invariants (v0.1)

This document defines the **non-negotiable invariants** of the Aero Arc Agent.  
These rules encode the intent and constraints of the system and must not be violated without an explicit design decision.

They are written to preserve correctness, durability, and operability under real-world failure conditions.

---

## 1. Core System Invariants

### 1.1 WAL Is the System of Record

- All telemetry frames **must be durably written before being sent** to the relay
- No frame is transmitted over gRPC unless it has been durably persisted
- SQLite and finalized spool files together are the authoritative source for
  replay, recovery, and resend
- If the WAL is unavailable, telemetry ingestion must fail rather than bypass durability
- `AppendAsync` acceptance owns an immutable copy but is not itself a disk
  durability boundary; an abrupt process or power loss can lose the current
  unflushed batch

**Rationale:**
Durability is more important than availability. Persisted frames are crash-safe,
while batching makes the pre-flush capture window an explicit performance and
durability tradeoff.

---

### 1.2 Delivery Is At Least Once After Durable Admission

- Telemetry frames may be delivered more than once
- Exactly-once delivery is **not** a goal
- Frames are only marked as delivered after an explicit ACK from the relay
- The guarantee begins when a frame reaches SQLite or a synced spool file, not
  when it first enters the asynchronous memory queue
- A Relay ACK confirms admission by the official telemetry consumer; it does
  not prove that every downstream sink has durably committed the frame

**Rationale:**
Distributed systems favor correctness and durability over strict deduplication guarantees.

---

### 1.3 ACKs Are the Only Source of Truth for Delivery

- A frame is considered delivered **only** after the relay acknowledges that
  exact pending frame with `STATUS_OK`
- `STATUS_TEMPORARY_ERROR` and `STATUS_RETRY_WITH_BACKOFF` return the frame to
  the written queue and reconnect through the normal transport backoff
- `STATUS_PERMANENT_ERROR` never means delivered: the Agent atomically preserves
  the original payload and Relay diagnostic in durable quarantine
- ACK transitions are monotonic. A late contradictory ACK must not move a
  delivered or quarantined row back into the retry queue
- Pending frames may be retried indefinitely. Their TTL starts from a durable
  written-to-pending send epoch, never from the frame's capture time, because
  replayed telemetry can be old while its current send is still active.
- Batch senders reserve each row immediately before its own network Send.
  Cleanup cannot reset rows while a live batch owns them, and successful batch
  completion renews the ACK window for every row that remains pending.
- Stream teardown waits for ACK handling to quiesce, then immediately returns
  every still-pending peer to written. Status-conditional updates preserve
  terminal ACKs that already committed. If a worker misses the bounded
  teardown deadline, its rows remain fenced, but the supervised transport loop
  continues backoff and reconnect instead of silently ending Relay activity.

The deployed ACK contract contains `seq` and an optional `frame_id`, but not
`wal_id`. Current Relay versions omit `frame_id`, so deployed correlation is
limited to the authenticated stream and Agent-local sequence. When `frame_id`
is present, the Agent validates it against the durable payload before mutation.
A future protocol revision must echo `(wal_id, seq)` to make correlation complete
across WAL generations without relying on stream lifetime.

**Rationale:**  
The relay is the downstream system of record for delivery confirmation.

---

### 1.4 WAL Cursor Identity Is Composite

- `seq` is monotonic only within its WAL append generation
- Every successful WAL open rotates to a fresh non-nil UUID for newly captured
  frames
- Every persisted frame owns the `(wal_id, seq)` pair stamped before its first
  durable write
- Retry, reconnect, spool import, and process restart must preserve that pair
- A cloned, restored, recreated, or rollback-appended database must not cause a
  cursor pair to be reused

**Rationale:**
A row sequence alone is ambiguous across WAL lifetimes. The generation-scoped
cursor lets downstream systems distinguish retry from new capture without
coupling identity to a process or Relay session.

---

## 2. Lifecycle & Shutdown Invariants

### 2.1 Context Cancellation Is Authoritative

- All long-running loops must respect `ctx.Done()`
- Context cancellation must propagate through:
  - MAVLink ingest
  - WAL operations
  - gRPC reconnect and streaming loops

**Rationale:**  
The context defines lifecycle ownership and enables coordinated shutdown.

---

### 2.2 Shutdown Must Be Bounded

- Agent shutdown **must complete within a bounded amount of time**
- Shutdown must not block indefinitely waiting on:
  - Hardware I/O
  - Network I/O
  - Third-party library behavior

**Rationale:**  
The agent must be killable under all circumstances, including partial system failure.

---

### 2.3 MAVLink Shutdown Is Best-Effort

- MAVLink node shutdown is **best-effort**
- The agent must not block indefinitely waiting for MAVLink to close
- A timeout-based shutdown is required for MAVLink resources

**Rationale:**  
Hardware I/O and serial connections may block forever. The agent prioritizes process termination over graceful MAVLink teardown.

---

### 2.4 WAL Durability Is Reserved Before Hardware Teardown

- Lifecycle cancellation stops new ingest and reconnect work
- WAL close rejects late appends, durably spools its accepted in-memory batch,
  and must not close SQLite beneath an active writer
- The WAL close attempt occurs before potentially blocking best-effort MAVLink
  teardown so hardware cleanup cannot consume the durability deadline
- Shutdown is bounded; a deadline failure must be surfaced rather than reported
  as a successful durable close

**Rationale:**  
Accepted telemetry gets the first opportunity to reach durable storage while
hardware I/O remains best-effort.

---

## 3. Failure & Fault Tolerance Invariants

### 3.1 Relay May Be Unavailable Indefinitely

- The relay may be down, unreachable, or misconfigured
- The agent must retry connection with exponential backoff
- Telemetry ingestion must continue while disconnected (subject to WAL capacity)

**Rationale:**  
Edge systems must tolerate long periods of upstream unavailability.

---

### 3.2 Hardware and OS Resources May Misbehave

- Serial devices may block forever
- UDP sockets may never close cleanly
- File descriptors may exhaust

**Rationale:**  
The agent is designed to operate in hostile and resource-constrained environments.

---

### 3.3 WAL Capacity Is Finite

- WAL and spool storage are finite and may fill up
- When WAL capacity is exhausted, ingestion must fail loudly
- Silent data loss is not acceptable

**Rationale:**  
Backpressure must surface explicitly rather than corrupting system correctness.

---

### 3.4 Poison Data Must Be Isolated

- Nil, unserializable, and oversized frames must be rejected before queueing
- Malformed legacy SQLite rows and spool files must be quarantined with their
  diagnostic evidence retained
- One malformed record must not permanently block later valid telemetry

**Rationale:**
Durability includes preserving failure evidence, but availability requires a
bad record to have a bounded blast radius.

---

## 4. Concurrency Invariants

### 4.1 Goroutines Must Be Accounted For

- All long-lived goroutines must be owned and tracked
- Goroutines must exit on context cancellation
- Shutdown must wait (bounded) for goroutines to exit

**Rationale:**  
Untracked goroutines cause leaks, deadlocks, and undefined shutdown behavior.

---

### 4.2 No Goroutine May Block Forever on External Systems

- No goroutine may block indefinitely on:
  - Network I/O
  - Disk I/O
  - Third-party libraries

**Rationale:**  
All blocking operations must be cancellable or time-bounded.

---

## 5. Mission Deployment Invariants

- A deployment command is durably fingerprinted before any MAVLink effect.
  Reusing its ID with another payload is rejected; an exact terminal retry
  replays the stored result.
- Durable command IDs share one namespace across operation-context mutations
  and mission deployments. Reusing an ID across command kinds is rejected
  transactionally before either journal can admit the conflicting command.
- Deployment fails closed unless the active operation context exactly matches
  aircraft, flight, intent, and intent version, and fresh autopilot samples
  independently show both disarmed and on-ground state.
- If ArduPilot has not streamed fresh `EXTENDED_SYS_STATE`, the Agent issues one
  target-bound `MAV_CMD_REQUEST_MESSAGE` for message 245 and waits within the
  command timeout for a newer sample from that exact channel/system/component.
  Timeout or target movement still fails closed; absence never implies landed.
- Schema v1 accepts at most 200 contiguous `MAV_FRAME_GLOBAL` (frame `0`)
  mission items and only waypoint (`16`), land (`21`), and takeoff (`22`)
  commands. The shared Protos `missiondigest` encoder, rather than protobuf
  wire serialization, defines the cross-runtime canonical bytes and SHA-256.
- Canonical mission items require `current=false`. The Agent normalizes
  readback `current` to false because ArduPilot derives that bit from the live
  execution cursor rather than immutable stored mission content.
- Canonical plans exclude ArduPilot's volatile wire-sequence-zero HOME record.
  The adapter reads and reuses onboard HOME, shifts canonical items by one for
  upload, excludes HOME from `uploaded_item_count`, then drops HOME and shifts
  sequences back before readback digest verification.
- An accepted mission ACK can end an upload epoch only after that epoch handed
  off every requested wire item, including HOME. An accepted ACK buffered from
  an older timed-out upload must not trigger premature readback or a terminal
  mismatch for a partially replaced list.
- A repeated valid `MISSION_COUNT` restarts readback sequence progress and item
  storage together at wire sequence zero; stale items from the previous
  transfer epoch cannot leave holes in canonical readback.
- The first ArduPilot slice requires `autocontinue=true`, positive-zero
  parameters except LAND param4 exactly `+1`, and float32 altitude that
  round-trips bit-for-bit through ArduPilot signed-centimeter storage. Canonical
  E7 coordinates are authoritative and remain exact over `MISSION_ITEM_INT`;
  float-coordinate losslessness is checked only when an autopilot actually
  requests the legacy `MISSION_ITEM` fallback.
- `APPLIED` and `ALREADY_APPLIED` require a complete onboard mission readback
  whose canonical digest matches the requested plan. An ambiguous handoff,
  timeout, or incomplete readback is durably `OUTCOME_UNKNOWN`.
- An uncertain retry always reads the onboard mission first. It reports already
  applied when the digest matches. Before expiry, a complete mismatch may
  re-upload only behind a fresh exact operation binding plus disarmed/on-ground
  fences, and authority is rechecked at the actual `MISSION_COUNT` handoff
  boundary after HOME readback. After expiry, recovery is readback-only:
  mismatch is terminal and can never replace the onboard mission. First-seen
  expired commands are rejected without durable admission, while exact
  terminal replay remains available.
- Mission deployment replaces the stored mission only. It does not arm, start,
  advance, or complete a flight.

---

## 6. Non-Goals (Explicit)

The following are **not goals** of the Aero Arc Agent v0.1:

- Exactly-once delivery
- Real-time or low-latency guarantees
- Graceful shutdown of hardware under all conditions
- Lossless operation under infinite upstream outage
- Automatic WAL compaction or remote offload

---

## 7. Invariant Changes

Any change that violates one or more invariants **must** include:

1. An explicit design discussion
2. Updated invariants
3. Clear justification for the tradeoff

Silent erosion of invariants is considered a correctness bug.

---

## Summary

These invariants define what the Aero Arc Agent **guarantees**, **accepts**, and **refuses to do**.

They exist to:
- Preserve durability
- Prevent deadlocks
- Enable safe shutdown
- Make failure modes explicit

They are as important as the code itself.
