
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
- Pending frames may be retried indefinitely
- Stuck pending frames may be reset after a TTL and retried

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

## 5. Non-Goals (Explicit)

The following are **not goals** of the Aero Arc Agent v0.1:

- Exactly-once delivery
- Real-time or low-latency guarantees
- Graceful shutdown of hardware under all conditions
- Lossless operation under infinite upstream outage
- Automatic WAL compaction or remote offload

---

## 6. Invariant Changes

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
