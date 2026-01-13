
## Aero Arc Agent — System Invariants (v0.1)

This document defines the **non-negotiable invariants** of the Aero Arc Agent.  
These rules encode the intent and constraints of the system and must not be violated without an explicit design decision.

They are written to preserve correctness, durability, and operability under real-world failure conditions.

---

## 1. Core System Invariants

### 1.1 WAL Is the System of Record

- All telemetry frames **must be written to the WAL before being sent** to the relay
- No frame is transmitted over gRPC unless it has been durably persisted
- The WAL is the authoritative source for replay, recovery, and resend
- If the WAL is unavailable, telemetry ingestion must fail rather than bypass durability

**Rationale:**  
Durability is more important than availability. The WAL provides crash safety and enables at-least-once delivery semantics.

---

### 1.2 At-Least-Once Delivery Is Guaranteed

- Telemetry frames may be delivered more than once
- Exactly-once delivery is **not** a goal
- Frames are only marked as delivered after an explicit ACK from the relay

**Rationale:**  
Distributed systems favor correctness and durability over strict deduplication guarantees.

---

### 1.3 ACKs Are the Only Source of Truth for Delivery

- A frame is considered delivered **only** after the relay acknowledges it
- Pending frames may be retried indefinitely
- Stuck pending frames may be reset after a TTL and retried

**Rationale:**  
The relay is the downstream system of record for delivery confirmation.

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

### 2.4 WAL Is Closed After Ingest Stops

- MAVLink ingest must stop before WAL shutdown
- WAL shutdown must unblock any waiting readers/writers
- WAL shutdown must not deadlock the process

**Rationale:**  
WAL durability and consistency depend on an orderly teardown sequence.

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

- WAL storage is finite and may fill up
- When WAL capacity is exhausted, ingestion must fail loudly
- Silent data loss is not acceptable

**Rationale:**  
Backpressure must surface explicitly rather than corrupting system correctness.

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
