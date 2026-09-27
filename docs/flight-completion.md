# Flight completion evidence

Completion tracking is bound to the immutable, applied MISSION_START command and
its verified terminal RTL/LAND mission. It requires observed airborne state,
terminal mission progress (or early RTL/LAND), and landed/disarmed samples within
five seconds of each other. A restart restores flight milestones but discards
cached vehicle samples. Missing evidence remains unresolved. While a watch is
active, Agent requests EXTENDED_SYS_STATE and MISSION_CURRENT once per second;
completion does not depend on ground-station stream configuration.

The immutable event and completed watch are committed together. Relay receipts
bind event ID and payload SHA-256. Unsupported Relays leave events queued without
interrupting telemetry. The journal survives process restart; retained delivery
tombstones prevent duplicate notifications from changing history.

Opt-in real simulator coverage (owns its simulator; never uses the demo):

```
AERO_AGENT_TEST_SITL_BINARY=/absolute/path/to/arducopter \
  go test -tags=sitl -run TestSITLOnboardRTLProducesDurableCompletion \
  -timeout=4m -v ./internal/agent
```

RTL uses the autopilot's HOME and recovery parameters. Automatic API closure
waits for actual landing/disarm, including when RTL settings leave it hovering.

Completion observation capture times must follow the persisted post-MAVLink
handoff boundary. Watches from older versions without that boundary fail closed
and require operator reconciliation; command issue time is not a substitute.
Malformed or identity/digest-mismatched completion rows are preserved in SQLite
and excluded through `flight_completion_quarantine`, with a logged reason for
operator repair. They never acknowledge delivery or block healthy later events.
