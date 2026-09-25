# Durable C2 execution

Agent advertises `mavlink_command_v1` and `mission_upload_v1` at registration.
The generic envelope carries numeric MAVLink long/int parameters, immutable
flight/intent authority, a deadline, and a canonical digest. API selects approved
definitions; Agent validates execution mechanisms and vehicle profile. New
numeric definitions do not require a new Agent build if they use existing
capabilities. New protocols or observation predicates require a capability rollout.

SQLite `c2_commands` persists immutable authority and evidence before effects.
An atomic first-effect permit prevents repeat generic MAVLink handoff after
response loss or restart. An interrupted effect becomes outcome unknown; recovery
replays facts or observes fresh vehicle messages. It never extends authorization.
Mission upload retains the existing journal/readback recovery rules.

Agent admission, autopilot acceptance, and observed state are independent facts.
Evidence returns over the existing authenticated telemetry stream and is replayed
on later exchanges. Telemetry WAL ACK semantics are unchanged. The API persists
command evidence and joins it into flight replay; it does not store telemetry.
No command journal retention/compaction is enabled yet.

Current execution profile is ArduCopter. ARM/DISARM retain the existing ACK
ambiguity fences. Other commands use a quiet ACK epoch and selected-target
checks. MAVLink does not echo the application command UUID, so a second GCS is
not fenced by this journal. PAUSE/RESUME observation is explicitly unavailable.
LAND observation requires fresh on-ground extended state, not just LAND mode.

Validate each definition with the target firmware in SITL and then hardware
before operational use. Unit/race tests exercise journal replay and simulated
ACK-versus-touchdown separation; they are not flight validation.
