# Aero Arc Agent

A lightweight edge-side agent that runs on a UAV’s companion computer and provides a reliable, backpressure-aware telemetry pipeline to the Aero Arc Relay.

The agent identifies the drone, registers with the relay, opens a bi-directional gRPC stream, and pushes telemetry frames with controlled flow — acting as the ingress point for drone data into modern cloud infrastructure.

This is the official client-side component of the Aero Arc open-source telemetry stack.

## Highlights

- Registration handshake (drone identity + hardware metadata)
- Duplex telemetry stream using gRPC
- Backpressure-aware sending honoring relay limits
- Automatic reconnection with exponential backoff
- Lightweight footprint suitable for Jetson, Raspberry Pi, and x86
- Pluggable telemetry sources (MAVLink ingestion module coming soon)
- Consistent, structured, predictable behavior under network instability

## Why the Aero Arc Agent Exists

Telemetry pipelines in the drone ecosystem are usually:

- Tightly coupled to proprietary cloud platforms  
- Implemented with fragile ad-hoc scripts  
- Relying on UDP broadcast without delivery guarantees  
- Lacking identity, schema, or structured flow control  

The Aero Arc Agent brings modern infra patterns — flow control, resilience, observability, typed RPC APIs — to UAV telemetry.

If you're building:

- Drone fleets
- Remote inspection services
- Autonomous agriculture systems
- Robotics R&D test rigs
- Digital twin pipelines

…you finally have a clean, open-source telemetry ingestion layer that doesn’t require reverse engineering or vendor lock-in.

## Quick Start

### Install

Using `go install`:

```bash
go install github.com/aero-arc/aero-arc-agent/cmd/aero-agent@latest
```

Or build from source:

```bash
git clone https://github.com/aero-arc/aero-arc-agent
cd aero-arc-agent
make build
```

### Run

```bash
aero-agent \
  --relay=relay.aeroarc.io:443 \
  --agent-id=$(hostname) \
  --drone-id=drone-001 \
  --model="CustomQuad" \
  --firmware="ArduPilot 4.5.0"
```

## How It Works

The agent performs three key tasks:

1. **Register the drone**
   - On startup, the agent sends a `RegisterRequest` containing:
     - agent ID  
     - drone ID  
     - hardware UID  
     - model, serial, firmware  
     - platform + agent version  
   - The relay responds with:
     - a session ID  
     - `max_inflight` (how many telemetry frames may be pending)  
   - This establishes the backpressure contract.

2. **Open a telemetry stream**
   - Once registered, the agent opens a gRPC stream:
     - Outbound: `TelemetryFrame`  
     - Inbound: `TelemetryAck`  
   - Frames stay in a bounded queue until acknowledged.
   - If the relay slows down, the agent slows down too — preventing memory blowup or firehose behavior.

3. **Recover automatically**
   - If the relay restarts or the agent loses network:
     - the stream closes  
     - the agent buffers locally  
     - retries with exponential backoff  
     - re-registers  
     - resumes streaming  
   - This ensures continuity even on unstable connections.

## Configuration

Common CLI flags:

| Flag              | Description                              |
| ----------------- | ---------------------------------------- |
| `--relay`         | Relay gRPC endpoint                      |
| `--agent-id`      | Unique ID for this agent                 |
| `--drone-id`      | Stable UAV identifier                    |
| `--model`         | Drone model name                         |
| `--firmware`      | Flight firmware version                  |
| `--hardware-uid`  | Override auto-detected hardware UID      |
| `--platform`      | OS + architecture string                 |
| `--agent-version` | Agent version override                   |

A config file format is planned for future releases.

## Project Status & Roadmap

The agent is early but functional. The core RPC contract is stable.

### v0.1

- [x] Registration  
- [x] Telemetry stream  
- [x] Backpressure enforcement  
- [x] Basic CLI + metadata  
- [x] Automatic reconnection  

### v0.2

- [ ] MAVLink ingestion module  
- [ ] Frame translation pipeline  
- [ ] Rate control for high-throughput sensors  

### v0.3

- [ ] Local durability (WAL)  
- [ ] Crash-safe replay  
- [ ] Delivery guarantees  

### v1.0

- [ ] Hardened APIs  
- [ ] Production reference deployments  
- [ ] ARM64/ARMv7 signed releases  
- [ ] Kubernetes integration for swarm testing  

## Contributing

Contributions are welcome — especially from:

- Robotics teams  
- Drone researchers  
- PX4 / ArduPilot developers  
- Platform integrators  
- Telemetry / infra engineers  

If you’re using the agent on custom hardware, please open an issue so we can track compatibility and improve the edge experience.

## Related Projects

- **Aero Arc Relay** — cloud-side telemetry ingestion and sink fan-out  
- **Aero Arc Protos** — shared protobuf API definitions  

Together, these form the foundation of the Aero Arc open-source ecosystem.