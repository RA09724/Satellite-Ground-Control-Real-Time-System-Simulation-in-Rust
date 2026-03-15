# RTS_IA_TP071496 — Satellite OCS Real-Time System

**CT087-3-3 Real-Time Systems | Asia Pacific University**  
**Student A — Satellite Onboard Control System (OCS)**

---

## Overview

This project simulates a real-time coordination system between a CubeSat in low Earth orbit and a Ground Control Station (GCS). It implements **Student A's** component — the Satellite Onboard Control System — in Rust.

---

## Tasks Implemented

| Task | Description |
|------|-------------|
| Task 1 | Sensor Data Acquisition & Prioritization |
| Task 2 | Real-Time Task Scheduling (Rate-Monotonic) |
| Task 3 | Downlink Data Management (TCP + UDP via Tokio) |
| Task 4 | Benchmarking & Fault Simulation |

---

## Requirements

- [Rust](https://rustup.rs/) (edition 2021)
- Windows recommended (thread affinity and TIME_CRITICAL priority used for accurate timing)

---

## How to Run

```bash
# Clone the repository
git clone https://github.com/<your-username>/RTS_IA_TP071496.git
cd RTS_IA_TP071496

# Build and run in release mode (required for real-time accuracy)
cargo run --release

# Run unit tests
cargo test
```

> **Important:** Always use `--release`. Debug mode disables optimisations that are critical for the spin-wait timing in Task 1.

---

## GCS Integration (Student B)

When the simulation runs, the OCS opens two network endpoints for Student B's GCS:

| Protocol | Address | Purpose |
|----------|---------|---------|
| TCP | `127.0.0.1:9000` | Telemetry stream (one JSON line per packet) |
| UDP | `127.0.0.1:9002` | Alerts and status heartbeats |

**Telemetry packet format (TCP):**
```json
{"kind":"TELEMETRY","seq":1,"timestamp_ms":1700000000000,"sensor":"THERMAL","cycle":1,"value":24.52,"drift_us":176.0,"latency_us":512.0,"status":"OK"}
```

**Alert packet format (UDP):**
```json
{"kind":"ALERT","timestamp_ms":1700000000000,"alert_type":"SAFETY_ALERT","sensor":"THERMAL","detail":"4 consecutive misses"}
```

**Status heartbeat format (UDP, every 1s):**
```json
{"kind":"STATUS","timestamp_ms":1700000000000,"buf_fill_pct":3.1,"packets_sent":48,"degraded_mode":false,"uptime_s":3}
```

---

## Project Structure

```
src/
├── main.rs           # Entry point — wires all tasks together
├── types.rs          # Shared data structures (SensorType, SensorReading, etc.)
├── buffer.rs         # Bounded priority buffer with overflow logging
├── metrics.rs        # Thread-safe running statistics (drift, jitter, latency)
├── a1_sensor.rs      # Task 1: Sensor acquisition threads
├── a2_scheduler.rs   # Task 2: Rate-Monotonic scheduler with preemption
├── a3_downlink.rs    # Task 3: TCP/UDP downlink via Tokio
└── a4_benchmark.rs   # Task 4: Fault injection and benchmarking
Cargo.toml            # Dependencies (tokio, serde, winapi)
```

---

## Simulation Output Files

| File | Description |
|------|-------------|
| `fault_log.txt` | Timestamped fault injection and recovery log (Task 4) |

---

## Key Design Decisions

- **Thread affinity** — each sensor thread is pinned to a dedicated CPU core (cores 1–3) and each scheduler task to cores 4–7, preventing spin-wait contention between threads.
- **TIME_CRITICAL priority** — sensor threads are elevated to Windows TIME_CRITICAL scheduling priority for sub-millisecond jitter.
- **Rate-Monotonic scheduling** — shorter period = higher priority. Liu & Layland schedulability verified at runtime (utilisation 0.30 < bound 0.757).
- **TCP for telemetry, UDP for alerts** — TCP guarantees ordered delivery for mission data; UDP minimises latency for time-critical alerts.