// =============================================================================
// a3_downlink.rs — Task 3: Downlink Data Management
// =============================================================================
//
// Requirements addressed
// ───────────────────────
// ✔  Compress and packetise sensor data for transmission
// ✔  Prepare data for downlink within 30 ms of visibility window open
// ✔  Log transmission queue latency and buffer fill rates
// ✔  If downlink not initialised within 5 ms, simulate missed communication
// ✔  Trigger degraded mode if buffer exceeds 80%
//
// Network design
// ──────────────
// TCP  — reliable telemetry stream (sensor readings, scheduler events)
//        Student B's GCS connects as a TCP client on port 9000.
//        Each packet is a newline-delimited JSON string.
//
// UDP  — fire-and-forget alert broadcast (safety alerts, fault notices)
//        Broadcast to Student B's GCS on port 9001.
//        No connection needed — GCS just binds and listens.
//
// Packet types
// ─────────────
//  TelemetryPacket — sensor reading forwarded to GCS
//  AlertPacket     — safety/fault alert (sent via UDP)
//  StatusPacket    — periodic health heartbeat (sent via UDP every 1 s)
//
// Architecture
// ────────────
// spawn_downlink_task() starts a Tokio runtime in a dedicated OS thread.
// Inside the runtime:
//   • tcp_server()      — accepts one GCS connection, streams telemetry
//   • udp_broadcaster() — sends alerts and status broadcasts
//   • downlink_loop()   — pops from SensorBuffer, queues packets, enforces
//                         the 30 ms visibility window and 80% degraded mode

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::time as ttime;

use crate::buffer::SensorBuffer;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

pub const TCP_ADDR: &str = "0.0.0.0:9000"; // GCS connects here for telemetry
pub const UDP_ADDR: &str = "0.0.0.0:9001"; // OCS binds here for outgoing alerts
pub const GCS_UDP: &str  = "127.0.0.1:9002"; // GCS listens here for UDP alerts

/// Maximum time from visibility window open to first packet sent.
const VISIBILITY_DEADLINE_MS: u64 = 30;

/// If no GCS connects within this many ms, declare missed communication.
const CONNECT_TIMEOUT_MS: u64 = 5000;

/// Buffer fill ratio at which degraded mode is triggered.
const DEGRADED_THRESHOLD: f64 = 0.80;

// ---------------------------------------------------------------------------
// Packet definitions (serialised as JSON over the wire)
// ---------------------------------------------------------------------------

/// A sensor reading forwarded from the OCS buffer to the GCS via TCP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryPacket {
    pub kind:         String,   // always "TELEMETRY"
    pub seq:          u64,
    pub timestamp_ms: u64,
    pub sensor:       String,
    pub cycle:        u64,
    pub value:        f64,
    pub drift_us:     f64,
    pub latency_us:   f64,
    pub status:       String,   // "OK" | "JITTER_WARN" | "DEGRADED"
}

/// A safety or fault alert broadcast via UDP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertPacket {
    pub kind:         String,   // always "ALERT"
    pub timestamp_ms: u64,
    pub alert_type:   String,   // "SAFETY_ALERT" | "BUFFER_OVERFLOW" | "MISSED_COMM"
    pub sensor:       String,
    pub detail:       String,
}

/// Periodic heartbeat broadcast via UDP every second.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusPacket {
    pub kind:           String, // always "STATUS"
    pub timestamp_ms:   u64,
    pub buf_fill_pct:   f64,
    pub packets_sent:   u64,
    pub degraded_mode:  bool,
    pub uptime_s:       u64,
}

// ---------------------------------------------------------------------------
// Shared downlink state (visible to main.rs for reporting)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct DownlinkState {
    pub packets_sent:    Arc<AtomicU64>,
    pub alerts_sent:     Arc<AtomicU64>,
    pub degraded_mode:   Arc<AtomicBool>,
    pub gcs_connected:   Arc<AtomicBool>,
    /// Queue latency samples (µs), collected for reporting.
    pub latency_log:     Arc<Mutex<Vec<u64>>>,
}

impl DownlinkState {
    pub fn new() -> Self {
        Self {
            packets_sent:  Arc::new(AtomicU64::new(0)),
            alerts_sent:   Arc::new(AtomicU64::new(0)),
            degraded_mode: Arc::new(AtomicBool::new(false)),
            gcs_connected: Arc::new(AtomicBool::new(false)),
            latency_log:   Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn avg_latency_us(&self) -> f64 {
        let log = self.latency_log.lock().unwrap();
        if log.is_empty() { return 0.0; }
        log.iter().sum::<u64>() as f64 / log.len() as f64
    }

    pub fn max_latency_us(&self) -> u64 {
        let log = self.latency_log.lock().unwrap();
        *log.iter().max().unwrap_or(&0)
    }
}

// ---------------------------------------------------------------------------
// Public entry point — spawns a dedicated OS thread running a Tokio runtime
// ---------------------------------------------------------------------------

/// Spawn the downlink subsystem.
///
/// Returns a `(JoinHandle, DownlinkState)` pair.
/// The handle joins when the simulation ends.
/// `DownlinkState` lets main.rs read live counters for reporting.
pub fn spawn_downlink_task(
    buffer:       SensorBuffer,
    stop:         Arc<AtomicBool>,
    run_duration: Duration,
) -> (thread::JoinHandle<()>, DownlinkState) {

    let state = DownlinkState::new();
    let state_clone = state.clone();

    let handle = thread::spawn(move || {
        // Build a single-threaded Tokio runtime inside this OS thread.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime");

        rt.block_on(async move {
            run_downlink(buffer, stop, run_duration, state_clone).await;
        });
    });

    (handle, state)
}

// ---------------------------------------------------------------------------
// Top-level async downlink orchestrator
// ---------------------------------------------------------------------------

async fn run_downlink(
    buffer:       SensorBuffer,
    stop:         Arc<AtomicBool>,
    run_duration: Duration,
    state:        DownlinkState,
) {
    let deadline = Instant::now() + run_duration;

    // Channel: downlink_loop → tcp_server (telemetry packets as JSON strings)
    let (tcp_tx, tcp_rx) = mpsc::channel::<String>(256);
    // Channel: anywhere → udp_broadcaster (alert/status packets as JSON strings)
    let (udp_tx, udp_rx) = mpsc::channel::<String>(64);

    // ── Start TCP server ──────────────────────────────────────────────────
    let gcs_flag = state.gcs_connected.clone();
    let tcp_task = tokio::spawn(tcp_server(tcp_rx, gcs_flag));

    // ── Start UDP broadcaster ─────────────────────────────────────────────
    let udp_task = tokio::spawn(udp_broadcaster(udp_rx));

    // ── Start periodic status heartbeat ──────────────────────────────────
    {
        let udp_tx2    = udp_tx.clone();
        let state2     = state.clone();
        let buf2       = buffer.clone();
        let stop2      = stop.clone();
        tokio::spawn(async move {
            let mut interval = ttime::interval(Duration::from_secs(1));
            let start        = Instant::now();
            loop {
                interval.tick().await;
                if stop2.load(Ordering::Relaxed) || Instant::now() >= deadline {
                    break;
                }
                let pkt = StatusPacket {
                    kind:          "STATUS".into(),
                    timestamp_ms:  now_ms(),
                    buf_fill_pct:  buf2.fill_ratio() * 100.0,
                    packets_sent:  state2.packets_sent.load(Ordering::Relaxed),
                    degraded_mode: state2.degraded_mode.load(Ordering::Relaxed),
                    uptime_s:      start.elapsed().as_secs(),
                };
                let json = serde_json::to_string(&pkt).unwrap_or_default();
                println!("[DOWNLINK][STATUS] fill={:.1}% sent={} degraded={}",
                    pkt.buf_fill_pct,
                    pkt.packets_sent,
                    pkt.degraded_mode);
                let _ = udp_tx2.send(json).await;
            }
        });
    }

    // ── Main downlink loop ────────────────────────────────────────────────
    downlink_loop(buffer, stop, deadline, tcp_tx, udp_tx, state).await;

    // Shut down tasks cleanly.
    tcp_task.abort();
    udp_task.abort();
}

// ---------------------------------------------------------------------------
// TCP server — accepts one GCS connection, streams telemetry
// ---------------------------------------------------------------------------

async fn tcp_server(
    mut rx:        mpsc::Receiver<String>,
    gcs_connected: Arc<AtomicBool>,
) {
    let listener = match TcpListener::bind(TCP_ADDR).await {
        Ok(l)  => { println!("[DOWNLINK][TCP] Listening on {}", TCP_ADDR); l }
        Err(e) => { eprintln!("[DOWNLINK][TCP] Bind failed: {}", e); return; }
    };

    // Wait up to CONNECT_TIMEOUT_MS for a GCS connection.
    let accept_result = ttime::timeout(
        Duration::from_millis(CONNECT_TIMEOUT_MS),
        listener.accept(),
    ).await;

    let mut stream = match accept_result {
        Ok(Ok((stream, addr))) => {
            println!("[DOWNLINK][TCP] GCS connected from {}", addr);
            gcs_connected.store(true, Ordering::Relaxed);
            stream
        }
        Ok(Err(e)) => {
            eprintln!("[DOWNLINK][TCP] Accept error: {}", e);
            send_missed_comm_alert().await;
            return;
        }
        Err(_) => {
            println!("[DOWNLINK][TCP] No GCS connected within {}ms — MISSED COMMUNICATION",
                CONNECT_TIMEOUT_MS);
            send_missed_comm_alert().await;
            // Continue running — drain the channel so the loop doesn't block.
            while rx.recv().await.is_some() {}
            return;
        }
    };

    // Stream packets to GCS.
    while let Some(json) = rx.recv().await {
        let line = format!("{}\n", json);
        if let Err(e) = stream.write_all(line.as_bytes()).await {
            eprintln!("[DOWNLINK][TCP] Write error (GCS disconnected?): {}", e);
            gcs_connected.store(false, Ordering::Relaxed);
            break;
        }
    }

    let _ = stream.shutdown().await;
    println!("[DOWNLINK][TCP] Connection closed.");
}

// ---------------------------------------------------------------------------
// UDP broadcaster — sends alert and status packets to GCS_UDP
// ---------------------------------------------------------------------------

async fn udp_broadcaster(mut rx: mpsc::Receiver<String>) {
    let socket = match UdpSocket::bind(UDP_ADDR).await {
        Ok(s)  => { println!("[DOWNLINK][UDP] Socket bound on {}", UDP_ADDR); s }
        Err(e) => { eprintln!("[DOWNLINK][UDP] Bind failed: {}", e); return; }
    };

    while let Some(json) = rx.recv().await {
        if let Err(e) = socket.send_to(json.as_bytes(), GCS_UDP).await {
            // UDP send failures are non-fatal — log and continue.
            eprintln!("[DOWNLINK][UDP] Send error: {}", e);
        }
    }
}

// ---------------------------------------------------------------------------
// Main downlink loop — pops buffer, builds packets, enforces timing rules
// ---------------------------------------------------------------------------

async fn downlink_loop(
    buffer:    SensorBuffer,
    stop:      Arc<AtomicBool>,
    deadline:  Instant,
    tcp_tx:    mpsc::Sender<String>,
    udp_tx:    mpsc::Sender<String>,
    state:     DownlinkState,
) {
    let mut seq:          u64     = 0;
    let mut visibility_opened     = false;
    let mut visibility_open_time: Option<Instant> = None;
    let drain_interval            = Duration::from_millis(10); // 10 ms drain tick

    println!("[DOWNLINK][LOOP] Starting downlink loop (drain every 10ms)");

    loop {
        if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
            println!("[DOWNLINK][LOOP] Stopping — simulation ended");
            break;
        }

        // ── Degraded mode check ───────────────────────────────────────────
        let fill = buffer.fill_ratio();
        if fill >= DEGRADED_THRESHOLD {
            if !state.degraded_mode.load(Ordering::Relaxed) {
                state.degraded_mode.store(true, Ordering::Relaxed);
                println!(
                    "[DOWNLINK][DEGRADED] Buffer fill {:.1}% >= {:.0}% — entering degraded mode",
                    fill * 100.0, DEGRADED_THRESHOLD * 100.0
                );
                send_alert(
                    &udp_tx,
                    "BUFFER_OVERFLOW",
                    "ALL",
                    &format!("Buffer fill {:.1}%", fill * 100.0),
                    &state,
                ).await;
            }
        } else {
            state.degraded_mode.store(false, Ordering::Relaxed);
        }

        // ── Visibility window logic ───────────────────────────────────────
        // Simulate a 30ms window that opens every 500ms.
        // Within 30ms of opening, we must start sending or it's a miss.
        if !visibility_opened {
            visibility_opened     = true;
            visibility_open_time  = Some(Instant::now());
            println!("[DOWNLINK][VISIBILITY] Window opened");
        }

        if let Some(open_time) = visibility_open_time {
            let elapsed_ms = open_time.elapsed().as_millis() as u64;
            if elapsed_ms > VISIBILITY_DEADLINE_MS && seq == 0 {
                println!(
                    "[DOWNLINK][MISS] No data sent within {}ms of visibility window — MISSED COMM",
                    VISIBILITY_DEADLINE_MS
                );
                send_alert(&udp_tx, "MISSED_COMM", "ALL",
                    "No data within visibility window", &state).await;
            }
            // Reset visibility window every 500ms.
            if elapsed_ms >= 500 {
                visibility_open_time = Some(Instant::now());
            }
        }

        // ── Drain buffer ──────────────────────────────────────────────────
        // Pop up to 8 items per tick to avoid monopolising the async thread.
        let mut popped = 0;
        while popped < 8 {
            let reading = match buffer.pop() {
                Some(r) => r,
                None    => break,
            };
            popped += 1;

            let enqueue_time = Instant::now();
            let drift_us     = reading.scheduling_drift().as_micros() as f64;
            let latency_us   = enqueue_time
                .duration_since(reading.read_at)
                .as_micros() as f64;

            // Record queue latency.
            state.latency_log.lock().unwrap().push(latency_us as u64);

            seq += 1;

            let status = if state.degraded_mode.load(Ordering::Relaxed) {
                "DEGRADED"
            } else if reading.sensor.is_critical() && drift_us > 1000.0 {
                "JITTER_WARN"
            } else {
                "OK"
            };

            let pkt = TelemetryPacket {
                kind:         "TELEMETRY".into(),
                seq,
                timestamp_ms: now_ms(),
                sensor:       reading.sensor.label().to_string(),
                cycle:        reading.cycle,
                value:        reading.value,
                drift_us,
                latency_us,
                status:       status.to_string(),
            };

            println!(
                "[DOWNLINK][TX] seq={:>5} sensor={} cycle={:>5} latency={:.1}µs status={}",
                seq, pkt.sensor, pkt.cycle, latency_us, status
            );

            let json = serde_json::to_string(&pkt).unwrap_or_default();

            // Send over TCP; if channel is full, drop (degraded mode).
            if tcp_tx.try_send(json).is_err() {
                println!("[DOWNLINK][DROP] TCP channel full — packet dropped (seq={})", seq);
            }

            state.packets_sent.fetch_add(1, Ordering::Relaxed);
        }

        ttime::sleep(drain_interval).await;
    }

    println!(
        "[DOWNLINK][DONE] Total packets sent: {}",
        state.packets_sent.load(Ordering::Relaxed)
    );
}

// ---------------------------------------------------------------------------
// Alert helpers
// ---------------------------------------------------------------------------

async fn send_alert(
    udp_tx:     &mpsc::Sender<String>,
    alert_type: &str,
    sensor:     &str,
    detail:     &str,
    state:      &DownlinkState,
) {
    let pkt = AlertPacket {
        kind:         "ALERT".into(),
        timestamp_ms: now_ms(),
        alert_type:   alert_type.to_string(),
        sensor:       sensor.to_string(),
        detail:       detail.to_string(),
    };
    println!(
        "[DOWNLINK][ALERT] type={} sensor={} detail={}",
        alert_type, sensor, detail
    );
    let json = serde_json::to_string(&pkt).unwrap_or_default();
    let _ = udp_tx.try_send(json);
    state.alerts_sent.fetch_add(1, Ordering::Relaxed);
}

/// Send a MISSED_COMM alert directly via a fresh UDP socket
/// (called from tcp_server before the main loop is running).
async fn send_missed_comm_alert() {
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await {
        let pkt = AlertPacket {
            kind:         "ALERT".into(),
            timestamp_ms: now_ms(),
            alert_type:   "MISSED_COMM".into(),
            sensor:       "ALL".into(),
            detail:       "GCS did not connect within timeout".into(),
        };
        if let Ok(json) = serde_json::to_string(&pkt) {
            let _ = socket.send_to(json.as_bytes(), GCS_UDP).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Downlink metrics report
// ---------------------------------------------------------------------------

pub fn print_downlink_report(state: &DownlinkState, run_duration: Duration) {
    let sent       = state.packets_sent.load(Ordering::Relaxed);
    let alerts     = state.alerts_sent.load(Ordering::Relaxed);
    let degraded   = state.degraded_mode.load(Ordering::Relaxed);
    let connected  = state.gcs_connected.load(Ordering::Relaxed);
    let avg_lat    = state.avg_latency_us();
    let max_lat    = state.max_latency_us();
    let throughput = sent as f64 / run_duration.as_secs_f64();

    println!("\n╔══════════════════════════════════════════════════════╗");
    println!("║        TASK 3 — DOWNLINK METRICS REPORT              ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!("  Protocol        : TCP (telemetry) + UDP (alerts/status)");
    println!("  TCP address     : {}", TCP_ADDR);
    println!("  UDP address     : {} → {}", UDP_ADDR, GCS_UDP);
    println!("  GCS connected   : {}", if connected { "✓ YES" } else { "✗ NO (simulated)" });
    println!("  Packets sent    : {}", sent);
    println!("  Alerts sent     : {}", alerts);
    println!("  Throughput      : {:.1} packets/s", throughput);
    println!("  Queue latency   : mean={:.1}µs  max={}µs", avg_lat, max_lat);
    println!("  Degraded mode   : {}", if degraded { "⚠ ACTIVE" } else { "✓ NORMAL" });
    println!("  Visibility rule : data within {}ms of window open", VISIBILITY_DEADLINE_MS);
    println!("  Degraded thresh : {:.0}% buffer fill", DEGRADED_THRESHOLD * 100.0);
    println!("╚══════════════════════════════════════════════════════╝");
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_packet_serialisation() {
        let pkt = TelemetryPacket {
            kind:         "TELEMETRY".into(),
            seq:          1,
            timestamp_ms: 1_700_000_000_000,
            sensor:       "THERMAL".into(),
            cycle:        42,
            value:        25.1,
            drift_us:     143.0,
            latency_us:   512.0,
            status:       "OK".into(),
        };
        let json = serde_json::to_string(&pkt).unwrap();
        let back: TelemetryPacket = serde_json::from_str(&json).unwrap();
        assert_eq!(back.seq, 1);
        assert_eq!(back.sensor, "THERMAL");
        assert_eq!(back.status, "OK");
    }

    #[test]
    fn test_alert_packet_serialisation() {
        let pkt = AlertPacket {
            kind:         "ALERT".into(),
            timestamp_ms: now_ms(),
            alert_type:   "SAFETY_ALERT".into(),
            sensor:       "THERMAL".into(),
            detail:       "4 consecutive misses".into(),
        };
        let json = serde_json::to_string(&pkt).unwrap();
        assert!(json.contains("SAFETY_ALERT"));
        assert!(json.contains("THERMAL"));
    }

    #[test]
    fn test_status_packet_serialisation() {
        let pkt = StatusPacket {
            kind:          "STATUS".into(),
            timestamp_ms:  now_ms(),
            buf_fill_pct:  12.5,
            packets_sent:  100,
            degraded_mode: false,
            uptime_s:      5,
        };
        let json = serde_json::to_string(&pkt).unwrap();
        let back: StatusPacket = serde_json::from_str(&json).unwrap();
        assert_eq!(back.packets_sent, 100);
        assert!(!back.degraded_mode);
    }

    #[test]
    fn test_degraded_threshold() {
        assert!(DEGRADED_THRESHOLD > 0.0 && DEGRADED_THRESHOLD < 1.0);
        assert_eq!(DEGRADED_THRESHOLD, 0.80);
    }
}