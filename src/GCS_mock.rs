// =============================================================================
// gcs_mock.rs — Student B Mock GCS (Ground Control Station)
// =============================================================================
//
// Simulates Student B's GCS receiving and depacketising data from Student A's
// OCS in real-time.
//
// TCP port 9000 — connects to OCS telemetry stream (one JSON line per packet)
// UDP port 9002 — listens for OCS alert and status datagrams
//
// Build and run BEFORE starting Student A's OCS:
//   rustc gcs_mock.rs -o gcs_mock --edition 2021
//   .\gcs_mock.exe
//
// OR add it as a separate binary in Cargo.toml (see instructions below)
// and run:
//   cargo run --release --bin gcs_mock
//
// Then in a separate terminal:
//   cargo run --release --bin RTS_IA_TP071496

use std::io::{BufRead, BufReader};
use std::net::{TcpStream, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const OCS_HOST:     &str = "127.0.0.1";
const TCP_PORT:     u16  = 9000;
const UDP_BIND:     &str = "0.0.0.0:9002";
const RETRY_DELAY:  u64  = 2;   // seconds between TCP reconnect attempts

// ---------------------------------------------------------------------------
// ANSI colour codes
// ---------------------------------------------------------------------------

const GREEN:  &str = "\x1b[92m";
const YELLOW: &str = "\x1b[93m";
const RED:    &str = "\x1b[91m";
const CYAN:   &str = "\x1b[96m";
const BOLD:   &str = "\x1b[1m";
const RESET:  &str = "\x1b[0m";

// ---------------------------------------------------------------------------
// Packet types (mirrors a3_downlink.rs structures)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct TelemetryPacket {
    seq:          u64,
    timestamp_ms: u64,
    sensor:       String,
    cycle:        u64,
    value:        f64,
    drift_us:     f64,
    latency_us:   f64,
    status:       String,
}

#[derive(Debug)]
struct AlertPacket {
    timestamp_ms: u64,
    alert_type:   String,
    sensor:       String,
    detail:       String,
}

#[derive(Debug)]
struct StatusPacket {
    timestamp_ms:  u64,
    buf_fill_pct:  f64,
    packets_sent:  u64,
    degraded_mode: bool,
    uptime_s:      u64,
}

#[derive(Debug)]
enum OcsPacket {
    Telemetry(TelemetryPacket),
    Alert(AlertPacket),
    Status(StatusPacket),
    Unknown(String),
}

// ---------------------------------------------------------------------------
// Running statistics (thread-safe counters)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct GcsStats {
    telemetry: Arc<AtomicU64>,
    alerts:    Arc<AtomicU64>,
    status:    Arc<AtomicU64>,
    corrupt:   Arc<AtomicU64>,
    jitter:    Arc<AtomicU64>,
    start:     Instant,
}

impl GcsStats {
    fn new() -> Self {
        Self {
            telemetry: Arc::new(AtomicU64::new(0)),
            alerts:    Arc::new(AtomicU64::new(0)),
            status:    Arc::new(AtomicU64::new(0)),
            corrupt:   Arc::new(AtomicU64::new(0)),
            jitter:    Arc::new(AtomicU64::new(0)),
            start:     Instant::now(),
        }
    }
}

// ---------------------------------------------------------------------------
// Depacketiser — parse raw JSON into typed packets
// ---------------------------------------------------------------------------

/// Parse a raw JSON string into an OcsPacket.
/// This is the core depacketisation function — it reads the "kind" field
/// first to determine packet type, then extracts all fields.
fn depacketise(raw: &str) -> OcsPacket {
    // Extract the "kind" field to determine packet type.
    let kind = extract_str_field(raw, "kind").unwrap_or_default();

    match kind.as_str() {
        "TELEMETRY" => {
            let pkt = TelemetryPacket {
                seq:          extract_u64_field(raw, "seq").unwrap_or(0),
                timestamp_ms: extract_u64_field(raw, "timestamp_ms").unwrap_or(0),
                sensor:       extract_str_field(raw, "sensor").unwrap_or_default(),
                cycle:        extract_u64_field(raw, "cycle").unwrap_or(0),
                value:        extract_f64_field(raw, "value").unwrap_or(f64::NAN),
                drift_us:     extract_f64_field(raw, "drift_us").unwrap_or(0.0),
                latency_us:   extract_f64_field(raw, "latency_us").unwrap_or(0.0),
                status:       extract_str_field(raw, "status").unwrap_or_default(),
            };
            OcsPacket::Telemetry(pkt)
        }
        "ALERT" => {
            let pkt = AlertPacket {
                timestamp_ms: extract_u64_field(raw, "timestamp_ms").unwrap_or(0),
                alert_type:   extract_str_field(raw, "alert_type").unwrap_or_default(),
                sensor:       extract_str_field(raw, "sensor").unwrap_or_default(),
                detail:       extract_str_field(raw, "detail").unwrap_or_default(),
            };
            OcsPacket::Alert(pkt)
        }
        "STATUS" => {
            let pkt = StatusPacket {
                timestamp_ms:  extract_u64_field(raw, "timestamp_ms").unwrap_or(0),
                buf_fill_pct:  extract_f64_field(raw, "buf_fill_pct").unwrap_or(0.0),
                packets_sent:  extract_u64_field(raw, "packets_sent").unwrap_or(0),
                degraded_mode: extract_bool_field(raw, "degraded_mode").unwrap_or(false),
                uptime_s:      extract_u64_field(raw, "uptime_s").unwrap_or(0),
            };
            OcsPacket::Status(pkt)
        }
        _ => OcsPacket::Unknown(raw.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Display functions — print depacketised data in a human-readable format
// ---------------------------------------------------------------------------

fn display_telemetry(pkt: &TelemetryPacket, stats: &GcsStats) {
    stats.telemetry.fetch_add(1, Ordering::Relaxed);

    // Determine colour and unit based on sensor and status.
    let colour = match pkt.status.as_str() {
        "JITTER_WARN" => {
            stats.jitter.fetch_add(1, Ordering::Relaxed);
            YELLOW
        }
        "DEGRADED" => YELLOW,
        _ if pkt.value.is_nan() => {
            stats.corrupt.fetch_add(1, Ordering::Relaxed);
            RED
        }
        _ => GREEN,
    };

    let unit = match pkt.sensor.as_str() {
        "THERMAL"  => "°C ",
        "POWER"    => "V  ",
        "ATTITUDE" => "rad",
        _          => "   ",
    };

    // Format value — NaN displays as CORRUPT.
    let val_str = if pkt.value.is_nan() {
        "  CORRUPT".to_string()
    } else {
        format!("{:>9.3}", pkt.value)
    };

    println!(
        "{colour}[TCP][TELEMETRY] {ts} \
         seq={seq:>5} | {sensor:<8} | cycle={cycle:>5} | \
         val={val} {unit}| drift={drift:>7.1}µs | \
         latency={lat:>8.1}µs | {status}{RESET}",
        colour  = colour,
        ts      = timestamp(),
        seq     = pkt.seq,
        sensor  = pkt.sensor,
        cycle   = pkt.cycle,
        val     = val_str,
        unit    = unit,
        drift   = pkt.drift_us,
        lat     = pkt.latency_us,
        status  = pkt.status,
        RESET   = RESET,
    );
}

fn display_alert(pkt: &AlertPacket, stats: &GcsStats) {
    stats.alerts.fetch_add(1, Ordering::Relaxed);

    let colour = if pkt.alert_type.contains("SAFETY") || pkt.detail.contains("ABORT") {
        RED
    } else {
        YELLOW
    };

    println!(
        "\n{colour}{BOLD}[UDP][ALERT]     {ts} \
         ⚠  TYPE={atype} | SENSOR={sensor} | {detail}{RESET}\n",
        colour  = colour,
        BOLD    = BOLD,
        ts      = timestamp(),
        atype   = pkt.alert_type,
        sensor  = pkt.sensor,
        detail  = pkt.detail,
        RESET   = RESET,
    );
}

fn display_status(pkt: &StatusPacket, stats: &GcsStats) {
    stats.status.fetch_add(1, Ordering::Relaxed);

    let (colour, mode_str) = if pkt.degraded_mode {
        (YELLOW, format!("{RED}⚠ DEGRADED{RESET}", RED = RED, RESET = RESET))
    } else {
        (CYAN, format!("{GREEN}NOMINAL{RESET}", GREEN = GREEN, RESET = RESET))
    };

    println!(
        "{colour}[UDP][STATUS]    {ts} \
         uptime={uptime:>4}s | buf_fill={fill:>5.1}% | \
         sent={sent:>5} | mode={mode}{RESET}",
        colour  = colour,
        ts      = timestamp(),
        uptime  = pkt.uptime_s,
        fill    = pkt.buf_fill_pct,
        sent    = pkt.packets_sent,
        mode    = mode_str,
        RESET   = RESET,
    );
}

fn display_stats(stats: &GcsStats) {
    let elapsed = stats.start.elapsed().as_secs_f64();
    let tel     = stats.telemetry.load(Ordering::Relaxed);
    let rate    = if elapsed > 0.0 { tel as f64 / elapsed } else { 0.0 };

    println!(
        "\n{CYAN}{BOLD}\
         ┌─ GCS RUNNING TOTALS ({elapsed:.0}s) ──────────────────────────────────┐\n\
         │  Telemetry packets : {tel:>6}  ({rate:.1} pkt/s)\n\
         │  Status heartbeats : {status:>6}\n\
         │  Alerts received   : {alerts:>6}\n\
         │  Jitter warnings   : {jitter:>6}\n\
         │  Corrupt readings  : {corrupt:>6}\n\
         └────────────────────────────────────────────────────────────────────┘\
         {RESET}\n",
        CYAN    = CYAN,
        BOLD    = BOLD,
        elapsed = elapsed,
        tel     = tel,
        rate    = rate,
        status  = stats.status.load(Ordering::Relaxed),
        alerts  = stats.alerts.load(Ordering::Relaxed),
        jitter  = stats.jitter.load(Ordering::Relaxed),
        corrupt = stats.corrupt.load(Ordering::Relaxed),
        RESET   = RESET,
    );
}

// ---------------------------------------------------------------------------
// TCP receiver — connects to OCS and reads telemetry stream
// ---------------------------------------------------------------------------

fn tcp_receiver(stats: GcsStats) {
    loop {
        println!(
            "{CYAN}[GCS] Connecting to OCS telemetry at {OCS_HOST}:{TCP_PORT}...{RESET}",
            CYAN  = CYAN,
            RESET = RESET,
        );

        match TcpStream::connect(format!("{OCS_HOST}:{TCP_PORT}")) {
            Ok(stream) => {
                println!(
                    "{GREEN}[GCS] TCP connected to OCS ✓{RESET}\n",
                    GREEN = GREEN,
                    RESET = RESET,
                );

                let reader = BufReader::new(stream);

                for line in reader.lines() {
                    match line {
                        Ok(raw) if !raw.trim().is_empty() => {
                            match depacketise(&raw) {
                                OcsPacket::Telemetry(pkt) => display_telemetry(&pkt, &stats),
                                OcsPacket::Alert(pkt)     => display_alert(&pkt, &stats),
                                OcsPacket::Status(pkt)    => display_status(&pkt, &stats),
                                OcsPacket::Unknown(raw)   => {
                                    eprintln!(
                                        "{RED}[GCS] Unknown packet: {}{RESET}",
                                        &raw[..raw.len().min(80)],
                                        RED   = RED,
                                        RESET = RESET,
                                    );
                                }
                            }
                        }
                        Ok(_) => {} // empty line — skip
                        Err(e) => {
                            println!(
                                "{YELLOW}[GCS] TCP stream ended: {e}{RESET}",
                                YELLOW = YELLOW,
                                RESET  = RESET,
                            );
                            break;
                        }
                    }
                }

                println!("{YELLOW}[GCS] OCS disconnected.{RESET}", YELLOW = YELLOW, RESET = RESET);
            }
            Err(_) => {
                println!(
                    "{YELLOW}[GCS] OCS not ready, retrying in {RETRY_DELAY}s...{RESET}",
                    YELLOW      = YELLOW,
                    RETRY_DELAY = RETRY_DELAY,
                    RESET       = RESET,
                );
            }
        }

        thread::sleep(Duration::from_secs(RETRY_DELAY));
    }
}

// ---------------------------------------------------------------------------
// UDP receiver — listens for alerts and status heartbeats
// ---------------------------------------------------------------------------

fn udp_receiver(stats: GcsStats) {
    let socket = UdpSocket::bind(UDP_BIND)
        .unwrap_or_else(|e| panic!("Failed to bind UDP on {}: {}", UDP_BIND, e));

    println!(
        "{CYAN}[GCS] UDP listening on {UDP_BIND}{RESET}",
        CYAN    = CYAN,
        UDP_BIND = UDP_BIND,
        RESET   = RESET,
    );

    let mut buf = vec![0u8; 65535];

    loop {
        match socket.recv_from(&mut buf) {
            Ok((n, _addr)) => {
                let raw = String::from_utf8_lossy(&buf[..n]);
                match depacketise(&raw) {
                    OcsPacket::Alert(pkt)   => display_alert(&pkt, &stats),
                    OcsPacket::Status(pkt)  => display_status(&pkt, &stats),
                    OcsPacket::Unknown(raw) => {
                        eprintln!(
                            "{RED}[GCS] Malformed UDP datagram: {}{RESET}",
                            &raw[..raw.len().min(80)],
                            RED   = RED,
                            RESET = RESET,
                        );
                    }
                    _ => {}
                }
            }
            Err(e) => eprintln!("{RED}[GCS] UDP recv error: {e}{RESET}", RED = RED, RESET = RESET),
        }
    }
}

// ---------------------------------------------------------------------------
// Stats printer — prints running totals every 30 seconds
// ---------------------------------------------------------------------------

fn stats_printer(stats: GcsStats) {
    loop {
        thread::sleep(Duration::from_secs(30));
        display_stats(&stats);
    }
}

// ---------------------------------------------------------------------------
// JSON field extractors — lightweight, no external crate needed
// ---------------------------------------------------------------------------

/// Extract a string value from a flat JSON object by key.
/// e.g. extract_str_field(`{"kind":"TELEMETRY","sensor":"THERMAL"}`, "sensor")
///      → Some("THERMAL")
fn extract_str_field(json: &str, key: &str) -> Option<String> {
    let pattern = format!("\"{}\":\"", key);
    let start   = json.find(&pattern)? + pattern.len();
    let end     = json[start..].find('"')? + start;
    Some(json[start..end].to_string())
}

/// Extract a u64 numeric value from a flat JSON object by key.
fn extract_u64_field(json: &str, key: &str) -> Option<u64> {
    let pattern = format!("\"{}\":", key);
    let start   = json.find(&pattern)? + pattern.len();
    let rest    = json[start..].trim_start();
    let end     = rest.find(|c: char| !c.is_ascii_digit())?;
    rest[..end].parse().ok()
}

/// Extract an f64 numeric value from a flat JSON object by key.
fn extract_f64_field(json: &str, key: &str) -> Option<f64> {
    let pattern = format!("\"{}\":", key);
    let start   = json.find(&pattern)? + pattern.len();
    let rest    = json[start..].trim_start();
    let end     = rest.find(|c: char| c == ',' || c == '}').unwrap_or(rest.len());
    rest[..end].trim().parse().ok()
}

/// Extract a bool value from a flat JSON object by key.
fn extract_bool_field(json: &str, key: &str) -> Option<bool> {
    let pattern = format!("\"{}\":", key);
    let start   = json.find(&pattern)? + pattern.len();
    let rest    = json[start..].trim_start();
    if rest.starts_with("true")  { return Some(true);  }
    if rest.starts_with("false") { return Some(false); }
    None
}

// ---------------------------------------------------------------------------
// Timestamp helper
// ---------------------------------------------------------------------------

fn timestamp() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let secs  = (ms / 1000) % 86400;
    let h     = secs / 3600;
    let m     = (secs % 3600) / 60;
    let s     = secs % 60;
    let millis = ms % 1000;
    format!("{:02}:{:02}:{:02}.{:03}", h, m, s, millis)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    // Enable ANSI colours on Windows.
    #[cfg(windows)]
    unsafe {
        use winapi::um::consoleapi::{GetConsoleMode, SetConsoleMode};
        use winapi::um::processenv::GetStdHandle;
        use winapi::um::winbase::STD_OUTPUT_HANDLE;
        let handle = GetStdHandle(STD_OUTPUT_HANDLE);
        let mut mode: u32 = 0;
        GetConsoleMode(handle, &mut mode);
        SetConsoleMode(handle, mode | 0x0004);
    }

    println!(
        "\n{CYAN}{BOLD}\
╔══════════════════════════════════════════════════════╗\n\
║   Student B — Mock GCS (Ground Control Station)      ║\n\
╠══════════════════════════════════════════════════════╣\n\
║  TCP  : connects to OCS at {OCS_HOST}:{TCP_PORT}         ║\n\
║         receives telemetry JSON stream               ║\n\
║  UDP  : listens on {UDP_BIND}                  ║\n\
║         receives alerts and status heartbeats        ║\n\
╠══════════════════════════════════════════════════════╣\n\
║  Colour key:                                         ║\n\
║   GREEN  = normal telemetry OK                       ║\n\
║   YELLOW = jitter warning / degraded / status        ║\n\
║   RED    = corrupted data / safety alert             ║\n\
║   CYAN   = status heartbeat / info                   ║\n\
╚══════════════════════════════════════════════════════╝\n\
{RESET}",
        CYAN  = CYAN,
        BOLD  = BOLD,
        OCS_HOST = OCS_HOST,
        TCP_PORT = TCP_PORT,
        UDP_BIND = UDP_BIND,
        RESET = RESET,
    );

    let stats = GcsStats::new();

    // Spawn UDP receiver thread.
    {
        let s = stats.clone();
        thread::spawn(move || udp_receiver(s));
    }

    // Spawn stats printer thread.
    {
        let s = stats.clone();
        thread::spawn(move || stats_printer(s));
    }

    // TCP receiver runs on main thread — Ctrl+C exits cleanly.
    tcp_receiver(stats);
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_depacketise_telemetry() {
        let raw = r#"{"kind":"TELEMETRY","seq":1,"timestamp_ms":1700000000000,"sensor":"THERMAL","cycle":42,"value":24.52,"drift_us":143.0,"latency_us":512.0,"status":"OK"}"#;
        match depacketise(raw) {
            OcsPacket::Telemetry(pkt) => {
                assert_eq!(pkt.seq, 1);
                assert_eq!(pkt.sensor, "THERMAL");
                assert_eq!(pkt.cycle, 42);
                assert!((pkt.value - 24.52).abs() < 0.01);
                assert_eq!(pkt.status, "OK");
            }
            other => panic!("Expected Telemetry, got {:?}", other),
        }
    }

    #[test]
    fn test_depacketise_alert() {
        let raw = r#"{"kind":"ALERT","timestamp_ms":1700000000000,"alert_type":"SAFETY_ALERT","sensor":"THERMAL","detail":"4 consecutive misses"}"#;
        match depacketise(raw) {
            OcsPacket::Alert(pkt) => {
                assert_eq!(pkt.alert_type, "SAFETY_ALERT");
                assert_eq!(pkt.sensor, "THERMAL");
            }
            other => panic!("Expected Alert, got {:?}", other),
        }
    }

    #[test]
    fn test_depacketise_status() {
        let raw = r#"{"kind":"STATUS","timestamp_ms":1700000000000,"buf_fill_pct":12.5,"packets_sent":100,"degraded_mode":false,"uptime_s":5}"#;
        match depacketise(raw) {
            OcsPacket::Status(pkt) => {
                assert_eq!(pkt.packets_sent, 100);
                assert!(!pkt.degraded_mode);
                assert!((pkt.buf_fill_pct - 12.5).abs() < 0.01);
            }
            other => panic!("Expected Status, got {:?}", other),
        }
    }

    #[test]
    fn test_depacketise_unknown() {
        let raw = r#"{"kind":"UNKNOWN","data":"garbage"}"#;
        match depacketise(raw) {
            OcsPacket::Unknown(_) => {}
            other => panic!("Expected Unknown, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_str_field() {
        let json = r#"{"sensor":"THERMAL","status":"OK"}"#;
        assert_eq!(extract_str_field(json, "sensor"), Some("THERMAL".into()));
        assert_eq!(extract_str_field(json, "status"), Some("OK".into()));
        assert_eq!(extract_str_field(json, "missing"), None);
    }

    #[test]
    fn test_extract_f64_field() {
        let json = r#"{"drift_us":143.5,"latency_us":512.0}"#;
        let val = extract_f64_field(json, "drift_us").unwrap();
        assert!((val - 143.5).abs() < 0.01);
    }

    #[test]
    fn test_extract_bool_field() {
        let json = r#"{"degraded_mode":false}"#;
        assert_eq!(extract_bool_field(json, "degraded_mode"), Some(false));
        let json2 = r#"{"degraded_mode":true}"#;
        assert_eq!(extract_bool_field(json2, "degraded_mode"), Some(true));
    }
}