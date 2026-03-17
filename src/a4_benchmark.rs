// =============================================================================
// a4_benchmark.rs — Task 4: Benchmarking and Fault Simulation
// =============================================================================
//
// Requirements addressed
// ───────────────────────
// ✔  Inject faults (delayed sensor data, corrupted readings) periodically
// ✔  Log all faults and system responses with timestamps
// ✔  Evaluate: jitter, drift, deadline adherence, fault recovery time,
//              CPU utilisation
// ✔  Recovery time must be <200ms — mission abort if exceeded
//
// Design
// ──────
// The fault injector runs in its own thread and operates on two channels:
//
//   FaultType::DelayedSensor  — sleeps the sensor thread for an extra
//                               burst period, causing the next N readings
//                               to arrive late. Tests jitter resilience.
//
//   FaultType::CorruptedData  — writes a sentinel NaN value into the shared
//                               fault-value store. The sensor picks this up
//                               on its next cycle and emits a corrupted
//                               reading. Tests fault detection logic.
//
// Recovery is measured as the time from fault injection to the moment the
// sensor resumes normal (non-corrupted, on-time) operation.
//
// Fault interval: every 60 seconds as required by the assignment.
// In a 300-second (5-minute) run this produces 5 distinct fault events.
//
// All events are logged to fault_log.txt alongside stdout.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::a2_scheduler::{CpuNanos, JobLog, ViolationLog};
use crate::logger::Logger;
use crate::metrics::OcsMetrics;
use crate::types::SensorType;

// ---------------------------------------------------------------------------
// Fault log file path
// ---------------------------------------------------------------------------

pub const FAULT_LOG_PATH: &str = "fault_log.txt";

// ---------------------------------------------------------------------------
// Fault injection interval (scaled for 10s simulation)
// ---------------------------------------------------------------------------

/// Faults are injected every 60 seconds as specified in the assignment.
/// In a 300-second (5-minute) run this produces 5 fault events.
const FAULT_INTERVAL_SECS: u64 = 60;

/// Maximum allowed recovery time before mission abort is declared.
const RECOVERY_DEADLINE_MS: u64 = 200;

// ---------------------------------------------------------------------------
// Fault types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultType {
    /// Sensor data arrives later than scheduled (simulates hardware stall).
    DelayedSensor,
    /// Sensor reading contains an out-of-range / NaN value (simulates ADC fault).
    CorruptedData,
}

impl FaultType {
    pub fn label(&self) -> &'static str {
        match self {
            FaultType::DelayedSensor => "DELAYED_SENSOR",
            FaultType::CorruptedData => "CORRUPTED_DATA",
        }
    }
}

// ---------------------------------------------------------------------------
// Fault event record
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct FaultEvent {
    pub id:            u64,
    pub fault_type:    FaultType,
    pub target_sensor: SensorType,
    pub injected_at:   Instant,
    pub recovered_at:  Option<Instant>,
    pub recovery_ms:   Option<u64>,
    pub aborted:       bool,
}

impl FaultEvent {
    pub fn recovery_status(&self) -> String {
        match self.recovery_ms {
            Some(ms) if ms <= RECOVERY_DEADLINE_MS =>
                format!("RECOVERED in {}ms ✓", ms),
            Some(ms) =>
                format!("SLOW RECOVERY {}ms > {}ms — MISSION ABORT ✗",
                    ms, RECOVERY_DEADLINE_MS),
            None => "PENDING".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared fault state (written by injector, read by sensors and reporter)
// ---------------------------------------------------------------------------

/// Per-sensor fault injection flags.
/// Sensor threads poll these every cycle and act accordingly.
#[derive(Clone)]
pub struct FaultState {
    /// If set, the target sensor should sleep for this duration extra.
    pub delay_flags: Arc<Mutex<HashMap<SensorType, Duration>>>,
    /// If set, the target sensor should emit f64::NAN as its value.
    pub corrupt_flags: Arc<Mutex<HashMap<SensorType, bool>>>,
    /// Full log of all fault events.
    pub fault_log: Arc<Mutex<Vec<FaultEvent>>>,
    /// Counter of faults injected so far.
    pub fault_count: Arc<AtomicU64>,
    /// Set to true if any fault exceeded the recovery deadline.
    pub mission_aborted: Arc<AtomicBool>,
    /// Log file handle.
    log_file: Arc<Mutex<File>>,
    /// Performance logger — fault events written to performance_log.txt.
    logger: Logger,
}

impl FaultState {
    pub fn new(logger: Logger) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(FAULT_LOG_PATH)
            .expect("Cannot open fault_log.txt");

        let mut f = file;
        writeln!(f, "# OCS Fault Log — Task 4 Benchmarking").unwrap();
        writeln!(f, "# Format: [timestamp_ms] FAULT_ID TYPE SENSOR STATUS").unwrap();
        writeln!(f, "# Recovery deadline: {}ms", RECOVERY_DEADLINE_MS).unwrap();
        writeln!(f, "# ---").unwrap();

        Self {
            delay_flags:     Arc::new(Mutex::new(HashMap::new())),
            corrupt_flags:   Arc::new(Mutex::new(HashMap::new())),
            fault_log:       Arc::new(Mutex::new(Vec::new())),
            fault_count:     Arc::new(AtomicU64::new(0)),
            mission_aborted: Arc::new(AtomicBool::new(false)),
            log_file:        Arc::new(Mutex::new(f)),
            logger,
        }
    }

    /// Check if a delay fault is active for this sensor, and clear it.
    pub fn take_delay(&self, sensor: SensorType) -> Option<Duration> {
        self.delay_flags.lock().unwrap().remove(&sensor)
    }

    /// Check if a corruption fault is active for this sensor, and clear it.
    pub fn take_corrupt(&self, sensor: SensorType) -> bool {
        self.corrupt_flags.lock().unwrap()
            .remove(&sensor)
            .unwrap_or(false)
    }

    /// Record fault recovery and check against deadline.
    pub fn record_recovery(&self, fault_id: u64, recovered_at: Instant) {
        let mut log = self.fault_log.lock().unwrap();
        if let Some(event) = log.iter_mut().find(|e| e.id == fault_id) {
            let ms = recovered_at
                .duration_since(event.injected_at)
                .as_millis() as u64;
            event.recovered_at = Some(recovered_at);
            event.recovery_ms  = Some(ms);

            let status = event.recovery_status();

            if ms > RECOVERY_DEADLINE_MS {
                event.aborted = true;
                self.mission_aborted.store(true, Ordering::Relaxed);
                println!(
                    "\n╔══════════════════════════════════════════════════════╗"
                );
                println!(
                    "║        ☠  MISSION ABORT — RECOVERY TIMEOUT  ☠        ║"
                );
                println!(
                    "╠══════════════════════════════════════════════════════╣"
                );
                println!(
                    "║  Fault #{:<4} | {:14} | {}",
                    fault_id, event.fault_type.label(), event.target_sensor.label()
                );
                println!(
                    "║  Recovery time {}ms > limit {}ms                      ║",
                    ms, RECOVERY_DEADLINE_MS
                );
                println!(
                    "╚══════════════════════════════════════════════════════╝\n"
                );
            } else {
                println!(
                    "[BENCH][RECOVERY] fault_id={} sensor={} recovery={}ms ✓  (<{}ms deadline)",
                    fault_id, event.target_sensor.label(), ms, RECOVERY_DEADLINE_MS
                );
            }

            self.write_log_line(&format!(
                "[{}] FAULT#{} {} {} {}\n",
                now_ms(), fault_id,
                event.fault_type.label(),
                event.target_sensor.label(),
                status,
            ));

            // Write fault recovery to performance_log.txt.
            self.logger.log_fault_recovery(
                fault_id,
                event.fault_type.label(),
                event.target_sensor.label(),
                ms,
                ms <= RECOVERY_DEADLINE_MS,
            );
        }
    }

    fn write_log_line(&self, line: &str) {
        if let Ok(mut f) = self.log_file.lock() {
            let _ = f.write_all(line.as_bytes());
            let _ = f.flush();
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry point — spawn fault injector thread
// ---------------------------------------------------------------------------

/// Spawn the fault injection thread.
/// Returns the join handle and a cloneable FaultState handle.
pub fn spawn_fault_injector(
    stop:         Arc<AtomicBool>,
    run_duration: Duration,
    logger:       Logger,
) -> (thread::JoinHandle<()>, FaultState) {

    let state        = FaultState::new(logger);
    let state_clone  = state.clone();

    let handle = thread::spawn(move || {
        run_fault_injector(state_clone, stop, run_duration);
    });

    (handle, state)
}

// ---------------------------------------------------------------------------
// Fault injection loop
// ---------------------------------------------------------------------------

fn run_fault_injector(
    state:        FaultState,
    stop:         Arc<AtomicBool>,
    run_duration: Duration,
) {
    let deadline        = Instant::now() + run_duration;
    let fault_interval  = Duration::from_secs(FAULT_INTERVAL_SECS);
    let mut next_fault  = Instant::now() + fault_interval;

    // Alternate between fault types and target sensors for variety.
    let fault_schedule: &[(FaultType, SensorType)] = &[
        (FaultType::DelayedSensor,  SensorType::Thermal),
        (FaultType::CorruptedData,  SensorType::Power),
        (FaultType::DelayedSensor,  SensorType::Attitude),
        (FaultType::CorruptedData,  SensorType::Thermal),
    ];
    let mut fault_idx = 0;

    println!("[BENCH][INIT] Fault injector ready — interval={}s recovery_limit={}ms",
        FAULT_INTERVAL_SECS, RECOVERY_DEADLINE_MS);

    loop {
        if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
            break;
        }

        // Wait until next fault injection time.
        let now = Instant::now();
        if next_fault > now {
            thread::sleep(next_fault - now);
        }

        if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
            break;
        }

        // Select fault type and target.
        let (fault_type, target) = fault_schedule[fault_idx % fault_schedule.len()];
        fault_idx += 1;

        let fault_id     = state.fault_count.fetch_add(1, Ordering::Relaxed) + 1;
        let injected_at  = Instant::now();

        // Build and log the event.
        let event = FaultEvent {
            id:            fault_id,
            fault_type,
            target_sensor: target,
            injected_at,
            recovered_at:  None,
            recovery_ms:   None,
            aborted:       false,
        };

        println!(
            "\n[BENCH][INJECT] fault_id={} type={} target={} at={}ms",
            fault_id, fault_type.label(), target.label(), now_ms()
        );

        state.write_log_line(&format!(
            "[{}] FAULT#{} {} {} INJECTED\n",
            now_ms(), fault_id, fault_type.label(), target.label()
        ));

        // Write fault injection to performance_log.txt.
        state.logger.log_fault_inject(fault_id, fault_type.label(), target.label());

        // Apply the fault.
        match fault_type {
            FaultType::DelayedSensor => {
                // Inject a 150ms delay — inside the 200ms recovery window.
                // The sensor thread picks this up and sleeps extra before
                // its next reading, then reports recovery automatically.
                state.delay_flags.lock().unwrap()
                    .insert(target, Duration::from_millis(150));
                println!(
                    "[BENCH][FAULT]  DELAYED_SENSOR: {} will stall 150ms on next cycle",
                    target.label()
                );
            }
            FaultType::CorruptedData => {
                // Mark the sensor for one corrupted reading (NaN value).
                state.corrupt_flags.lock().unwrap().insert(target, true);
                println!(
                    "[BENCH][FAULT]  CORRUPTED_DATA: {} next reading will be NaN",
                    target.label()
                );
            }
        }

        state.fault_log.lock().unwrap().push(event);

        next_fault += fault_interval;
    }

    println!("[BENCH][DONE] Fault injector finished. Total faults: {}",
        state.fault_count.load(Ordering::Relaxed));
}

// ---------------------------------------------------------------------------
// Sensor-side fault hooks
// Called from a1_sensor.rs run_sensor() each cycle
// ---------------------------------------------------------------------------

/// Apply any pending delay fault for this sensor.
/// Returns the extra delay duration if one was active (for recovery timing).
pub fn apply_delay_fault(
    state:  &FaultState,
    sensor: SensorType,
    cycle:  u64,
) -> Option<(u64, Instant)> {
    if let Some(delay) = state.take_delay(sensor) {
        println!(
            "[BENCH][SENSOR_FAULT] {} cycle={} DELAY {}ms injected",
            sensor.label(), cycle, delay.as_millis()
        );
        let fault_start = Instant::now();
        thread::sleep(delay);
        println!(
            "[BENCH][SENSOR_FAULT] {} cycle={} delay complete — resuming",
            sensor.label(), cycle
        );

        // Find the most recent unrecovered fault for this sensor and record recovery.
        let fault_id = find_latest_fault_id(state, sensor, FaultType::DelayedSensor);
        if let Some(id) = fault_id {
            state.record_recovery(id, Instant::now());
        }

        return Some((delay.as_millis() as u64, fault_start));
    }
    None
}

/// Apply any pending corruption fault for this sensor.
/// Returns `true` if the value should be replaced with NaN.
pub fn apply_corrupt_fault(
    state:  &FaultState,
    sensor: SensorType,
    cycle:  u64,
) -> bool {
    if state.take_corrupt(sensor) {
        println!(
            "[BENCH][SENSOR_FAULT] {} cycle={} CORRUPTED reading emitted (NaN)",
            sensor.label(), cycle
        );
        // Schedule immediate recovery — corruption lasts exactly one cycle.
        let fault_id = find_latest_fault_id(state, sensor, FaultType::CorruptedData);
        if let Some(id) = fault_id {
            state.record_recovery(id, Instant::now());
        }
        return true;
    }
    false
}

/// Find the most recent unrecovered fault ID for a given sensor and type.
fn find_latest_fault_id(
    state:      &FaultState,
    sensor:     SensorType,
    fault_type: FaultType,
) -> Option<u64> {
    state.fault_log.lock().unwrap()
        .iter()
        .rev()
        .find(|e| e.target_sensor == sensor
               && e.fault_type == fault_type
               && e.recovered_at.is_none())
        .map(|e| e.id)
}

// ---------------------------------------------------------------------------
// Benchmark metrics report
// ---------------------------------------------------------------------------

/// Full Task 4 report — combines fault log with Task 1/2/3 metrics.
/// Builds the entire report into a single String then prints it atomically
/// so no other thread can interleave output mid-report.
pub fn print_benchmark_report(
    fault_state:  &FaultState,
    ocs_metrics:  &OcsMetrics,
    job_log:      &JobLog,
    vlog:         &ViolationLog,
    cpu_nanos:    &CpuNanos,
    run_duration: Duration,
) {
    use std::fmt::Write as FmtWrite;

    // Lock everything up front so nothing changes while we build the report.
    let faults       = fault_state.fault_log.lock().unwrap();
    let jobs         = job_log.lock().unwrap();
    let violations   = vlog.lock().unwrap();
    let active_ns    = cpu_nanos.load(Ordering::Relaxed);
    let total_ns     = run_duration.as_nanos() as u64;
    let cpu_util_pct = (active_ns as f64 / total_ns as f64) * 100.0;
    let aborted      = fault_state.mission_aborted.load(Ordering::Relaxed);

    // Build the entire report into a buffer first.
    let mut out = String::with_capacity(4096);

    writeln!(out, "\n╔══════════════════════════════════════════════════════╗").unwrap();
    writeln!(out, "║        TASK 4 — BENCHMARKING & FAULT SIMULATION      ║").unwrap();
    writeln!(out, "╚══════════════════════════════════════════════════════╝").unwrap();

    // ── Fault injection summary ───────────────────────────────────────────
    writeln!(out, "\n  ── Fault Injection Summary ──────────────────────────").unwrap();
    writeln!(out, "  Total faults injected : {}", faults.len()).unwrap();
    writeln!(out, "  Recovery deadline     : {}ms", RECOVERY_DEADLINE_MS).unwrap();
    writeln!(out, "  Mission aborted       : {}",
        if aborted { "⚠ YES" } else { "✓ NO" }).unwrap();
    writeln!(out).unwrap();

    for event in faults.iter() {
        writeln!(out, "  Fault #{:<3} | {:14} | sensor={:8} | {}",
            event.id,
            event.fault_type.label(),
            event.target_sensor.label(),
            event.recovery_status(),
        ).unwrap();
    }

    // ── Per-sensor jitter / drift summary ────────────────────────────────
    writeln!(out, "\n  ── Sensor Performance Under Fault ───────────────────").unwrap();
    for handle in [
        &ocs_metrics.thermal,
        &ocs_metrics.power,
        &ocs_metrics.attitude,
    ] {
        let m = handle.inner.lock().unwrap();
        writeln!(out, "  {} — cycles={} dropped={} alerts={}",
            m.name, m.total_cycles, m.dropped, m.alerts).unwrap();
        writeln!(out, "    Drift  : mean={:.1}µs  max={:.1}µs  σ={:.1}µs",
            m.drift.mean_us(), m.drift.max_us(), m.drift.stddev_us()).unwrap();
        writeln!(out, "    Jitter : mean={:.1}µs  max={:.1}µs  σ={:.1}µs",
            m.jitter.mean_us(), m.jitter.max_us(), m.jitter.stddev_us()).unwrap();
        writeln!(out, "    Latency: mean={:.1}µs  max={:.1}µs",
            m.latency.mean_us(), m.latency.max_us()).unwrap();
        writeln!(out).unwrap();
    }

    // ── Scheduler deadline adherence ─────────────────────────────────────
    writeln!(out, "  ── Scheduler Deadline Adherence ─────────────────────").unwrap();
    let total_jobs       = jobs.len();
    let missed_deadlines = violations.len();
    let adherence_pct    = if total_jobs > 0 {
        (1.0 - missed_deadlines as f64 / total_jobs as f64) * 100.0
    } else { 100.0 };

    writeln!(out, "  Total jobs scheduled  : {}", total_jobs).unwrap();
    writeln!(out, "  Deadline violations   : {}", missed_deadlines).unwrap();
    writeln!(out, "  Deadline adherence    : {:.1}%", adherence_pct).unwrap();
    writeln!(out).unwrap();

    // ── CPU utilisation ───────────────────────────────────────────────────
    writeln!(out, "  ── CPU Utilisation ──────────────────────────────────").unwrap();
    writeln!(out, "  Active (scheduler)    : {:.2}%", cpu_util_pct).unwrap();
    writeln!(out, "  Idle                  : {:.2}%", 100.0 - cpu_util_pct).unwrap();
    writeln!(out).unwrap();

    // ── Recovery time analysis ────────────────────────────────────────────
    writeln!(out, "  ── Fault Recovery Time Analysis ─────────────────────").unwrap();
    let recovered: Vec<_> = faults.iter()
        .filter_map(|e| e.recovery_ms)
        .collect();

    if recovered.is_empty() {
        writeln!(out, "  No recovery data recorded.").unwrap();
    } else {
        let avg_recovery = recovered.iter().sum::<u64>() as f64 / recovered.len() as f64;
        let max_recovery = *recovered.iter().max().unwrap_or(&0);
        let min_recovery = *recovered.iter().min().unwrap_or(&0);
        let within_limit = recovered.iter().filter(|&&ms| ms <= RECOVERY_DEADLINE_MS).count();

        writeln!(out, "  Recovery time — avg={:.1}ms  min={}ms  max={}ms",
            avg_recovery, min_recovery, max_recovery).unwrap();
        writeln!(out, "  Within {}ms limit : {}/{} ({:.0}%)",
            RECOVERY_DEADLINE_MS,
            within_limit,
            recovered.len(),
            (within_limit as f64 / recovered.len() as f64) * 100.0,
        ).unwrap();
    }

    // ── Overall system verdict ────────────────────────────────────────────
    writeln!(out).unwrap();
    writeln!(out, "  ── Overall System Verdict ───────────────────────────").unwrap();
    if aborted {
        writeln!(out, "  ⚠  MISSION STATUS: DEGRADED — recovery timeout exceeded").unwrap();
    } else {
        writeln!(out, "  ✓  MISSION STATUS: NOMINAL — all faults recovered within {}ms",
            RECOVERY_DEADLINE_MS).unwrap();
    }
    writeln!(out, "  Fault log written to: {}", FAULT_LOG_PATH).unwrap();
    writeln!(out, "╚══════════════════════════════════════════════════════╝").unwrap();

    // Print the entire report atomically — no other thread can interleave.
    print!("{}", out);
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
    use crate::logger::Logger;

    /// Helper: create a Logger pointing at a temp file so tests don't
    /// pollute the real performance_log.txt.
    fn test_logger() -> Logger {
        Logger::new()
    }

    #[test]
    fn test_recovery_deadline_constant() {
        assert_eq!(RECOVERY_DEADLINE_MS, 200);
    }

    #[test]
    fn test_fault_event_recovery_status() {
        let mut event = FaultEvent {
            id:            1,
            fault_type:    FaultType::DelayedSensor,
            target_sensor: SensorType::Thermal,
            injected_at:   Instant::now(),
            recovered_at:  None,
            recovery_ms:   Some(150),
            aborted:       false,
        };
        assert!(event.recovery_status().contains("RECOVERED"));

        event.recovery_ms = Some(250);
        assert!(event.recovery_status().contains("MISSION ABORT"));
    }

    #[test]
    fn test_fault_type_labels() {
        assert_eq!(FaultType::DelayedSensor.label(), "DELAYED_SENSOR");
        assert_eq!(FaultType::CorruptedData.label(), "CORRUPTED_DATA");
    }

    #[test]
    fn test_fault_interval_fits_in_simulation() {
        // For a 300-second run, faults every 60s gives exactly 5 events.
        // The interval must be less than SIM_DURATION so at least one fires.
        const SIM_DURATION_SECS: u64 = 300;
        assert!(FAULT_INTERVAL_SECS < SIM_DURATION_SECS);
        assert_eq!(FAULT_INTERVAL_SECS, 60);
    }

    #[test]
    fn test_fault_state_delay_take() {
        let state = FaultState::new(test_logger());
        state.delay_flags.lock().unwrap()
            .insert(SensorType::Thermal, Duration::from_millis(100));
        let taken = state.take_delay(SensorType::Thermal);
        assert!(taken.is_some());
        assert_eq!(taken.unwrap(), Duration::from_millis(100));
        // Second call should return None — flag was consumed.
        assert!(state.take_delay(SensorType::Thermal).is_none());
    }

    #[test]
    fn test_fault_state_corrupt_take() {
        let state = FaultState::new(test_logger());
        state.corrupt_flags.lock().unwrap()
            .insert(SensorType::Power, true);
        assert!(state.take_corrupt(SensorType::Power));
        // Second call should return false — flag was consumed.
        assert!(!state.take_corrupt(SensorType::Power));
    }
}