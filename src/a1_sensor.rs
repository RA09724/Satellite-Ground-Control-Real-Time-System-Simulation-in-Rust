// =============================================================================
// a1_sensor.rs — Task 1: Sensor Data Acquisition and Prioritization
// =============================================================================
//
// Requirements addressed
// ───────────────────────
// ✔  Three onboard sensors with distinct sampling intervals and priorities.
// ✔  Jitter budget <1 ms for THERMAL (critical sensor).
// ✔  Bounded priority buffer with overflow drop logging.
// ✔  Scheduling drift = actual start − scheduled start, every cycle.
// ✔  Insertion latency = sensor read → buffer insertion.
// ✔  Safety alert when critical sensor misses >3 consecutive cycles.
// ✔  Task 4: fault hooks — delay and corruption faults applied per cycle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::a4_benchmark::{self, FaultState};
use crate::buffer::SensorBuffer;
use crate::logger::Logger;
use crate::metrics::OcsMetrics;
use crate::types::{SafetyAlert, SensorReading, SensorType};

// ---------------------------------------------------------------------------
// Windows real-time helpers
// ---------------------------------------------------------------------------

#[cfg(windows)]
extern crate winapi;
#[cfg(windows)]
use winapi::um::processthreadsapi::{GetCurrentThread, SetThreadPriority};
#[cfg(windows)]
use winapi::um::winbase::SetThreadAffinityMask;
#[cfg(windows)]
const THREAD_PRIORITY_TIME_CRITICAL: i32 = 15;

fn set_realtime_priority() {
    #[cfg(windows)]
    unsafe {
        SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
    }
}

fn pin_to_core(core_index: usize) {
    #[cfg(windows)]
    unsafe {
        let mask: usize = 1 << core_index;
        SetThreadAffinityMask(GetCurrentThread(), mask);
    }
}

fn precise_sleep_until(target: Instant) {
    const SPIN_GUARD: Duration = Duration::from_millis(2);
    let now = Instant::now();
    if target <= now { return; }
    let remaining = target - now;
    if remaining > SPIN_GUARD {
        thread::sleep(remaining - SPIN_GUARD);
    }
    while Instant::now() < target {
        std::hint::spin_loop();
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Spawn all three sensor-acquisition threads.
///
/// `fault_state` is `None` when Task 4 is not active (Tasks 1–3 only).
/// When `Some`, each sensor thread polls for injected faults each cycle.
pub fn spawn_sensor_threads(
    buffer:       SensorBuffer,
    metrics:      OcsMetrics,
    epoch:        Instant,
    run_duration: Duration,
    stop:         Arc<AtomicBool>,
    fault_state:  Option<FaultState>,
    logger:       Logger,
) -> Vec<thread::JoinHandle<Vec<SafetyAlert>>> {

    let assignments: &[(SensorType, usize)] = &[
        (SensorType::Thermal,  1),
        (SensorType::Power,    2),
        (SensorType::Attitude, 3),
    ];

    assignments
        .iter()
        .map(|&(sensor, core)| {
            let buf      = buffer.clone();
            let met      = metrics.clone();
            let stp      = stop.clone();
            let fs       = fault_state.clone();
            let log      = logger.clone();
            let deadline = Instant::now() + run_duration;

            thread::spawn(move || {
                set_realtime_priority();
                pin_to_core(core);
                run_sensor(sensor, buf, met, epoch, deadline, stp, fs, log)
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Per-sensor acquisition loop
// ---------------------------------------------------------------------------

fn run_sensor(
    sensor:      SensorType,
    buffer:      SensorBuffer,
    metrics:     OcsMetrics,
    epoch:       Instant,
    deadline:    Instant,
    stop:        Arc<AtomicBool>,
    fault_state: Option<FaultState>,
    logger:      Logger,
) -> Vec<SafetyAlert> {

    let period        = sensor.period();
    let jitter_budget = sensor.jitter_budget();
    let handle        = sensor_metrics_handle(&metrics, sensor);

    let mut cycle:            u64 = 0;
    let mut consecutive_miss: u32 = 0;
    let mut alerts:           Vec<SafetyAlert> = Vec::new();
    let mut next_scheduled    = Instant::now();
    let mut last_read_us:     Option<u64> = None;

    println!(
        "[SENSOR][INIT] {} | period={:?} | priority={}",
        sensor.label(), period, sensor.priority()
    );

    loop {
        if stop.load(Ordering::Relaxed) || Instant::now() >= deadline {
            break;
        }

        cycle += 1;
        let scheduled_at = next_scheduled;

        // ── Task 4: apply delay fault before sleeping ─────────────────────
        // If a delay fault is pending, it fires here — the extra sleep
        // causes the sensor to arrive late, increasing drift measurably.
        if let Some(ref fs) = fault_state {
            a4_benchmark::apply_delay_fault(fs, sensor, cycle);
        }

        precise_sleep_until(scheduled_at);

        let actual_start = Instant::now();
        let drift        = actual_start.saturating_duration_since(scheduled_at);
        let drift_us     = drift.as_micros() as f64;

        if sensor.is_critical() && drift > jitter_budget {
            println!(
                "[SENSOR][JITTER_WARN] {} cycle={} drift={:.1}µs > budget={:.1}µs",
                sensor.label(), cycle, drift_us,
                jitter_budget.as_micros() as f64,
            );
        }

        // ── Simulate sensor read ──────────────────────────────────────────
        let mut value = simulate_sensor_read(sensor, cycle);
        thread::sleep(Duration::from_micros(50));
        let read_done = Instant::now();

        // ── Task 4: apply corruption fault ────────────────────────────────
        // If a corruption fault is pending, replace the value with NaN.
        // The downlink will see status="CORRUPTED" for this packet.
        if let Some(ref fs) = fault_state {
            if a4_benchmark::apply_corrupt_fault(fs, sensor, cycle) {
                value = f64::NAN;
            }
        }

        let reading = SensorReading {
            sensor,
            value,
            read_at:      read_done,
            scheduled_at: scheduled_at,
            cycle,
        };

        // Log NaN as "CORRUPT" in the output so it's obvious.
        if value.is_nan() {
            println!(
                "[SENSOR][READ] {} | cycle={:>6} | val=  CORRUPT | drift={:>6.1}µs",
                sensor.label(), cycle, drift_us,
            );
        } else {
            println!(
                "[SENSOR][READ] {} | cycle={:>6} | val={:>8.3} | drift={:>6.1}µs",
                sensor.label(), cycle, value, drift_us,
            );
        }

        let accepted       = buffer.push(reading, read_done);
        let insert_latency = Instant::now().duration_since(read_done);
        let read_epoch_us  = read_done.duration_since(epoch).as_micros() as u64;

        // ── Log drift, latency, and jitter to performance_log.txt ─────────
        logger.log_sensor_drift(sensor.label(), cycle, drift_us);
        logger.log_sensor_latency(
            sensor.label(), cycle,
            insert_latency.as_micros() as f64,
        );
        // Jitter = deviation from ideal period between successive reads.
        if let Some(prev_us) = last_read_us {
            let ideal_us   = period.as_micros() as f64;
            let actual_us  = read_epoch_us.saturating_sub(prev_us) as f64;
            let jitter_us  = (actual_us - ideal_us).abs();
            logger.log_sensor_jitter(sensor.label(), cycle, jitter_us);
        }
        last_read_us = Some(read_epoch_us);

        handle.record_cycle(drift_us, insert_latency, read_epoch_us);

        if accepted {
            consecutive_miss = 0;
            println!(
                "[SENSOR][BUFFERED] {} | cycle={:>6} | latency={:.1}µs | buf_len={}/{}",
                sensor.label(), cycle,
                insert_latency.as_micros() as f64,
                buffer.len(), buffer.capacity(),
            );
        } else {
            consecutive_miss += 1;
            handle.record_drop();
            println!(
                "[SENSOR][MISS] {} | cycle={:>6} | consecutive_miss={}",
                sensor.label(), cycle, consecutive_miss,
            );

            if sensor.is_critical() && consecutive_miss > SafetyAlert::MISS_THRESHOLD {
                let alert = SafetyAlert {
                    sensor,
                    consecutive_misses: consecutive_miss,
                    raised_at:          Instant::now(),
                };
                handle.record_alert();
                print_safety_alert(&alert);
                alerts.push(alert);
                consecutive_miss = 0;
            }
        }

        next_scheduled += period;
    }

    println!("[SENSOR][DONE] {} completed {} cycles", sensor.label(), cycle);
    alerts
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sensor_metrics_handle(
    metrics: &OcsMetrics,
    sensor:  SensorType,
) -> crate::metrics::MetricsHandle {
    match sensor {
        SensorType::Thermal  => metrics.thermal.clone(),
        SensorType::Power    => metrics.power.clone(),
        SensorType::Attitude => metrics.attitude.clone(),
    }
}

fn simulate_sensor_read(sensor: SensorType, cycle: u64) -> f64 {
    let phase = (cycle % 100) as f64 / 100.0;
    let noise = if phase < 0.5 { phase } else { 1.0 - phase };
    match sensor {
        SensorType::Thermal  => 24.5 + noise * 2.0,
        SensorType::Power    => 4.95 + noise * 0.1,
        SensorType::Attitude => (cycle as f64 * 0.5_f64).to_radians().sin() * 15.0,
    }
}

fn print_safety_alert(alert: &SafetyAlert) {
    println!();
    println!("╔════════════════════════════════════════════════════╗");
    println!("║            ⚠  SAFETY ALERT RAISED  ⚠              ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║  Sensor : {:40} ║", alert.sensor.label());
    println!("║  Consecutive misses : {:>3}  (threshold = {:>3})       ║",
        alert.consecutive_misses, SafetyAlert::MISS_THRESHOLD);
    println!("╚════════════════════════════════════════════════════╝");
    println!();
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::SensorBuffer;

    #[test]
    fn test_buffer_overflow_logging() {
        let buf = SensorBuffer::new(2);
        for i in 1..=4_u64 {
            let r = SensorReading {
                sensor: SensorType::Attitude, value: i as f64,
                read_at: Instant::now(), scheduled_at: Instant::now(), cycle: i,
            };
            buf.push(r, Instant::now());
        }
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.total_dropped(), 2);
    }

    #[test]
    fn test_priority_displacement() {
        let buf = SensorBuffer::new(1);
        let low = SensorReading {
            sensor: SensorType::Attitude, value: 1.0,
            read_at: Instant::now(), scheduled_at: Instant::now(), cycle: 1,
        };
        assert!(buf.push(low, Instant::now()));
        let high = SensorReading {
            sensor: SensorType::Thermal, value: 25.0,
            read_at: Instant::now(), scheduled_at: Instant::now(), cycle: 1,
        };
        assert!(buf.push(high, Instant::now()));
        assert_eq!(buf.pop().unwrap().sensor, SensorType::Thermal);
    }

    #[test]
    fn test_drift_non_negative() {
        let scheduled = Instant::now();
        thread::sleep(Duration::from_millis(1));
        let r = SensorReading {
            sensor: SensorType::Thermal, value: 25.0,
            read_at: Instant::now(), scheduled_at: scheduled, cycle: 1,
        };
        assert!(r.drift_us() >= 0);
    }

    #[test]
    fn test_rate_monotonic_ordering() {
        assert!(SensorType::Thermal.period()   < SensorType::Power.period());
        assert!(SensorType::Power.period()     < SensorType::Attitude.period());
        assert!(SensorType::Thermal.priority() < SensorType::Power.priority());
        assert!(SensorType::Power.priority()   < SensorType::Attitude.priority());
    }

    #[test]
    fn test_precise_sleep_accuracy() {
        set_realtime_priority();
        let target = Instant::now() + Duration::from_millis(50);
        precise_sleep_until(target);
        let error = Instant::now().duration_since(target);
        assert!(
            error < Duration::from_millis(3),
            "precise_sleep_until overshot by {:?}", error
        );
    }
}