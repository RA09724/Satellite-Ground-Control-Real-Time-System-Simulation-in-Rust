// =============================================================================
// metrics.rs — Real-time performance metrics for the OCS
// =============================================================================
//
// This module provides lightweight, thread-safe accumulators for the three
// key real-time metrics required by the assignment:
//
//   • Scheduling drift  – difference between scheduled and actual task start
//   • Insertion latency – time from sensor read to buffer insertion
//   • Jitter            – variation in periodic task timing (deviation from ideal period)

use std::sync::{Arc, Mutex};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Generic running-statistics accumulator (min / max / mean / stddev)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct RunningStats {
    count:   u64,
    sum:     f64,    // µs
    sum_sq:  f64,    // µs²
    min:     f64,    // µs
    max:     f64,    // µs
}

impl RunningStats {
    pub fn new() -> Self {
        Self { min: f64::MAX, ..Default::default() }
    }

    /// Record one sample (value in microseconds).
    pub fn record(&mut self, value_us: f64) {
        self.count  += 1;
        self.sum    += value_us;
        self.sum_sq += value_us * value_us;
        if value_us < self.min { self.min = value_us; }
        if value_us > self.max { self.max = value_us; }
    }

   
    pub fn mean_us(&self) -> f64  {
        if self.count == 0 { 0.0 } else { self.sum / self.count as f64 }
    }
    pub fn min_us(&self)  -> f64  { if self.count == 0 { 0.0 } else { self.min } }
    pub fn max_us(&self)  -> f64  { self.max }

    /// Population standard deviation (µs).
    pub fn stddev_us(&self) -> f64 {
        if self.count < 2 { return 0.0; }
        let mean = self.mean_us();
        let variance = self.sum_sq / self.count as f64 - mean * mean;
        variance.max(0.0).sqrt()
    }

    /// Pretty one-line summary.
    pub fn summary(&self, label: &str) -> String {
        format!(
            "{}: n={} mean={:.1}µs min={:.1}µs max={:.1}µs σ={:.1}µs",
            label,
            self.count,
            self.mean_us(),
            self.min_us(),
            self.max_us(),
            self.stddev_us(),
        )
    }
}

// ---------------------------------------------------------------------------
// Per-sensor metric set (shared across threads via Arc<Mutex<…>>)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SensorMetrics {
    pub name:           &'static str,
    pub drift:          RunningStats,   // scheduled-start deviation
    pub latency:        RunningStats,   // sensor-read to buffer-insert
    pub jitter:         RunningStats,   // inter-arrival variation
    pub total_cycles:   u64,
    pub dropped:        u64,
    pub alerts:         u64,
    /// The `Instant` of the most recent successful read (for jitter calc).
    pub last_read_us:   Option<u64>,    // stored as µs since an epoch
    pub ideal_period_us: u64,
}

impl SensorMetrics {
    pub fn new(name: &'static str, period: Duration) -> Self {
        Self {
            name,
            drift:           RunningStats::new(),
            latency:         RunningStats::new(),
            jitter:          RunningStats::new(),
            total_cycles:    0,
            dropped:         0,
            alerts:          0,
            last_read_us:    None,
            ideal_period_us: period.as_micros() as u64,
        }
    }
}

// ---------------------------------------------------------------------------
// Thread-safe handle
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct MetricsHandle {
    pub inner: Arc<Mutex<SensorMetrics>>,
}

impl MetricsHandle {
    pub fn new(name: &'static str, period: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SensorMetrics::new(name, period))),
        }
    }

    /// Record one complete sensor cycle.
    ///
    /// * `drift_us`   — scheduling drift for this cycle (µs, always ≥ 0)
    /// * `latency`    — sensor-read to buffer-insertion duration
    /// * `read_epoch_us` — the read timestamp expressed as µs since a shared
    ///                     epoch (used to compute inter-arrival jitter)
    pub fn record_cycle(&self, drift_us: f64, latency: Duration, read_epoch_us: u64) {
        let mut m = self.inner.lock().unwrap();
        m.total_cycles += 1;
        m.drift.record(drift_us);
        m.latency.record(latency.as_micros() as f64);

        // Jitter: deviation of actual inter-arrival from ideal period.
        if let Some(prev_us) = m.last_read_us {
            let actual_interval = read_epoch_us.saturating_sub(prev_us) as f64;
            let jitter = (actual_interval - m.ideal_period_us as f64).abs();
            m.jitter.record(jitter);
        }
        m.last_read_us = Some(read_epoch_us);
    }

    pub fn record_drop(&self) {
        self.inner.lock().unwrap().dropped += 1;
    }

    pub fn record_alert(&self) {
        self.inner.lock().unwrap().alerts += 1;
    }

    /// Print a formatted metrics summary for this sensor.
    pub fn print_summary(&self) {
        let m = self.inner.lock().unwrap();
        println!("  ┌─ Sensor: {}", m.name);
        println!("  │  cycles={} dropped={} alerts={}",
            m.total_cycles, m.dropped, m.alerts);
        println!("  │  {}", m.drift.summary("Drift   "));
        println!("  │  {}", m.latency.summary("Latency "));
        println!("  └  {}", m.jitter.summary("Jitter  "));
    }
}

// ---------------------------------------------------------------------------
// Global OCS metric collection
// ---------------------------------------------------------------------------

/// Holds one MetricsHandle per sensor, plus system-wide counters.
#[derive(Clone)]
pub struct OcsMetrics {
    pub thermal:  MetricsHandle,
    pub power:    MetricsHandle,
    pub attitude: MetricsHandle,
}

impl OcsMetrics {
    pub fn new() -> Self {
        use std::time::Duration;
        Self {
            thermal:  MetricsHandle::new("THERMAL",  Duration::from_millis(100)),
            power:    MetricsHandle::new("POWER",    Duration::from_millis(250)),
            attitude: MetricsHandle::new("ATTITUDE", Duration::from_millis(500)),
        }
    }

    /// Print a complete metrics report for all three sensors.
    pub fn print_report(&self) {
        println!("\n╔══════════════════════════════════════════════╗");
        println!("║     TASK 1 — SENSOR ACQUISITION METRICS      ║");
        println!("╚══════════════════════════════════════════════╝");
        self.thermal.print_summary();
        println!();
        self.power.print_summary();
        println!();
        self.attitude.print_summary();
        println!();
    }
}