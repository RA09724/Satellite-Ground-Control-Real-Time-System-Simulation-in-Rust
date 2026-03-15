// =============================================================================
// types.rs — Shared types for the Satellite Onboard Control System (OCS)
// =============================================================================

use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Sensor identity & scheduling parameters
// ---------------------------------------------------------------------------

/// The three onboard sensors, each with a distinct priority tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SensorType {
    Thermal,     // CRITICAL  – highest priority, tightest jitter budget
    Power,       // HIGH      – medium priority
    Attitude,    // MEDIUM    – lowest priority among the three
}

impl SensorType {
    /// Human-readable label used in log output.
    pub fn label(&self) -> &'static str {
        match self {
            SensorType::Thermal  => "THERMAL",
            SensorType::Power    => "POWER",
            SensorType::Attitude => "ATTITUDE",
        }
    }

    /// Numeric priority — lower value == higher priority (used for buffer ordering).
    pub fn priority(&self) -> u8 {
        match self {
            SensorType::Thermal  => 0,
            SensorType::Power    => 1,
            SensorType::Attitude => 2,
        }
    }

    /// Nominal sampling period for Rate-Monotonic scheduling.
    /// Thermal is sampled most frequently because it is safety-critical.
    pub fn period(&self) -> Duration {
        match self {
            SensorType::Thermal  => Duration::from_millis(100),  // 10 Hz
            SensorType::Power    => Duration::from_millis(250),  //  4 Hz
            SensorType::Attitude => Duration::from_millis(500),  //  2 Hz
        }
    }

    /// Whether this sensor is considered safety-critical.
    pub fn is_critical(&self) -> bool {
        matches!(self, SensorType::Thermal)
    }

    /// Maximum allowable jitter for this sensor (used in validation).
    pub fn jitter_budget(&self) -> Duration {
        match self {
            SensorType::Thermal  => Duration::from_micros(1_000), // <1 ms
            SensorType::Power    => Duration::from_millis(5),
            SensorType::Attitude => Duration::from_millis(10),
        }
    }
}

// ---------------------------------------------------------------------------
// A single sensor reading captured by the acquisition task
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SensorReading {
    /// Which sensor produced this reading.
    pub sensor: SensorType,

    /// The simulated measurement value (arbitrary units).
    pub value: f64,

    /// Absolute time at which the sensor was actually read.
    pub read_at: Instant,

    /// The time at which this cycle was *scheduled* to start.
    /// Used to compute scheduling drift.
    pub scheduled_at: Instant,

    /// Monotonically increasing cycle index for this sensor.
    pub cycle: u64,
}

impl SensorReading {
    /// Scheduling drift = actual read time − scheduled start time.
    /// Positive value means the task started late (common in real systems).
    pub fn scheduling_drift(&self) -> Duration {
        self.read_at.saturating_duration_since(self.scheduled_at)
    }

    /// Convenience: drift as signed microseconds for logging.
    pub fn drift_us(&self) -> i64 {
        self.scheduling_drift().as_micros() as i64
    }
}

// ---------------------------------------------------------------------------
// Buffer-level events
// ---------------------------------------------------------------------------

/// Logged whenever the bounded buffer is full and a sample must be discarded.
#[derive(Debug, Clone)]
pub struct DroppedSample {
    pub sensor: SensorType,
    pub cycle: u64,
    pub dropped_at: Instant,
    pub buffer_len: usize,
}

// ---------------------------------------------------------------------------
// Latency record: sensor read → successful buffer insertion
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct InsertionLatency {
    pub sensor: SensorType,
    pub cycle: u64,
    /// Duration from sensor read to the moment the item was placed in the buffer.
    pub latency: Duration,
}

// ---------------------------------------------------------------------------
// Safety alert raised when a critical sensor misses too many cycles
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SafetyAlert {
    pub sensor: SensorType,
    /// How many consecutive cycles were missed at the point the alert fired.
    pub consecutive_misses: u32,
    pub raised_at: Instant,
}

impl SafetyAlert {
    pub const MISS_THRESHOLD: u32 = 3;
}

// ---------------------------------------------------------------------------
// Aggregated per-sensor statistics (printed in benchmarking)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct SensorStats {
    pub total_cycles: u64,
    pub dropped: u64,
    pub alerts_raised: u64,
    pub total_drift_us: i64,
    pub max_drift_us: i64,
    pub total_latency_us: u64,
    pub max_latency_us: u64,
    /// Running sum of squared jitter values (for stddev computation).
    pub jitter_sq_sum: f64,
    pub jitter_samples: u64,
    pub last_read_at: Option<Instant>,
    pub jitter_sum_us: f64,
}

impl SensorStats {
    /// Record one successful cycle.
    pub fn record_cycle(&mut self, drift_us: i64, latency: Duration, actual_read: Instant, period: Duration) {
        self.total_cycles += 1;
        self.total_drift_us += drift_us;
        if drift_us > self.max_drift_us {
            self.max_drift_us = drift_us;
        }
        let lat_us = latency.as_micros() as u64;
        self.total_latency_us += lat_us;
        if lat_us > self.max_latency_us {
            self.max_latency_us = lat_us;
        }

        // Jitter = deviation from ideal period between successive reads
        if let Some(prev) = self.last_read_at {
            let actual_interval = actual_read.duration_since(prev).as_micros() as f64;
            let ideal_interval  = period.as_micros() as f64;
            let jitter_us       = (actual_interval - ideal_interval).abs();
            self.jitter_sum_us  += jitter_us;
            self.jitter_sq_sum  += jitter_us * jitter_us;
            self.jitter_samples += 1;
        }
        self.last_read_at = Some(actual_read);
    }

    pub fn avg_drift_us(&self) -> f64 {
        if self.total_cycles == 0 { return 0.0; }
        self.total_drift_us as f64 / self.total_cycles as f64
    }

    pub fn avg_latency_us(&self) -> f64 {
        if self.total_cycles == 0 { return 0.0; }
        self.total_latency_us as f64 / self.total_cycles as f64
    }

    pub fn avg_jitter_us(&self) -> f64 {
        if self.jitter_samples == 0 { return 0.0; }
        self.jitter_sum_us / self.jitter_samples as f64
    }

    /// Population standard deviation of jitter (microseconds).
    pub fn jitter_stddev_us(&self) -> f64 {
        if self.jitter_samples == 0 { return 0.0; }
        let mean = self.avg_jitter_us();
        let variance = self.jitter_sq_sum / self.jitter_samples as f64 - mean * mean;
        variance.max(0.0).sqrt()
    }
}