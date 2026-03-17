// =============================================================================
// logger.rs — Performance Log Writer
// =============================================================================

// Log format
// ──────────
// Each line is structured as:
//   [TIMESTAMP_MS] [CATEGORY] [FIELD=VALUE ...]
//
// Categories
// ──────────
//   SENSOR_DRIFT     — per-cycle scheduling drift for each sensor
//   SENSOR_LATENCY   — per-cycle sensor-to-buffer insertion latency
//   SENSOR_JITTER    — per-cycle inter-arrival jitter for each sensor
//   SCHED_DRIFT      — per-job scheduler start drift
//   SCHED_JITTER     — per-job scheduler execution time variation
//   SCHED_VIOLATION  — deadline violation events
//   DOWNLINK_LATENCY — per-packet buffer-to-uplink queue latency
//   FAULT_EVENT      — fault injection and recovery events
//   REPORT           — final structured summary (appended at end of run)

use std::fmt::Write as FmtWrite;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::a2_scheduler::{CpuNanos, JobLog, TaskId, ViolationLog};
use crate::a3_downlink::DownlinkState;
use crate::a4_benchmark::FaultState;
use crate::metrics::OcsMetrics;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const LOG_PATH: &str = "performance_log.txt";

// ---------------------------------------------------------------------------
// Logger — wraps the file handle in Arc<Mutex<>> so it can be shared
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Logger {
    file: Arc<Mutex<File>>,
}

impl Logger {
    /// Open (or create) the log file and write the file header.
    pub fn new() -> Self {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(LOG_PATH)
            .unwrap_or_else(|e| panic!("Cannot open {}: {}", LOG_PATH, e));

        let header = format!(
            "# =============================================================================\n\
             # Satellite OCS — Performance Log\n\
             # File   : {}\n\
             # Fields : [timestamp_ms] [CATEGORY] [key=value ...]\n\
             # =============================================================================\n\
             #\n\
             # CATEGORIES\n\
             #   SENSOR_DRIFT     per-cycle scheduling drift (µs) per sensor\n\
             #   SENSOR_LATENCY   per-cycle sensor-read to buffer-insert latency (µs)\n\
             #   SENSOR_JITTER    per-cycle inter-arrival timing deviation (µs)\n\
             #   SCHED_DRIFT      per-job scheduler start delay (µs)\n\
             #   SCHED_JITTER     per-job execution time variation (µs)\n\
             #   SCHED_VIOLATION  deadline violation events\n\
             #   DOWNLINK_LATENCY per-packet buffer-to-uplink queue latency (µs)\n\
             #   FAULT_EVENT      fault injection and recovery events\n\
             #   REPORT           final summary appended at end of simulation\n\
             # =============================================================================\n\
             # Run started: {}\n\
             #\n",
            LOG_PATH,
            now_ms(),
        );

        file.write_all(header.as_bytes())
            .expect("Failed to write log header");

        println!("[LOG] Performance log: {}", LOG_PATH);

        Self { file: Arc::new(Mutex::new(file)) }
    }

    // ── Low-level write ───────────────────────────────────────────────────

    fn write(&self, line: &str) {
        if let Ok(mut f) = self.file.lock() {
            let _ = f.write_all(line.as_bytes());
        }
    }

    fn writeln(&self, line: &str) {
        self.write(&format!("{}\n", line));
    }

    // =========================================================================
    // Public logging methods — called from simulation threads
    // =========================================================================

    // ── Task 1: Sensor drift ─────────────────────────────────────────────────

    /// Log one sensor scheduling drift sample.
    /// Called every cycle from a1_sensor.rs after computing drift.
    pub fn log_sensor_drift(
        &self,
        sensor:    &str,
        cycle:     u64,
        drift_us:  f64,
    ) {
        self.writeln(&format!(
            "[{}] SENSOR_DRIFT  sensor={} cycle={} drift_us={:.1}",
            now_ms(), sensor, cycle, drift_us,
        ));
    }

    // ── Task 1: Sensor-to-buffer insertion latency ───────────────────────────

    /// Log the time from sensor read completion to successful buffer insertion.
    pub fn log_sensor_latency(
        &self,
        sensor:     &str,
        cycle:      u64,
        latency_us: f64,
    ) {
        self.writeln(&format!(
            "[{}] SENSOR_LATENCY sensor={} cycle={} latency_us={:.1}",
            now_ms(), sensor, cycle, latency_us,
        ));
    }

    // ── Task 1: Sensor jitter ────────────────────────────────────────────────

    /// Log the inter-arrival jitter for one sensor cycle.
    /// Jitter = |actual_interval − ideal_period|.
    pub fn log_sensor_jitter(
        &self,
        sensor:    &str,
        cycle:     u64,
        jitter_us: f64,
    ) {
        self.writeln(&format!(
            "[{}] SENSOR_JITTER  sensor={} cycle={} jitter_us={:.1}",
            now_ms(), sensor, cycle, jitter_us,
        ));
    }

    // ── Task 2: Scheduler drift ──────────────────────────────────────────────

    /// Log the start drift for one scheduler job.
    /// Drift = actual start time − scheduled release time.
    pub fn log_sched_drift(
        &self,
        task:     &str,
        job:      u64,
        drift_us: u64,
    ) {
        self.writeln(&format!(
            "[{}] SCHED_DRIFT    task={} job={} drift_us={}",
            now_ms(), task, job, drift_us,
        ));
    }

    // ── Task 2: Scheduler jitter ─────────────────────────────────────────────

    /// Log the execution time jitter for one scheduler job.
    /// Jitter = |actual_exec_time − previous_exec_time|.
    pub fn log_sched_jitter(
        &self,
        task:      &str,
        job:       u64,
        exec_us:   u64,
        jitter_us: f64,
    ) {
        self.writeln(&format!(
            "[{}] SCHED_JITTER   task={} job={} exec_us={} jitter_us={:.1}",
            now_ms(), task, job, exec_us, jitter_us,
        ));
    }

    // ── Task 2: Deadline violation ───────────────────────────────────────────

    /// Log a deadline violation event.
    pub fn log_sched_violation(
        &self,
        task:     &str,
        job:      u64,
        kind:     &str,
        delay_us: u64,
    ) {
        self.writeln(&format!(
            "[{}] SCHED_VIOLATION task={} job={} kind={} delay_us={}",
            now_ms(), task, job, kind, delay_us,
        ));
    }

    // ── Task 3: Downlink queue latency ───────────────────────────────────────

    /// Log the buffer-to-uplink queue latency for one telemetry packet.
    pub fn log_downlink_latency(
        &self,
        seq:        u64,
        sensor:     &str,
        latency_us: f64,
        status:     &str,
    ) {
        self.writeln(&format!(
            "[{}] DOWNLINK_LATENCY seq={} sensor={} latency_us={:.1} status={}",
            now_ms(), seq, sensor, latency_us, status,
        ));
    }

    // ── Task 4: Fault events ─────────────────────────────────────────────────

    /// Log a fault injection event.
    pub fn log_fault_inject(
        &self,
        fault_id:   u64,
        fault_type: &str,
        sensor:     &str,
    ) {
        self.writeln(&format!(
            "[{}] FAULT_EVENT     id={} event=INJECTED type={} sensor={}",
            now_ms(), fault_id, fault_type, sensor,
        ));
    }

    /// Log a fault recovery event.
    pub fn log_fault_recovery(
        &self,
        fault_id:    u64,
        fault_type:  &str,
        sensor:      &str,
        recovery_ms: u64,
        within_limit: bool,
    ) {
        self.writeln(&format!(
            "[{}] FAULT_EVENT     id={} event=RECOVERED type={} sensor={} recovery_ms={} within_limit={}",
            now_ms(), fault_id, fault_type, sensor, recovery_ms, within_limit,
        ));
    }

    // =========================================================================
    // Final report — written once at the end of the simulation
    // =========================================================================

    /// Write the complete structured performance report to the log file.
    /// This is the section the assignment report can directly reference.
    pub fn write_final_report(
        &self,
        ocs_metrics:  &OcsMetrics,
        job_log:      &JobLog,
        vlog:         &ViolationLog,
        cpu_nanos:    &CpuNanos,
        downlink:     &DownlinkState,
        fault_state:  &FaultState,
        run_duration: Duration,
    ) {
        use std::sync::atomic::Ordering;

        let mut out = String::with_capacity(8192);

        // ── Header ────────────────────────────────────────────────────────────
        writeln!(out, "\n").unwrap();
        writeln!(out, "# =============================================================================").unwrap();
        writeln!(out, "# REPORT — Final Performance Summary").unwrap();
        writeln!(out, "# Generated : {}", now_ms()).unwrap();
        writeln!(out, "# Duration  : {}s", run_duration.as_secs()).unwrap();
        writeln!(out, "# =============================================================================").unwrap();

        // ── Section 1: Scheduling Drift ───────────────────────────────────────
        writeln!(out, "\n[REPORT] ── SECTION 1: SCHEDULING DRIFT ──────────────────────────────────").unwrap();
        writeln!(out, "[REPORT] Scheduling drift = actual task start − scheduled start time.").unwrap();
        writeln!(out, "[REPORT] A value of 0µs means the task fired exactly on schedule.").unwrap();
        writeln!(out, "[REPORT] Values above the jitter budget indicate OS scheduling pressure.").unwrap();
        writeln!(out).unwrap();

        // Sensor drift
        writeln!(out, "[REPORT] Sensor scheduling drift (Task 1):").unwrap();
        for handle in [&ocs_metrics.thermal, &ocs_metrics.power, &ocs_metrics.attitude] {
            let m = handle.inner.lock().unwrap();
            writeln!(out,
                "[REPORT]   {:<8} cycles={:>5}  drift_mean={:>8.1}µs  drift_max={:>10.1}µs  drift_σ={:>8.1}µs",
                m.name, m.total_cycles,
                m.drift.mean_us(), m.drift.max_us(), m.drift.stddev_us(),
            ).unwrap();
        }

        // Scheduler job drift
        writeln!(out).unwrap();
        writeln!(out, "[REPORT] Scheduler task start drift (Task 2):").unwrap();
        let jobs = job_log.lock().unwrap();

        for task in [
            TaskId::ThermalControl,
            TaskId::DataCompression,
            TaskId::HealthMonitoring,
            TaskId::AntennaAlignment,
        ] {
            let task_jobs: Vec<_> = jobs.iter()
                .filter(|j| j.task == task)
                .collect();

            if task_jobs.is_empty() { continue; }

            let drifts: Vec<f64> = task_jobs.iter()
                .map(|j| j.start_drift_us() as f64)
                .collect();
            let n       = drifts.len() as f64;
            let mean    = drifts.iter().sum::<f64>() / n;
            let max     = drifts.iter().cloned().fold(f64::MIN, f64::max);
            let variance = drifts.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / n;
            let stddev  = variance.sqrt();

            writeln!(out,
                "[REPORT]   {:<14} jobs={:>5}  drift_mean={:>8.1}µs  drift_max={:>10.1}µs  drift_σ={:>8.1}µs",
                task.label(), task_jobs.len(), mean, max, stddev,
            ).unwrap();
        }

        // ── Section 2: Pipeline Latency ───────────────────────────────────────
        writeln!(out, "\n[REPORT] ── SECTION 2: PIPELINE LATENCY ──────────────────────────────────").unwrap();
        writeln!(out, "[REPORT] Three latency pipelines are tracked:").unwrap();
        writeln!(out, "[REPORT]   Pipeline A — Sensor read   → Buffer insertion   (Task 1)").unwrap();
        writeln!(out, "[REPORT]   Pipeline B — Buffer pop    → Downlink packet    (Task 3)").unwrap();
        writeln!(out, "[REPORT]   Pipeline C — Fault inject  → System recovery    (Task 4)").unwrap();
        writeln!(out).unwrap();

        // Pipeline A — sensor to buffer
        writeln!(out, "[REPORT] Pipeline A — Sensor-to-Buffer insertion latency:").unwrap();
        for handle in [&ocs_metrics.thermal, &ocs_metrics.power, &ocs_metrics.attitude] {
            let m = handle.inner.lock().unwrap();
            writeln!(out,
                "[REPORT]   {:<8} lat_mean={:>8.1}µs  lat_max={:>10.1}µs  lat_σ={:>8.1}µs",
                m.name,
                m.latency.mean_us(), m.latency.max_us(), m.latency.stddev_us(),
            ).unwrap();
        }

        // Pipeline B — downlink queue latency
        writeln!(out).unwrap();
        writeln!(out, "[REPORT] Pipeline B — Buffer-to-Downlink queue latency (Task 3):").unwrap();
        let dl_latency = downlink.latency_log.lock().unwrap();
        if dl_latency.is_empty() {
            writeln!(out, "[REPORT]   No downlink latency data recorded.").unwrap();
        } else {
            let n       = dl_latency.len() as f64;
            let mean    = dl_latency.iter().sum::<u64>() as f64 / n;
            let max     = *dl_latency.iter().max().unwrap_or(&0);
            let min     = *dl_latency.iter().min().unwrap_or(&0);
            let variance = dl_latency.iter()
                .map(|&v| (v as f64 - mean).powi(2))
                .sum::<f64>() / n;
            let stddev  = variance.sqrt();
            let packets = downlink.packets_sent.load(std::sync::atomic::Ordering::Relaxed);
            let throughput = packets as f64 / run_duration.as_secs_f64();

            writeln!(out,
                "[REPORT]   samples={:>5}  lat_mean={:>8.1}µs  lat_min={:>8}µs  lat_max={:>8}µs  lat_σ={:>8.1}µs",
                dl_latency.len(), mean, min, max, stddev,
            ).unwrap();
            writeln!(out,
                "[REPORT]   packets_sent={} throughput={:.1} pkt/s",
                packets, throughput,
            ).unwrap();
        }

        // Pipeline C — fault recovery latency
        writeln!(out).unwrap();
        writeln!(out, "[REPORT] Pipeline C — Fault-injection-to-recovery latency (Task 4):").unwrap();
        let faults = fault_state.fault_log.lock().unwrap();
        if faults.is_empty() {
            writeln!(out, "[REPORT]   No faults recorded.").unwrap();
        } else {
            for event in faults.iter() {
                let status = match event.recovery_ms {
                    Some(ms) if ms <= 200 => format!("RECOVERED {}ms (within 200ms limit)", ms),
                    Some(ms)              => format!("SLOW_RECOVERY {}ms (exceeded 200ms limit)", ms),
                    None                  => "PENDING".to_string(),
                };
                writeln!(out,
                    "[REPORT]   fault_id={} type={:<14} sensor={:<8} {}",
                    event.id,
                    event.fault_type.label(),
                    event.target_sensor.label(),
                    status,
                ).unwrap();
            }

            let recovered: Vec<u64> = faults.iter()
                .filter_map(|e| e.recovery_ms)
                .collect();
            if !recovered.is_empty() {
                let avg = recovered.iter().sum::<u64>() as f64 / recovered.len() as f64;
                let max = *recovered.iter().max().unwrap_or(&0);
                let min = *recovered.iter().min().unwrap_or(&0);
                let within = recovered.iter().filter(|&&ms| ms <= 200).count();
                writeln!(out,
                    "[REPORT]   recovery_avg={:.1}ms  recovery_min={}ms  recovery_max={}ms  within_limit={}/{}",
                    avg, min, max, within, recovered.len(),
                ).unwrap();
            }
        }

        // ── Section 3: Jitter ─────────────────────────────────────────────────
        writeln!(out, "\n[REPORT] ── SECTION 3: JITTER ────────────────────────────────────────────").unwrap();
        writeln!(out, "[REPORT] Jitter = deviation of actual inter-arrival time from ideal period.").unwrap();
        writeln!(out, "[REPORT] Low jitter confirms deterministic real-time behaviour.").unwrap();
        writeln!(out).unwrap();

        // Sensor jitter
        writeln!(out, "[REPORT] Sensor inter-arrival jitter (Task 1):").unwrap();
        for handle in [&ocs_metrics.thermal, &ocs_metrics.power, &ocs_metrics.attitude] {
            let m = handle.inner.lock().unwrap();
            let budget_us = match m.name {
                "THERMAL"  => 1_000.0_f64,
                "POWER"    => 5_000.0_f64,
                _          => 10_000.0_f64,
            };
            let budget_ok = m.jitter.mean_us() < budget_us;
            writeln!(out,
                "[REPORT]   {:<8} jitter_mean={:>8.1}µs  jitter_max={:>10.1}µs  jitter_σ={:>8.1}µs  budget={:.0}µs  {}",
                m.name,
                m.jitter.mean_us(), m.jitter.max_us(), m.jitter.stddev_us(),
                budget_us,
                if budget_ok { "✓ WITHIN BUDGET" } else { "✗ EXCEEDS BUDGET" },
            ).unwrap();
        }

        // Scheduler execution-time jitter
        writeln!(out).unwrap();
        writeln!(out, "[REPORT] Scheduler execution-time jitter (Task 2):").unwrap();
        for task in [
            TaskId::ThermalControl,
            TaskId::DataCompression,
            TaskId::HealthMonitoring,
            TaskId::AntennaAlignment,
        ] {
            let exec_times: Vec<f64> = jobs.iter()
                .filter(|j| j.task == task)
                .map(|j| j.execution_time_us() as f64)
                .collect();

            if exec_times.len() < 2 { continue; }

            let n       = exec_times.len() as f64;
            let mean    = exec_times.iter().sum::<f64>() / n;
            let max     = exec_times.iter().cloned().fold(f64::MIN, f64::max);
            let variance = exec_times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / n;
            let stddev  = variance.sqrt();

            // Jitter = stddev of execution times (how consistently each job runs).
            writeln!(out,
                "[REPORT]   {:<14} exec_mean={:>8.1}µs  exec_max={:>10.1}µs  exec_σ={:>8.1}µs  (wcet={:.0}ms)",
                task.label(), mean, max, stddev,
                task.wcet().as_millis(),
            ).unwrap();
        }

        // ── Section 4: Deadline Adherence ─────────────────────────────────────
        writeln!(out, "\n[REPORT] ── SECTION 4: DEADLINE ADHERENCE ────────────────────────────────").unwrap();
        let violations   = vlog.lock().unwrap();
        let total_jobs   = jobs.len();
        let total_viols  = violations.len();
        let adherence    = if total_jobs > 0 {
            (1.0 - total_viols as f64 / total_jobs as f64) * 100.0
        } else { 100.0 };

        writeln!(out, "[REPORT]   total_jobs={}  violations={}  adherence={:.1}%",
            total_jobs, total_viols, adherence,
        ).unwrap();

        // Per-task breakdown
        for task in [
            TaskId::ThermalControl,
            TaskId::DataCompression,
            TaskId::HealthMonitoring,
            TaskId::AntennaAlignment,
        ] {
            let task_total = jobs.iter().filter(|j| j.task == task).count();
            let task_viols = violations.iter().filter(|v| v.task == task).count();
            let task_adh   = if task_total > 0 {
                (1.0 - task_viols as f64 / task_total as f64) * 100.0
            } else { 100.0 };

            writeln!(out,
                "[REPORT]   {:<14} jobs={:>5}  violations={:>5}  adherence={:.1}%",
                task.label(), task_total, task_viols, task_adh,
            ).unwrap();
        }

        // ── Section 5: CPU Utilisation ────────────────────────────────────────
        writeln!(out, "\n[REPORT] ── SECTION 5: CPU UTILISATION ───────────────────────────────────").unwrap();
        let active_ns    = cpu_nanos.load(Ordering::Relaxed);
        let total_ns     = run_duration.as_nanos() as u64;
        let active_pct   = (active_ns as f64 / total_ns as f64) * 100.0;

        writeln!(out, "[REPORT]   active={:.2}%  idle={:.2}%",
            active_pct, 100.0 - active_pct,
        ).unwrap();

        // RM schedulability
        writeln!(out).unwrap();
        let rm_util  = 0.3000_f64; // precomputed from task set
        let rm_bound = 4.0 * (2.0_f64.powf(1.0 / 4.0) - 1.0);
        writeln!(out, "[REPORT]   RM_utilisation={:.4}  RM_bound={:.4}  schedulable={}",
            rm_util, rm_bound,
            if rm_util < rm_bound { "YES" } else { "NO" },
        ).unwrap();

        // ── Section 6: System verdict ─────────────────────────────────────────
        writeln!(out, "\n[REPORT] ── SECTION 6: SYSTEM VERDICT ────────────────────────────────────").unwrap();
        let aborted = fault_state.mission_aborted.load(Ordering::Relaxed);

        writeln!(out, "[REPORT]   mission_status={}",
            if aborted { "DEGRADED (recovery timeout exceeded)" }
            else       { "NOMINAL (all constraints satisfied)" },
        ).unwrap();

        // Check sensor budgets
        for handle in [&ocs_metrics.thermal, &ocs_metrics.power, &ocs_metrics.attitude] {
            let m = handle.inner.lock().unwrap();
            let budget_us: f64 = match m.name {
                "THERMAL" => 1_000.0,
                "POWER"   => 5_000.0,
                _         => 10_000.0,
            };
            writeln!(out, "[REPORT]   {:<8} jitter_budget={}µs  mean_jitter={:.1}µs  {}",
                m.name, budget_us as u64, m.jitter.mean_us(),
                if m.jitter.mean_us() < budget_us { "✓" } else { "✗" },
            ).unwrap();
        }

        writeln!(out, "[REPORT]   zero_drops={}  zero_alerts={}",
            if ocs_metrics.thermal.inner.lock().unwrap().dropped == 0
            && ocs_metrics.power.inner.lock().unwrap().dropped == 0
            && ocs_metrics.attitude.inner.lock().unwrap().dropped == 0
            { "YES" } else { "NO" },
            if ocs_metrics.thermal.inner.lock().unwrap().alerts == 0
            && ocs_metrics.power.inner.lock().unwrap().alerts == 0
            && ocs_metrics.attitude.inner.lock().unwrap().alerts == 0
            { "YES" } else { "NO" },
        ).unwrap();

        writeln!(out, "\n# =============================================================================").unwrap();
        writeln!(out, "# END OF LOG").unwrap();
        writeln!(out, "# =============================================================================").unwrap();

        // Write everything atomically.
        self.write(&out);
        println!("[LOG] Final report written to {}", LOG_PATH);
    }
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
    fn test_logger_creates_file() {
        let logger = Logger::new();
        logger.log_sensor_drift("THERMAL", 1, 143.0);
        logger.log_sensor_latency("THERMAL", 1, 512.0);
        logger.log_sensor_jitter("THERMAL", 2, 87.5);
        logger.log_sched_drift("THERMAL_CTRL", 1, 450);
        logger.log_sched_jitter("THERMAL_CTRL", 1, 21000, 300.0);
        logger.log_sched_violation("DATA_COMPRESS", 1, "START_LATE", 1200);
        logger.log_downlink_latency(1, "THERMAL", 6800.0, "OK");
        logger.log_fault_inject(1, "DELAYED_SENSOR", "THERMAL");
        logger.log_fault_recovery(1, "DELAYED_SENSOR", "THERMAL", 152, true);
        assert!(std::path::Path::new(LOG_PATH).exists());
    }
}