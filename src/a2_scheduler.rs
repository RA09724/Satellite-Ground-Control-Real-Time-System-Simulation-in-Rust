// =============================================================================
// a2_scheduler.rs — Task 2: Real-Time Task Scheduling
// =============================================================================
//
// Requirements addressed
// ───────────────────────
// ✔  Three schedulable tasks: DataCompression, HealthMonitoring, AntennaAlignment
// ✔  Rate-Monotonic (RM) scheduling — shorter period == higher priority
// ✔  Preemption: ThermalControl overrides all lower-priority tasks
// ✔  Deadline violation logging (start delay AND completion delay)
// ✔  Scheduling drift and task execution jitter measured per task
// ✔  CPU utilisation: % active time vs idle time
//
// Design
// ──────
// Each task runs in its own thread pinned to a dedicated CPU core (cores 4–7),
// keeping them isolated from the Task 1 sensor threads (cores 1–3).
// A shared AtomicBool preempt_flag lets ThermalControl signal lower-priority
// tasks to suspend. Tasks poll this flag every 5 ms work-slice.
//
// Rate-Monotonic task set:
//   ThermalControl   — 200 ms period, priority 0  (CRITICAL, preempts others)
//   DataCompression  — 300 ms period, priority 1
//   HealthMonitoring — 500 ms period, priority 2
//   AntennaAlignment — 800 ms period, priority 3  (lowest)

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::logger::Logger;

// ---------------------------------------------------------------------------
// Task identity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskId {
    ThermalControl,
    DataCompression,
    HealthMonitoring,
    AntennaAlignment,
}

impl TaskId {
    pub fn label(&self) -> &'static str {
        match self {
            TaskId::ThermalControl   => "THERMAL_CTRL",
            TaskId::DataCompression  => "DATA_COMPRESS",
            TaskId::HealthMonitoring => "HEALTH_MON",
            TaskId::AntennaAlignment => "ANTENNA_ALIGN",
        }
    }

    pub fn priority(&self) -> u8 {
        match self {
            TaskId::ThermalControl   => 0,
            TaskId::DataCompression  => 1,
            TaskId::HealthMonitoring => 2,
            TaskId::AntennaAlignment => 3,
        }
    }

    pub fn period(&self) -> Duration {
        match self {
            TaskId::ThermalControl   => Duration::from_millis(200),
            TaskId::DataCompression  => Duration::from_millis(300),
            TaskId::HealthMonitoring => Duration::from_millis(500),
            TaskId::AntennaAlignment => Duration::from_millis(800),
        }
    }

    /// Simulated worst-case execution time — well below period for RM schedulability.
    pub fn wcet(&self) -> Duration {
        match self {
            TaskId::ThermalControl   => Duration::from_millis(20),
            TaskId::DataCompression  => Duration::from_millis(30),
            TaskId::HealthMonitoring => Duration::from_millis(25),
            TaskId::AntennaAlignment => Duration::from_millis(40),
        }
    }

    /// Implicit deadline = period (standard RM assumption).
    pub fn deadline(&self) -> Duration { self.period() }

    /// CPU core — offset from Task 1 cores (1–3) to avoid contention.
    pub fn cpu_core(&self) -> usize {
        match self {
            TaskId::ThermalControl   => 4,
            TaskId::DataCompression  => 5,
            TaskId::HealthMonitoring => 6,
            TaskId::AntennaAlignment => 7,
        }
    }
}

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DeadlineViolation {
    pub task:        TaskId,
    pub job:         u64,
    pub kind:        ViolationKind,
    pub delay_us:    u64,
    pub detected_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub enum ViolationKind {
    StartDelay,
    CompletionDelay,
}

impl ViolationKind {
    pub fn label(&self) -> &'static str {
        match self {
            ViolationKind::StartDelay      => "START_LATE",
            ViolationKind::CompletionDelay => "FINISH_LATE",
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobRecord {
    pub task:        TaskId,
    pub job:         u64,
    pub released_at: Instant,
    pub started_at:  Instant,
    pub finished_at: Instant,
    pub preempted:   bool,
}

impl JobRecord {
    pub fn start_drift_us(&self) -> u64 {
        self.started_at
            .saturating_duration_since(self.released_at)
            .as_micros() as u64
    }
    pub fn execution_time_us(&self) -> u64 {
        self.finished_at
            .duration_since(self.started_at)
            .as_micros() as u64
    }
}

// ---------------------------------------------------------------------------
// Shared state type aliases
// ---------------------------------------------------------------------------

pub type PreemptFlag  = Arc<AtomicBool>;
pub type ViolationLog = Arc<Mutex<Vec<DeadlineViolation>>>;
pub type JobLog       = Arc<Mutex<Vec<JobRecord>>>;
pub type CpuNanos     = Arc<AtomicU64>;

// ---------------------------------------------------------------------------
// Windows helpers
// ---------------------------------------------------------------------------

#[cfg(windows)]
extern crate winapi;
#[cfg(windows)]
use winapi::um::processthreadsapi::{GetCurrentThread, SetThreadPriority};
#[cfg(windows)]
use winapi::um::winbase::SetThreadAffinityMask;
#[cfg(windows)]
const THREAD_PRIORITY_ABOVE_NORMAL:  i32 = 1;
#[cfg(windows)]
const THREAD_PRIORITY_TIME_CRITICAL: i32 = 15;

fn set_task_priority(task: TaskId) {
    #[cfg(windows)]
    unsafe {
        let p = if task == TaskId::ThermalControl {
            THREAD_PRIORITY_TIME_CRITICAL
        } else {
            THREAD_PRIORITY_ABOVE_NORMAL
        };
        SetThreadPriority(GetCurrentThread(), p);
    }
}

fn pin_to_core(core: usize) {
    #[cfg(windows)]
    unsafe {
        SetThreadAffinityMask(GetCurrentThread(), 1 << core);
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub fn spawn_scheduler_threads(
    stop:         Arc<AtomicBool>,
    run_duration: Duration,
    logger:       Logger,
) -> (Vec<thread::JoinHandle<()>>, ViolationLog, JobLog, CpuNanos) {

    let preempt_flag  : PreemptFlag  = Arc::new(AtomicBool::new(false));
    let violation_log : ViolationLog = Arc::new(Mutex::new(Vec::new()));
    let job_log       : JobLog       = Arc::new(Mutex::new(Vec::new()));
    let cpu_nanos     : CpuNanos     = Arc::new(AtomicU64::new(0));

    let tasks = [
        TaskId::ThermalControl,
        TaskId::DataCompression,
        TaskId::HealthMonitoring,
        TaskId::AntennaAlignment,
    ];

    let handles = tasks.iter().map(|&task| {
        let stp          = stop.clone();
        let pf           = preempt_flag.clone();
        let vlog         = violation_log.clone();
        let jlog         = job_log.clone();
        let cpu          = cpu_nanos.clone();
        let log          = logger.clone();
        let deadline_end = Instant::now() + run_duration;

        thread::spawn(move || {
            set_task_priority(task);
            pin_to_core(task.cpu_core());
            run_task(task, stp, pf, vlog, jlog, cpu, deadline_end, log);
        })
    }).collect();

    (handles, violation_log, job_log, cpu_nanos)
}

// ---------------------------------------------------------------------------
// Per-task loop
// ---------------------------------------------------------------------------

fn run_task(
    task:         TaskId,
    stop:         Arc<AtomicBool>,
    preempt_flag: PreemptFlag,
    vlog:         ViolationLog,
    jlog:         JobLog,
    cpu_nanos:    CpuNanos,
    deadline_end: Instant,
    logger:       Logger,
) {
    let period     = task.period();
    let wcet       = task.wcet();
    let is_thermal = task == TaskId::ThermalControl;

    let mut job:          u64           = 0;
    let mut next_release               = Instant::now();
    let mut last_finish: Option<Instant> = None;

    println!(
        "[SCHED][INIT] {} | period={:?} | wcet={:?} | priority={}",
        task.label(), period, wcet, task.priority()
    );

    loop {
        if stop.load(Ordering::Relaxed) || Instant::now() >= deadline_end {
            break;
        }

        job += 1;
        let released_at  = next_release;
        let abs_deadline = released_at + task.deadline();

        // Sleep until release time.
        sleep_until(released_at);

        let started_at      = Instant::now();
        let start_drift_us  = started_at
            .saturating_duration_since(released_at)
            .as_micros() as u64;

        // Log scheduling drift for every job.
        logger.log_sched_drift(task.label(), job, start_drift_us);

        // Start delay violation (> 500 µs threshold).
        if start_drift_us > 500 {
            let v = DeadlineViolation {
                task, job,
                kind:        ViolationKind::StartDelay,
                delay_us:    start_drift_us,
                detected_at: started_at,
            };
            log_violation(&v);
            logger.log_sched_violation(task.label(), job, v.kind.label(), start_drift_us);
            vlog.lock().unwrap().push(v);
        }

        // ThermalControl raises the preempt flag so lower tasks suspend.
        if is_thermal {
            preempt_flag.store(true, Ordering::SeqCst);
        }

        // Execute the task body.
        let work_start   = Instant::now();
        let mut preempted = false;
        simulate_work(task, wcet, &stop, &preempt_flag, is_thermal, &mut preempted);
        let finished_at  = Instant::now();

        // Accumulate active CPU time.
        let exec_ns = finished_at.duration_since(work_start).as_nanos() as u64;
        cpu_nanos.fetch_add(exec_ns, Ordering::Relaxed);

        // ThermalControl clears the flag when done.
        if is_thermal {
            preempt_flag.store(false, Ordering::SeqCst);
        }

        // Completion deadline violation.
        if finished_at > abs_deadline {
            let overrun_us = finished_at
                .duration_since(abs_deadline)
                .as_micros() as u64;
            let v = DeadlineViolation {
                task, job,
                kind:        ViolationKind::CompletionDelay,
                delay_us:    overrun_us,
                detected_at: finished_at,
            };
            log_violation(&v);
            logger.log_sched_violation(task.label(), job, v.kind.label(), overrun_us);
            vlog.lock().unwrap().push(v);
        }

        // Jitter calculation — log to file.
        let jitter_str = if let Some(prev) = last_finish {
            let actual    = finished_at.duration_since(prev).as_micros() as f64;
            let ideal     = period.as_micros() as f64;
            let jitter_us = (actual - ideal).abs();
            logger.log_sched_jitter(task.label(), job, exec_ns / 1_000, jitter_us);
            format!("jitter={:.1}µs", jitter_us)
        } else {
            "jitter=N/A".to_string()
        };
        last_finish = Some(finished_at);

        println!(
            "[SCHED][JOB]  {} | job={:>4} | drift={:>5}µs | exec={:>5}µs | {} | {}",
            task.label(), job,
            start_drift_us,
            exec_ns / 1_000,
            jitter_str,
            if preempted { "PREEMPTED" } else { "OK" },
        );

        jlog.lock().unwrap().push(JobRecord {
            task, job, released_at, started_at, finished_at, preempted,
        });

        next_release += period;
    }

    println!("[SCHED][DONE] {} completed {} jobs", task.label(), job);
}

// ---------------------------------------------------------------------------
// Simulated work with preemption polling
// ---------------------------------------------------------------------------

fn simulate_work(
    task:         TaskId,
    wcet:         Duration,
    stop:         &Arc<AtomicBool>,
    preempt_flag: &PreemptFlag,
    is_thermal:   bool,
    preempted:    &mut bool,
) {
    const SLICE: Duration = Duration::from_millis(5);
    let mut remaining = wcet;

    while remaining > Duration::ZERO {
        if stop.load(Ordering::Relaxed) { break; }

        // Non-thermal tasks suspend while ThermalControl is active.
        if !is_thermal && preempt_flag.load(Ordering::SeqCst) {
            *preempted = true;
            println!("[SCHED][PREEMPT] {} suspended by THERMAL_CTRL", task.label());
            while preempt_flag.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
            println!("[SCHED][RESUME]  {} resumed", task.label());
        }

        let slice = remaining.min(SLICE);
        thread::sleep(slice);
        remaining = remaining.saturating_sub(slice);
    }
}

// ---------------------------------------------------------------------------
// Metrics report
// ---------------------------------------------------------------------------

pub fn print_scheduler_report(
    job_log:      &JobLog,
    vlog:         &ViolationLog,
    cpu_nanos:    &CpuNanos,
    run_duration: Duration,
) {
    let jobs       = job_log.lock().unwrap();
    let violations = vlog.lock().unwrap();
    let active_ns  = cpu_nanos.load(Ordering::Relaxed);
    let total_ns   = run_duration.as_nanos() as u64;
    let util_pct   = (active_ns as f64 / total_ns as f64) * 100.0;

    println!("\n╔══════════════════════════════════════════════════════╗");
    println!("║        TASK 2 — SCHEDULER METRICS REPORT             ║");
    println!("╚══════════════════════════════════════════════════════╝");

    for &task in &[
        TaskId::ThermalControl,
        TaskId::DataCompression,
        TaskId::HealthMonitoring,
        TaskId::AntennaAlignment,
    ] {
        let task_jobs: Vec<_> = jobs.iter().filter(|j| j.task == task).collect();
        if task_jobs.is_empty() {
            println!("  {} — no jobs recorded", task.label());
            continue;
        }
        let n = task_jobs.len() as f64;
        let avg_drift = task_jobs.iter().map(|j| j.start_drift_us() as f64).sum::<f64>() / n;
        let max_drift = task_jobs.iter().map(|j| j.start_drift_us()).max().unwrap_or(0);
        let avg_exec  = task_jobs.iter().map(|j| j.execution_time_us() as f64).sum::<f64>() / n;
        let preempts  = task_jobs.iter().filter(|j| j.preempted).count();
        let viols     = violations.iter().filter(|v| v.task == task).count();

        println!("  ┌─ {} (priority={})", task.label(), task.priority());
        println!("  │  jobs={}  preemptions={}  violations={}", task_jobs.len(), preempts, viols);
        println!("  │  Start drift — mean={:.1}µs  max={}µs", avg_drift, max_drift);
        println!("  └  Exec time   — mean={:.1}µs  wcet={:?}", avg_exec, task.wcet());
        println!();
    }

    println!("  Deadline violations: {}", violations.len());
    for v in violations.iter() {
        println!("    [{}] {} job={} delay={}µs",
            v.kind.label(), v.task.label(), v.job, v.delay_us);
    }

    println!();
    println!("  CPU Utilisation");
    println!("  ───────────────────────────────");
    println!("  Active  : {:.2}%", util_pct);
    println!("  Idle    : {:.2}%", 100.0 - util_pct);

    // RM schedulability bound (Liu & Layland, 1973).
    let tasks = [
        TaskId::ThermalControl,
        TaskId::DataCompression,
        TaskId::HealthMonitoring,
        TaskId::AntennaAlignment,
    ];
    let n        = tasks.len() as f64;
    let rm_bound = n * (2.0_f64.powf(1.0 / n) - 1.0);
    let util_rm: f64 = tasks.iter()
        .map(|t| t.wcet().as_millis() as f64 / t.period().as_millis() as f64)
        .sum();

    println!();
    println!("  RM Schedulability (Liu & Layland 1973)");
    println!("  Actual utilisation : {:.4}", util_rm);
    println!("  RM bound (n={})    : {:.4}", tasks.len(), rm_bound);
    println!("  Result             : {}",
        if util_rm <= rm_bound { "✓ SCHEDULABLE" } else { "✗ OVERLOADED" });
    println!("╚══════════════════════════════════════════════════════╝");
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn sleep_until(target: Instant) {
    let now = Instant::now();
    if target > now {
        thread::sleep(target - now);
    }
}

fn log_violation(v: &DeadlineViolation) {
    println!(
        "[SCHED][VIOLATION] {} job={} kind={} delay={}µs",
        v.task.label(), v.job, v.kind.label(), v.delay_us
    );
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rm_priority_ordering() {
        assert!(TaskId::ThermalControl.period()   < TaskId::DataCompression.period());
        assert!(TaskId::DataCompression.period()  < TaskId::HealthMonitoring.period());
        assert!(TaskId::HealthMonitoring.period() < TaskId::AntennaAlignment.period());
        assert!(TaskId::ThermalControl.priority() < TaskId::DataCompression.priority());
        assert!(TaskId::DataCompression.priority() < TaskId::HealthMonitoring.priority());
    }

    #[test]
    fn test_rm_schedulability() {
        let tasks = [
            TaskId::ThermalControl,
            TaskId::DataCompression,
            TaskId::HealthMonitoring,
            TaskId::AntennaAlignment,
        ];
        let n        = tasks.len() as f64;
        let rm_bound = n * (2.0_f64.powf(1.0 / n) - 1.0);
        let util: f64 = tasks.iter()
            .map(|t| t.wcet().as_millis() as f64 / t.period().as_millis() as f64)
            .sum();
        assert!(util <= rm_bound,
            "Utilisation {:.4} exceeds RM bound {:.4}", util, rm_bound);
    }

    #[test]
    fn test_wcet_less_than_period() {
        for t in &[
            TaskId::ThermalControl,
            TaskId::DataCompression,
            TaskId::HealthMonitoring,
            TaskId::AntennaAlignment,
        ] {
            assert!(t.wcet() < t.period(),
                "{}: wcet {:?} >= period {:?}", t.label(), t.wcet(), t.period());
        }
    }

    #[test]
    fn test_thermal_highest_priority() {
        assert_eq!(TaskId::ThermalControl.priority(), 0);
        for t in &[
            TaskId::DataCompression,
            TaskId::HealthMonitoring,
            TaskId::AntennaAlignment,
        ] {
            assert!(TaskId::ThermalControl.priority() < t.priority());
        }
    }
}