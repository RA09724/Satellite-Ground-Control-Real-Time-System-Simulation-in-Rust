// =============================================================================
// main.rs — Satellite Onboard Control System (OCS)
// =============================================================================
//
// Run:  cargo run --release
// Test: cargo test

#![allow(dead_code)]

mod types;
mod buffer;
mod metrics;
mod a1_sensor;
mod a2_scheduler;
mod a3_downlink;
mod a4_benchmark;

use std::sync::atomic::{AtomicBool};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use buffer::SensorBuffer;
use metrics::OcsMetrics;

#[cfg(windows)]
extern crate winapi;
#[cfg(windows)]
use winapi::um::timeapi::{timeBeginPeriod, timeEndPeriod};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const SIM_DURATION_SECS: u64   = 10;
const BUFFER_CAPACITY:   usize = 32;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    #[cfg(windows)]
    unsafe { timeBeginPeriod(1); }

    println!("╔══════════════════════════════════════════════════════╗");
    println!("║   Satellite OCS — Real-Time Simulation (Tasks 1–4)   ║");
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  Sensors  : Thermal (100ms) | Power (250ms) |        ║");
    println!("║             Attitude (500ms)                          ║");
    println!("║  Tasks    : ThermalCtrl | DataCompress |             ║");
    println!("║             HealthMon   | AntennaAlign               ║");
    println!("║  Downlink : TCP :9000 (telemetry)                    ║");
    println!("║             UDP :9001 → :9002 (alerts/status)        ║");
    println!("║  Faults   : injected every {}s | limit {}ms          ║",
        a4_benchmark::FAULT_LOG_PATH.len(), 200); // placeholder widths
    println!("║  Buffer   : capacity = {}                            ║", BUFFER_CAPACITY);
    println!("║  Duration : {} seconds                                ║", SIM_DURATION_SECS);
    println!("╚══════════════════════════════════════════════════════╝\n");

    let epoch        = Instant::now();
    let buffer       = SensorBuffer::new(BUFFER_CAPACITY);
    let metrics      = OcsMetrics::new();
    let stop         = Arc::new(AtomicBool::new(false));
    let run_duration = Duration::from_secs(SIM_DURATION_SECS);

    // Ctrl-C stub.
    { let s = stop.clone(); thread::spawn(move || { let _ = s; }); }

    // ── Task 4: Fault injector ────────────────────────────────────────────
    // Must start before sensors so faults are ready to fire from t=0.
    let (fault_handle, fault_state) =
        a4_benchmark::spawn_fault_injector(stop.clone(), run_duration);

    // ── Task 3: Downlink (TCP + UDP) ──────────────────────────────────────
    let (downlink_handle, downlink_state) = a3_downlink::spawn_downlink_task(
        buffer.clone(), stop.clone(), run_duration,
    );

    // ── Task 1: Sensor acquisition (with fault hooks) ─────────────────────
    let sensor_handles = a1_sensor::spawn_sensor_threads(
        buffer.clone(),
        metrics.clone(),
        epoch,
        run_duration,
        stop.clone(),
        Some(fault_state.clone()),  // pass fault state to sensors
    );

    // ── Task 2: Scheduler threads ─────────────────────────────────────────
    let (sched_handles, violation_log, job_log, cpu_nanos) =
        a2_scheduler::spawn_scheduler_threads(stop.clone(), run_duration);

    // ── Wait for sensor threads ───────────────────────────────────────────
    let mut all_alerts = Vec::new();
    for h in sensor_handles {
        match h.join() {
            Ok(alerts) => all_alerts.extend(alerts),
            Err(e)     => eprintln!("[MAIN][ERROR] sensor thread panicked: {:?}", e),
        }
    }

    // ── Wait for scheduler threads ────────────────────────────────────────
    for h in sched_handles {
        if let Err(e) = h.join() {
            eprintln!("[MAIN][ERROR] scheduler thread panicked: {:?}", e);
        }
    }

    // ── Wait for downlink thread ──────────────────────────────────────────
    if let Err(e) = downlink_handle.join() {
        eprintln!("[MAIN][ERROR] downlink thread panicked: {:?}", e);
    }

    // ── Wait for fault injector ───────────────────────────────────────────
    if let Err(e) = fault_handle.join() {
        eprintln!("[MAIN][ERROR] fault injector thread panicked: {:?}", e);
    }

    // ── Task 1 report ─────────────────────────────────────────────────────
    metrics.print_report();

    println!("═══════════════════════════════════════════════════════");
    println!("  Buffer statistics");
    println!("  Total items dropped : {}", buffer.total_dropped());
    println!("  Safety alerts raised: {}", all_alerts.len());
    println!("═══════════════════════════════════════════════════════");

    if all_alerts.is_empty() {
        println!("\n  ✓  No safety alerts — all critical sensor constraints met.");
    } else {
        println!("\n  ⚠  {} safety alert(s) raised.", all_alerts.len());
        for a in &all_alerts {
            println!("      → {} missed {} consecutive cycles",
                a.sensor.label(), a.consecutive_misses);
        }
    }

    // ── Task 2 report ─────────────────────────────────────────────────────
    a2_scheduler::print_scheduler_report(
        &job_log, &violation_log, &cpu_nanos, run_duration,
    );

    // ── Task 3 report ─────────────────────────────────────────────────────
    a3_downlink::print_downlink_report(&downlink_state, run_duration);

    // ── Task 4 report — full benchmark with fault analysis ────────────────
    a4_benchmark::print_benchmark_report(
        &fault_state,
        &metrics,
        &job_log,
        &violation_log,
        &cpu_nanos,
        run_duration,
    );

    println!("\n[MAIN] Simulation complete.");
    println!("[MAIN] Student B GCS: TCP {}  |  UDP {}",
        a3_downlink::TCP_ADDR, a3_downlink::GCS_UDP);
    println!("[MAIN] Fault log: {}", a4_benchmark::FAULT_LOG_PATH);

    #[cfg(windows)]
    unsafe { timeEndPeriod(1); }
}