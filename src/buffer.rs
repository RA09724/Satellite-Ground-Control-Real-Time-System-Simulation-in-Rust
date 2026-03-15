// =============================================================================
// buffer.rs — Bounded, priority-ordered sensor buffer
// =============================================================================
//
// Design rationale
// ─────────────────
// • Capacity is fixed at construction time so memory usage is bounded,
//   satisfying the real-time requirement of predictable allocation.
// • Insertions keep the internal Vec sorted by sensor priority so the
//   consumer always dequeues the highest-priority sample first.
// • When the buffer is full, the *lowest*-priority item is evicted to make
//   room for an incoming item of strictly higher priority; if the incoming
//   item has lower-or-equal priority than every item already present, it is
//   dropped instead.  Both eviction and drop events are logged with timestamps.
// • All access goes through a Mutex so the buffer can be shared between the
//   sensor-acquisition thread and the downlink/scheduler threads.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::types::{DroppedSample, InsertionLatency, SensorReading};

// ---------------------------------------------------------------------------
// Inner state (held under a Mutex)
// ---------------------------------------------------------------------------

struct Inner {
    capacity: usize,
    /// Items stored sorted ascending by priority value (0 = most important).
    items: VecDeque<SensorReading>,
    /// All drop events since the buffer was created.
    dropped_log: Vec<DroppedSample>,
    /// All successful insertion latency records.
    latency_log: Vec<InsertionLatency>,
}

impl Inner {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            items: VecDeque::with_capacity(capacity),
            dropped_log: Vec::new(),
            latency_log: Vec::new(),
        }
    }

    /// Insert a reading, maintaining priority order.
    /// Returns `true` when the item was accepted, `false` when it was dropped.
    fn try_insert(&mut self, reading: SensorReading, read_time: Instant) -> bool {
        if self.items.len() < self.capacity {
            // Buffer not full — just insert in priority order.
            self.insert_sorted(reading, read_time);
            return true;
        }

        // Buffer full: check whether we can evict the lowest-priority tail item.
        let tail_priority = self.items.back().map(|r| r.sensor.priority()).unwrap_or(u8::MAX);

        if reading.sensor.priority() < tail_priority {
            // Incoming item is more important — evict the tail and record the drop.
            let evicted = self.items.pop_back().unwrap();
            let dropped = DroppedSample {
                sensor:      evicted.sensor,
                cycle:       evicted.cycle,
                dropped_at:  Instant::now(),
                buffer_len:  self.items.len() + 1, // len before eviction
            };
            self.dropped_log.push(dropped.clone());
            log_drop(&dropped, "EVICTED (lower-priority item displaced)");

            self.insert_sorted(reading, read_time);
            true
        } else {
            // Incoming item has equal-or-lower priority — drop it.
            let dropped = DroppedSample {
                sensor:     reading.sensor,
                cycle:      reading.cycle,
                dropped_at: Instant::now(),
                buffer_len: self.items.len(),
            };
            self.dropped_log.push(dropped.clone());
            log_drop(&dropped, "DROPPED (buffer full, insufficient priority)");
            false
        }
    }

    /// Insert `reading` into the VecDeque so that items remain sorted by
    /// ascending priority value (i.e. highest-priority items at the front).
    fn insert_sorted(&mut self, reading: SensorReading, read_time: Instant) {
        let insert_time = Instant::now();
        let latency = insert_time.duration_since(read_time);

        // Binary-search for the first position whose priority > incoming.
        let pos = self
            .items
            .iter()
            .position(|r| r.sensor.priority() > reading.sensor.priority())
            .unwrap_or(self.items.len());

        let lat_record = InsertionLatency {
            sensor:  reading.sensor,
            cycle:   reading.cycle,
            latency,
        };
        self.latency_log.push(lat_record);

        self.items.insert(pos, reading);
    }
}

// ---------------------------------------------------------------------------
// Public handle — cheaply cloneable (Arc)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct SensorBuffer {
    inner: Arc<Mutex<Inner>>,
}

impl SensorBuffer {
    /// Create a new bounded buffer with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new(capacity))),
        }
    }

    /// Attempt to insert a sensor reading.
    ///
    /// `read_time` is the `Instant` captured immediately after the simulated
    /// sensor read; the difference between now and `read_time` becomes the
    /// insertion-latency record.
    ///
    /// Returns `true` if accepted, `false` if dropped.
    pub fn push(&self, reading: SensorReading, read_time: Instant) -> bool {
        self.inner.lock().unwrap().try_insert(reading, read_time)
    }

    /// Dequeue the highest-priority item (front of the sorted deque).
    pub fn pop(&self) -> Option<SensorReading> {
        self.inner.lock().unwrap().items.pop_front()
    }

    /// Non-destructive peek at the number of items currently buffered.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().items.len()
    }

    /// Buffer capacity as configured at construction.
    pub fn capacity(&self) -> usize {
        self.inner.lock().unwrap().capacity
    }

    /// Fill ratio in [0.0, 1.0].
    pub fn fill_ratio(&self) -> f64 {
        let g = self.inner.lock().unwrap();
        g.items.len() as f64 / g.capacity as f64
    }

    /// Clone the full drop log (for reporting).
    pub fn drop_log(&self) -> Vec<DroppedSample> {
        self.inner.lock().unwrap().dropped_log.clone()
    }

    /// Clone the full latency log (for reporting).
    pub fn latency_log(&self) -> Vec<InsertionLatency> {
        self.inner.lock().unwrap().latency_log.clone()
    }

    /// Total number of items dropped since creation.
    pub fn total_dropped(&self) -> usize {
        self.inner.lock().unwrap().dropped_log.len()
    }
}

// ---------------------------------------------------------------------------
// Internal logging helper
// ---------------------------------------------------------------------------

fn log_drop(d: &DroppedSample, reason: &str) {
    println!(
        "[BUFFER][DROP] sensor={} cycle={:>6}  reason={}  buf_len={}",
        d.sensor.label(),
        d.cycle,
        reason,
        d.buffer_len,
    );
}