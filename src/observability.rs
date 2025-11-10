//! Production observability with metrics and tracing
//!
//! This module provides:
//! - Prometheus metrics for monitoring
//! - Structured tracing for debugging
//! - Performance metrics collection

use metrics::{counter, gauge, histogram, describe_counter, describe_gauge, describe_histogram};
use std::time::Instant;
use tracing::{info, warn, error};

/// Initialize observability system
pub fn init() {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
        )
        .with_target(false)
        .with_thread_ids(true)
        .init();
    
    // Register metrics
    describe_counter!("persistq.append.total", "Total number of append operations");
    describe_counter!("persistq.append.bytes_total", "Total bytes appended");
    describe_counter!("persistq.append.errors_total", "Total append errors");
    
    describe_counter!("persistq.read.total", "Total number of read operations");
    describe_counter!("persistq.read.bytes_total", "Total bytes read");
    describe_counter!("persistq.read.errors_total", "Total read errors");
    describe_counter!("persistq.read.cache_hits", "Total cache hits");
    describe_counter!("persistq.read.cache_misses", "Total cache misses");
    
    describe_histogram!("persistq.append.latency_us", "Append operation latency in microseconds");
    describe_histogram!("persistq.read.latency_us", "Read operation latency in microseconds");
    
    describe_gauge!("persistq.segments.active", "Number of active segments");
    describe_gauge!("persistq.segments.total_bytes", "Total bytes in segments");
    describe_gauge!("persistq.partitions.count", "Number of partitions");
    describe_gauge!("persistq.records.total", "Total number of records");
    
    describe_counter!("persistq.segment.rollover_total", "Total segment rollovers");
    describe_counter!("persistq.segment.flush_total", "Total segment flushes");
    describe_counter!("persistq.segment.flush_errors_total", "Total segment flush errors");
    
    info!("Observability system initialized");
}

/// Record append operation metrics
pub fn record_append(bytes: usize, latency_us: u64, success: bool) {
    counter!("persistq.append.total").increment(1);
    
    if success {
        counter!("persistq.append.bytes_total").increment(bytes as u64);
        histogram!("persistq.append.latency_us").record(latency_us as f64);
    } else {
        counter!("persistq.append.errors_total").increment(1);
    }
}

/// Record read operation metrics
pub fn record_read(bytes: usize, latency_us: u64, cache_hit: bool, success: bool) {
    counter!("persistq.read.total").increment(1);
    
    if success {
        counter!("persistq.read.bytes_total").increment(bytes as u64);
        histogram!("persistq.read.latency_us").record(latency_us as f64);
        
        if cache_hit {
            counter!("persistq.read.cache_hits").increment(1);
        } else {
            counter!("persistq.read.cache_misses").increment(1);
        }
    } else {
        counter!("persistq.read.errors_total").increment(1);
    }
}

/// Update segment metrics
pub fn update_segment_metrics(active_segments: usize, total_bytes: u64) {
    gauge!("persistq.segments.active").set(active_segments as f64);
    gauge!("persistq.segments.total_bytes").set(total_bytes as f64);
}

/// Update partition metrics
pub fn update_partition_metrics(partition_count: usize) {
    gauge!("persistq.partitions.count").set(partition_count as f64);
}

/// Update record count
pub fn update_record_count(total_records: u64) {
    gauge!("persistq.records.total").set(total_records as f64);
}

/// Record segment rollover
pub fn record_segment_rollover() {
    counter!("persistq.segment.rollover_total").increment(1);
}

/// Record segment flush
pub fn record_segment_flush(success: bool) {
    counter!("persistq.segment.flush_total").increment(1);
    if !success {
        counter!("persistq.segment.flush_errors_total").increment(1);
    }
}

/// Record compaction operation
pub fn record_compaction(records_copied: u64, records_removed: u64, bytes_saved: u64, duration: std::time::Duration) {
    counter!("persistq.compaction.total").increment(1);
    counter!("persistq.compaction.records_copied").increment(records_copied);
    counter!("persistq.compaction.records_removed").increment(records_removed);
    counter!("persistq.compaction.bytes_reclaimed").increment(bytes_saved);
    histogram!("persistq.compaction.duration_ms").record(duration.as_millis() as f64);
}

/// Performance timer for measuring operations
pub struct PerfTimer {
    start: Instant,
    operation: &'static str,
}

impl PerfTimer {
    pub fn new(operation: &'static str) -> Self {
        Self {
            start: Instant::now(),
            operation,
        }
    }
    
    pub fn elapsed_micros(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
    
    pub fn finish(self) -> u64 {
        let elapsed = self.elapsed_micros();
        info!(operation = self.operation, latency_us = elapsed);
        elapsed
    }
}

/// Log health check
pub fn log_health_check(component: &str, healthy: bool, message: &str) {
    if healthy {
        info!(component = component, status = "healthy", message = message);
    } else {
        warn!(component = component, status = "unhealthy", message = message);
    }
}

/// Log error with context
pub fn log_error(component: &str, operation: &str, error: &str) {
    error!(component = component, operation = operation, error = error);
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_perf_timer() {
        let timer = PerfTimer::new("test_operation");
        std::thread::sleep(std::time::Duration::from_micros(100));
        let elapsed = timer.elapsed_micros();
        assert!(elapsed >= 100);
    }
}

