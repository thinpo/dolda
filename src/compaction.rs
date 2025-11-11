//! Segment compaction and space reclamation
//!
//! This module provides:
//! - Automatic segment compaction to reclaim deleted space
//! - Merging of small segments
//! - Background compaction worker
//! - Configurable compaction policies

use crate::record::{Record, RecordError};
use crate::storage::{RecordSegment, SegmentStats, StorageError};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use parking_lot::RwLock;

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("Record error: {0}")]
    Record(#[from] RecordError),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    
    #[error("Compaction already in progress")]
    InProgress,
    
    #[error("No segments need compaction")]
    NoWork,
}

pub type Result<T> = std::result::Result<T, CompactionError>;

/// Compaction policy configuration
#[derive(Debug, Clone)]
pub struct CompactionPolicy {
    /// Minimum segment utilization to trigger compaction (e.g., 0.5 = 50%)
    pub min_utilization: f64,
    
    /// Minimum segment age before compaction
    pub min_age: Duration,
    
    /// Maximum segments to compact in one operation
    pub max_segments_per_run: usize,
    
    /// Target segment size after compaction
    pub target_segment_size: usize,
    
    /// Enable automatic background compaction
    pub auto_compact: bool,
    
    /// Compaction check interval
    pub check_interval: Duration,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            min_utilization: 0.5,
            min_age: Duration::from_secs(300), // 5 minutes
            max_segments_per_run: 5,
            target_segment_size: 10 * 1024 * 1024, // 10MB
            auto_compact: true,
            check_interval: Duration::from_secs(60),
        }
    }
}

/// Compaction statistics
#[derive(Debug, Clone)]
pub struct CompactionStats {
    /// Total compactions performed
    pub total_compactions: u64,
    
    /// Total bytes reclaimed
    pub bytes_reclaimed: u64,
    
    /// Total records removed
    pub records_removed: u64,
    
    /// Last compaction time
    pub last_compaction: Option<Instant>,
    
    /// Total compaction time
    pub total_duration: Duration,
}

impl Default for CompactionStats {
    fn default() -> Self {
        Self {
            total_compactions: 0,
            bytes_reclaimed: 0,
            records_removed: 0,
            last_compaction: None,
            total_duration: Duration::from_secs(0),
        }
    }
}

/// Segment compaction manager
pub struct CompactionManager {
    /// Compaction policy
    policy: CompactionPolicy,
    
    /// Compaction statistics
    stats: Arc<RwLock<CompactionStats>>,
    
    /// Compaction in progress flag
    in_progress: Arc<RwLock<bool>>,
}

impl CompactionManager {
    /// Create a new compaction manager
    pub fn new(policy: CompactionPolicy) -> Self {
        Self {
            policy,
            stats: Arc::new(RwLock::new(CompactionStats::default())),
            in_progress: Arc::new(RwLock::new(false)),
        }
    }
    
    /// Check if a segment needs compaction
    pub fn needs_compaction(&self, stats: &SegmentStats, age: Duration) -> bool {
        let utilization = if stats.max_size > 0 {
            stats.used_bytes as f64 / stats.max_size as f64
        } else {
            1.0
        };
        
        utilization < self.policy.min_utilization && age >= self.policy.min_age
    }
    
    /// Compact a single segment
    pub async fn compact_segment(
        &self,
        segment: &RecordSegment,
        output_path: String,
    ) -> Result<RecordSegment> {
        tracing::info!("Starting segment compaction: {}", segment.stats().path);
        let start = Instant::now();
        
        // Check if compaction is already running
        {
            let mut in_progress = self.in_progress.write();
            if *in_progress {
                return Err(CompactionError::InProgress);
            }
            *in_progress = true;
        }
        
        // Create new compacted segment
        let new_segment = RecordSegment::new(
            output_path,
            self.policy.target_segment_size,
            segment.stats().index_enabled,
        )
        .await?;
        
        // Copy live records to new segment
        let mut records_copied = 0;
        let mut records_skipped = 0;
        let mut bytes_saved = 0;
        
        for result in segment.iter_records() {
            match result {
                Ok((offset, record)) => {
                    // Filter logic (could be extended with tombstones, TTL, etc.)
                    if self.should_keep_record(&record) {
                        new_segment.append_record(&record)?;
                        records_copied += 1;
                    } else {
                        records_skipped += 1;
                        bytes_saved += offset.size as u64;
                    }
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupted record during compaction: {}", e);
                    records_skipped += 1;
                }
            }
        }
        
        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_compactions += 1;
            stats.bytes_reclaimed += bytes_saved;
            stats.records_removed += records_skipped;
            stats.last_compaction = Some(Instant::now());
            stats.total_duration += start.elapsed();
        }
        
        // Clear in-progress flag
        *self.in_progress.write() = false;
        
        let elapsed = start.elapsed();
        tracing::info!(
            "Compaction complete: {} records copied, {} records removed, {} bytes saved, took {:?}",
            records_copied,
            records_skipped,
            bytes_saved,
            elapsed
        );
        
        // Record metrics
        crate::observability::record_compaction(records_copied, records_skipped, bytes_saved, elapsed);
        
        Ok(new_segment)
    }
    
    /// Determine if a record should be kept during compaction
    fn should_keep_record(&self, _record: &Record) -> bool {
        // Basic policy: keep all records
        // Can be extended with:
        // - Tombstone detection
        // - TTL expiration
        // - Duplicate detection
        // - Custom filters
        true
    }
    
    /// Get compaction statistics
    pub fn stats(&self) -> CompactionStats {
        self.stats.read().clone()
    }
    
    /// Start background compaction worker
    pub async fn start_background_worker(
        self: Arc<Self>,
        mut segment_provider: impl FnMut() -> Vec<(Arc<RecordSegment>, Duration)> + Send + 'static,
    ) {
        if !self.policy.auto_compact {
            tracing::info!("Auto-compaction disabled");
            return;
        }
        
        tracing::info!("Starting background compaction worker");
        let mut interval = tokio::time::interval(self.policy.check_interval);
        
        loop {
            interval.tick().await;
            
            // Get segments that need compaction
            let candidates = segment_provider();
            let mut compacted = 0;
            
            for (segment, age) in candidates {
                let stats = segment.stats();
                
                if self.needs_compaction(&stats, age) {
                    tracing::info!(
                        "Segment {} needs compaction: {:.1}% utilized, age {:?}",
                        stats.path,
                        segment.utilization(),
                        age
                    );
                    
                    let output_path = format!("{}.compacted", stats.path);
                    
                    match self.compact_segment(&segment, output_path.clone()).await {
                        Ok(_) => {
                            compacted += 1;
                            tracing::info!("Compacted segment: {}", stats.path);
                            
                            // Could implement segment replacement here
                        }
                        Err(e) => {
                            tracing::error!("Compaction failed for {}: {}", stats.path, e);
                        }
                    }
                    
                    if compacted >= self.policy.max_segments_per_run {
                        break;
                    }
                }
            }
            
            if compacted > 0 {
                tracing::info!("Background compaction: {} segments compacted", compacted);
            }
        }
    }
}

/// Merge multiple small segments into one
pub async fn merge_segments(
    segments: Vec<Arc<RecordSegment>>,
    output_path: String,
    target_size: usize,
    index_enabled: bool,
) -> Result<RecordSegment> {
    tracing::info!("Merging {} segments into {}", segments.len(), output_path);
    let start = Instant::now();
    
    let merged = RecordSegment::new(output_path.clone(), target_size, index_enabled).await?;
    
    let mut total_records = 0;
    
    for segment in segments {
        let stats = segment.stats();
        tracing::debug!("Merging segment: {} ({} records)", stats.path, stats.record_count);
        
        for result in segment.iter_records() {
            match result {
                Ok((_offset, record)) => {
                    merged.append_record(&record)?;
                    total_records += 1;
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupted record during merge: {}", e);
                }
            }
        }
    }
    
    let elapsed = start.elapsed();
    tracing::info!(
        "Merge complete: {} records merged, took {:?}",
        total_records,
        elapsed
    );
    
    Ok(merged)
}

/// Observability integration
mod observability_ext {
    use super::*;
    
    #[allow(dead_code)]
    pub fn record_compaction(
        records_copied: u64,
        records_removed: u64,
        bytes_saved: u64,
        duration: Duration,
    ) {
        use metrics::{counter, histogram};
        
        counter!("dolda.compaction.total").increment(1);
        counter!("dolda.compaction.records_copied").increment(records_copied);
        counter!("dolda.compaction.records_removed").increment(records_removed);
        counter!("dolda.compaction.bytes_reclaimed").increment(bytes_saved);
        histogram!("dolda.compaction.duration_ms").record(duration.as_millis() as f64);
        
        tracing::info!(
            compaction.records_copied = records_copied,
            compaction.records_removed = records_removed,
            compaction.bytes_reclaimed = bytes_saved,
            compaction.duration_ms = duration.as_millis(),
            "Compaction metrics recorded"
        );
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_compaction_policy_default() {
        let policy = CompactionPolicy::default();
        assert_eq!(policy.min_utilization, 0.5);
        assert!(policy.auto_compact);
    }
    
    #[test]
    fn test_needs_compaction() {
        let policy = CompactionPolicy {
            min_utilization: 0.5,
            min_age: Duration::from_secs(60),
            ..Default::default()
        };
        let manager = CompactionManager::new(policy);
        
        let stats = SegmentStats {
            path: "test".to_string(),
            max_size: 1000,
            used_bytes: 400, // 40% utilization
            record_count: 10,
            index_enabled: false,
            index_size: 0,
        };
        
        // Too young
        assert!(!manager.needs_compaction(&stats, Duration::from_secs(30)));
        
        // Old enough and low utilization
        assert!(manager.needs_compaction(&stats, Duration::from_secs(120)));
        
        // High utilization
        let high_util_stats = SegmentStats {
            used_bytes: 800, // 80% utilization
            ..stats
        };
        assert!(!manager.needs_compaction(&high_util_stats, Duration::from_secs(120)));
    }
    
    #[tokio::test]
    async fn test_compact_segment() {
        let test_dir = "/tmp/dolda_compaction_test";
        std::fs::create_dir_all(test_dir).ok();
        
        // Create source segment with data
        let source_path = format!("{}/source.dat", test_dir);
        let source = RecordSegment::new(source_path, 1024 * 1024, false).await.unwrap();
        
        for i in 0..100 {
            let data = format!("Record {}", i);
            let record = Record::new(data.as_bytes().to_vec()).unwrap();
            source.append_record(&record).unwrap();
        }
        
        // Compact segment
        let policy = CompactionPolicy::default();
        let manager = CompactionManager::new(policy);
        
        let output_path = format!("{}/compacted.dat", test_dir);
        let compacted = manager.compact_segment(&source, output_path).await.unwrap();
        
        // Verify all records were copied
        let compacted_stats = compacted.stats();
        assert_eq!(compacted_stats.record_count, 100);
        
        // Check stats were updated
        let stats = manager.stats();
        assert_eq!(stats.total_compactions, 1);
        
        std::fs::remove_dir_all(test_dir).ok();
    }
    
    #[tokio::test]
    async fn test_merge_segments() {
        let test_dir = "/tmp/dolda_merge_test";
        std::fs::create_dir_all(test_dir).ok();
        
        // Create multiple segments
        let mut segments = Vec::new();
        
        for i in 0..3 {
            let path = format!("{}/segment_{}.dat", test_dir, i);
            let segment = Arc::new(RecordSegment::new(path, 1024 * 1024, false).await.unwrap());
            
            for j in 0..10 {
                let data = format!("Segment {} Record {}", i, j);
                let record = Record::new(data.as_bytes().to_vec()).unwrap();
                segment.append_record(&record).unwrap();
            }
            
            segments.push(segment);
        }
        
        // Merge segments
        let output_path = format!("{}/merged.dat", test_dir);
        let merged = merge_segments(segments, output_path, 10 * 1024 * 1024, false).await.unwrap();
        
        // Verify all records were merged
        let stats = merged.stats();
        assert_eq!(stats.record_count, 30); // 3 segments * 10 records
        
        std::fs::remove_dir_all(test_dir).ok();
    }
}

