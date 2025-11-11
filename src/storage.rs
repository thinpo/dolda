//! Production storage layer with Record integration
//!
//! This module provides the core storage engine that uses the production
//! Record format for all data operations.

use crate::record::{Record, RecordError};
use crate::observability;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use memmap2::MmapMut;
use parking_lot::RwLock;
use tokio::sync::Mutex as AsyncMutex;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Segment full: {0} bytes needed, {1} bytes available")]
    SegmentFull(usize, usize),
    
    #[error("Record error: {0}")]
    Record(#[from] RecordError),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Offset out of bounds: {0}")]
    OffsetOutOfBounds(u64),
    
    #[error("Invalid segment state")]
    InvalidState,
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Record location within a segment
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordOffset {
    /// Byte offset within the segment
    pub offset: u64,
    /// Record size in bytes
    pub size: usize,
}

/// Record index entry for efficient seeks
#[derive(Debug, Clone)]
struct IndexEntry {
    /// Sequential record number
    record_num: u64,
    /// Byte offset in segment
    offset: u64,
    /// Record size (reserved for future use)
    #[allow(dead_code)]
    size: usize,
    /// Timestamp (reserved for future use)
    #[allow(dead_code)]
    timestamp: u64,
}

/// Production-quality segment with Record integration
pub struct RecordSegment {
    /// Segment file path
    path: String,
    /// Maximum size
    max_size: usize,
    /// Current write position (atomic)
    write_pos: AtomicU64,
    /// Number of records written
    record_count: AtomicU64,
    /// File handle
    _file: File,
    /// Memory-mapped region
    mmap: RwLock<MmapMut>,
    /// Record index for seeking (offset -> entry)
    index: RwLock<BTreeMap<u64, IndexEntry>>,
    /// Index building enabled
    index_enabled: bool,
}

impl RecordSegment {
    /// Create a new segment
    pub async fn new(path: String, max_size: usize, index_enabled: bool) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        
        // Pre-allocate file
        file.set_len(max_size as u64)?;
        
        // Create memory map
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        
        Ok(Self {
            path,
            max_size,
            write_pos: AtomicU64::new(0),
            record_count: AtomicU64::new(0),
            _file: file,
            mmap: RwLock::new(mmap),
            index: RwLock::new(BTreeMap::new()),
            index_enabled,
        })
    }
    
    /// Append a record to the segment
    pub fn append_record(&self, record: &Record) -> Result<RecordOffset> {
        let timer = observability::PerfTimer::new("segment_append");
        
        // Serialize record with framing
        let framed = record.serialize();
        let record_size = framed.size();
        
        // Atomically reserve space
        let offset = self.write_pos.fetch_add(record_size as u64, Ordering::SeqCst);
        
        // Check if we have space
        if offset + record_size as u64 > self.max_size as u64 {
            // Rollback
            self.write_pos.fetch_sub(record_size as u64, Ordering::SeqCst);
            return Err(StorageError::SegmentFull(
                record_size,
                (self.max_size as u64 - offset) as usize,
            ));
        }
        
        // Write to mmap (zero-copy)
        {
            let mut mmap = self.mmap.write();
            let start = offset as usize;
            let end = start + record_size;
            mmap[start..end].copy_from_slice(&framed.bytes);
        }
        
        // Update record count
        let record_num = self.record_count.fetch_add(1, Ordering::SeqCst);
        
        // Build index entry if enabled
        if self.index_enabled {
            let entry = IndexEntry {
                record_num,
                offset,
                size: record_size,
                timestamp: record.timestamp,
            };
            self.index.write().insert(offset, entry);
        }
        
        // Record metrics
        let latency = timer.elapsed_micros();
        observability::record_append(record_size, latency, true);
        
        Ok(RecordOffset {
            offset,
            size: record_size,
        })
    }
    
    /// Read a record at the given offset
    pub fn read_record(&self, offset: u64) -> Result<Record> {
        let timer = observability::PerfTimer::new("segment_read");
        
        let current_pos = self.write_pos.load(Ordering::SeqCst);
        
        if offset >= current_pos {
            return Err(StorageError::OffsetOutOfBounds(offset));
        }
        
        // Read from mmap
        let mmap = self.mmap.read();
        let data = &mmap[offset as usize..current_pos as usize];
        
        // Deserialize and validate
        let (record, size) = Record::deserialize(data)?;
        
        // Record metrics
        let latency = timer.elapsed_micros();
        observability::record_read(size, latency, false, true);
        
        Ok(record)
    }
    
    /// Read record by sequential number (if index enabled)
    pub fn read_record_by_num(&self, record_num: u64) -> Result<Record> {
        if !self.index_enabled {
            return Err(StorageError::InvalidState);
        }
        
        // Find the record in index
        let index = self.index.read();
        let entry = index.values()
            .find(|e| e.record_num == record_num)
            .ok_or(StorageError::OffsetOutOfBounds(record_num))?;
        
        self.read_record(entry.offset)
    }
    
    /// Iterate over all records
    pub fn iter_records(&self) -> RecordIterator<'_> {
        let current_pos = self.write_pos.load(Ordering::SeqCst);
        RecordIterator {
            segment: self,
            current_offset: 0,
            end_offset: current_pos,
        }
    }
    
    /// Get segment statistics
    pub fn stats(&self) -> SegmentStats {
        SegmentStats {
            path: self.path.clone(),
            max_size: self.max_size,
            used_bytes: self.write_pos.load(Ordering::SeqCst),
            record_count: self.record_count.load(Ordering::SeqCst),
            index_enabled: self.index_enabled,
            index_size: self.index.read().len(),
        }
    }
    
    /// Check if segment has space for a record
    pub fn has_space(&self, record_size: usize) -> bool {
        let current_pos = self.write_pos.load(Ordering::SeqCst);
        current_pos + record_size as u64 <= self.max_size as u64
    }
    
    /// Flush segment to disk
    pub fn flush(&self) -> Result<()> {
        let mmap = self.mmap.read();
        mmap.flush()?;
        observability::record_segment_flush(true);
        Ok(())
    }
    
    /// Get current utilization percentage
    pub fn utilization(&self) -> f64 {
        let used = self.write_pos.load(Ordering::SeqCst) as f64;
        let total = self.max_size as f64;
        (used / total) * 100.0
    }
}

/// Iterator over records in a segment
pub struct RecordIterator<'a> {
    segment: &'a RecordSegment,
    current_offset: u64,
    end_offset: u64,
}

impl<'a> Iterator for RecordIterator<'a> {
    type Item = Result<(RecordOffset, Record)>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.end_offset {
            return None;
        }
        
        let offset = self.current_offset;
        
        // Read record
        match self.segment.read_record(offset) {
            Ok(record) => {
                let size = record.serialized_size();
                self.current_offset += size as u64;
                
                Some(Ok((RecordOffset { offset, size }, record)))
            }
            Err(e) => Some(Err(e))
        }
    }
}

/// Segment statistics
#[derive(Debug, Clone)]
pub struct SegmentStats {
    pub path: String,
    pub max_size: usize,
    pub used_bytes: u64,
    pub record_count: u64,
    pub index_enabled: bool,
    pub index_size: usize,
}

/// Manager for multiple segments with rotation
pub struct SegmentManager {
    /// Partition ID
    pub partition_id: u32,
    /// Active segments
    pub segments: RwLock<Vec<Arc<RecordSegment>>>,
    /// Current write segment
    current_segment: RwLock<Option<Arc<RecordSegment>>>,
    /// Segment creation lock
    segment_creation_lock: AsyncMutex<()>,
    /// Segment configuration
    segment_size: usize,
    storage_dir: String,
    index_enabled: bool,
}

impl SegmentManager {
    /// Create a new segment manager
    pub async fn new(
        partition_id: u32,
        segment_size: usize,
        storage_dir: String,
        index_enabled: bool,
    ) -> Result<Self> {
        std::fs::create_dir_all(&storage_dir)?;
        
        let manager = Self {
            partition_id,
            segments: RwLock::new(Vec::new()),
            current_segment: RwLock::new(None),
            segment_creation_lock: AsyncMutex::new(()),
            segment_size,
            storage_dir,
            index_enabled,
        };
        
        // Create initial segment
        manager.create_new_segment().await?;
        
        Ok(manager)
    }
    
    /// Append a record
    pub async fn append_record(&self, record: &Record) -> Result<RecordOffset> {
        loop {
            // Fast path: try current segment
            let segment_arc = {
                let current = self.current_segment.read();
                if let Some(seg) = current.as_ref() {
                    if seg.has_space(record.serialized_size()) {
                        Some(Arc::clone(seg))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            
            if let Some(segment) = segment_arc {
                match segment.append_record(record) {
                    Ok(offset) => return Ok(offset),
                    Err(StorageError::SegmentFull(_, _)) => {
                        // Fall through to segment creation
                    }
                    Err(e) => return Err(e),
                }
            }
            
            // Slow path: create new segment
            let _guard = self.segment_creation_lock.lock().await;
            
            // Double-check
            let has_space = {
                let current = self.current_segment.read();
                current.as_ref()
                    .map(|s| s.has_space(record.serialized_size()))
                    .unwrap_or(false)
            };
            
            if has_space {
                continue;
            }
            
            // Create new segment
            self.create_new_segment().await?;
            observability::record_segment_rollover();
        }
    }
    
    /// Get aggregate statistics
    pub fn stats(&self) -> ManagerStats {
        let segments = self.segments.read();
        let total_records: u64 = segments.iter()
            .map(|s| s.record_count.load(Ordering::SeqCst))
            .sum();
        let total_bytes: u64 = segments.iter()
            .map(|s| s.write_pos.load(Ordering::SeqCst))
            .sum();
        
        ManagerStats {
            partition_id: self.partition_id,
            segment_count: segments.len(),
            total_records,
            total_bytes,
            avg_utilization: if !segments.is_empty() {
                segments.iter().map(|s| s.utilization()).sum::<f64>() / segments.len() as f64
            } else {
                0.0
            },
        }
    }
    
    /// Flush all segments
    pub async fn flush(&self) -> Result<()> {
        let segments = self.segments.read();
        for segment in segments.iter() {
            segment.flush()?;
        }
        Ok(())
    }
    
    /// Create a new segment
    async fn create_new_segment(&self) -> Result<()> {
        let segment_id = self.segments.read().len();
        let segment_path = format!(
            "{}/partition_{}_segment_{}.dat",
            self.storage_dir, self.partition_id, segment_id
        );
        
        let segment = Arc::new(
            RecordSegment::new(segment_path, self.segment_size, self.index_enabled).await?
        );
        
        let mut segments = self.segments.write();
        segments.push(Arc::clone(&segment));
        
        let mut current = self.current_segment.write();
        *current = Some(segment);
        
        Ok(())
    }
}

/// Manager statistics
#[derive(Debug, Clone)]
pub struct ManagerStats {
    pub partition_id: u32,
    pub segment_count: usize,
    pub total_records: u64,
    pub total_bytes: u64,
    pub avg_utilization: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_record_segment_basic() {
        let test_dir = "/tmp/dolda_storage_test";
        std::fs::create_dir_all(test_dir).ok();
        
        let segment = RecordSegment::new(
            format!("{}/test.dat", test_dir),
            1024 * 1024,
            true,
        ).await.unwrap();
        
        // Append record
        let record = Record::new(b"Hello, production!".to_vec()).unwrap();
        let offset = segment.append_record(&record).unwrap();
        
        // Read back
        let retrieved = segment.read_record(offset.offset).unwrap();
        assert_eq!(retrieved.data, b"Hello, production!");
        
        // Check stats
        let stats = segment.stats();
        assert_eq!(stats.record_count, 1);
        assert!(stats.used_bytes > 0);
        
        std::fs::remove_dir_all(test_dir).ok();
    }
    
    #[tokio::test]
    async fn test_record_iteration() {
        let test_dir = "/tmp/dolda_storage_iter";
        std::fs::create_dir_all(test_dir).ok();
        
        let segment = RecordSegment::new(
            format!("{}/test.dat", test_dir),
            1024 * 1024,
            false,
        ).await.unwrap();
        
        // Append multiple records
        let records = vec![
            Record::new(b"Record 1".to_vec()).unwrap(),
            Record::new(b"Record 2".to_vec()).unwrap(),
            Record::new(b"Record 3".to_vec()).unwrap(),
        ];
        
        for record in &records {
            segment.append_record(record).unwrap();
        }
        
        // Iterate and verify
        let mut count = 0;
        for result in segment.iter_records() {
            let (_offset, record) = result.unwrap();
            assert_eq!(record.data, records[count].data);
            count += 1;
        }
        assert_eq!(count, 3);
        
        std::fs::remove_dir_all(test_dir).ok();
    }
    
    #[tokio::test]
    async fn test_segment_manager() {
        let test_dir = "/tmp/dolda_storage_manager";
        std::fs::create_dir_all(test_dir).ok();
        
        let manager = SegmentManager::new(
            1,
            64 * 1024,  // Small segments to force rollover
            test_dir.to_string(),
            true,
        ).await.unwrap();
        
        // Append many records to force rollover
        let large_data = vec![0u8; 10 * 1024];
        for _ in 0..10 {
            let record = Record::new(large_data.clone()).unwrap();
            manager.append_record(&record).await.unwrap();
        }
        
        // Should have multiple segments
        let stats = manager.stats();
        assert!(stats.segment_count > 1);
        assert_eq!(stats.total_records, 10);
        
        std::fs::remove_dir_all(test_dir).ok();
    }
}

