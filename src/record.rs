//! Production-quality record format with framing and checksums
//!
//! Record Format (on disk):
//! ```text
//! +----------------+----------------+----------------+----------------+
//! | Magic (4 bytes)| Length (4 bytes)| CRC32 (4 bytes)| Timestamp (8 bytes) |
//! +----------------+----------------+----------------+----------------+
//! | Data (variable length)                                          |
//! +----------------------------------------------------------------+
//! ```
//!
//! Total header size: 20 bytes
//! Magic: 0xD01DAC0D (DOLDA COD)

use crc32fast::Hasher;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Magic number for record identification (DOLDA COD)
pub const RECORD_MAGIC: u32 = 0xD01DAC0D;

/// Record header size in bytes
pub const RECORD_HEADER_SIZE: usize = 20;

/// Maximum record size (100MB)
pub const MAX_RECORD_SIZE: usize = 100 * 1024 * 1024;

/// Production-quality record with integrity checks
#[derive(Debug, Clone)]
pub struct Record {
    /// Timestamp when record was created (microseconds since epoch)
    pub timestamp: u64,
    /// Record data payload
    pub data: Vec<u8>,
}

/// Serialized record with framing
#[derive(Debug)]
pub struct FramedRecord {
    /// Complete serialized record (header + data)
    pub bytes: Vec<u8>,
    /// Offset of data within segment
    pub offset: u64,
}

#[derive(Error, Debug)]
pub enum RecordError {
    #[error("Invalid record magic: expected {RECORD_MAGIC:#x}, got {0:#x}")]
    InvalidMagic(u32),
    
    #[error("Record too large: {0} bytes (max {MAX_RECORD_SIZE})")]
    RecordTooLarge(usize),
    
    #[error("Checksum mismatch: expected {expected:#x}, got {actual:#x}")]
    ChecksumMismatch { expected: u32, actual: u32 },
    
    #[error("Incomplete record: expected {expected} bytes, got {actual}")]
    IncompleteRecord { expected: usize, actual: usize },
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl Record {
    /// Create a new record with current timestamp
    pub fn new(data: Vec<u8>) -> Result<Self, RecordError> {
        if data.len() > MAX_RECORD_SIZE {
            return Err(RecordError::RecordTooLarge(data.len()));
        }
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        
        Ok(Self { timestamp, data })
    }
    
    /// Create record with explicit timestamp
    pub fn with_timestamp(data: Vec<u8>, timestamp: u64) -> Result<Self, RecordError> {
        if data.len() > MAX_RECORD_SIZE {
            return Err(RecordError::RecordTooLarge(data.len()));
        }
        Ok(Self { timestamp, data })
    }
    
    /// Serialize record with framing and checksum
    pub fn serialize(&self) -> FramedRecord {
        let data_len = self.data.len() as u32;
        let total_size = RECORD_HEADER_SIZE + self.data.len();
        let mut bytes = Vec::with_capacity(total_size);
        
        // Write header
        bytes.extend_from_slice(&RECORD_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&data_len.to_le_bytes());
        
        // Calculate CRC32 of timestamp + data
        let checksum = Self::calculate_checksum(self.timestamp, &self.data);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        
        // Write timestamp
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());
        
        // Write data
        bytes.extend_from_slice(&self.data);
        
        FramedRecord { bytes, offset: 0 }
    }
    
    /// Deserialize record from bytes with validation
    pub fn deserialize(bytes: &[u8]) -> Result<(Self, usize), RecordError> {
        if bytes.len() < RECORD_HEADER_SIZE {
            return Err(RecordError::IncompleteRecord {
                expected: RECORD_HEADER_SIZE,
                actual: bytes.len(),
            });
        }
        
        // Read and validate magic
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != RECORD_MAGIC {
            return Err(RecordError::InvalidMagic(magic));
        }
        
        // Read length
        let data_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        if data_len > MAX_RECORD_SIZE {
            return Err(RecordError::RecordTooLarge(data_len));
        }
        
        let total_size = RECORD_HEADER_SIZE + data_len;
        if bytes.len() < total_size {
            return Err(RecordError::IncompleteRecord {
                expected: total_size,
                actual: bytes.len(),
            });
        }
        
        // Read and validate checksum
        let stored_checksum = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        
        // Read timestamp
        let timestamp = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        
        // Read data
        let data = bytes[20..total_size].to_vec();
        
        // Verify checksum
        let calculated_checksum = Self::calculate_checksum(timestamp, &data);
        if stored_checksum != calculated_checksum {
            return Err(RecordError::ChecksumMismatch {
                expected: stored_checksum,
                actual: calculated_checksum,
            });
        }
        
        Ok((Self { timestamp, data }, total_size))
    }
    
    /// Calculate CRC32 checksum of timestamp + data
    fn calculate_checksum(timestamp: u64, data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(data);
        hasher.finalize()
    }
    
    /// Get the size this record will occupy on disk
    pub fn serialized_size(&self) -> usize {
        RECORD_HEADER_SIZE + self.data.len()
    }
}

impl FramedRecord {
    /// Set the offset for this framed record
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }
    
    /// Get the size of the framed record in bytes
    pub fn size(&self) -> usize {
        self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_record_roundtrip() {
        let data = b"Hello, production world!".to_vec();
        let record = Record::new(data.clone()).unwrap();
        
        let framed = record.serialize();
        let (deserialized, size) = Record::deserialize(&framed.bytes).unwrap();
        
        assert_eq!(deserialized.data, data);
        assert_eq!(deserialized.timestamp, record.timestamp);
        assert_eq!(size, framed.bytes.len());
    }
    
    #[test]
    fn test_invalid_magic() {
        let mut bytes = vec![0u8; RECORD_HEADER_SIZE + 10];
        // Invalid magic
        bytes[0..4].copy_from_slice(&0xDEADBEEF_u32.to_le_bytes());
        
        let result = Record::deserialize(&bytes);
        assert!(matches!(result, Err(RecordError::InvalidMagic(_))));
    }
    
    #[test]
    fn test_checksum_corruption() {
        let data = b"Test data".to_vec();
        let record = Record::new(data).unwrap();
        
        let mut framed = record.serialize();
        // Corrupt the checksum
        framed.bytes[8] ^= 0xFF;
        
        let result = Record::deserialize(&framed.bytes);
        assert!(matches!(result, Err(RecordError::ChecksumMismatch { .. })));
    }
    
    #[test]
    fn test_data_corruption() {
        let data = b"Test data".to_vec();
        let record = Record::new(data).unwrap();
        
        let mut framed = record.serialize();
        // Corrupt the data
        framed.bytes[RECORD_HEADER_SIZE] ^= 0xFF;
        
        let result = Record::deserialize(&framed.bytes);
        assert!(matches!(result, Err(RecordError::ChecksumMismatch { .. })));
    }
    
    #[test]
    fn test_incomplete_record() {
        let data = b"Test data".to_vec();
        let record = Record::new(data).unwrap();
        
        let framed = record.serialize();
        // Truncate the record
        let incomplete = &framed.bytes[..framed.bytes.len() - 5];
        
        let result = Record::deserialize(incomplete);
        assert!(matches!(result, Err(RecordError::IncompleteRecord { .. })));
    }
    
    #[test]
    fn test_max_record_size() {
        let large_data = vec![0u8; MAX_RECORD_SIZE + 1];
        let result = Record::new(large_data);
        assert!(matches!(result, Err(RecordError::RecordTooLarge(_))));
    }
    
    #[test]
    fn test_empty_record() {
        let record = Record::new(vec![]).unwrap();
        let framed = record.serialize();
        let (deserialized, _) = Record::deserialize(&framed.bytes).unwrap();
        
        assert_eq!(deserialized.data.len(), 0);
    }
    
    #[test]
    fn test_multiple_records() {
        let records = vec![
            Record::new(b"Record 1".to_vec()).unwrap(),
            Record::new(b"Record 2 with more data".to_vec()).unwrap(),
            Record::new(b"Record 3".to_vec()).unwrap(),
        ];
        
        let mut buffer = Vec::new();
        for record in &records {
            let framed = record.serialize();
            buffer.extend_from_slice(&framed.bytes);
        }
        
        // Deserialize all records
        let mut offset = 0;
        for original in &records {
            let (deserialized, size) = Record::deserialize(&buffer[offset..]).unwrap();
            assert_eq!(deserialized.data, original.data);
            offset += size;
        }
        
        assert_eq!(offset, buffer.len());
    }
}

