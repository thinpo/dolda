//! Utility functions and helpers
//!
//! This module contains common utility functions used throughout
//! the DOLDA distributed database system.

use std::path::Path;

/// Path utilities for filesystem operations
pub mod path_utils {
    use super::*;

    /// Check if a path is absolute
    pub fn is_absolute(path: &str) -> bool {
        Path::new(path).is_absolute()
    }

    /// Normalize a path by resolving . and .. components
    pub fn normalize_path(path: &str) -> String {
        let path = Path::new(path);

        // For now, simple normalization - in a real implementation
        // this would handle .. and . properly
        path.to_string_lossy().to_string()
    }

    /// Join two path components
    pub fn join_paths(base: &str, relative: &str) -> String {
        let base_path = Path::new(base);
        let joined = base_path.join(relative);
        joined.to_string_lossy().to_string()
    }

    /// Get the parent directory of a path
    pub fn get_parent(path: &str) -> Option<String> {
        Path::new(path).parent()
            .map(|p| p.to_string_lossy().to_string())
    }

    /// Get the basename (filename) of a path
    pub fn get_basename(path: &str) -> String {
        Path::new(path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string())
    }

    /// Check if a path represents a directory
    pub fn is_directory(path: &str) -> bool {
        // Simple check - in a real implementation this would
        // query the filesystem metadata
        path.ends_with('/')
    }
}

/// Hash utilities for data partitioning
pub mod hash_utils {
    use crc32fast::Hasher;

    /// Compute CRC32 hash of data
    pub fn crc32_hash(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    /// Compute hash for string data
    pub fn string_hash(s: &str) -> u64 {
        crc32_hash(s.as_bytes()) as u64
    }

    /// Compute hash for integer data
    pub fn int_hash(i: i64) -> u64 {
        crc32_hash(&i.to_be_bytes()) as u64
    }

    /// Compute hash for timestamp data
    pub fn timestamp_hash(ts: u64) -> u64 {
        crc32_hash(&ts.to_be_bytes()) as u64
    }
}

/// Time utilities
pub mod time_utils {
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Get current timestamp in milliseconds
    pub fn current_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Get current timestamp in seconds
    pub fn current_timestamp_s() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Check if a timestamp is older than a certain age in milliseconds
    pub fn is_older_than(timestamp: u64, age_ms: u64) -> bool {
        let now = current_timestamp_ms();
        now.saturating_sub(timestamp) > age_ms
    }
}

/// Resource monitoring utilities
pub mod resource_utils {
    use sysinfo::{CpuExt, System, SystemExt};

    /// Get current memory usage information
    pub fn get_memory_info() -> (u64, u64, u64) { // total, used, available
        let mut sys = System::new_all();
        sys.refresh_all();

        let total = sys.total_memory() * 1024; // Convert to bytes
        let used = sys.used_memory() * 1024;
        let available = sys.available_memory() * 1024;

        (total, used, available)
    }

    /// Get current CPU usage
    pub fn get_cpu_usage() -> f32 {
        let mut sys = System::new_all();
        sys.refresh_all();

        sys.global_cpu_info().cpu_usage()
    }

    /// Get system information
    pub fn get_system_info() -> (String, String, String) { // (os, kernel, arch)
        let mut sys = System::new_all();
        sys.refresh_all();

        let os = sys.name().unwrap_or_else(|| "Unknown".to_string());
        let kernel = sys.kernel_version().unwrap_or_else(|| "Unknown".to_string());
        let arch = std::env::consts::ARCH.to_string();

        (os, kernel, arch)
    }
}

/// Serialization utilities
pub mod serialization_utils {
    use crate::{DOLDAError, Result};
    use bincode;
    use serde::{Deserialize, Serialize};

    /// Serialize data to bytes
    pub fn serialize<T: Serialize>(data: &T) -> Result<Vec<u8>> {
        bincode::serialize(data)
            .map_err(|e| DOLDAError::Serialization(e))
    }

    /// Deserialize data from bytes
    pub fn deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
        bincode::deserialize(data)
            .map_err(|e| DOLDAError::Serialization(e))
    }

    /// Calculate serialized size of data
    pub fn serialized_size<T: Serialize>(data: &T) -> Result<usize> {
        serialize(data).map(|v| v.len())
    }
}

/// Configuration utilities
pub mod config_utils {
    use serde::{Deserialize, Serialize};
    use std::fs;
    use std::path::Path;

    /// Load configuration from a TOML file
    pub fn load_config<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: T = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Save configuration to a TOML file
    pub fn save_config<T: Serialize>(config: &T, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let contents = toml::to_string_pretty(config)?;
        fs::write(path, contents)?;
        Ok(())
    }
}

/// Validation utilities
pub mod validation_utils {
    /// Validate that a port number is valid
    pub fn is_valid_port(port: u16) -> bool {
        port > 0 && port < 65535
    }

    /// Validate that an IP address is valid
    pub fn is_valid_ip(ip: &str) -> bool {
        ip.parse::<std::net::IpAddr>().is_ok()
    }

    /// Validate that a path is valid for the filesystem
    pub fn is_valid_path(path: &str) -> bool {
        !path.is_empty() &&
        !path.contains("..") && // Prevent directory traversal
        !path.contains('\0') && // Prevent null bytes
        path.len() <= 4096 // Reasonable path length limit
    }

    /// Validate that a node name is valid
    pub fn is_valid_node_name(name: &str) -> bool {
        !name.is_empty() &&
        name.len() <= 64 &&
        name.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_utils() {
        assert!(path_utils::is_absolute("/absolute/path"));
        assert!(!path_utils::is_absolute("relative/path"));

        assert_eq!(path_utils::get_basename("/path/to/file.txt"), "file.txt");
        assert_eq!(path_utils::get_basename("file.txt"), "file.txt");

        assert_eq!(path_utils::join_paths("/base", "relative"), "/base/relative");
    }

    #[test]
    fn test_hash_utils() {
        let hash1 = hash_utils::string_hash("test");
        let hash2 = hash_utils::string_hash("test");
        let hash3 = hash_utils::string_hash("different");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);

        let int_hash = hash_utils::int_hash(42);
        assert_ne!(int_hash, 0);
    }

    #[test]
    fn test_validation_utils() {
        assert!(validation_utils::is_valid_port(8080));
        assert!(!validation_utils::is_valid_port(0));
        assert!(!validation_utils::is_valid_port(65535));

        assert!(validation_utils::is_valid_ip("127.0.0.1"));
        assert!(validation_utils::is_valid_ip("::1"));
        assert!(!validation_utils::is_valid_ip("invalid"));

        assert!(validation_utils::is_valid_path("/valid/path"));
        assert!(!validation_utils::is_valid_path("../invalid"));
        assert!(!validation_utils::is_valid_path(""));

        assert!(validation_utils::is_valid_node_name("valid-node-1"));
        assert!(!validation_utils::is_valid_node_name("invalid@node"));
        assert!(!validation_utils::is_valid_node_name(""));
    }

    #[test]
    fn test_time_utils() {
        let now = time_utils::current_timestamp_ms();
        assert!(now > 0);

        let past = now - 1000; // 1 second ago
        assert!(time_utils::is_older_than(past, 500)); // Should be older than 500ms
        assert!(!time_utils::is_older_than(past, 2000)); // Should not be older than 2s
    }
}
