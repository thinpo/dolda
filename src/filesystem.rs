//! Logical filesystem module
//!
//! This module implements a distributed filesystem with global namespace,
//! block-level distribution, and metadata management.

use crate::cluster::Cluster;
use crate::node::Node;
use crate::types::*;
use crate::{DOLDAError, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// Distributed file representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedFile {
    /// Global logical path
    pub logical_path: String,
    /// Internal physical name (UUID-based)
    pub physical_name: String,
    /// File attributes
    pub attrs: FileAttrs,
    /// Block locations across nodes
    pub blocks: Vec<BlockLocation>,
    /// Replica node IDs
    pub replica_nodes: Vec<u32>,
    /// Primary node ID
    pub primary_node: u32,
}

impl DistributedFile {
    /// Create a new distributed file
    pub fn new(logical_path: String, file_type: FileType, replica_count: u32) -> Self {
        let physical_name = Uuid::new_v4().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            logical_path,
            physical_name,
            attrs: FileAttrs {
                size: 0,
                created_time: now,
                modified_time: now,
                permissions: 0o644,
                file_type,
                replica_count,
            },
            blocks: Vec::new(),
            replica_nodes: Vec::new(),
            primary_node: 0,
        }
    }
}

/// Directory entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub is_directory: bool,
    pub attrs: FileAttrs,
}

/// Logical directory
#[derive(Debug)]
pub struct LogicalDirectory {
    pub path: String,
    pub entries: DashMap<String, DirEntry>,
    pub primary_node: u32,
    pub replica_nodes: Vec<u32>,
}

/// Global filesystem metadata
#[derive(Debug)]
pub struct Filesystem {
    /// Root directory
    root_dir: LogicalDirectory,
    /// File table (logical_path -> DistributedFile)
    file_table: DashMap<String, Arc<DistributedFile>>,
    /// Metadata node reference
    metadata_node: Option<Arc<Node>>,
    /// Cluster reference
    cluster: Arc<Cluster>,
    /// Filesystem statistics
    stats: Arc<RwLock<FilesystemStats>>,
}

/// Filesystem statistics
#[derive(Debug, Clone, Default)]
pub struct FilesystemStats {
    pub total_files: u64,
    pub total_size: u64,
    pub total_blocks: u64,
}

impl Filesystem {
    /// Create a new filesystem
    pub fn new(cluster: Arc<Cluster>, metadata_node: Option<Arc<Node>>) -> Self {
        let root_dir = LogicalDirectory {
            path: "/".to_string(),
            entries: DashMap::new(),
            primary_node: metadata_node.as_ref().map(|n| n.node_id).unwrap_or(0),
            replica_nodes: Vec::new(),
        };

        Self {
            root_dir,
            file_table: DashMap::new(),
            metadata_node,
            cluster,
            stats: Arc::new(RwLock::new(FilesystemStats::default())),
        }
    }

    /// Create a new file
    pub async fn create_file(
        &self,
        logical_path: &str,
        file_type: FileType,
        replica_count: u32,
    ) -> Result<Arc<DistributedFile>> {
        // Check if file already exists
        if self.file_table.contains_key(logical_path) {
            return Err(DOLDAError::Filesystem(format!("File {} already exists", logical_path)));
        }

        // Create distributed file
        let mut file = DistributedFile::new(logical_path.to_string(), file_type, replica_count);

        // Select primary node
        if let Some(primary_node) = self.cluster.select_data_node(0) {
            file.primary_node = primary_node.node_id;
        }

        // Allocate initial blocks (empty file for now)
        self.allocate_blocks(&mut file, 0).await?;

        let file_arc = Arc::new(file);
        self.file_table.insert(logical_path.to_string(), Arc::clone(&file_arc));

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_files += 1;
        }

        log::info!("Created file: {}", logical_path);
        Ok(file_arc)
    }

    /// Open an existing file
    pub fn open_file(&self, logical_path: &str) -> Result<Arc<DistributedFile>> {
        self.file_table
            .get(logical_path)
            .map(|f| Arc::clone(f.value()))
            .ok_or_else(|| DOLDAError::Filesystem(format!("File {} not found", logical_path)))
    }

    /// Delete a file
    pub async fn delete_file(&self, logical_path: &str) -> Result<()> {
        if let Some((_, file)) = self.file_table.remove(logical_path) {
            // Free blocks
            self.free_blocks(&file).await?;

            // Update statistics
            {
                let mut stats = self.stats.write();
                stats.total_files = stats.total_files.saturating_sub(1);
                stats.total_size = stats.total_size.saturating_sub(file.attrs.size);
                stats.total_blocks = stats.total_blocks.saturating_sub(file.blocks.len() as u64);
            }

            log::info!("Deleted file: {}", logical_path);
        }

        Ok(())
    }

    /// Read data from a file
    pub async fn read_file(
        &self,
        file: &DistributedFile,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        if offset >= file.attrs.size {
            return Ok(Vec::new());
        }

        let available = (file.attrs.size - offset) as usize;
        let to_read = size.min(available);

        // In a real implementation, this would coordinate reads across multiple nodes
        // For now, return dummy data
        let data = vec![0u8; to_read];

        Ok(data)
    }

    /// Write data to a file
    pub async fn write_file(
        &self,
        file: &mut DistributedFile,
        offset: u64,
        data: &[u8],
    ) -> Result<usize> {
        let new_size = offset + data.len() as u64;

        // Allocate additional blocks if needed
        let needed_blocks = ((new_size + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64) as usize;
        if needed_blocks > file.blocks.len() {
            self.allocate_blocks(file, needed_blocks - file.blocks.len()).await?;
        }

        // Update file size and modification time
        if new_size > file.attrs.size {
            file.attrs.size = new_size;
        }
        file.attrs.modified_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // In a real implementation, this would distribute writes across nodes
        // For now, just update statistics

        {
            let mut stats = self.stats.write();
            stats.total_size = stats.total_size.max(file.attrs.size);
        }

        Ok(data.len())
    }

    /// Sync file to disk
    pub async fn sync_file(&self, _file: &DistributedFile) -> Result<()> {
        // In a real implementation, this would ensure all replicas are synchronized
        Ok(())
    }

    /// List directory contents
    pub fn list_directory(&self, path: &str) -> Result<Vec<DirEntry>> {
        // Simplified implementation - in a real system this would traverse the hierarchy
        if path == "/" {
            let entries: Vec<DirEntry> = self.file_table
                .iter()
                .filter(|entry| {
                    let file_path = entry.key();
                    file_path.starts_with("/data/") ||
                    file_path.starts_with("/index/") ||
                    file_path.starts_with("/logs/")
                })
                .map(|entry| {
                    let file = entry.value();
                    DirEntry {
                        name: entry.key().clone(),
                        is_directory: false,
                        attrs: file.attrs.clone(),
                    }
                })
                .collect();

            Ok(entries)
        } else {
            Ok(Vec::new())
        }
    }

    /// Get filesystem statistics
    pub fn get_stats(&self) -> FilesystemStats {
        self.stats.read().clone()
    }

    /// Allocate blocks for a file across cluster nodes
    async fn allocate_blocks(&self, file: &mut DistributedFile, additional_blocks: usize) -> Result<()> {
        let data_nodes = self.cluster.get_data_nodes();
        if data_nodes.is_empty() {
            return Err(DOLDAError::Filesystem("No data nodes available".to_string()));
        }

        let start_block = file.blocks.len();
        for i in 0..additional_blocks {
            let node_index = (start_block + i) % data_nodes.len();
            let node = &data_nodes[node_index];

            let block_location = BlockLocation::new(
                node.node_id,
                (start_block + i) as u64 * BLOCK_SIZE as u64,
                BLOCK_SIZE as u32,
            );

            file.blocks.push(block_location);
        }

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_blocks += additional_blocks as u64;
        }

        Ok(())
    }

    /// Free blocks allocated to a file
    async fn free_blocks(&self, _file: &DistributedFile) -> Result<()> {
        // In a real implementation, this would notify nodes to free the blocks
        // and update the cluster's block allocation table
        Ok(())
    }
}

impl Default for Filesystem {
    fn default() -> Self {
        Self::new(Arc::new(Cluster::default()), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_filesystem_creation() {
        let cluster = Arc::new(Cluster::default());
        let fs = Filesystem::new(cluster, None);

        let stats = fs.get_stats();
        assert_eq!(stats.total_files, 0);
        assert_eq!(stats.total_size, 0);
    }

    #[tokio::test]
    async fn test_file_operations() {
        let cluster = Arc::new(Cluster::default());
        let fs = Filesystem::new(Arc::clone(&cluster), None);

        // Create a file
        let file = fs.create_file("/test/data.txt", FileType::Data, 3).await.unwrap();
        assert_eq!(file.logical_path, "/test/data.txt");
        assert_eq!(file.attrs.file_type, FileType::Data);

        // Open the file
        let opened_file = fs.open_file("/test/data.txt").unwrap();
        assert_eq!(opened_file.logical_path, file.logical_path);

        // Write to file
        let mut file_mut = Arc::try_unwrap(file)
            .map_err(|_| "File still has references").unwrap();
        let written = fs.write_file(&mut file_mut, 0, b"Hello, World!").await.unwrap();
        assert_eq!(written, 13);

        // Delete the file
        fs.delete_file("/test/data.txt").await.unwrap();

        // Verify file is gone
        assert!(fs.open_file("/test/data.txt").is_err());
    }

    #[test]
    fn test_list_directory() {
        let cluster = Arc::new(Cluster::default());
        let fs = Filesystem::new(cluster, None);

        // Create some test files
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            fs.create_file("/data/table1", FileType::Data, 3).await.unwrap();
            fs.create_file("/index/primary", FileType::Index, 2).await.unwrap();
        });

        // List root directory
        let entries = fs.list_directory("/").unwrap();
        assert!(!entries.is_empty());
    }
}
