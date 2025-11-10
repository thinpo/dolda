//! Common types and constants used throughout the DOLDA system

use serde::{Deserialize, Serialize};
use std::fmt;

/// Maximum number of nodes in the cluster
pub const MAX_NODES: usize = 256;

/// Maximum length of node names
pub const MAX_NODE_NAME: usize = 64;

/// Maximum IP address length
pub const MAX_IP_LEN: usize = 16;

/// Default control node port
pub const DEFAULT_CONTROL_PORT: u16 = 25080;

/// Default data node base port
pub const DEFAULT_DATA_PORT_BASE: u16 = 25081;

/// Block size for file operations (4KB)
pub const BLOCK_SIZE: usize = 4096;

/// Maximum number of partitions
pub const MAX_PARTITIONS: usize = 65536;

/// Maximum number of shards per partition
pub const MAX_SHARDS_PER_PARTITION: usize = 32;

/// Partition key size
pub const PARTITION_KEY_SIZE: usize = 256;

/// Maximum replicas per partition
pub const MAX_REPLICAS: usize = 3;

/// Node types in the distributed system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    /// Manages metadata and coordination
    Control,
    /// Stores partitioned data
    Data,
    /// Both control and data functions
    Mixed,
}

impl fmt::Display for NodeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeType::Control => write!(f, "Control"),
            NodeType::Data => write!(f, "Data"),
            NodeType::Mixed => write!(f, "Mixed"),
        }
    }
}

/// Node states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is initializing
    Init,
    /// Node is active and healthy
    Active,
    /// Node is degraded but still operational
    Degraded,
    /// Node is offline
    Offline,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeState::Init => write!(f, "Init"),
            NodeState::Active => write!(f, "Active"),
            NodeState::Degraded => write!(f, "Degraded"),
            NodeState::Offline => write!(f, "Offline"),
        }
    }
}

/// File types in the distributed filesystem
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    /// Partitioned data file
    Data,
    /// System metadata
    Metadata,
    /// Index file
    Index,
    /// Transaction log
    Log,
    /// Temporary file
    Temp,
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileType::Data => write!(f, "Data"),
            FileType::Metadata => write!(f, "Metadata"),
            FileType::Index => write!(f, "Index"),
            FileType::Log => write!(f, "Log"),
            FileType::Temp => write!(f, "Temp"),
        }
    }
}

/// Partitioning strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Hash-based partitioning
    Hash,
    /// Range-based partitioning
    Range,
    /// List-based partitioning
    List,
    /// Round-robin distribution
    RoundRobin,
}

impl fmt::Display for PartitionStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionStrategy::Hash => write!(f, "Hash"),
            PartitionStrategy::Range => write!(f, "Range"),
            PartitionStrategy::List => write!(f, "List"),
            PartitionStrategy::RoundRobin => write!(f, "RoundRobin"),
        }
    }
}

/// Partition key types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionKeyType {
    String,
    Integer,
    Timestamp,
    Composite,
}

impl fmt::Display for PartitionKeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionKeyType::String => write!(f, "String"),
            PartitionKeyType::Integer => write!(f, "Integer"),
            PartitionKeyType::Timestamp => write!(f, "Timestamp"),
            PartitionKeyType::Composite => write!(f, "Composite"),
        }
    }
}

/// Resource information for each node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResources {
    /// Total memory in bytes
    pub total_memory: u64,
    /// Available memory in bytes
    pub available_memory: u64,
    /// Total storage in bytes
    pub total_storage: u64,
    /// Available storage in bytes
    pub available_storage: u64,
    /// Number of CPU cores
    pub cpu_cores: u32,
    /// CPU usage percentage
    pub cpu_usage: f64,
}

impl Default for NodeResources {
    fn default() -> Self {
        Self {
            total_memory: 8 * 1024 * 1024 * 1024, // 8GB
            available_memory: 6 * 1024 * 1024 * 1024, // 6GB
            total_storage: 100 * 1024 * 1024 * 1024, // 100GB
            available_storage: 80 * 1024 * 1024 * 1024, // 80GB
            cpu_cores: 4,
            cpu_usage: 25.0,
        }
    }
}

/// File attributes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileAttrs {
    /// File size in bytes
    pub size: u64,
    /// Creation timestamp
    pub created_time: u64,
    /// Last modification timestamp
    pub modified_time: u64,
    /// Unix-style permissions
    pub permissions: u32,
    /// File type
    pub file_type: FileType,
    /// Number of replicas
    pub replica_count: u32,
}

impl Default for FileAttrs {
    fn default() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            size: 0,
            created_time: now,
            modified_time: now,
            permissions: 0o644,
            file_type: FileType::Data,
            replica_count: 3,
        }
    }
}

/// Network endpoint for node communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkEndpoint {
    /// IP address
    pub ip: String,
    /// Port number
    pub port: u16,
    /// Optional socket address
    #[serde(skip)]
    pub socket_addr: Option<std::net::SocketAddr>,
}

impl NetworkEndpoint {
    pub fn new(ip: String, port: u16) -> Self {
        let socket_addr = format!("{}:{}", ip, port).parse().ok();
        Self {
            ip,
            port,
            socket_addr,
        }
    }
}

/// Physical block location
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockLocation {
    /// Node hosting this block
    pub node_id: u32,
    /// Offset within node's storage
    pub offset: u64,
    /// Block size
    pub size: u32,
}

impl BlockLocation {
    pub fn new(node_id: u32, offset: u64, size: u32) -> Self {
        Self {
            node_id,
            offset,
            size,
        }
    }
}
