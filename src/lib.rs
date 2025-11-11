//! # DOLDA - Distributed Object Layer for Data Access
//!
//! DOLDA is a high-performance distributed persistent queue built in Rust,
//! featuring zero-copy operations, Raft consensus, and production-grade reliability.
//!
//! ## Architecture Features
//!
//! ### Shared-Nothing Distributed Architecture
//! - **Independent Nodes**: Each node has its own compute and storage resources
//! - **Async Communication**: Nodes communicate via Tokio async runtime
//! - **Node Types**: Control nodes (metadata), Data nodes (partitioned data), Mixed nodes
//!
//! ### Logical File System
//! - **Global Namespace**: Abstract layer hiding physical storage locations
//! - **Distributed Files**: Files are partitioned across multiple nodes
//! - **Metadata Management**: Centralized metadata with distributed data
//! - **Replication**: Configurable data replication for fault tolerance
//!
//! ### Data Partitioning and Sharding
//! - **Multiple Strategies**: Hash-based, range-based, and round-robin partitioning
//! - **Elastic Scaling**: Support for adding/removing nodes without resharding
//! - **Load Balancing**: Automatic distribution of data across nodes
//! - **Key-based Routing**: Efficient query routing based on partition keys

pub mod demo;
pub mod record; // Production-quality record format
pub mod storage; // Production storage engine with Record integration
pub mod compaction; // Segment compaction and space reclamation
pub mod network; // Network layer for distributed communication
pub mod raft; // Raft consensus algorithm
pub mod persistq; // persistQ implementation using DOLDA
pub mod observability; // Metrics and tracing
pub mod health; // Health check and monitoring server
pub mod resilience; // Error recovery and resilience patterns
pub mod datafusion_layer; // SQL query layer with Apache DataFusion
pub mod mailbox; // Actor-like mailbox system for inter-queue messaging

// Error handling
pub type Result<T> = std::result::Result<T, DOLDAError>;

#[derive(Debug)]
pub enum DOLDAError {
    Io(std::io::Error),
    Network(String),
    Partition(String),
    Filesystem(String),
    Node(String),
    Config(String),
    Serialization(Box<bincode::ErrorKind>),
}

impl std::fmt::Display for DOLDAError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DOLDAError::Io(e) => write!(f, "IO error: {}", e),
            DOLDAError::Network(s) => write!(f, "Network error: {}", s),
            DOLDAError::Partition(s) => write!(f, "Partition error: {}", s),
            DOLDAError::Filesystem(s) => write!(f, "Filesystem error: {}", s),
            DOLDAError::Node(s) => write!(f, "Node error: {}", s),
            DOLDAError::Config(s) => write!(f, "Config error: {}", s),
            DOLDAError::Serialization(e) => write!(f, "Serialization error: {}", e),
        }
    }
}

impl std::error::Error for DOLDAError {}

impl From<std::io::Error> for DOLDAError {
    fn from(err: std::io::Error) -> Self {
        DOLDAError::Io(err)
    }
}

impl From<Box<bincode::ErrorKind>> for DOLDAError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        DOLDAError::Serialization(err)
    }
}
