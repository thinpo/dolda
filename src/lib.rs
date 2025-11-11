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
pub mod mailbox_processor; // Message processors with SQL query support

// Error handling
pub type Result<T> = std::result::Result<T, DOLDAError>;

/// Unified error type for all DOLDA operations
#[derive(Debug, thiserror::Error)]
pub enum DOLDAError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Partition error: {0}")]
    Partition(String),
    
    #[error("Filesystem error: {0}")]
    Filesystem(String),
    
    #[error("Node error: {0}")]
    Node(String),
    
    #[error("Config error: {0}")]
    Config(String),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] Box<bincode::ErrorKind>),
    
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    
    #[error("Record error: {0}")]
    Record(#[from] crate::record::RecordError),
    
    #[error("Compaction error: {0}")]
    Compaction(#[from] crate::compaction::CompactionError),
    
    #[error("Resilience error: {0}")]
    Resilience(#[from] crate::resilience::ResilienceError),
    
    #[error("Network error: {0}")]
    NetworkLayer(#[from] crate::network::NetworkError),
    
    #[error("Raft error: {0}")]
    Raft(#[from] crate::raft::RaftError),
    
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] crate::datafusion_layer::DataFusionError),
    
    #[error("Mailbox error: {0}")]
    Mailbox(#[from] crate::mailbox::MailboxError),
    
    #[error("Processor error: {0}")]
    Processor(#[from] crate::mailbox_processor::ProcessorError),
}
