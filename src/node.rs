//! Node management and communication module
//!
//! This module implements the shared-nothing node architecture with
//! TCP-based communication and automatic serialization.

use crate::types::*;
use crate::{DOLDAError, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

/// Configuration for a distributed node
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub name: String,
    pub node_type: NodeType,
    pub ip: String,
    pub port: u16,
    pub resources: NodeResources,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            name: "default-node".to_string(),
            node_type: NodeType::Data,
            ip: "127.0.0.1".to_string(),
            port: DEFAULT_DATA_PORT_BASE,
            resources: NodeResources::default(),
        }
    }
}

/// Distributed node representation
#[derive(Debug)]
pub struct Node {
    /// Unique node identifier
    pub node_id: u32,
    /// Node configuration
    pub config: NodeConfig,
    /// Current node state
    pub state: Arc<RwLock<NodeState>>,
    /// Network endpoint
    pub endpoint: NetworkEndpoint,
    /// Last heartbeat timestamp
    pub last_heartbeat: Arc<RwLock<u64>>,
    /// Failure count
    pub failure_count: Arc<RwLock<u32>>,
    /// Owned partitions (for data nodes)
    pub owned_partitions: Arc<RwLock<Vec<u32>>>,
    /// TCP listener (for server mode)
    pub listener: Option<TcpListener>,
    /// Communication channels
    pub command_tx: mpsc::UnboundedSender<NodeCommand>,
    pub command_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<NodeCommand>>>,
}

/// Commands that can be sent to a node
#[derive(Debug)]
pub enum NodeCommand {
    /// Start the node
    Start,
    /// Stop the node
    Stop,
    /// Send heartbeat
    Heartbeat,
    /// Handle incoming connection
    HandleConnection(TcpStream),
    /// Update resources
    UpdateResources,
}

/// Message types for inter-node communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeMessage {
    /// Heartbeat message
    Heartbeat { node_id: u32, timestamp: u64 },
    /// Data request
    DataRequest { partition_id: u32, offset: u64, size: usize },
    /// Data response
    DataResponse { data: Vec<u8> },
    /// Metadata synchronization
    MetadataSync { file_path: String, attrs: FileAttrs },
    /// Partition assignment
    PartitionAssignment { partition_id: u32, node_id: u32 },
}

impl Node {
    /// Create a new node
    pub async fn new(config: NodeConfig) -> Result<Self> {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let mut node = Self {
            node_id: 0, // Will be assigned by cluster
            config,
            state: Arc::new(RwLock::new(NodeState::Init)),
            endpoint: NetworkEndpoint::new(
                DEFAULT_CONTROL_PORT.to_string(),
                DEFAULT_DATA_PORT_BASE,
            ),
            last_heartbeat: Arc::new(RwLock::new(Self::current_timestamp())),
            failure_count: Arc::new(RwLock::new(0)),
            owned_partitions: Arc::new(RwLock::new(Vec::new())),
            listener: None,
            command_tx,
            command_rx: Arc::new(tokio::sync::Mutex::new(command_rx)),
        };

        // Initialize endpoint
        node.endpoint = NetworkEndpoint::new(node.config.ip.clone(), node.config.port);

        Ok(node)
    }

    /// Start the node
    pub async fn start(&mut self) -> Result<()> {
        *self.state.write() = NodeState::Active;
        *self.last_heartbeat.write() = Self::current_timestamp();

        // Start TCP listener if this is a server node
        if self.config.node_type != NodeType::Control {
            let addr = format!("{}:{}", self.config.ip, self.config.port);
            self.listener = Some(TcpListener::bind(&addr).await?);
            log::info!("Node {} listening on {}", self.config.name, addr);
        }

        // Spawn node worker task
        let state = Arc::clone(&self.state);
        let last_heartbeat = Arc::clone(&self.last_heartbeat);
        let command_rx = Arc::clone(&self.command_rx);

        tokio::spawn(async move {
            Self::run_worker(state, last_heartbeat, command_rx).await;
        });

        Ok(())
    }

    /// Stop the node
    pub async fn stop(&mut self) -> Result<()> {
        *self.state.write() = NodeState::Offline;
        self.listener = None;
        Ok(())
    }

    /// Connect to another node
    pub async fn connect_to(&self, target: &Node) -> Result<TcpStream> {
        if let Some(addr) = &target.endpoint.socket_addr {
            let stream = TcpStream::connect(addr).await?;
            Ok(stream)
        } else {
            Err(DOLDAError::Network("Invalid socket address".to_string()))
        }
    }

    /// Send a message to another node
    pub async fn send_message(
        &self,
        stream: &mut TcpStream,
        message: &NodeMessage,
    ) -> Result<()> {
        let data = bincode::serialize(message)?;
        let size = data.len() as u32;

        // Send size first
        stream.write_all(&size.to_be_bytes()).await?;

        // Send data
        stream.write_all(&data).await?;

        Ok(())
    }

    /// Receive a message from another node
    pub async fn receive_message(&self, stream: &mut TcpStream) -> Result<NodeMessage> {
        // Read size first
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf).await?;
        let size = u32::from_be_bytes(size_buf) as usize;

        // Read data
        let mut data = vec![0u8; size];
        stream.read_exact(&mut data).await?;

        // Deserialize message
        let message: NodeMessage = bincode::deserialize(&data)?;
        Ok(message)
    }

    /// Check if node is healthy
    pub fn is_healthy(&self) -> bool {
        let state = *self.state.read();
        let now = Self::current_timestamp();
        let last_heartbeat = *self.last_heartbeat.read();

        state == NodeState::Active && (now - last_heartbeat) < 30000 // 30 seconds
    }

    /// Get load factor (0.0 to 1.0)
    pub fn load_factor(&self) -> f64 {
        let resources = &self.config.resources;

        let memory_usage = 1.0 - (resources.available_memory as f64 / resources.total_memory as f64);
        let storage_usage = 1.0 - (resources.available_storage as f64 / resources.total_storage as f64);
        let cpu_usage = resources.cpu_usage / 100.0;

        (memory_usage + storage_usage + cpu_usage) / 3.0
    }

    /// Update node resources
    pub fn update_resources(&mut self) {
        // In a real implementation, this would query system metrics
        // For now, we'll simulate some resource updates
        self.config.resources.cpu_usage = (self.config.resources.cpu_usage + 1.0) % 100.0;
        self.config.resources.available_memory =
            self.config.resources.available_memory.saturating_sub(1024 * 1024); // 1MB
    }

    /// Add a partition to this node's ownership
    pub fn add_partition(&self, partition_id: u32) {
        let mut partitions = self.owned_partitions.write();
        if !partitions.contains(&partition_id) {
            partitions.push(partition_id);
        }
    }

    /// Remove a partition from this node's ownership
    pub fn remove_partition(&self, partition_id: u32) {
        let mut partitions = self.owned_partitions.write();
        partitions.retain(|&id| id != partition_id);
    }

    /// Get current timestamp in milliseconds
    fn current_timestamp() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Node worker task
    async fn run_worker(
        state: Arc<RwLock<NodeState>>,
        last_heartbeat: Arc<RwLock<u64>>,
        command_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<NodeCommand>>>,
    ) {
        let mut command_rx = command_rx.lock().await;

        while *state.read() == NodeState::Active {
            tokio::select! {
                Some(command) = command_rx.recv() => {
                    match command {
                        NodeCommand::Stop => break,
                        NodeCommand::Heartbeat => {
                            *last_heartbeat.write() = Self::current_timestamp();
                        }
                        NodeCommand::UpdateResources => {
                            // Resources would be updated here
                        }
                        _ => {
                            // Handle other commands
                        }
                    }
                }
                _ = time::sleep(Duration::from_millis(100)) => {
                    // Periodic maintenance
                }
            }
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Cleanup resources
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_creation() {
        let config = NodeConfig::default();
        let node = Node::new(config).await.unwrap();

        assert_eq!(node.node_id, 0);
        assert_eq!(node.config.node_type, NodeType::Data);
        assert_eq!(*node.state.read(), NodeState::Init);
    }

    #[tokio::test]
    async fn test_node_start_stop() {
        let config = NodeConfig::default();
        let mut node = Node::new(config).await.unwrap();

        node.start().await.unwrap();
        assert_eq!(*node.state.read(), NodeState::Active);

        node.stop().await.unwrap();
        assert_eq!(*node.state.read(), NodeState::Offline);
    }

    #[test]
    fn test_load_factor() {
        let config = NodeConfig::default();
        let node = Node::new(config).await.unwrap();

        let load = node.load_factor();
        assert!(load >= 0.0 && load <= 1.0);
    }
}
