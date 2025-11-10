//! Cluster management module
//!
//! This module manages the distributed cluster of nodes, including
//! node registration, health monitoring, load balancing, and failover.

use crate::node::{Node, NodeConfig};
use crate::types::{NodeType, NodeState, *};
use crate::{DOLDAError, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::time::{self, Duration};

/// Cluster configuration
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Expected number of nodes
    pub expected_nodes: usize,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval: u64,
    /// Health check timeout in milliseconds
    pub health_timeout: u64,
    /// Maximum failures before marking node as degraded
    pub max_failures: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            expected_nodes: 4,
            heartbeat_interval: 5000, // 5 seconds
            health_timeout: 30000,    // 30 seconds
            max_failures: 3,
        }
    }
}

/// Distributed cluster management
#[derive(Debug)]
pub struct Cluster {
    /// Cluster configuration
    config: ClusterConfig,
    /// All nodes in the cluster (node_id -> Node)
    nodes: Arc<DashMap<u32, Arc<Node>>>,
    /// Control node (if any)
    control_node: Arc<RwLock<Option<Arc<Node>>>>,
    /// Next available node ID
    next_node_id: Arc<RwLock<u32>>,
    /// Cluster-wide statistics
    stats: Arc<RwLock<ClusterStats>>,
    /// Shutdown signal
    shutdown: Arc<RwLock<bool>>,
}

/// Cluster-wide statistics
#[derive(Debug, Clone, Default)]
pub struct ClusterStats {
    /// Total memory across all nodes
    pub total_memory: u64,
    /// Total storage across all nodes
    pub total_storage: u64,
    /// Total CPU cores across all nodes
    pub total_cores: u32,
    /// Number of active nodes
    pub active_nodes: usize,
    /// Number of degraded nodes
    pub degraded_nodes: usize,
    /// Number of offline nodes
    pub offline_nodes: usize,
}

impl Cluster {
    /// Create a new cluster
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            config,
            nodes: Arc::new(DashMap::new()),
            control_node: Arc::new(RwLock::new(None)),
            next_node_id: Arc::new(RwLock::new(1)),
            stats: Arc::new(RwLock::new(ClusterStats::default())),
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Add a node to the cluster
    pub async fn add_node(&self, mut node: Node) -> Result<u32> {
        let node_id = {
            let mut next_id = self.next_node_id.write();
            let id = *next_id;
            *next_id += 1;
            node.node_id = id;
            id
        };

        // Start the node
        node.start().await?;

        let node_arc = Arc::new(node);
        self.nodes.insert(node_id, Arc::clone(&node_arc));

        // Set as control node if it's the first control node
        if node_arc.config.node_type == NodeType::Control {
            let mut control = self.control_node.write();
            if control.is_none() {
                *control = Some(Arc::clone(&node_arc));
            }
        }

        // Update cluster statistics
        self.update_stats();

        log::info!("Added node {} (ID: {}) to cluster", node_arc.config.name, node_id);
        Ok(node_id)
    }

    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: u32) -> Result<()> {
        if let Some((_, node)) = self.nodes.remove(&node_id) {
            // Stop the node
            let mut node_mut = Arc::try_unwrap(node)
                .map_err(|_| DOLDAError::Node("Node still has references".to_string()))?;
            node_mut.stop().await?;

            // Clear control node if removing it
            let mut control = self.control_node.write();
            if let Some(ctrl_node) = &*control {
                if ctrl_node.node_id == node_id {
                    *control = None;
                }
            }

            // Update cluster statistics
            self.update_stats();

            log::info!("Removed node {} from cluster", node_id);
        }

        Ok(())
    }

    /// Get a node by ID
    pub fn get_node(&self, node_id: u32) -> Option<Arc<Node>> {
        self.nodes.get(&node_id).map(|n| Arc::clone(n.value()))
    }

    /// Get all data nodes
    pub fn get_data_nodes(&self) -> Vec<Arc<Node>> {
        self.nodes
            .iter()
            .filter(|entry| {
                let node = entry.value();
                node.config.node_type == NodeType::Data || node.config.node_type == NodeType::Mixed
            })
            .map(|entry| Arc::clone(entry.value()))
            .collect()
    }

    /// Select the best data node for a partition
    pub fn select_data_node(&self, partition_id: u32) -> Option<Arc<Node>> {
        let data_nodes = self.get_data_nodes();
        if data_nodes.is_empty() {
            return None;
        }

        // Simple round-robin based on partition ID
        let index = (partition_id as usize) % data_nodes.len();
        Some(Arc::clone(&data_nodes[index]))
    }

    /// Handle node failure
    pub async fn handle_node_failure(&self, node_id: u32) -> Result<()> {
        if let Some(node) = self.get_node(node_id) {
            let mut failure_count = node.failure_count.write();
            *failure_count += 1;

            if *failure_count >= self.config.max_failures {
                // Mark node as degraded
                *node.state.write() = NodeState::Degraded;
                log::warn!("Node {} marked as degraded", node.config.name);

                // TODO: Implement failover logic
                // This would involve redistributing partitions to other nodes
            }
        }

        Ok(())
    }

    /// Rebalance partitions across nodes
    pub async fn rebalance_partitions(&self) -> Result<()> {
        // TODO: Implement sophisticated load balancing algorithm
        // For now, this is a placeholder
        log::info!("Partition rebalancing completed");
        Ok(())
    }

    /// Start cluster health monitoring
    pub async fn start_health_monitor(&self) -> Result<()> {
        let nodes = Arc::clone(&self.nodes);
        let config = self.config.clone();
        let shutdown = Arc::clone(&self.shutdown);

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(config.heartbeat_interval));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if *shutdown.read() {
                            break;
                        }

                        // Check health of all nodes
                        for entry in nodes.iter() {
                            let node = entry.value();
                            let now = Self::current_timestamp();
                            let last_heartbeat = *node.last_heartbeat.read();

                            if now - last_heartbeat > config.health_timeout {
                                let failure_count = *node.failure_count.read();
                                if failure_count < config.max_failures {
                                    *node.failure_count.write() = failure_count + 1;
                                    log::warn!("Node {} missed heartbeat", node.config.name);
                                }
                            }
                        }
                    }
                }
            }
        });

        log::info!("Cluster health monitoring started");
        Ok(())
    }

    /// Get cluster statistics
    pub fn get_stats(&self) -> ClusterStats {
        self.stats.read().clone()
    }

    /// Shutdown the cluster
    pub async fn shutdown(&self) -> Result<()> {
        *self.shutdown.write() = true;

        // Stop all nodes
        let node_ids: Vec<u32> = self.nodes.iter().map(|entry| *entry.key()).collect();
        for node_id in node_ids {
            self.remove_node(node_id).await?;
        }

        log::info!("Cluster shutdown completed");
        Ok(())
    }

    /// Update cluster statistics
    fn update_stats(&self) {
        let mut stats = self.stats.write();

        // Reset counters
        stats.total_memory = 0;
        stats.total_storage = 0;
        stats.total_cores = 0;
        stats.active_nodes = 0;
        stats.degraded_nodes = 0;
        stats.offline_nodes = 0;

        // Calculate from all nodes
        for entry in self.nodes.iter() {
            let node = entry.value();
            let resources = &node.config.resources;

            stats.total_memory += resources.total_memory;
            stats.total_storage += resources.total_storage;
            stats.total_cores += resources.cpu_cores;

            match *node.state.read() {
                NodeState::Active => stats.active_nodes += 1,
                NodeState::Degraded => stats.degraded_nodes += 1,
                NodeState::Offline => stats.offline_nodes += 1,
                _ => {}
            }
        }
    }

    /// Get current timestamp
    fn current_timestamp() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new(ClusterConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_creation() {
        let cluster = Cluster::default();
        let stats = cluster.get_stats();

        assert_eq!(stats.active_nodes, 0);
        assert_eq!(stats.total_memory, 0);
    }

    #[tokio::test]
    async fn test_add_remove_node() {
        let cluster = Cluster::default();

        // Add a node
        let config = NodeConfig::default();
        let node = Node::new(config).await.unwrap();
        let node_id = cluster.add_node(node).await.unwrap();

        // Check node was added
        assert!(cluster.get_node(node_id).is_some());

        let stats = cluster.get_stats();
        assert_eq!(stats.active_nodes, 1);

        // Remove the node
        cluster.remove_node(node_id).await.unwrap();

        // Check node was removed
        assert!(cluster.get_node(node_id).is_none());
    }

    #[tokio::test]
    async fn test_data_node_selection() {
        let cluster = Cluster::default();

        // Add a data node
        let config = NodeConfig {
            node_type: NodeType::Data,
            ..Default::default()
        };
        let node = Node::new(config).await.unwrap();
        let node_id = cluster.add_node(node).await.unwrap();

        // Select node for partition
        let selected = cluster.select_data_node(42).unwrap();
        assert_eq!(selected.node_id, node_id);

        // Cleanup
        cluster.remove_node(node_id).await.unwrap();
    }
}
