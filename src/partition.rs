//! Data partitioning and sharding module
//!
//! This module implements data partitioning strategies for distributing
//! data evenly across cluster nodes with elastic scaling support.

use crate::cluster::Cluster;
use crate::node::Node;
use crate::types::*;
use crate::{DOLDAError, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Partition key that can be used for routing and partitioning
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PartitionKey {
    String(String),
    Integer(i64),
    Timestamp(u64),
    Composite(Vec<u8>),
}

impl PartitionKey {
    /// Create a string partition key
    pub fn string<S: Into<String>>(s: S) -> Self {
        Self::String(s.into())
    }

    /// Create an integer partition key
    pub fn integer(i: i64) -> Self {
        Self::Integer(i)
    }

    /// Create a timestamp partition key
    pub fn timestamp(ts: u64) -> Self {
        Self::Timestamp(ts)
    }

    /// Create a composite partition key
    pub fn composite(data: Vec<u8>) -> Self {
        Self::Composite(data)
    }

    /// Get the partition key type
    pub fn key_type(&self) -> PartitionKeyType {
        match self {
            Self::String(_) => PartitionKeyType::String,
            Self::Integer(_) => PartitionKeyType::Integer,
            Self::Timestamp(_) => PartitionKeyType::Timestamp,
            Self::Composite(_) => PartitionKeyType::Composite,
        }
    }

    /// Compute hash for partitioning
    pub fn hash(&self) -> u64 {
        let mut hasher = crc32fast::Hasher::new();
        match self {
            Self::String(s) => {
                hasher.update(s.as_bytes());
            }
            Self::Integer(i) => {
                hasher.update(&i.to_be_bytes());
            }
            Self::Timestamp(ts) => {
                hasher.update(&ts.to_be_bytes());
            }
            Self::Composite(data) => {
                hasher.update(data);
            }
        }
        hasher.finalize() as u64
    }
}

/// Physical data shard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataShard {
    pub shard_id: u32,
    pub node_id: u32,
    pub size_bytes: u64,
    pub record_count: u64,
    pub data_path: String,
    pub is_primary: bool,
}

impl DataShard {
    pub fn new(shard_id: u32, node_id: u32, is_primary: bool) -> Self {
        Self {
            shard_id,
            node_id,
            size_bytes: 0,
            record_count: 0,
            data_path: format!("/shards/shard_{}", shard_id),
            is_primary,
        }
    }
}

/// Partition definition
#[derive(Debug)]
pub struct Partition {
    pub partition_id: u32,
    pub strategy: PartitionStrategy,
    pub min_key: Option<PartitionKey>,
    pub max_key: Option<PartitionKey>,
    pub shards: Vec<Arc<RwLock<DataShard>>>,
    pub assigned_nodes: Vec<u32>,
    pub total_records: u64,
    pub total_size: u64,
    pub created_time: u64,
}

impl Partition {
    pub fn new(
        partition_id: u32,
        strategy: PartitionStrategy,
        min_key: Option<PartitionKey>,
        max_key: Option<PartitionKey>,
        _replica_count: u32,
    ) -> Self {
        Self {
            partition_id,
            strategy,
            min_key,
            max_key,
            shards: Vec::new(),
            assigned_nodes: Vec::new(),
            total_records: 0,
            total_size: 0,
            created_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

/// Partition manager for the cluster
#[derive(Debug)]
pub struct PartitionManager {
    /// All partitions (partition_id -> Partition)
    partitions: DashMap<u32, Arc<Partition>>,
    /// Partitioning configuration
    pub default_strategy: PartitionStrategy,
    pub default_replicas: u32,
    pub max_shards_per_partition: usize,
    /// Cluster reference
    cluster: Arc<Cluster>,
    /// Statistics
    stats: Arc<RwLock<PartitionStats>>,
}

/// Partition manager statistics
#[derive(Debug, Clone, Default)]
pub struct PartitionStats {
    pub total_partitions: u64,
    pub total_shards: u64,
    pub total_data_size: u64,
}

impl PartitionManager {
    /// Create a new partition manager
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self {
            partitions: DashMap::new(),
            default_strategy: PartitionStrategy::Hash,
            default_replicas: 3,
            max_shards_per_partition: MAX_SHARDS_PER_PARTITION,
            cluster,
            stats: Arc::new(RwLock::new(PartitionStats::default())),
        }
    }

    /// Create a new partition
    pub async fn create_partition(
        &self,
        strategy: PartitionStrategy,
        min_key: Option<PartitionKey>,
        max_key: Option<PartitionKey>,
        replica_count: u32,
    ) -> Result<u32> {
        let partition_id = self.generate_partition_id();

        let mut partition = Partition::new(partition_id, strategy, min_key, max_key, replica_count);

        // Assign nodes to the partition
        self.assign_nodes_to_partition(&mut partition).await?;

        // Create shards for the partition
        self.create_shards_for_partition(&mut partition).await?;

        let partition_arc = Arc::new(partition);
        self.partitions.insert(partition_id, Arc::clone(&partition_arc));

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_partitions += 1;
            stats.total_shards += partition_arc.shards.len() as u64;
        }

        log::info!("Created partition {} with {} shards", partition_id, partition_arc.shards.len());
        Ok(partition_id)
    }

    /// Get partition for a key
    pub fn get_partition_for_key(&self, key: &PartitionKey) -> Option<u32> {
        // Simple hash-based partitioning for now
        let hash = key.hash();
        let partition_count = self.partitions.len() as u64;

        if partition_count == 0 {
            return None;
        }

        let partition_id = (hash % partition_count + 1) as u32;
        Some(partition_id)
    }

    /// Get node for a key
    pub fn get_node_for_key(&self, key: &PartitionKey) -> Option<Arc<Node>> {
        let partition_id = self.get_partition_for_key(key)?;
        let partition = self.partitions.get(&partition_id)?;

        if partition.assigned_nodes.is_empty() {
            return None;
        }

        // Return the first assigned node (could be more sophisticated)
        let node_id = partition.assigned_nodes[0];
        self.cluster.get_node(node_id)
    }

    /// Get shard for a key
    pub fn get_shard_for_key(&self, key: &PartitionKey) -> Option<Arc<RwLock<DataShard>>> {
        let partition_id = self.get_partition_for_key(key)?;
        let partition = self.partitions.get(&partition_id)?;

        if partition.shards.is_empty() {
            return None;
        }

        // Return primary shard (first one)
        Some(Arc::clone(&partition.shards[0]))
    }

    /// Split a partition
    pub async fn split_partition(
        &self,
        _partition_id: u32,
        _split_key: &PartitionKey,
    ) -> Result<(u32, u32)> {
        // TODO: Implement partition splitting
        Err(DOLDAError::Partition("Partition splitting not implemented".to_string()))
    }

    /// Merge two partitions
    pub async fn merge_partitions(
        &self,
        _partition1_id: u32,
        _partition2_id: u32,
    ) -> Result<u32> {
        // TODO: Implement partition merging
        Err(DOLDAError::Partition("Partition merging not implemented".to_string()))
    }

    /// Add a node to the partition manager
    pub async fn add_node(&self, node: Arc<Node>) -> Result<()> {
        // TODO: Redistribute existing partitions to include new node
        log::info!("Added node {} to partition manager", node.config.name);
        Ok(())
    }

    /// Remove a node from the partition manager
    pub async fn remove_node(&self, node_id: u32) -> Result<()> {
        // TODO: Migrate partitions away from removed node
        log::info!("Removed node {} from partition manager", node_id);
        Ok(())
    }

    /// Rebalance partitions across nodes
    pub async fn rebalance_partitions(&self) -> Result<()> {
        // TODO: Implement sophisticated load balancing
        log::info!("Partition rebalancing completed");
        Ok(())
    }

    /// Get partition statistics
    pub fn get_stats(&self) -> PartitionStats {
        self.stats.read().clone()
    }

    /// Generate a unique partition ID
    fn generate_partition_id(&self) -> u32 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u32;

        // Simple ID generation - in production, this would be more robust
        timestamp % MAX_PARTITIONS as u32 + 1
    }

    /// Assign nodes to a partition
    async fn assign_nodes_to_partition(&self, partition: &mut Partition) -> Result<()> {
        let data_nodes = self.cluster.get_data_nodes();

        if data_nodes.is_empty() {
            return Err(DOLDAError::Partition("No data nodes available".to_string()));
        }

        // Assign all data nodes to this partition
        partition.assigned_nodes = data_nodes.iter()
            .map(|node| node.node_id)
            .collect();

        Ok(())
    }

    /// Create shards for a partition
    async fn create_shards_for_partition(&self, partition: &mut Partition) -> Result<()> {
        let node_count = partition.assigned_nodes.len();

        if node_count == 0 {
            return Err(DOLDAError::Partition("No nodes assigned to partition".to_string()));
        }

        // Create one primary shard per node
        for (i, &node_id) in partition.assigned_nodes.iter().enumerate() {
            let shard_id = partition.partition_id * 1000 + i as u32;
            let shard = DataShard::new(shard_id, node_id, i == 0); // First shard is primary
            partition.shards.push(Arc::new(RwLock::new(shard)));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_key_hash() {
        let key1 = PartitionKey::string("test");
        let key2 = PartitionKey::integer(42);
        let key3 = PartitionKey::timestamp(1234567890);

        assert_ne!(key1.hash(), key2.hash());
        assert_ne!(key2.hash(), key3.hash());

        // Same key should have same hash
        let key1_copy = PartitionKey::string("test");
        assert_eq!(key1.hash(), key1_copy.hash());
    }

    #[tokio::test]
    async fn test_partition_manager() {
        let cluster = Arc::new(Cluster::default());
        let pm = PartitionManager::new(Arc::clone(&cluster));

        // Create a partition
        let partition_id = pm.create_partition(
            PartitionStrategy::Hash,
            None,
            None,
            3,
        ).await.unwrap();

        // Verify partition was created
        assert!(pm.partitions.contains_key(&partition_id));

        // Test key routing
        let key = PartitionKey::string("test_key");
        let found_partition = pm.get_partition_for_key(&key);
        assert_eq!(found_partition, Some(partition_id));

        let stats = pm.get_stats();
        assert_eq!(stats.total_partitions, 1);
    }

    #[test]
    fn test_data_shard_creation() {
        let shard = DataShard::new(1, 100, true);

        assert_eq!(shard.shard_id, 1);
        assert_eq!(shard.node_id, 100);
        assert!(shard.is_primary);
        assert_eq!(shard.size_bytes, 0);
        assert_eq!(shard.record_count, 0);
    }
}
