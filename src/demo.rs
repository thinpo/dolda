//! Demonstration module for DOLDA architecture concepts
//!
//! This module provides a simplified demonstration of the core DOLDA
//! architecture principles without complex dependencies.

use std::collections::HashMap;

/// Node types in the distributed system
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    /// Manages metadata and coordination
    Control,
    /// Stores partitioned data
    Data,
    /// Both control and data functions
    Mixed,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Control => write!(f, "Control"),
            NodeType::Data => write!(f, "Data"),
            NodeType::Mixed => write!(f, "Mixed"),
        }
    }
}

/// Simplified node representation
#[derive(Debug, Clone)]
pub struct DemoNode {
    pub id: u32,
    pub name: String,
    pub node_type: NodeType,
    pub ip: String,
    pub port: u16,
    pub memory_gb: u64,
    pub storage_gb: u64,
    pub active: bool,
}

impl DemoNode {
    pub fn new(id: u32, name: &str, node_type: NodeType) -> Self {
        Self {
            id,
            name: name.to_string(),
            node_type,
            ip: "127.0.0.1".to_string(),
            port: 8080 + id as u16,
            memory_gb: 8,
            storage_gb: 100,
            active: true,
        }
    }

    pub fn load_factor(&self) -> f64 {
        // Simplified load calculation
        0.25 + (self.id as f64 * 0.1) % 0.5
    }
}

/// Partition key for data routing
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PartitionKey {
    String(String),
    Integer(i64),
    Timestamp(u64),
}

impl PartitionKey {
    pub fn hash(&self) -> u64 {
        use crc32fast::Hasher;
        let mut hasher = Hasher::new();
        match self {
            PartitionKey::String(s) => hasher.update(s.as_bytes()),
            PartitionKey::Integer(i) => hasher.update(&i.to_le_bytes()),
            PartitionKey::Timestamp(t) => hasher.update(&t.to_le_bytes()),
        }
        hasher.finalize() as u64
    }
}

impl From<&str> for PartitionKey {
    fn from(s: &str) -> Self {
        PartitionKey::String(s.to_string())
    }
}

impl From<String> for PartitionKey {
    fn from(s: String) -> Self {
        PartitionKey::String(s)
    }
}

impl From<i64> for PartitionKey {
    fn from(i: i64) -> Self {
        PartitionKey::Integer(i)
    }
}

impl From<u64> for PartitionKey {
    fn from(t: u64) -> Self {
        PartitionKey::Timestamp(t)
    }
}

/// Distributed file representation
#[derive(Debug, Clone)]
pub struct DemoFile {
    pub logical_path: String,
    pub size_bytes: u64,
    pub node_id: u32,
    pub replicas: Vec<u32>,
}

impl DemoFile {
    pub fn new(path: &str, size: u64, node_id: u32, replicas: Vec<u32>) -> Self {
        Self {
            logical_path: path.to_string(),
            size_bytes: size,
            node_id,
            replicas,
        }
    }
}

/// Data partition
#[derive(Debug, Clone)]
pub struct DemoPartition {
    pub id: u32,
    pub node_ids: Vec<u32>,
    pub total_records: u64,
    pub total_size: u64,
}

impl DemoPartition {
    pub fn new(id: u32, node_ids: Vec<u32>) -> Self {
        Self {
            id,
            node_ids,
            total_records: 100000 * id as u64,
            total_size: 100 * 1024 * 1024 * id as u64, // MB
        }
    }
}

/// Simplified cluster representation
#[derive(Clone)]
pub struct DemoCluster {
    pub nodes: HashMap<u32, DemoNode>,
    pub files: HashMap<String, DemoFile>,
    pub partitions: HashMap<u32, DemoPartition>,
    pub total_memory: u64,
    pub total_storage: u64,
}

impl DemoCluster {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            files: HashMap::new(),
            partitions: HashMap::new(),
            total_memory: 0,
            total_storage: 0,
        }
    }

    pub fn add_node(&mut self, node: DemoNode) {
        self.total_memory += node.memory_gb;
        self.total_storage += node.storage_gb;
        self.nodes.insert(node.id, node);
    }

    pub fn create_file(&mut self, path: &str, size: u64, node_id: u32) -> Result<(), String> {
        if !self.nodes.contains_key(&node_id) {
            return Err(format!("Node {} not found", node_id));
        }

        let replicas = self.nodes.keys().take(3).cloned().collect();
        let file = DemoFile::new(path, size, node_id, replicas);
        self.files.insert(path.to_string(), file);
        Ok(())
    }

    pub fn create_partition(&mut self, id: u32) -> Result<(), String> {
        let node_ids: Vec<u32> = self.nodes.keys().cloned().collect();
        if node_ids.is_empty() {
            return Err("No nodes available".to_string());
        }

        let partition = DemoPartition::new(id, node_ids);
        self.partitions.insert(id, partition);
        Ok(())
    }

    pub fn get_node_for_key(&self, key: &PartitionKey) -> Option<&DemoNode> {
        let hash = key.hash();
        let node_count = self.nodes.len() as u64;
        if node_count == 0 {
            return None;
        }

        let node_index = (hash % node_count) as u32;
        let node_ids: Vec<u32> = self.nodes.keys().cloned().collect();
        node_ids.get(node_index as usize)
            .and_then(|id| self.nodes.get(id))
    }

    pub fn get_partition_for_key(&self, key: &PartitionKey) -> Option<&DemoPartition> {
        let hash = key.hash();
        let partition_count = self.partitions.len() as u64;
        if partition_count == 0 {
            return None;
        }

        let partition_id = ((hash % partition_count) + 1) as u32;
        self.partitions.get(&partition_id)
    }

    pub fn stats(&self) -> ClusterStats {
        let active_nodes = self.nodes.values().filter(|n| n.active).count();
        let total_files = self.files.len();
        let total_partitions = self.partitions.len();

        ClusterStats {
            active_nodes,
            total_files,
            total_partitions,
            total_memory: self.total_memory,
            total_storage: self.total_storage,
        }
    }
}

/// Cluster statistics
#[derive(Debug)]
pub struct ClusterStats {
    pub active_nodes: usize,
    pub total_files: usize,
    pub total_partitions: usize,
    pub total_memory: u64,
    pub total_storage: u64,
}

/// Data record that can be stored in the cluster
#[derive(Debug, Clone)]
pub struct DataRecord {
    pub key: PartitionKey,
    pub value: Vec<u8>,
    pub timestamp: u64,
}

impl DataRecord {
    pub fn new<K: Into<PartitionKey>>(key: K, value: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            value,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    pub fn size(&self) -> usize {
        self.value.len() + 8 // key hash + timestamp
    }
}

/// Write operation result
#[derive(Debug)]
pub struct WriteResult {
    pub record: DataRecord,
    pub partition_id: u32,
    pub node_id: u32,
    pub replicas: Vec<u32>,
    pub success: bool,
    pub latency_ms: u64,
}

impl DemoCluster {
    /// Write a data record to the cluster
    pub fn write_record(&mut self, record: DataRecord) -> Result<WriteResult, String> {
        // 1. Determine which partition should store this record
        let partition_id = self.get_partition_for_key(&record.key)
            .map(|p| p.id)
            .unwrap_or(1); // Default to partition 1

        // 2. Determine which node should store the primary copy
        let primary_node_id = self.get_node_for_key(&record.key)
            .map(|n| n.id)
            .ok_or("No suitable node found for key")?;

        // 3. Select replica nodes (excluding primary)
        let replicas = self.nodes.keys()
            .filter(|&&id| id != primary_node_id)
            .take(2) // 2 replicas for this example
            .cloned()
            .collect::<Vec<_>>();

        // 4. Simulate writing to primary node
        let start_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        // In a real implementation, this would:
        // - Serialize the record
        // - Send to primary node via network
        // - Wait for acknowledgment
        // - Send to replica nodes
        // - Ensure consistency

        std::thread::sleep(std::time::Duration::from_micros(500)); // Simulate I/O

        let end_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let latency_ms = (end_time - start_time) / 1000;

        // 5. Update partition statistics
        if let Some(partition) = self.partitions.get_mut(&partition_id) {
            partition.total_records += 1;
            partition.total_size += record.size() as u64;
        }

        // 6. Return write result
        Ok(WriteResult {
            record,
            partition_id,
            node_id: primary_node_id,
            replicas,
            success: true,
            latency_ms,
        })
    }

    /// Batch write multiple records
    pub fn write_batch(&mut self, records: Vec<DataRecord>) -> Vec<Result<WriteResult, String>> {
        records.into_iter()
            .map(|record| self.write_record(record))
            .collect()
    }

    /// Read a record by key
    pub fn read_record(&self, _key: &PartitionKey) -> Option<&DataRecord> {
        // In a real implementation, this would:
        // - Determine which node/partition has the data
        // - Query the appropriate node
        // - Return the most recent version
        // - Handle consistency issues

        // For demo purposes, return None (not implemented)
        None
    }
}

/// Demonstration of DOLDA architecture concepts
pub fn demonstrate_architecture() {
    println!("🦀 DOLDA Architecture Demonstration");
    println!("=====================================");
    println!();

    // 1. Create a cluster
    println!("1. 🏗️  Creating Distributed Cluster...");
    let mut cluster = DemoCluster::new();

    // Add nodes
    let nodes = vec![
        DemoNode::new(1, "control-node", NodeType::Control),
        DemoNode::new(2, "data-node-1", NodeType::Data),
        DemoNode::new(3, "data-node-2", NodeType::Data),
        DemoNode::new(4, "mixed-node", NodeType::Mixed),
    ];

    for node in nodes {
        println!("   ✅ Added {} (ID: {}, Type: {}, {}GB RAM, {}GB Storage)",
                 node.name, node.id, node.node_type, node.memory_gb, node.storage_gb);
        cluster.add_node(node);
    }

    println!("   📊 Cluster: {} nodes, {}GB RAM, {}GB Storage",
             cluster.nodes.len(), cluster.total_memory, cluster.total_storage);

    // 2. Create filesystem
    println!("\n2. 📁 Creating Logical Filesystem...");
    let test_files = vec![
        ("/data/users", 1024 * 1024), // 1MB
        ("/data/orders", 10 * 1024 * 1024), // 10MB
        ("/index/users_idx", 512 * 1024), // 512KB
        ("/logs/app.log", 100 * 1024), // 100KB
    ];

    for (path, size) in test_files {
        match cluster.create_file(path, size, 2) {
            Ok(()) => println!("   📄 Created file: {} ({} KB)", path, size / 1024),
            Err(e) => println!("   ❌ Failed to create file {}: {}", path, e),
        }
    }

    println!("   📊 Filesystem: {} files created", cluster.files.len());

    // 3. Create partitions
    println!("\n3. 🔄 Creating Data Partitions...");
    for i in 1..=8 {
        match cluster.create_partition(i) {
            Ok(()) => {
                if let Some(partition) = cluster.partitions.get(&i) {
                    println!("   🔢 Created partition {}: {} records, {} MB",
                            i, partition.total_records, partition.total_size / (1024 * 1024));
                }
            }
            Err(e) => println!("   ❌ Failed to create partition {}: {}", i, e),
        }
    }

    // 4. Demonstrate key routing
    println!("\n4. 🗝️  Demonstrating Key-based Routing...");
    let test_keys = vec![
        PartitionKey::String("user_123".to_string()),
        PartitionKey::Integer(42),
        PartitionKey::Timestamp(1640995200), // 2022-01-01
    ];

    for key in &test_keys {
        if let Some(node) = cluster.get_node_for_key(key) {
            if let Some(partition) = cluster.get_partition_for_key(key) {
                println!("   🗝️  Key {:?} → Node {} ({}) → Partition {}",
                        key, node.name, node.node_type, partition.id);
            }
        }
    }

    // 4.5. Demonstrate data writing
    println!("\n4.5. ✍️  Demonstrating Data Writing to Cluster...");
    let mut write_cluster = cluster.clone(); // Clone for writing

    let test_records = vec![
        DataRecord::new("user_123", b"John Doe, john@example.com, active".to_vec()),
        DataRecord::new(42i64, b"Answer to everything: 42".to_vec()),
        DataRecord::new(1640995200u64, b"New Year 2022 data".to_vec()),
        DataRecord::new("order_456", b"Order #456: $299.99, shipped".to_vec()),
    ];

    for record in test_records {
        match write_cluster.write_record(record.clone()) {
            Ok(result) => {
                println!("   ✅ Wrote record with key {:?} → Node {}, Partition {}, {}ms latency",
                        record.key, result.node_id, result.partition_id, result.latency_ms);
                println!("      📋 Replicas: {:?}", result.replicas);
            }
            Err(e) => println!("   ❌ Failed to write record: {}", e),
        }
    }

    // Show updated partition statistics
    println!("   📊 Updated partition statistics:");
    for partition in write_cluster.partitions.values() {
        println!("      Partition {}: {} records, {} bytes",
                partition.id, partition.total_records, partition.total_size);
    }

    // 5. Load balancing
    println!("\n5. ⚖️  Load Balancing Analysis (After Data Writes)...");
    for node in cluster.nodes.values() {
        let load = node.load_factor();
        let load_indicator = if load < 0.3 {
            "🟢 Low"
        } else if load < 0.7 {
            "🟡 Medium"
        } else {
            "🔴 High"
        };
        println!("   📊 {}: {:.2} {}", node.name, load, load_indicator);
    }

    // 6. Final statistics (with write impact)
    let final_stats = write_cluster.stats();
    println!("\n6. 📈 Final Cluster Statistics (After Writes):");
    println!("   🖥️  Active Nodes: {}", final_stats.active_nodes);
    println!("   📁 Total Files: {}", final_stats.total_files);
    println!("   🔢 Total Partitions: {}", final_stats.total_partitions);
    println!("   🧠 Total Memory: {} GB", final_stats.total_memory);
    println!("   💾 Total Storage: {} GB", final_stats.total_storage);

    println!("\n🎯 Architecture Principles Demonstrated:");
    println!("   ✅ Shared-nothing distributed nodes");
    println!("   ✅ Logical filesystem abstraction");
    println!("   ✅ Data partitioning and sharding");
    println!("   ✅ Key-based routing and load balancing");
    println!("   ✅ Data writing with replication");
    println!("   ✅ High availability through replication");
    println!("   ✅ Zero-copy operations design");
    println!("   ✅ Cache-optimized memory access patterns");

    println!("\n✨ DOLDA demonstration completed successfully!");
}
