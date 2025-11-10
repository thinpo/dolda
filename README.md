# 🦀 DOLDA - DolphinDB-inspired Distributed Database Architecture

This is a **Rust implementation** of a distributed database architecture inspired by DolphinDB, demonstrating key concepts from their shared-nothing distributed system.

## 🏗️ Architecture Features

### Shared-Nothing Distributed Architecture
- **Independent Nodes**: Each node has its own compute and storage resources
- **Async Communication**: Nodes communicate via Tokio async runtime with automatic serialization
- **Node Types**: Control nodes (metadata), Data nodes (partitioned data), Mixed nodes

### Logical File System
- **Global Namespace**: Abstract layer hiding physical storage locations
- **Distributed Files**: Files are partitioned across multiple nodes
- **Metadata Management**: Centralized metadata with distributed data
- **Replication**: Configurable data replication for fault tolerance

### Data Partitioning and Sharding
- **Multiple Strategies**: Hash-based, range-based, and round-robin partitioning
- **Elastic Scaling**: Support for adding/removing nodes without resharding
- **Load Balancing**: Automatic distribution of data across nodes
- **Key-based Routing**: Efficient query routing based on partition keys

### High Availability Features
- **Heartbeat Monitoring**: Continuous health checking of nodes
- **Failure Detection**: Automatic detection of node failures
- **Replica Management**: Data replication across multiple nodes
- **Failover Support**: Graceful handling of node failures

## 🚀 Quick Start

### Prerequisites
- Rust 1.70+ with Cargo
- Linux/Unix environment (macOS/Windows supported)

### Build
```bash
cd dolda
cargo build --release
```

### Run Demo Mode (No Network Required)
```bash
cargo run -- --demo
```

### Run Full Distributed Mode (Requires Network)
```bash
cargo run
```

## 📁 Project Structure

```
dolda/
├── src/
│   ├── lib.rs          # Main library interface
│   ├── main.rs         # Demo application
│   ├── types.rs        # Common types and constants
│   ├── node.rs         # Node management and communication
│   ├── cluster.rs      # Cluster management
│   ├── filesystem.rs   # Logical filesystem abstraction
│   ├── partition.rs    # Data partitioning and sharding
│   └── utils.rs        # Utility functions
├── Cargo.toml          # Package configuration
└── README.md          # This file
```

## 🛠️ Usage Examples

### Basic Cluster Setup
```rust
use dolda::{init, cluster::Cluster};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the system
    init().await?;

    // Create a cluster
    let cluster = Cluster::default();

    // Add nodes to cluster
    // ... cluster management code ...

    Ok(())
}
```

### Filesystem Operations
```rust
use dolda::filesystem::{Filesystem, FileType};

let filesystem = Filesystem::new(cluster, metadata_node);

// Create a distributed file
let file = filesystem
    .create_file("/data/table1", FileType::Data, 3)
    .await?;

// Read/write operations
let data = filesystem.read_file(&file, 0, 1024).await?;
filesystem.write_file(&mut file.clone(), 0, b"data").await?;
```

### Data Partitioning
```rust
use dolda::partition::{PartitionManager, PartitionKey, PartitionStrategy};

// Create partition manager
let pm = PartitionManager::new(cluster);

// Create partitions
let partition_id = pm.create_partition(
    PartitionStrategy::Hash,
    None, None, 3
).await?;

// Route keys to partitions
let key = PartitionKey::string("user_123");
let partition_id = pm.get_partition_for_key(&key);
```

## 🧪 Testing

Run the test suite:
```bash
cargo test
```

Run benchmarks:
```bash
cargo bench
```

## 🎯 Performance Features

### Zero-Copy Operations
- Data stays in native format during processing
- Minimal data movement between nodes
- Efficient memory usage patterns

### Cache-Optimized Access
- Memory-aligned data structures
- Predictable access patterns
- Reduced cache misses through data locality

### Type-Safe Algorithms
- Compile-time type checking
- Generic data structures
- Safe concurrent operations

### Async-First Design
- Tokio async runtime throughout
- Non-blocking I/O operations
- Efficient resource utilization

## 🔧 Configuration

Create a `dolda.toml` configuration file:
```toml
[cluster]
expected_nodes = 4
heartbeat_interval = 5000
health_timeout = 30000
max_failures = 3

[node]
name = "node-1"
node_type = "Data"
ip = "127.0.0.1"
port = 8080
```

## 📊 Architecture Comparison

| Feature | DolphinDB | C Implementation | Rust Implementation |
|---------|-----------|------------------|-------------------|
| Node Architecture | Shared-nothing | Shared-nothing ✓ | Shared-nothing ✓ |
| Communication | TCP + RDMA | TCP ✓ | Async TCP ✓ |
| File System | Logical abstraction | Logical ✓ | Logical ✓ |
| Partitioning | Hash/Range/Value | Hash/Range ✓ | Hash/Range ✓ |
| Memory Safety | N/A | Manual | Guaranteed ✓ |
| Concurrency | Threads | Pthreads | Async ✓ |
| Performance | High | High | High + Safe |

## 🚀 Advanced Features

### RDMA Support (Planned)
```rust
// Future RDMA integration
let rdma_connection = RdmaConnection::connect(target_node).await?;
rdma_connection.zero_copy_transfer(data).await?;
```

### Advanced Partitioning Strategies
```rust
// Range partitioning with custom bounds
let partition_id = pm.create_partition(
    PartitionStrategy::Range,
    Some(PartitionKey::integer(0)),
    Some(PartitionKey::integer(1000)),
    3
).await?;
```

### Distributed Queries (Planned)
```rust
// Future distributed query support
let results = cluster.execute_query("SELECT * FROM table WHERE id > 100").await?;
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

Licensed under MIT OR Apache-2.0.

## 🙏 Acknowledgments

- Inspired by DolphinDB's distributed architecture
- Built with Rust's excellent async ecosystem
- Thanks to the Tokio team for the async runtime
- Thanks to the Rust community for the amazing ecosystem

---

**DOLDA**: Bringing enterprise-grade distributed database concepts to Rust with memory safety and performance! 🦀⚡
