# 🦀 DOLDA - Distributed Object Layer for Data Access

DOLDA is a **high-performance distributed persistent queue** built in Rust, featuring zero-copy operations, Raft consensus, and production-grade reliability.

---

## 🚀 Quick Start

```bash
# Build the project
cd dolda
cargo build --release

# Run demo mode
cargo run -- --demo

# Run examples
cargo run --example mailbox_example --release
cargo run --example mailbox_processor_example --release
```

---

## 🌟 Key Features

- **📬 Mailbox System** - Actor-like inter-queue messaging (like email addresses)
- **🔄 Message Processors** - Automatic processing with SQL queries
- **🌐 Network Access** - TCP/UDP remote mailbox access
- **💾 Persistent Storage** - Zero-copy memory-mapped segments
- **🔍 SQL Queries** - Apache Arrow DataFusion integration
- **⚡ High Performance** - 1.76M ops/sec, 2.66 GB/s throughput
- **🔐 Raft Consensus** - Distributed coordination and replication
- **📊 Observability** - Prometheus metrics and tracing
- **🐳 Docker/K8s** - Production deployment configs

---

## 📚 Documentation

All documentation is in the [`docs/`](docs/) directory:

### Getting Started
- **[README](docs/README.md)** - Architecture and features overview
- **[Quick Start Guide](docs/MAILBOX_GUIDE.md)** - Basic mailbox usage
- **[Examples](examples/)** - Working code examples

### Core Features
- **[Mailbox System](docs/MAILBOX_GUIDE.md)** - Actor-like messaging between queues
- **[Mailbox Processors](docs/MAILBOX_PROCESSOR_GUIDE.md)** - Automatic message processing
- **[DataFusion SQL](docs/DATAFUSION_GUIDE.md)** - SQL queries over queue data

### Deployment
- **[Cluster Setup](docs/CLUSTER_SETUP_GUIDE.md)** - Multi-node deployment
- **[Docker & Kubernetes](docs/DEPLOYMENT.md)** - Container orchestration
- **[Production Roadmap](docs/PRODUCTION_ROADMAP.md)** - Production features

### Development
- **[Code Quality Report](docs/CODE_QUALITY_REPORT.md)** - Quality improvements
- **[Stress Testing](docs/STRESS_TESTING_GUIDE.md)** - Performance testing

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    DOLDA System                          │
│                                                          │
│  ┌─────────────┐      ┌─────────────┐      ┌──────────┐│
│  │  Mailbox A  │─────▶│  Mailbox B  │─────▶│ Mailbox C││
│  │ (PersistQ)  │◀─────│ (PersistQ)  │◀─────│(PersistQ)││
│  └─────────────┘      └─────────────┘      └──────────┘│
│         │                    │                    │     │
│         └────────────────────┴────────────────────┘     │
│                  Message Router                         │
│                                                          │
│  Features:                                               │
│  • Actor-like messaging                                  │
│  • SQL queries (DataFusion)                              │
│  • Automatic processors                                  │
│  • TCP/UDP network access                                │
│  • Raft consensus                                        │
└──────────────────────────────────────────────────────────┘
```

---

## 💡 Use Cases

- **Microservices Communication** - Service-to-service messaging
- **Actor Systems** - Erlang/Akka-style actors in Rust
- **Stream Processing** - Real-time data pipelines
- **Event Sourcing** - Event distribution and replay
- **Task Queues** - Background job processing
- **Workflow Engines** - Step coordination

---

## 🧪 Testing

```bash
# Run all tests
cargo test --release

# Run specific module tests
cargo test --release --lib mailbox
cargo test --release --lib mailbox_processor

# Run benchmarks
cargo bench
```

---

## 📈 Performance

- **Throughput**: 1.76M operations/sec
- **Bandwidth**: 2.66 GB/s
- **Latency**: ~microseconds (memory-mapped I/O)
- **Compression**: 80-85% (CRC32 checksums)

---

## 🔧 Configuration

Create `dolda.toml`:

```toml
[cluster]
expected_nodes = 3
heartbeat_interval = 5000

[storage]
segment_size = 67108864  # 64MB
base_dir = "/data/dolda"
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

---

## 📄 License

Licensed under MIT OR Apache-2.0.

---

## 🙏 Acknowledgments

- Built with Rust's excellent async ecosystem
- Apache Arrow DataFusion for SQL capabilities
- Tokio for async runtime
- Thanks to the Rust community

---

**DOLDA**: Bringing enterprise-grade distributed database concepts to Rust with memory safety and performance! 🦀⚡

