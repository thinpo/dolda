# DOLDA Documentation

Complete documentation for the DOLDA distributed persistent queue system.

---

## 📚 Table of Contents

### Getting Started
- **[Main README](../README.md)** - Project overview and quick start
- **[Mailbox Guide](MAILBOX_GUIDE.md)** - Basic mailbox usage and patterns
- **[Mailbox Processor Guide](MAILBOX_PROCESSOR_GUIDE.md)** - Automatic message processing

### Core Features
- **[Mailbox System](MAILBOX_GUIDE.md)** - Actor-like inter-queue messaging
- **[Mailbox Processors](MAILBOX_PROCESSOR_GUIDE.md)** - SQL queries and transformations
- **[Network Access](MAILBOX_NETWORK_GUIDE.md)** - TCP/UDP remote mailboxes
- **[DataFusion SQL](DATAFUSION_GUIDE.md)** - SQL queries over queue data
- **[Mailbox Summary](MAILBOX_SUMMARY.md)** - Quick reference

### Deployment & Operations
- **[Cluster Setup](CLUSTER_SETUP_GUIDE.md)** - Multi-node deployment
- **[Docker & Kubernetes](DEPLOYMENT.md)** - Container orchestration
- **[Docker/K8s Summary](DOCKER_K8S_SUMMARY.md)** - Quick deployment reference
- **[Production Roadmap](PRODUCTION_ROADMAP.md)** - Production features and timeline
- **[Production Status](PRODUCTION_STATUS.md)** - Current production readiness

### Testing & Quality
- **[Stress Testing Guide](STRESS_TESTING_GUIDE.md)** - Performance testing
- **[Stress Test Results](STRESS_TEST_RESULTS.md)** - Performance metrics
- **[Code Quality Report](CODE_QUALITY_REPORT.md)** - Quality improvements
- **[Code Quality Improvements](CODE_QUALITY_IMPROVEMENTS.md)** - Quality checklist

### Reference
- **[GitHub Ready](GITHUB_READY.md)** - Open source preparation

---

## 🎯 Quick Navigation

### I want to...

**Learn the basics**
→ Start with [Mailbox Guide](MAILBOX_GUIDE.md)

**Add automatic processing**
→ See [Mailbox Processor Guide](MAILBOX_PROCESSOR_GUIDE.md)

**Enable remote access**
→ Read [Network Access Guide](MAILBOX_NETWORK_GUIDE.md)

**Run SQL queries**
→ Check [DataFusion SQL Guide](DATAFUSION_GUIDE.md)

**Deploy to production**
→ Follow [Cluster Setup](CLUSTER_SETUP_GUIDE.md) and [Deployment Guide](DEPLOYMENT.md)

**Run performance tests**
→ Use [Stress Testing Guide](STRESS_TESTING_GUIDE.md)

**Understand architecture**
→ Read [Production Status](PRODUCTION_STATUS.md)

---

## 📖 Documentation Structure

```
docs/
├── README.md                    # This file
├── MAILBOX_GUIDE.md            # Core mailbox concepts
├── MAILBOX_PROCESSOR_GUIDE.md  # Automatic processing
├── MAILBOX_NETWORK_GUIDE.md    # TCP/UDP network access
├── MAILBOX_SUMMARY.md          # Quick reference
├── DATAFUSION_GUIDE.md         # SQL queries
├── CLUSTER_SETUP_GUIDE.md      # Multi-node deployment
├── DEPLOYMENT.md               # Docker & Kubernetes
├── DOCKER_K8S_SUMMARY.md       # Deployment reference
├── PRODUCTION_ROADMAP.md       # Feature timeline
├── PRODUCTION_STATUS.md        # Readiness assessment
├── STRESS_TESTING_GUIDE.md     # Performance testing
├── STRESS_TEST_RESULTS.md      # Benchmark results
├── CODE_QUALITY_REPORT.md      # Quality metrics
├── CODE_QUALITY_IMPROVEMENTS.md# Quality checklist
└── GITHUB_READY.md             # Open source prep
```

---

## 🌟 Key Concepts

### Mailboxes
Each mailbox is like an email address - it can send and receive messages from other mailboxes. Mailboxes are backed by persistent queues with zero-copy operations.

### Processors
Automatic message handlers that can run SQL queries, transform data, filter messages, and forward results to other mailboxes.

### Network Access
Mailboxes can be accessed remotely via TCP (reliable) or UDP (low latency), enabling distributed microservices communication.

### SQL Queries
Apache Arrow DataFusion integration allows SQL queries over mailbox data for analytics and aggregations.

### Production Features
- Zero-copy memory-mapped I/O
- Raft consensus for coordination
- Segment-based storage with compression
- Observability with Prometheus metrics
- Docker and Kubernetes support

---

## 🚀 Quick Start

```bash
# Clone and build
cd dolda
cargo build --release

# Run mailbox example
cargo run --example mailbox_example --release

# Run network example
cargo run --example mailbox_network_example --release

# Run all tests
cargo test --release
```

---

## 📊 Feature Matrix

| Feature | Status | Documentation |
|---------|--------|---------------|
| Mailbox System | ✅ Ready | [MAILBOX_GUIDE.md](MAILBOX_GUIDE.md) |
| Message Processors | ✅ Ready | [MAILBOX_PROCESSOR_GUIDE.md](MAILBOX_PROCESSOR_GUIDE.md) |
| Network Access (TCP/UDP) | ✅ Ready | [MAILBOX_NETWORK_GUIDE.md](MAILBOX_NETWORK_GUIDE.md) |
| DataFusion SQL | 🔄 In Progress | [DATAFUSION_GUIDE.md](DATAFUSION_GUIDE.md) |
| Persistent Storage | ✅ Ready | [PRODUCTION_STATUS.md](PRODUCTION_STATUS.md) |
| Raft Consensus | 🔄 In Progress | [CLUSTER_SETUP_GUIDE.md](CLUSTER_SETUP_GUIDE.md) |
| Docker/K8s | ✅ Ready | [DEPLOYMENT.md](DEPLOYMENT.md) |
| Observability | ✅ Ready | [PRODUCTION_STATUS.md](PRODUCTION_STATUS.md) |

---

## 💡 Use Cases

- **Microservices Communication** - Service-to-service messaging
- **Actor Systems** - Erlang/Akka-style actors in Rust
- **Stream Processing** - Real-time data pipelines
- **Event Sourcing** - Event distribution and replay
- **Task Queues** - Background job processing
- **IoT Communication** - Device-to-cloud messaging
- **Distributed Analytics** - SQL queries over distributed data

---

## 🤝 Contributing

See the [main README](../README.md) for contribution guidelines.

---

## 📄 License

Licensed under MIT OR Apache-2.0.

---

**Last Updated:** 2025-11-11
