# Mailbox Processor Guide - Automatic Message Processing

DOLDA mailboxes now support **automatic message processing** with SQL query capabilities and result forwarding!

---

## 🎯 Concept

**Mailbox Processors** are like **event handlers** or **AWS Lambda triggers** for your mailboxes:
- Automatically process incoming messages
- Run SQL queries over mailbox data
- Forward results to other mailboxes
- Support multiple storage backends (binary, Parquet)
- Build stream processing pipelines

```
┌─────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│   Input Queue   │─────▶│    Processor     │─────▶│  Output Queue    │
│  alice@dolda    │      │  (SQL Query +    │      │   bob@dolda      │
│                 │      │   Transform)     │      │                  │
└─────────────────┘      └──────────────────┘      └──────────────────┘
```

---

## 🚀 Quick Start

### Basic Processor

```rust
use dolda::mailbox_processor::{MailboxProcessor, ProcessorContext, ProcessorResult};
use dolda::mailbox::Message;
use async_trait::async_trait;

struct MyProcessor;

#[async_trait]
impl MailboxProcessor for MyProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        // Process the message
        println!("Received: {:?}", String::from_utf8_lossy(&msg.body));
        
        // Forward to another mailbox
        ctx.send_to("output@dolda", &msg.body).await?;
        
        Ok(())
    }
}
```

### Register Processor

```rust
use dolda::mailbox_processor::{ProcessorManager, ProcessorConfig};
use std::sync::Arc;

// Create processor manager
let manager = ProcessorManager::new(Arc::new(RwLock::new(system)));

// Register processor
manager.register_processor(
    "my-mailbox@dolda",
    Arc::new(MyProcessor),
    ProcessorConfig::default(),
).await?;

// Processor now automatically handles incoming messages!
```

---

## 📨 Built-in Processors

### 1. Echo Processor

Sends received messages back to the sender:

```rust
use dolda::mailbox_processor::EchoProcessor;

manager.register_processor(
    "echo@dolda",
    Arc::new(EchoProcessor),
    ProcessorConfig::default(),
).await?;

// Send message
alice.send_to("echo@dolda", b"Hello!").await?;

// Receive echo
let echo = alice.receive().await?.unwrap();
assert_eq!(echo.body, b"Hello!");
```

### 2. Filter Processor

Filters messages based on a predicate:

```rust
use dolda::mailbox_processor::FilterProcessor;

// Only forward messages containing "urgent"
let processor = FilterProcessor::new(
    Arc::new(|msg: &Message| {
        String::from_utf8_lossy(&msg.body).contains("urgent")
    }),
    "urgent@dolda".to_string(),
);

manager.register_processor(
    "filter@dolda",
    Arc::new(processor),
    ProcessorConfig::default(),
).await?;
```

### 3. Transform Processor

Applies a transformation function:

```rust
use dolda::mailbox_processor::TransformProcessor;

// Uppercase transformer
let processor = TransformProcessor::new(
    Arc::new(|data: &[u8]| {
        String::from_utf8_lossy(data).to_uppercase().into_bytes()
    }),
    "output@dolda".to_string(),
);

manager.register_processor(
    "transform@dolda",
    Arc::new(processor),
    ProcessorConfig::default(),
).await?;
```

### 4. Aggregation Processor

Runs SQL queries and forwards results:

```rust
use dolda::mailbox_processor::AggregationProcessor;

let processor = AggregationProcessor::new(
    "SELECT event_type, COUNT(*) FROM messages GROUP BY event_type".to_string(),
    "results@dolda".to_string(),
);

manager.register_processor(
    "aggregator@dolda",
    Arc::new(processor),
    ProcessorConfig {
        enable_sql: true,  // Enable SQL queries
        ..Default::default()
    },
).await?;
```

### 5. Parquet Export Processor

Exports messages to Parquet files:

```rust
use dolda::mailbox_processor::ParquetExportProcessor;
use std::path::PathBuf;

let processor = ParquetExportProcessor::new(
    PathBuf::from("/data/exports"),
    1000,  // Batch size
);

manager.register_processor(
    "export@dolda",
    Arc::new(processor),
    ProcessorConfig {
        enable_sql: true,
        ..Default::default()
    },
).await?;
```

---

## 🔧 Custom Processors

### Processor Lifecycle

```rust
#[async_trait]
impl MailboxProcessor for MyProcessor {
    /// Called once when processor is registered
    async fn init(&self, ctx: &ProcessorContext) -> ProcessorResult {
        println!("Processor starting for {}", ctx.address);
        Ok(())
    }
    
    /// Called for each incoming message
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        // Your processing logic here
        Ok(())
    }
    
    /// Called when processor is unregistered
    async fn shutdown(&self, ctx: &ProcessorContext) -> ProcessorResult {
        println!("Processor shutting down");
        Ok(())
    }
}
```

### ProcessorContext API

The `ProcessorContext` provides access to mailbox operations:

```rust
// Send message to another mailbox
ctx.send_to("output@dolda", b"data").await?;

// Run SQL query (if SQL enabled)
let results = ctx.query("SELECT * FROM messages WHERE amount > 100").await?;

// Export to Parquet
ctx.export_parquet("SELECT * FROM messages", "/data/export.parquet").await?;

// Get storage backend type
match ctx.storage_backend() {
    StorageBackend::Binary => println!("Using binary storage"),
    StorageBackend::Parquet { path } => println!("Using Parquet: {:?}", path),
}
```

---

## 💡 Common Patterns

### 1. Stream Processing Pipeline

Chain processors to create data pipelines:

```rust
// Input -> Filter -> Transform -> Aggregate -> Output

// Filter processor
manager.register_processor(
    "filter@dolda",
    Arc::new(FilterProcessor::new(
        Arc::new(|msg| is_valid(msg)),
        "transform@dolda".to_string(),
    )),
    ProcessorConfig::default(),
).await?;

// Transform processor
manager.register_processor(
    "transform@dolda",
    Arc::new(TransformProcessor::new(
        Arc::new(|data| normalize(data)),
        "aggregate@dolda".to_string(),
    )),
    ProcessorConfig::default(),
).await?;

// Aggregation processor
manager.register_processor(
    "aggregate@dolda",
    Arc::new(AggregationProcessor::new(
        "SELECT * FROM messages".to_string(),
        "output@dolda".to_string(),
    )),
    ProcessorConfig::default(),
).await?;
```

### 2. Fan-Out Pattern

One message triggers multiple processors:

```rust
struct FanOutProcessor {
    destinations: Vec<String>,
}

#[async_trait]
impl MailboxProcessor for FanOutProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        for dest in &self.destinations {
            ctx.send_to(dest, &msg.body).await?;
        }
        Ok(())
    }
}
```

### 3. Batch Processing

Accumulate messages and process in batches:

```rust
struct BatchProcessor {
    batch: Arc<RwLock<Vec<Message>>>,
    batch_size: usize,
    output: String,
}

#[async_trait]
impl MailboxProcessor for BatchProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        let mut batch = self.batch.write().await;
        batch.push(msg);
        
        if batch.len() >= self.batch_size {
            // Process batch
            let combined = process_batch(&batch);
            ctx.send_to(&self.output, &combined).await?;
            batch.clear();
        }
        
        Ok(())
    }
}
```

### 4. State Machine Processor

Process messages based on state:

```rust
struct StateMachine {
    state: Arc<RwLock<State>>,
}

#[async_trait]
impl MailboxProcessor for StateMachine {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        let mut state = self.state.write().await;
        
        match *state {
            State::Waiting => {
                if is_start_message(&msg) {
                    *state = State::Processing;
                }
            }
            State::Processing => {
                if is_complete_message(&msg) {
                    ctx.send_to("output@dolda", b"Complete!").await?;
                    *state = State::Done;
                }
            }
            State::Done => {}
        }
        
        Ok(())
    }
}
```

### 5. Router Processor

Route messages based on content:

```rust
struct RouterProcessor;

#[async_trait]
impl MailboxProcessor for RouterProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        let text = String::from_utf8_lossy(&msg.body);
        
        let destination = if text.contains("urgent") {
            "urgent@dolda"
        } else if text.contains("info") {
            "info@dolda"
        } else {
            "general@dolda"
        };
        
        ctx.send_to(destination, &msg.body).await?;
        Ok(())
    }
}
```

### 6. Enrichment Processor

Add metadata to messages:

```rust
struct EnrichmentProcessor {
    database: Arc<Database>,
}

#[async_trait]
impl MailboxProcessor for EnrichmentProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        // Look up additional data
        let user_id = extract_user_id(&msg.body);
        let user_data = self.database.get_user(user_id).await?;
        
        // Create enriched message
        let enriched = EnrichedMessage {
            original: msg.body,
            user_data,
        };
        
        ctx.send_to("enriched@dolda", &serialize(&enriched)).await?;
        Ok(())
    }
}
```

---

## ⚙️ Configuration

### ProcessorConfig

```rust
pub struct ProcessorConfig {
    /// Enable SQL query support
    pub enable_sql: bool,
    
    /// Storage backend
    pub storage_backend: StorageBackend,
    
    /// Batch size for processing messages
    pub batch_size: usize,
    
    /// Poll interval in milliseconds
    pub poll_interval_ms: u64,
}
```

### Storage Backends

#### Binary Storage (Default)

```rust
ProcessorConfig {
    storage_backend: StorageBackend::Binary,
    ..Default::default()
}
```

#### Parquet Storage

```rust
use std::path::PathBuf;

ProcessorConfig {
    storage_backend: StorageBackend::Parquet {
        path: PathBuf::from("/data/mailbox.parquet"),
    },
    enable_sql: true,  // SQL queries work great with Parquet
    ..Default::default()
}
```

---

## 📊 SQL Query Support

When SQL is enabled, processors can query mailbox data:

### Basic Queries

```rust
// Count messages
let results = ctx.query("SELECT COUNT(*) FROM messages").await?;

// Filter by field
let results = ctx.query(
    "SELECT * FROM messages WHERE timestamp > 1000000"
).await?;

// Aggregation
let results = ctx.query(
    "SELECT event_type, COUNT(*) as count 
     FROM messages 
     GROUP BY event_type"
).await?;
```

### Time-Series Queries

```rust
// Messages per hour
ctx.query(
    "SELECT 
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as count
     FROM messages
     GROUP BY hour
     ORDER BY hour"
).await?;
```

### Join Queries

```rust
// Join with external Parquet data
ctx.query(
    "SELECT m.*, u.name
     FROM messages m
     JOIN users u ON m.user_id = u.id"
).await?;
```

---

## 🎭 Actor Pattern with Processors

Processors enable the Actor model:

```rust
struct AccountActor {
    balance: Arc<RwLock<f64>>,
}

#[async_trait]
impl MailboxProcessor for AccountActor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        match msg.subject.as_deref() {
            Some("Deposit") => {
                let amount: f64 = String::from_utf8_lossy(&msg.body).parse().unwrap();
                let mut balance = self.balance.write().await;
                *balance += amount;
                
                ctx.send_to(msg.from.as_str(), 
                    format!("Deposited {}. New balance: {}", amount, *balance).as_bytes()
                ).await?;
            }
            Some("Withdraw") => {
                let amount: f64 = String::from_utf8_lossy(&msg.body).parse().unwrap();
                let mut balance = self.balance.write().await;
                
                if *balance >= amount {
                    *balance -= amount;
                    ctx.send_to(msg.from.as_str(),
                        format!("Withdrew {}. New balance: {}", amount, *balance).as_bytes()
                    ).await?;
                } else {
                    ctx.send_to(msg.from.as_str(), b"Insufficient funds").await?;
                }
            }
            Some("GetBalance") => {
                let balance = *self.balance.read().await;
                ctx.send_to(msg.from.as_str(), 
                    format!("{}", balance).as_bytes()
                ).await?;
            }
            _ => {}
        }
        
        Ok(())
    }
}
```

---

## 🔐 Best Practices

### 1. Error Handling

Always handle errors gracefully:

```rust
async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
    match self.process_internal(ctx, msg).await {
        Ok(_) => Ok(()),
        Err(e) => {
            log::error!("Processing error: {}", e);
            // Send to dead letter queue
            ctx.send_to("dead-letter@dolda", &format!("Error: {}", e).as_bytes()).await?;
            Ok(())
        }
    }
}
```

### 2. Idempotency

Make processors idempotent when possible:

```rust
async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
    // Check if message already processed
    if self.processed.contains(&msg.id) {
        return Ok(());
    }
    
    // Process message
    // ...
    
    // Mark as processed
    self.processed.insert(msg.id.clone());
    Ok(())
}
```

### 3. Resource Cleanup

Use shutdown for cleanup:

```rust
async fn shutdown(&self, ctx: &ProcessorContext) -> ProcessorResult {
    // Flush pending data
    self.flush().await?;
    
    // Close connections
    self.database.close().await?;
    
    // Save state
    self.save_checkpoint().await?;
    
    Ok(())
}
```

### 4. Performance

- Use batching for high-throughput scenarios
- Configure appropriate poll intervals
- Enable SQL only when needed
- Use Parquet for analytical workloads

---

## 📈 Performance Characteristics

- **Processing Latency**: Configurable via `poll_interval_ms` (default: 100ms)
- **Throughput**: Inherits PersistQ's 1.76M ops/sec
- **SQL Queries**: Zero-copy reads with DataFusion
- **Parquet**: Columnar storage for analytics

---

## 🚧 Current Status

### ✅ Working Features
- Processor registration and lifecycle
- Automatic message processing
- Built-in processors (Echo, Filter, Transform)
- Custom processor support
- Batch processing
- Resource cleanup

### 🔄 In Progress
- SQL query integration (API ready, implementation pending)
- Parquet storage backend (structure ready)

### 🚀 Future Enhancements
- [ ] Processor metrics and monitoring
- [ ] Backpressure handling
- [ ] Parallel processing
- [ ] Processor composition
- [ ] Dynamic processor hot-reload
- [ ] Processor versioning

---

## 📚 Examples

See `examples/mailbox_processor_example.rs` for comprehensive demos:
- Echo processor
- Statistics processor
- Transform processor (uppercase)
- Filter processor
- Router processor

Run with:
```bash
cargo run --example mailbox_processor_example --release
```

---

## 🧪 Testing

```bash
# Run processor tests
cargo test --release --lib mailbox_processor

# Run specific test
cargo test --release --lib mailbox_processor::tests::test_echo_processor
```

---

## 💡 Use Cases

Perfect for:
- **Stream Processing** - Real-time data pipelines
- **ETL Pipelines** - Extract, transform, load
- **Event-Driven Systems** - React to events
- **Actor Systems** - Stateful message handlers
- **Microservices** - Service orchestration
- **Data Enrichment** - Augment messages with context
- **Message Routing** - Content-based routing
- **Analytics** - SQL queries over message streams

---

**Status**: ✅ **Core Functionality Working**

Build powerful stream processing pipelines with automatic message processors! 🔄📊

