# Mailbox System Guide - Actor-like Messaging

DOLDA now includes a mailbox system where each queue acts like an email address, and mailboxes can send messages to each other!

---

## 🎯 Concept

Think of each persistent queue as an **email mailbox**:
- Each mailbox has an **address** (like `alice@dolda`)
- Mailboxes can **send messages** to other mailboxes
- Messages are **persistent** and **reliable**
- Perfect for **actor model** and **microservices** architectures

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  alice@dolda│─────▶│  bob@dolda  │─────▶│charlie@dolda│
│             │◀─────│             │◀─────│             │
└─────────────┘      └─────────────┘      └─────────────┘
```

---

## 🚀 Quick Start

### Basic Example

```rust
use dolda::mailbox::MailboxSystem;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create mailbox system
    let mut system = MailboxSystem::new("/data".to_string()).await?;
    
    // 2. Create mailboxes (like email addresses)
    let alice = system.create_mailbox("alice@dolda").await?;
    let bob = system.create_mailbox("bob@dolda").await?;
    
    // 3. Alice sends to Bob
    alice.send_to("bob@dolda", b"Hello Bob!").await?;
    
    // 4. Bob receives
    let msg = bob.receive().await?.unwrap();
    println!("From: {}, Body: {:?}", msg.from, String::from_utf8_lossy(&msg.body));
    
    Ok(())
}
```

---

## 📨 Sending Messages

### Simple Send

```rust
// Send raw bytes
alice.send_to("bob@dolda", b"Hello!").await?;

// Send JSON
let data = serde_json::to_vec(&my_struct)?;
alice.send_to("bob@dolda", &data).await?;
```

### Send with Subject

```rust
alice.send_with_subject(
    "bob@dolda",
    "Meeting Request",
    b"Can we meet tomorrow?"
).await?;
```

### Reply to Message

```rust
// Bob receives a message
let msg = bob.receive().await?.unwrap();

// Bob replies
bob.reply(&msg, b"Sure, sounds good!").await?;
```

---

## 📬 Receiving Messages

### Basic Receive

```rust
// Receive next message (consumes it)
if let Some(msg) = mailbox.receive().await? {
    println!("From: {}", msg.from);
    println!("To: {}", msg.to);
    println!("Body: {:?}", msg.body);
    println!("Timestamp: {}", msg.timestamp);
    println!("ID: {}", msg.id);
}
```

### Peek Without Consuming

```rust
// Look at next message without removing it
if let Some(msg) = mailbox.peek().await? {
    println!("Next message from: {}", msg.from);
}

// Message is still in queue
let msg = mailbox.receive().await?; // Same message
```

### Check Pending Count

```rust
let count = mailbox.pending_count().await;
println!("Pending messages: {}", count);
```

---

## 💡 Common Patterns

### Request-Response Pattern

```rust
// Alice sends request
alice.send_with_subject("bob@dolda", "Get User", b"user_id:123").await?;

// Bob processes and replies
let request = bob.receive().await?.unwrap();
let user_data = fetch_user(&request.body)?;
bob.reply(&request, &user_data).await?;

// Alice receives response
let response = alice.receive().await?.unwrap();
```

### Broadcast Pattern

```rust
// Send to multiple mailboxes
let message = b"System maintenance at 2pm";
alice.send_to("bob@dolda", message).await?;
alice.send_to("charlie@dolda", message).await?;
alice.send_to("david@dolda", message).await?;
```

### Worker Queue Pattern

```rust
// Multiple workers receive from same mailbox
let worker1 = system.get_mailbox("tasks@dolda").await?.unwrap();
let worker2 = system.get_mailbox("tasks@dolda").await?.unwrap();

// Each worker processes messages
tokio::spawn(async move {
    while let Some(task) = worker1.receive().await.unwrap() {
        process_task(task).await;
    }
});

tokio::spawn(async move {
    while let Some(task) = worker2.receive().await.unwrap() {
        process_task(task).await;
    }
});
```

### Event Sourcing Pattern

```rust
// Event log mailbox
let events = system.create_mailbox("events@dolda").await?;

// Publish events
events.send_to("events@dolda", b"UserCreated:alice").await?;
events.send_to("events@dolda", b"UserLoggedIn:alice").await?;

// Event consumer
while let Some(event) = events.receive().await? {
    handle_event(event);
}
```

---

## 🔧 Message Structure

### Message Fields

```rust
pub struct Message {
    pub from: MailboxAddress,      // Sender
    pub to: MailboxAddress,        // Recipient
    pub id: String,                // Unique message ID (UUID)
    pub timestamp: u64,            // Microseconds since epoch
    pub body: Vec<u8>,             // Message content
    pub subject: Option<String>,   // Optional subject line
    pub reply_to: Option<MailboxAddress>, // Optional reply-to
}
```

### Creating Custom Messages

```rust
let message = Message::new(
    MailboxAddress::new("alice@dolda")?,
    MailboxAddress::new("bob@dolda")?,
    b"Hello".to_vec(),
)
.with_subject("Greeting".to_string())
.with_reply_to(MailboxAddress::new("alice@dolda")?);
```

---

## 🏗️ System Management

### List All Mailboxes

```rust
let mailboxes = system.list_mailboxes().await;
for addr in mailboxes {
    println!("Mailbox: {}", addr);
}
```

### Get Existing Mailbox

```rust
if let Some(mailbox) = system.get_mailbox("alice@dolda").await? {
    println!("Found mailbox: {}", mailbox.address());
}
```

### Get Statistics

```rust
let stats = system.stats().await;
for (addr, stat) in stats {
    println!("{}: {} total messages", addr, stat.total_messages);
}
```

---

## 📐 Architecture

### How It Works

1. **Mailbox System** - Manages all mailboxes and routes messages
2. **Mailbox** - Each mailbox is backed by a `PersistQ` queue
3. **Messages** - Serialized as JSON with envelope (from, to, id, etc.)
4. **Routing** - System routes messages to destination mailboxes
5. **Persistence** - All messages stored durably on disk

### Storage Layout

```
/data/
├── mailbox_alice_dolda/
│   ├── partition_0/
│   │   ├── segment_0.dat
│   │   └── segment_1.dat
├── mailbox_bob_dolda/
│   ├── partition_0/
│   │   ├── segment_0.dat
│   │   └── segment_1.dat
└── mailbox_charlie_dolda/
    └── partition_0/
        └── segment_0.dat
```

---

## 🎭 Actor Model

The mailbox system enables classic actor patterns:

### Actor Definition

```rust
struct UserActor {
    mailbox: Mailbox,
    state: UserState,
}

impl UserActor {
    async fn run(&mut self) {
        while let Some(msg) = self.mailbox.receive().await.unwrap() {
            match msg.subject.as_deref() {
                Some("GetBalance") => {
                    let balance = self.state.balance;
                    self.mailbox.reply(&msg, &balance.to_string().as_bytes()).await.unwrap();
                }
                Some("Deposit") => {
                    let amount: f64 = String::from_utf8_lossy(&msg.body).parse().unwrap();
                    self.state.balance += amount;
                }
                _ => {}
            }
        }
    }
}
```

### Supervisor Pattern

```rust
// Supervisor mailbox monitors workers
let supervisor = system.create_mailbox("supervisor@dolda").await?;
let worker1 = system.create_mailbox("worker1@dolda").await?;
let worker2 = system.create_mailbox("worker2@dolda").await?;

// Workers send heartbeats
worker1.send_to("supervisor@dolda", b"heartbeat").await?;

// Supervisor monitors and restarts failed workers
while let Some(msg) = supervisor.receive().await? {
    if msg.body == b"heartbeat" {
        println!("Worker {} is alive", msg.from);
    }
}
```

---

## 🔐 Best Practices

### 1. Use Meaningful Addresses

```rust
// ✅ Good: Descriptive addresses
"users@orders.dolda"
"payment-processor@dolda"
"notifications@email.dolda"

// ❌ Bad: Generic addresses
"queue1@dolda"
"mb@dolda"
```

### 2. Handle Errors Gracefully

```rust
match mailbox.receive().await {
    Ok(Some(msg)) => process(msg),
    Ok(None) => {}, // No messages
    Err(e) => log::error!("Receive error: {}", e),
}
```

### 3. Use Subjects for Message Types

```rust
// Instead of parsing body for type
alice.send_with_subject("bob@dolda", "UserCreated", user_json).await?;

// Bob can route based on subject
match msg.subject.as_deref() {
    Some("UserCreated") => handle_user_created(&msg.body),
    Some("UserUpdated") => handle_user_updated(&msg.body),
    _ => {}
}
```

### 4. Implement Timeouts

```rust
use tokio::time::{timeout, Duration};

match timeout(Duration::from_secs(5), mailbox.receive()).await {
    Ok(Ok(Some(msg))) => process(msg),
    Ok(Ok(None)) => {},
    Ok(Err(e)) => log::error!("Receive error: {}", e),
    Err(_) => log::warn!("Receive timeout"),
}
```

---

## 🚧 Current Limitations

1. **Single Node**: Currently works on single node only
   - Future: Distributed mailbox routing across cluster

2. **No Message Deletion**: Messages are read sequentially
   - Future: Add message acknowledgment and deletion

3. **No Priority**: FIFO order only
   - Future: Priority queues for urgent messages

4. **No TTL**: Messages persist indefinitely
   - Future: Message expiration and cleanup

---

## 🔮 Future Enhancements

- [ ] Distributed mailbox routing
- [ ] Message acknowledgment
- [ ] Dead letter queue
- [ ] Message TTL and expiration
- [ ] Priority queues
- [ ] Message filtering and routing rules
- [ ] Mailbox aliases
- [ ] Transaction support
- [ ] Message compression

---

## 📚 Examples

See `examples/mailbox_example.rs` for a comprehensive demo showing:
- Simple message exchange
- Reply functionality
- Multiple messages
- Peek without consuming
- Broadcast pattern
- System statistics

Run with:
```bash
cargo run --example mailbox_example --release
```

---

## 🧪 Testing

```bash
# Run mailbox tests
cargo test --release --lib mailbox

# Run specific test
cargo test --release --lib mailbox::tests::test_send_and_receive
```

---

## 💡 Use Cases

Perfect for:
- **Microservices** - Service-to-service communication
- **Actor Systems** - Erlang-style actors in Rust
- **Event Sourcing** - Event distribution
- **Task Queues** - Background job processing
- **Notification Systems** - Alert delivery
- **Workflow Engines** - Step coordination
- **Message Bus** - Pub/sub patterns

---

**Status**: ✅ **Basic Functionality Ready**

Build actor-like systems with persistent, reliable messaging! 🎭📬

