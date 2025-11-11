//! Mailbox System - Actor-like messaging between persistent queues
//!
//! Each mailbox is like an email address - it can send and receive messages
//! from other mailboxes. Messages are persisted and delivered reliably.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
//! │  Mailbox A  │─────▶│  Mailbox B  │─────▶│  Mailbox C  │
//! │  (Queue 1)  │◀─────│  (Queue 2)  │◀─────│  (Queue 3)  │
//! └─────────────┘      └─────────────┘      └─────────────┘
//!       │                    │                    │
//!       └────────────────────┴────────────────────┘
//!                  Message Router
//! ```
//!
//! ## Example
//!
//! ```rust,no_run
//! use dolda::mailbox::{MailboxSystem, MailboxAddress, Message};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create mailbox system
//! let mut system = MailboxSystem::new("/data".to_string()).await?;
//!
//! // Create mailboxes
//! let alice = system.create_mailbox("alice@dolda").await?;
//! let bob = system.create_mailbox("bob@dolda").await?;
//!
//! // Alice sends message to Bob
//! alice.send_to("bob@dolda", b"Hello Bob!").await?;
//!
//! // Bob receives message
//! if let Some(msg) = bob.receive().await? {
//!     println!("From: {}, Body: {:?}", msg.from, msg.body);
//! }
//! # Ok(())
//! # }
//! ```

use crate::persistq::{PersistQ, PersistQConfig, CacheConfig, Cluster};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Error, Debug)]
pub enum MailboxError {
    #[error("PersistQ error: {0}")]
    PersistQ(String),
    
    #[error("Mailbox not found: {0}")]
    MailboxNotFound(String),
    
    #[error("Invalid address: {0}")]
    InvalidAddress(String),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("Message too large: {0} bytes")]
    MessageTooLarge(usize),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, MailboxError>;

/// A mailbox address (like an email address)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MailboxAddress(String);

impl MailboxAddress {
    /// Create a new mailbox address
    pub fn new(address: &str) -> Result<Self> {
        if address.is_empty() {
            return Err(MailboxError::InvalidAddress("Address cannot be empty".to_string()));
        }
        Ok(Self(address.to_string()))
    }
    
    /// Get the address as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for MailboxAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A message envelope with sender, recipient, and body
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Sender mailbox address
    pub from: MailboxAddress,
    
    /// Recipient mailbox address
    pub to: MailboxAddress,
    
    /// Message ID (unique)
    pub id: String,
    
    /// Timestamp (microseconds since epoch)
    pub timestamp: u64,
    
    /// Message body (arbitrary bytes)
    pub body: Vec<u8>,
    
    /// Optional subject/topic
    pub subject: Option<String>,
    
    /// Optional reply-to address
    pub reply_to: Option<MailboxAddress>,
}

impl Message {
    /// Create a new message
    pub fn new(from: MailboxAddress, to: MailboxAddress, body: Vec<u8>) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        
        Self {
            from,
            to,
            id,
            timestamp,
            body,
            subject: None,
            reply_to: None,
        }
    }
    
    /// Set the subject
    pub fn with_subject(mut self, subject: String) -> Self {
        self.subject = Some(subject);
        self
    }
    
    /// Set reply-to address
    pub fn with_reply_to(mut self, reply_to: MailboxAddress) -> Self {
        self.reply_to = Some(reply_to);
        self
    }
    
    /// Serialize to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
    
    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// A mailbox - persistent queue with address-based messaging
pub struct Mailbox {
    /// Mailbox address
    address: MailboxAddress,
    
    /// Underlying persistent queue
    queue: Arc<PersistQ>,
    
    /// Reference to the mailbox system for sending
    system: Arc<RwLock<MailboxSystemInner>>,
    
    /// Current read offset
    read_offset: Arc<RwLock<u64>>,
}

impl Mailbox {
    /// Send a message to another mailbox
    pub async fn send_to(&self, to: &str, body: &[u8]) -> Result<String> {
        let to_addr = MailboxAddress::new(to)?;
        
        let message = Message::new(
            self.address.clone(),
            to_addr.clone(),
            body.to_vec(),
        );
        
        // Send via the system router
        let system = self.system.read().await;
        system.route_message(message).await
    }
    
    /// Send a message with subject
    pub async fn send_with_subject(&self, to: &str, subject: &str, body: &[u8]) -> Result<String> {
        let to_addr = MailboxAddress::new(to)?;
        
        let message = Message::new(
            self.address.clone(),
            to_addr.clone(),
            body.to_vec(),
        ).with_subject(subject.to_string());
        
        let system = self.system.read().await;
        system.route_message(message).await
    }
    
    /// Reply to a message
    pub async fn reply(&self, original: &Message, body: &[u8]) -> Result<String> {
        let to_addr = original.reply_to.clone()
            .unwrap_or_else(|| original.from.clone());
        
        let message = Message::new(
            self.address.clone(),
            to_addr,
            body.to_vec(),
        ).with_subject(
            original.subject.as_ref()
                .map(|s| format!("Re: {}", s))
                .unwrap_or_else(|| "Re: (no subject)".to_string())
        );
        
        let system = self.system.read().await;
        system.route_message(message).await
    }
    
    /// Receive the next message
    pub async fn receive(&self) -> Result<Option<Message>> {
        let mut offset = self.read_offset.write().await;
        
        let result = self.queue.read(*offset).await
            .map_err(|e| MailboxError::PersistQ(e.to_string()))?;
        
        if !result.record.data.is_empty() {
            *offset += 1;
            let message = Message::from_bytes(&result.record.data)?;
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }
    
    /// Peek at next message without consuming
    pub async fn peek(&self) -> Result<Option<Message>> {
        let offset = self.read_offset.read().await;
        
        let result = self.queue.read(*offset).await
            .map_err(|e| MailboxError::PersistQ(e.to_string()))?;
        
        if !result.record.data.is_empty() {
            let message = Message::from_bytes(&result.record.data)?;
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }
    
    /// Get mailbox address
    pub fn address(&self) -> &MailboxAddress {
        &self.address
    }
    
    /// Get number of pending messages (approximate based on current offset)
    pub async fn pending_count(&self) -> u64 {
        // Note: This is approximate since PersistQ doesn't expose total count directly
        // We try to read ahead to check if messages exist
        let offset = *self.read_offset.read().await;
        
        // Quick check: try to read at current offset
        match self.queue.read(offset).await {
            Ok(result) if !result.record.data.is_empty() => {
                // At least one message pending; for simplicity return 1
                // A full implementation would need to count all messages
                1
            }
            _ => 0
        }
    }
}

/// Internal mailbox system state
struct MailboxSystemInner {
    /// Directory of mailboxes
    mailboxes: HashMap<MailboxAddress, Arc<PersistQ>>,
    
    /// Base directory for storage
    base_dir: String,
    
    /// Default configuration for new mailboxes
    default_config: PersistQConfig,
}

impl MailboxSystemInner {
    async fn route_message(&self, message: Message) -> Result<String> {
        // Find destination mailbox
        let dest_queue = self.mailboxes.get(&message.to)
            .ok_or_else(|| MailboxError::MailboxNotFound(message.to.to_string()))?;
        
        // Serialize message
        let bytes = message.to_bytes()?;
        
        // Append to destination queue
        let msg_id = message.id.clone();
        dest_queue.append(&bytes).await
            .map_err(|e| MailboxError::PersistQ(e.to_string()))?;
        
        Ok(msg_id)
    }
}

/// Mailbox system - manages multiple mailboxes and message routing
pub struct MailboxSystem {
    inner: Arc<RwLock<MailboxSystemInner>>,
}

impl MailboxSystem {
    /// Create a new mailbox system
    pub     async fn new(base_dir: String) -> Result<Self> {
        std::fs::create_dir_all(&base_dir)?;
        
        let default_config = PersistQConfig {
            storage_dir: base_dir.clone(),
            segment_size: 64 * 1024 * 1024, // 64MB
            num_partitions: 1,
            replication_factor: 1,
            cache_config: CacheConfig {
                enabled: false,
                size_bytes: 0,
                hot_data_threshold: 0.0,
            },
        };
        
        let inner = MailboxSystemInner {
            mailboxes: HashMap::new(),
            base_dir,
            default_config,
        };
        
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }
    
    /// Create a new mailbox with the given address
    pub async fn create_mailbox(&mut self, address: &str) -> Result<Mailbox> {
        let addr = MailboxAddress::new(address)?;
        
        let mut inner = self.inner.write().await;
        
        // Check if mailbox already exists
        if inner.mailboxes.contains_key(&addr) {
            return Err(MailboxError::InvalidAddress(
                format!("Mailbox '{}' already exists", address)
            ));
        }
        
        // Create subdirectory for this mailbox
        let mailbox_dir = format!("{}/mailbox_{}", inner.base_dir, address.replace('@', "_"));
        std::fs::create_dir_all(&mailbox_dir)?;
        
        // Create PersistQ for this mailbox
        let mut config = inner.default_config.clone();
        config.storage_dir = mailbox_dir;
        
        // Create a simple cluster (single node for mailbox)
        let cluster = Arc::new(Cluster);
        
        let queue = PersistQ::new(config, cluster).await
            .map_err(|e| MailboxError::PersistQ(e.to_string()))?;
        
        let queue_arc = Arc::new(queue);
        inner.mailboxes.insert(addr.clone(), Arc::clone(&queue_arc));
        
        Ok(Mailbox {
            address: addr,
            queue: queue_arc,
            system: Arc::clone(&self.inner),
            read_offset: Arc::new(RwLock::new(0)),
        })
    }
    
    /// Get an existing mailbox
    pub async fn get_mailbox(&self, address: &str) -> Result<Option<Mailbox>> {
        let addr = MailboxAddress::new(address)?;
        let inner = self.inner.read().await;
        
        if let Some(queue) = inner.mailboxes.get(&addr) {
            Ok(Some(Mailbox {
                address: addr,
                queue: Arc::clone(queue),
                system: Arc::clone(&self.inner),
                read_offset: Arc::new(RwLock::new(0)),
            }))
        } else {
            Ok(None)
        }
    }
    
    /// List all mailbox addresses
    pub async fn list_mailboxes(&self) -> Vec<MailboxAddress> {
        let inner = self.inner.read().await;
        inner.mailboxes.keys().cloned().collect()
    }
    
    /// Get statistics for all mailboxes
    pub async fn stats(&self) -> HashMap<MailboxAddress, MailboxStats> {
        let inner = self.inner.read().await;
        let mut stats = HashMap::new();
        
        for (addr, _queue) in &inner.mailboxes {
            // Note: We can't easily get total count from PersistQ
            // For now, return a placeholder
            stats.insert(addr.clone(), MailboxStats { total_messages: 0 });
        }
        
        stats
    }
}

/// Statistics for a mailbox
#[derive(Debug, Clone)]
pub struct MailboxStats {
    pub total_messages: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_system() -> (MailboxSystem, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let system = MailboxSystem::new(temp_dir.path().to_str().unwrap().to_string())
            .await
            .unwrap();
        (system, temp_dir)
    }

    #[tokio::test]
    async fn test_create_mailbox() {
        let (mut system, _temp) = create_test_system().await;
        
        let mailbox = system.create_mailbox("alice@dolda").await;
        assert!(mailbox.is_ok());
        
        let mailbox = mailbox.unwrap();
        assert_eq!(mailbox.address().as_str(), "alice@dolda");
    }

    #[tokio::test]
    async fn test_send_and_receive() {
        let (mut system, _temp) = create_test_system().await;
        
        let alice = system.create_mailbox("alice@dolda").await.unwrap();
        let bob = system.create_mailbox("bob@dolda").await.unwrap();
        
        // Alice sends to Bob
        let msg_id = alice.send_to("bob@dolda", b"Hello Bob!").await.unwrap();
        assert!(!msg_id.is_empty());
        
        // Bob receives
        let msg = bob.receive().await.unwrap();
        assert!(msg.is_some());
        
        let msg = msg.unwrap();
        assert_eq!(msg.from.as_str(), "alice@dolda");
        assert_eq!(msg.to.as_str(), "bob@dolda");
        assert_eq!(msg.body, b"Hello Bob!");
    }

    #[tokio::test]
    async fn test_multiple_messages() {
        let (mut system, _temp) = create_test_system().await;
        
        let alice = system.create_mailbox("alice@dolda").await.unwrap();
        let bob = system.create_mailbox("bob@dolda").await.unwrap();
        
        // Send multiple messages
        alice.send_to("bob@dolda", b"Message 1").await.unwrap();
        alice.send_to("bob@dolda", b"Message 2").await.unwrap();
        alice.send_to("bob@dolda", b"Message 3").await.unwrap();
        
        // Check pending count (at least 1)
        assert!(bob.pending_count().await > 0);
        
        // Receive all messages
        let msg1 = bob.receive().await.unwrap().unwrap();
        assert_eq!(msg1.body, b"Message 1");
        
        let msg2 = bob.receive().await.unwrap().unwrap();
        assert_eq!(msg2.body, b"Message 2");
        
        let msg3 = bob.receive().await.unwrap().unwrap();
        assert_eq!(msg3.body, b"Message 3");
        
        // No more messages
        let msg4 = bob.receive().await.unwrap();
        assert!(msg4.is_none());
    }

    #[tokio::test]
    async fn test_reply() {
        let (mut system, _temp) = create_test_system().await;
        
        let alice = system.create_mailbox("alice@dolda").await.unwrap();
        let bob = system.create_mailbox("bob@dolda").await.unwrap();
        
        // Alice sends to Bob
        alice.send_with_subject("bob@dolda", "Question", b"How are you?").await.unwrap();
        
        // Bob receives and replies
        let msg = bob.receive().await.unwrap().unwrap();
        bob.reply(&msg, b"I'm good, thanks!").await.unwrap();
        
        // Alice receives reply
        let reply = alice.receive().await.unwrap().unwrap();
        assert_eq!(reply.from.as_str(), "bob@dolda");
        assert_eq!(reply.body, b"I'm good, thanks!");
        assert!(reply.subject.as_ref().unwrap().starts_with("Re:"));
    }

    #[tokio::test]
    async fn test_peek() {
        let (mut system, _temp) = create_test_system().await;
        
        let alice = system.create_mailbox("alice@dolda").await.unwrap();
        let bob = system.create_mailbox("bob@dolda").await.unwrap();
        
        alice.send_to("bob@dolda", b"Test").await.unwrap();
        
        // Peek doesn't consume
        let msg1 = bob.peek().await.unwrap().unwrap();
        let msg2 = bob.peek().await.unwrap().unwrap();
        assert_eq!(msg1.id, msg2.id);
        
        // Receive consumes
        let msg3 = bob.receive().await.unwrap().unwrap();
        assert_eq!(msg1.id, msg3.id);
        
        // No more messages
        assert!(bob.peek().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_mailboxes() {
        let (mut system, _temp) = create_test_system().await;
        
        system.create_mailbox("alice@dolda").await.unwrap();
        system.create_mailbox("bob@dolda").await.unwrap();
        system.create_mailbox("charlie@dolda").await.unwrap();
        
        let mailboxes = system.list_mailboxes().await;
        assert_eq!(mailboxes.len(), 3);
        
        let addresses: Vec<String> = mailboxes.iter()
            .map(|a| a.as_str().to_string())
            .collect();
        assert!(addresses.contains(&"alice@dolda".to_string()));
        assert!(addresses.contains(&"bob@dolda".to_string()));
        assert!(addresses.contains(&"charlie@dolda".to_string()));
    }

    #[tokio::test]
    async fn test_mailbox_stats() {
        let (mut system, _temp) = create_test_system().await;
        
        let alice = system.create_mailbox("alice@dolda").await.unwrap();
        let _bob = system.create_mailbox("bob@dolda").await.unwrap();
        
        alice.send_to("bob@dolda", b"msg1").await.unwrap();
        alice.send_to("bob@dolda", b"msg2").await.unwrap();
        
        let stats = system.stats().await;
        let bob_addr = MailboxAddress::new("bob@dolda").unwrap();
        assert!(stats.contains_key(&bob_addr));
    }
}

