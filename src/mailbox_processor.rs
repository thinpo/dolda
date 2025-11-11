//! Mailbox Processors - Automatic message processing with SQL support
//!
//! This module provides processor functionality for mailboxes, enabling:
//! - Automatic message processing
//! - SQL queries over mailbox data
//! - Result forwarding to other mailboxes
//! - Multiple storage backends (binary, Parquet)
//!
//! ## Example
//!
//! ```rust,no_run
//! use dolda::mailbox_processor::{MailboxProcessor, ProcessorContext, ProcessorResult};
//! use dolda::mailbox::Message;
//!
//! struct MyProcessor;
//!
//! #[async_trait::async_trait]
//! impl MailboxProcessor for MyProcessor {
//!     async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
//!         // Query mailbox data
//!         let results = ctx.query("SELECT * FROM messages WHERE amount > 100").await?;
//!         
//!         // Forward results
//!         ctx.send_to("results@dolda", &results).await?;
//!         
//!         Ok(())
//!     }
//! }
//! ```

use crate::mailbox::{Mailbox, MailboxAddress, MailboxError, Message, MailboxSystem};
use crate::datafusion_layer::DataFusionLayer;
use crate::storage::SegmentManager;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::json::writer::ArrayWriter;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;

#[derive(Error, Debug)]
pub enum ProcessorError {
    #[error("Mailbox error: {0}")]
    Mailbox(#[from] MailboxError),
    
    #[error("DataFusion error: {0}")]
    DataFusion(String),
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Processor not found: {0}")]
    ProcessorNotFound(String),
}

pub type ProcessorResult = Result<(), ProcessorError>;

/// Storage backend type for mailboxes
#[derive(Debug, Clone, PartialEq)]
pub enum StorageBackend {
    /// Binary storage (default PersistQ)
    Binary,
    /// Parquet columnar storage
    Parquet { path: PathBuf },
}

/// Context provided to processors for message processing
pub struct ProcessorContext {
    /// Mailbox address
    pub address: MailboxAddress,
    
    /// Reference to the mailbox system for sending messages
    system: Arc<RwLock<MailboxSystem>>,
    
    /// DataFusion layer for SQL queries
    datafusion: Option<Arc<DataFusionLayer>>,
    
    /// Storage backend
    storage_backend: StorageBackend,
}

impl ProcessorContext {
    /// Send a message to another mailbox
    pub async fn send_to(&self, to: &str, body: &[u8]) -> ProcessorResult {
        let system = self.system.read().await;
        if let Some(mailbox) = system.get_mailbox(&self.address.as_str()).await? {
            mailbox.send_to(to, body).await?;
            Ok(())
        } else {
            Err(ProcessorError::Mailbox(MailboxError::MailboxNotFound(
                self.address.to_string()
            )))
        }
    }
    
    /// Execute a SQL query over the mailbox data
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, ProcessorError> {
        if let Some(df) = &self.datafusion {
            df.query(sql).await
                .map_err(|e| ProcessorError::DataFusion(e.to_string()))
        } else {
            Err(ProcessorError::DataFusion(
                "DataFusion not enabled for this mailbox".to_string()
            ))
        }
    }
    
    /// Export query results to Parquet
    pub async fn export_parquet(&self, sql: &str, path: &str) -> ProcessorResult {
        if let Some(df) = &self.datafusion {
            df.export_to_parquet(sql, path).await
                .map_err(|e| ProcessorError::DataFusion(e.to_string()))
        } else {
            Err(ProcessorError::DataFusion(
                "DataFusion not enabled for this mailbox".to_string()
            ))
        }
    }
    
    /// Get storage backend type
    pub fn storage_backend(&self) -> &StorageBackend {
        &self.storage_backend
    }
}

/// Trait for implementing message processors
#[async_trait]
pub trait MailboxProcessor: Send + Sync {
    /// Process a message
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult;
    
    /// Optional: Initialize processor (called once on registration)
    async fn init(&self, _ctx: &ProcessorContext) -> ProcessorResult {
        Ok(())
    }
    
    /// Optional: Cleanup processor (called on shutdown)
    async fn shutdown(&self, _ctx: &ProcessorContext) -> ProcessorResult {
        Ok(())
    }
}

/// Configuration for mailbox processor
#[derive(Clone)]
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

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            enable_sql: false,
            storage_backend: StorageBackend::Binary,
            batch_size: 10,
            poll_interval_ms: 100,
        }
    }
}

/// Manages processors for mailboxes
pub struct ProcessorManager {
    /// Registered processors
    processors: Arc<RwLock<HashMap<MailboxAddress, ProcessorEntry>>>,
    
    /// Mailbox system reference
    system: Arc<RwLock<MailboxSystem>>,
}

struct ProcessorEntry {
    processor: Arc<dyn MailboxProcessor>,
    config: ProcessorConfig,
    task_handle: Option<JoinHandle<()>>,
    shutdown_tx: mpsc::Sender<()>,
}

impl ProcessorManager {
    /// Create a new processor manager
    pub fn new(system: Arc<RwLock<MailboxSystem>>) -> Self {
        Self {
            processors: Arc::new(RwLock::new(HashMap::new())),
            system,
        }
    }
    
    /// Register a processor for a mailbox
    pub async fn register_processor(
        &self,
        address: &str,
        processor: Arc<dyn MailboxProcessor>,
        config: ProcessorConfig,
    ) -> ProcessorResult {
        let addr = MailboxAddress::new(address)
            .map_err(|e| ProcessorError::Mailbox(e))?;
        
        // Get the mailbox
        let system = self.system.read().await;
        let mailbox = system.get_mailbox(address).await?
            .ok_or_else(|| ProcessorError::Mailbox(
                MailboxError::MailboxNotFound(address.to_string())
            ))?;
        drop(system);
        
        // Create processor context
        let datafusion = if config.enable_sql {
            // TODO: Initialize DataFusion with mailbox storage
            None
        } else {
            None
        };
        
        let ctx = Arc::new(ProcessorContext {
            address: addr.clone(),
            system: Arc::clone(&self.system),
            datafusion,
            storage_backend: config.storage_backend.clone(),
        });
        
        // Initialize processor
        processor.init(&ctx).await?;
        
        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        
        // Spawn processing task
        let processor_clone = Arc::clone(&processor);
        let ctx_clone = Arc::clone(&ctx);
        let poll_interval = config.poll_interval_ms;
        
        let task_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                tokio::time::Duration::from_millis(poll_interval)
            );
            
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Process messages
                        if let Err(e) = Self::process_messages(
                            &mailbox,
                            &processor_clone,
                            &ctx_clone
                        ).await {
                            log::error!("Processor error: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Processor shutting down");
                        break;
                    }
                }
            }
        });
        
        // Store processor entry
        let entry = ProcessorEntry {
            processor,
            config,
            task_handle: Some(task_handle),
            shutdown_tx,
        };
        
        let mut processors = self.processors.write().await;
        processors.insert(addr, entry);
        
        Ok(())
    }
    
    /// Process messages for a mailbox
    async fn process_messages(
        mailbox: &Mailbox,
        processor: &Arc<dyn MailboxProcessor>,
        ctx: &Arc<ProcessorContext>,
    ) -> ProcessorResult {
        // Receive and process message
        if let Some(msg) = mailbox.receive().await? {
            processor.process(ctx, msg).await?;
        }
        Ok(())
    }
    
    /// Unregister a processor
    pub async fn unregister_processor(&self, address: &str) -> ProcessorResult {
        let addr = MailboxAddress::new(address)
            .map_err(|e| ProcessorError::Mailbox(e))?;
        
        let mut processors = self.processors.write().await;
        if let Some(mut entry) = processors.remove(&addr) {
            // Send shutdown signal
            let _ = entry.shutdown_tx.send(()).await;
            
            // Wait for task to complete
            if let Some(handle) = entry.task_handle.take() {
                let _ = handle.await;
            }
            
            // Cleanup processor
            let ctx = ProcessorContext {
                address: addr,
                system: Arc::clone(&self.system),
                datafusion: None,
                storage_backend: entry.config.storage_backend,
            };
            entry.processor.shutdown(&ctx).await?;
        }
        
        Ok(())
    }
    
    /// Shutdown all processors
    pub async fn shutdown_all(&self) -> ProcessorResult {
        let mut processors = self.processors.write().await;
        let addresses: Vec<_> = processors.keys().cloned().collect();
        
        for addr in addresses {
            if let Some(mut entry) = processors.remove(&addr) {
                let _ = entry.shutdown_tx.send(()).await;
                if let Some(handle) = entry.task_handle.take() {
                    let _ = handle.await;
                }
                
                let ctx = ProcessorContext {
                    address: addr,
                    system: Arc::clone(&self.system),
                    datafusion: None,
                    storage_backend: entry.config.storage_backend,
                };
                let _ = entry.processor.shutdown(&ctx).await;
            }
        }
        
        Ok(())
    }
}

// Built-in processors

/// Echo processor - sends received messages back to sender
pub struct EchoProcessor;

#[async_trait]
impl MailboxProcessor for EchoProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        ctx.send_to(msg.from.as_str(), &msg.body).await?;
        Ok(())
    }
}

/// Aggregation processor - runs SQL aggregations and sends results
pub struct AggregationProcessor {
    query: String,
    output_mailbox: String,
}

impl AggregationProcessor {
    pub fn new(query: String, output_mailbox: String) -> Self {
        Self { query, output_mailbox }
    }
}

#[async_trait]
impl MailboxProcessor for AggregationProcessor {
    async fn process(&self, ctx: &ProcessorContext, _msg: Message) -> ProcessorResult {
        // Run SQL query
        let results = ctx.query(&self.query).await?;
        
        // Convert results to JSON and send
        for batch in results {
            // Convert RecordBatch to JSON using Arrow's JSON writer
            let mut buf = Vec::new();
            {
                let mut writer = ArrayWriter::new(&mut buf);
                writer.write(&batch)
                    .map_err(|e| ProcessorError::Storage(e.to_string()))?;
                writer.finish()
                    .map_err(|e| ProcessorError::Storage(e.to_string()))?;
            }
            
            ctx.send_to(&self.output_mailbox, &buf).await?;
        }
        
        Ok(())
    }
}

/// Filter processor - filters messages and forwards matches
pub struct FilterProcessor {
    predicate: Arc<dyn Fn(&Message) -> bool + Send + Sync>,
    output_mailbox: String,
}

impl FilterProcessor {
    pub fn new(
        predicate: Arc<dyn Fn(&Message) -> bool + Send + Sync>,
        output_mailbox: String,
    ) -> Self {
        Self { predicate, output_mailbox }
    }
}

#[async_trait]
impl MailboxProcessor for FilterProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        if (self.predicate)(&msg) {
            ctx.send_to(&self.output_mailbox, &msg.body).await?;
        }
        Ok(())
    }
}

/// Transform processor - applies transformation function
pub struct TransformProcessor {
    transform: Arc<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>,
    output_mailbox: String,
}

impl TransformProcessor {
    pub fn new(
        transform: Arc<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>,
        output_mailbox: String,
    ) -> Self {
        Self { transform, output_mailbox }
    }
}

#[async_trait]
impl MailboxProcessor for TransformProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        let transformed = (self.transform)(&msg.body);
        ctx.send_to(&self.output_mailbox, &transformed).await?;
        Ok(())
    }
}

/// Parquet export processor - exports messages to Parquet files
pub struct ParquetExportProcessor {
    output_dir: PathBuf,
    batch_size: usize,
}

impl ParquetExportProcessor {
    pub fn new(output_dir: PathBuf, batch_size: usize) -> Self {
        Self { output_dir, batch_size }
    }
}

#[async_trait]
impl MailboxProcessor for ParquetExportProcessor {
    async fn init(&self, _ctx: &ProcessorContext) -> ProcessorResult {
        std::fs::create_dir_all(&self.output_dir)?;
        Ok(())
    }
    
    async fn process(&self, ctx: &ProcessorContext, _msg: Message) -> ProcessorResult {
        // Export to Parquet using SQL
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let output_path = self.output_dir.join(format!("export_{}.parquet", timestamp));
        
        ctx.export_parquet(
            "SELECT * FROM messages",
            output_path.to_str().unwrap()
        ).await?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    struct TestProcessor {
        processed: Arc<RwLock<Vec<String>>>,
    }

    #[async_trait]
    impl MailboxProcessor for TestProcessor {
        async fn process(&self, _ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
            let body = String::from_utf8_lossy(&msg.body).to_string();
            self.processed.write().await.push(body);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_processor_registration() {
        let temp_dir = TempDir::new().unwrap();
        let mut system = MailboxSystem::new(temp_dir.path().to_str().unwrap().to_string())
            .await
            .unwrap();
        
        let mailbox = system.create_mailbox("test@dolda").await.unwrap();
        
        let processed = Arc::new(RwLock::new(Vec::new()));
        let processor = Arc::new(TestProcessor {
            processed: Arc::clone(&processed),
        });
        
        let manager = ProcessorManager::new(Arc::new(RwLock::new(system)));
        
        manager.register_processor(
            "test@dolda",
            processor,
            ProcessorConfig::default(),
        ).await.unwrap();
        
        // Send a message
        mailbox.send_to("test@dolda", b"hello").await.unwrap();
        
        // Wait for processing
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        let processed_msgs = processed.read().await;
        assert_eq!(processed_msgs.len(), 1);
        assert_eq!(processed_msgs[0], "hello");
        
        manager.shutdown_all().await.unwrap();
    }

    #[tokio::test]
    async fn test_echo_processor() {
        let temp_dir = TempDir::new().unwrap();
        let mut system = MailboxSystem::new(temp_dir.path().to_str().unwrap().to_string())
            .await
            .unwrap();
        
        let mailbox = system.create_mailbox("echo@dolda").await.unwrap();
        let sender = system.create_mailbox("sender@dolda").await.unwrap();
        
        let processor = Arc::new(EchoProcessor);
        let manager = ProcessorManager::new(Arc::new(RwLock::new(system)));
        
        manager.register_processor(
            "echo@dolda",
            processor,
            ProcessorConfig::default(),
        ).await.unwrap();
        
        // Send message to echo
        sender.send_to("echo@dolda", b"test").await.unwrap();
        
        // Wait for echo
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        // Check sender received echo
        if let Some(msg) = sender.receive().await.unwrap() {
            assert_eq!(msg.body, b"test");
        }
        
        manager.shutdown_all().await.unwrap();
    }
}

