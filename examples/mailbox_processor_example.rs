//! Mailbox Processor Example - Automatic message processing with SQL
//!
//! This example demonstrates:
//! 1. Registering processors for mailboxes
//! 2. Automatic message processing
//! 3. SQL queries over mailbox data (future)
//! 4. Result forwarding between mailboxes
//! 5. Built-in processors (echo, filter, transform)
//!
//! Run with:
//! ```bash
//! cargo run --example mailbox_processor_example --release
//! ```

use dolda::mailbox::MailboxSystem;
use dolda::mailbox_processor::{
    ProcessorManager, ProcessorConfig, MailboxProcessor, ProcessorContext,
    ProcessorResult, EchoProcessor, FilterProcessor, TransformProcessor,
};
use dolda::mailbox::Message;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

// Custom processor that counts messages and logs statistics
struct StatisticsProcessor {
    count: Arc<RwLock<u64>>,
}

#[async_trait]
impl MailboxProcessor for StatisticsProcessor {
    async fn init(&self, ctx: &ProcessorContext) -> ProcessorResult {
        println!("📊 Statistics processor initialized for {}", ctx.address);
        Ok(())
    }
    
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        let mut count = self.count.write().await;
        *count += 1;
        
        println!("📈 Message #{} received at {}", count, ctx.address);
        println!("   From: {}", msg.from);
        println!("   Body: {}", String::from_utf8_lossy(&msg.body));
        
        // Every 5 messages, send summary
        if *count % 5 == 0 {
            let summary = format!("Processed {} messages", count);
            ctx.send_to("summary@dolda", summary.as_bytes()).await?;
        }
        
        Ok(())
    }
    
    async fn shutdown(&self, ctx: &ProcessorContext) -> ProcessorResult {
        let count = *self.count.read().await;
        println!("📊 Statistics processor shutting down: {} total messages", count);
        println!("   Mailbox: {}", ctx.address);
        Ok(())
    }
}

// Processor that uppercases text messages
struct UppercaseProcessor {
    output: String,
}

#[async_trait]
impl MailboxProcessor for UppercaseProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        let text = String::from_utf8_lossy(&msg.body);
        let uppercase = text.to_uppercase();
        ctx.send_to(&self.output, uppercase.as_bytes()).await?;
        Ok(())
    }
}

// Processor that routes messages based on content
struct RouterProcessor;

#[async_trait]
impl MailboxProcessor for RouterProcessor {
    async fn process(&self, ctx: &ProcessorContext, msg: Message) -> ProcessorResult {
        let text = String::from_utf8_lossy(&msg.body);
        
        // Route based on content
        if text.contains("urgent") {
            ctx.send_to("urgent@dolda", &msg.body).await?;
        } else if text.contains("info") {
            ctx.send_to("info@dolda", &msg.body).await?;
        } else {
            ctx.send_to("general@dolda", &msg.body).await?;
        }
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔄 DOLDA Mailbox Processor Example\n");
    println!("{}", "=".repeat(70));
    
    // 1. Create mailbox system
    println!("\n🏗️  Creating mailbox system...");
    let temp_dir = tempfile::tempdir()?;
    let mut system = MailboxSystem::new(temp_dir.path().to_str().unwrap().to_string()).await?;
    println!("✅ System created");
    
    // 2. Create mailboxes
    println!("\n📫 Creating mailboxes...");
    let input = system.create_mailbox("input@dolda").await?;
    let echo = system.create_mailbox("echo@dolda").await?;
    let stats = system.create_mailbox("stats@dolda").await?;
    let summary = system.create_mailbox("summary@dolda").await?;
    let transform_in = system.create_mailbox("transform@dolda").await?;
    let transform_out = system.create_mailbox("transformed@dolda").await?;
    let router = system.create_mailbox("router@dolda").await?;
    let urgent = system.create_mailbox("urgent@dolda").await?;
    let info = system.create_mailbox("info@dolda").await?;
    let general = system.create_mailbox("general@dolda").await?;
    
    println!("   ✅ input@dolda");
    println!("   ✅ echo@dolda");
    println!("   ✅ stats@dolda");
    println!("   ✅ summary@dolda");
    println!("   ✅ transform@dolda");
    println!("   ✅ transformed@dolda");
    println!("   ✅ router@dolda");
    println!("   ✅ urgent@dolda");
    println!("   ✅ info@dolda");
    println!("   ✅ general@dolda");
    
    // 3. Create processor manager
    println!("\n🔧 Creating processor manager...");
    let system_arc = Arc::new(RwLock::new(system));
    let manager = ProcessorManager::new(Arc::clone(&system_arc));
    println!("✅ Manager created");
    
    // 4. Register processors
    println!("\n{}", "=".repeat(70));
    println!("📝 SCENARIO 1: Echo Processor");
    println!("{}", "=".repeat(70));
    
    println!("\n🔄 Registering echo processor...");
    manager.register_processor(
        "echo@dolda",
        Arc::new(EchoProcessor),
        ProcessorConfig::default(),
    ).await?;
    println!("✅ Echo processor registered");
    
    // Send message to echo
    println!("\n📨 Sending message to echo...");
    input.send_to("echo@dolda", b"Hello Echo!").await?;
    
    // Wait for processing
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    // Check for echo reply
    println!("\n📬 Checking for echo reply...");
    if let Some(msg) = input.receive().await? {
        println!("   ✅ Received echo: {}", String::from_utf8_lossy(&msg.body));
    }
    
    // 5. Statistics processor
    println!("\n{}", "=".repeat(70));
    println!("📊 SCENARIO 2: Statistics Processor");
    println!("{}", "=".repeat(70));
    
    println!("\n📊 Registering statistics processor...");
    let stats_processor = Arc::new(StatisticsProcessor {
        count: Arc::new(RwLock::new(0)),
    });
    manager.register_processor(
        "stats@dolda",
        stats_processor,
        ProcessorConfig::default(),
    ).await?;
    println!("✅ Statistics processor registered");
    
    // Send multiple messages
    println!("\n📨 Sending 7 messages to stats processor...");
    for i in 1..=7 {
        let msg = format!("Message number {}", i);
        input.send_to("stats@dolda", msg.as_bytes()).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    
    // Wait for processing
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    
    // Check summary mailbox
    println!("\n📬 Checking summary mailbox...");
    if let Some(msg) = summary.receive().await? {
        println!("   📊 Summary: {}", String::from_utf8_lossy(&msg.body));
    }
    
    // 6. Transform processor
    println!("\n{}", "=".repeat(70));
    println!("🔄 SCENARIO 3: Transform Processor (Uppercase)");
    println!("{}", "=".repeat(70));
    
    println!("\n🔄 Registering uppercase processor...");
    let uppercase_processor = Arc::new(UppercaseProcessor {
        output: "transformed@dolda".to_string(),
    });
    manager.register_processor(
        "transform@dolda",
        uppercase_processor,
        ProcessorConfig::default(),
    ).await?;
    println!("✅ Uppercase processor registered");
    
    // Send message
    println!("\n📨 Sending message to transform...");
    input.send_to("transform@dolda", b"hello world").await?;
    
    // Wait and check result
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    println!("\n📬 Checking transformed output...");
    if let Some(msg) = transform_out.receive().await? {
        println!("   ✅ Transformed: {}", String::from_utf8_lossy(&msg.body));
    }
    
    // 7. Filter processor
    println!("\n{}", "=".repeat(70));
    println!("🔍 SCENARIO 4: Filter Processor");
    println!("{}", "=".repeat(70));
    
    println!("\n🔍 Registering filter processor (only numbers)...");
    let filter_processor = Arc::new(FilterProcessor::new(
        Arc::new(|msg: &Message| {
            String::from_utf8_lossy(&msg.body).chars().all(|c| c.is_numeric())
        }),
        "filtered@dolda".to_string(),
    ));
    
    let filtered = {
        let mut system = system_arc.write().await;
        system.create_mailbox("filtered@dolda").await?
    };
    
    manager.register_processor(
        "filtered@dolda",
        filter_processor,
        ProcessorConfig::default(),
    ).await?;
    println!("✅ Filter processor registered");
    
    // Send mixed messages
    println!("\n📨 Sending mixed messages (text and numbers)...");
    input.send_to("filtered@dolda", b"12345").await?;
    input.send_to("filtered@dolda", b"hello").await?;
    input.send_to("filtered@dolda", b"67890").await?;
    
    // Wait for processing
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    println!("\n📬 Checking filtered output (should only have numbers)...");
    let mut count = 0;
    while let Some(msg) = filtered.receive().await? {
        println!("   ✅ Filtered: {}", String::from_utf8_lossy(&msg.body));
        count += 1;
        if count >= 2 { break; }
    }
    
    // 8. Router processor
    println!("\n{}", "=".repeat(70));
    println!("🔀 SCENARIO 5: Router Processor");
    println!("{}", "=".repeat(70));
    
    println!("\n🔀 Registering router processor...");
    manager.register_processor(
        "router@dolda",
        Arc::new(RouterProcessor),
        ProcessorConfig::default(),
    ).await?;
    println!("✅ Router processor registered");
    
    // Send messages with different priorities
    println!("\n📨 Sending messages to router...");
    input.send_to("router@dolda", b"This is urgent!").await?;
    input.send_to("router@dolda", b"Just some info").await?;
    input.send_to("router@dolda", b"General message").await?;
    
    // Wait for routing
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    // Check routed messages
    println!("\n📬 Checking routed messages...");
    if let Some(msg) = urgent.receive().await? {
        println!("   🚨 Urgent: {}", String::from_utf8_lossy(&msg.body));
    }
    if let Some(msg) = info.receive().await? {
        println!("   ℹ️  Info: {}", String::from_utf8_lossy(&msg.body));
    }
    if let Some(msg) = general.receive().await? {
        println!("   📝 General: {}", String::from_utf8_lossy(&msg.body));
    }
    
    // 9. Cleanup
    println!("\n{}", "=".repeat(70));
    println!("🛑 SHUTTING DOWN");
    println!("{}", "=".repeat(70));
    
    println!("\n🛑 Shutting down all processors...");
    manager.shutdown_all().await?;
    println!("✅ All processors shut down");
    
    println!("\n{}", "=".repeat(70));
    println!("✨ Example complete!");
    println!("{}", "=".repeat(70));
    
    println!("\n💡 Key Takeaways:");
    println!("   • Processors automatically handle incoming messages");
    println!("   • Built-in processors: Echo, Filter, Transform");
    println!("   • Custom processors: Statistics, Uppercase, Router");
    println!("   • Processors can send to other mailboxes");
    println!("   • SQL query support (future enhancement)");
    println!("   • Parquet export support (future enhancement)");
    
    Ok(())
}

