//! Network Mailbox Example - TCP/UDP remote mailbox access
//!
//! This example demonstrates:
//! 1. Starting a mailbox server (TCP or UDP)
//! 2. Connecting from remote clients
//! 3. Sending messages over the network
//! 4. Receiving messages from remote mailboxes
//!
//! Run with:
//! ```bash
//! cargo run --example mailbox_network_example --release
//! ```

use dolda::mailbox::MailboxSystem;
use dolda::mailbox_network::{MailboxServer, MailboxClient, NetworkConfig, Protocol};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌐 DOLDA Network Mailbox Example\n");
    println!("{}", "=".repeat(70));
    
    // 1. Create mailbox system
    println!("\n🏗️  Creating mailbox system...");
    let temp_dir = tempfile::tempdir()?;
    let system = Arc::new(RwLock::new(
        MailboxSystem::new(temp_dir.path().to_str().unwrap().to_string()).await?
    ));
    println!("✅ System created");
    
    // 2. Start TCP server
    println!("\n{}", "=".repeat(70));
    println!("📡 SCENARIO 1: TCP Server");
    println!("{}", "=".repeat(70));
    
    let tcp_config = NetworkConfig {
        host: "127.0.0.1".to_string(),
        port: 19090,
        protocol: Protocol::Tcp,
        ..Default::default()
    };
    
    println!("\n🚀 Starting TCP server on {}:{}...", tcp_config.host, tcp_config.port);
    let tcp_server = MailboxServer::new(tcp_config.clone(), Arc::clone(&system)).await?;
    
    // Start server in background
    tokio::spawn(async move {
        if let Err(e) = tcp_server.start().await {
            eprintln!("❌ TCP server error: {}", e);
        }
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    println!("✅ TCP server started");
    
    // 3. Create TCP client
    println!("\n{}", "=".repeat(70));
    println!("📱 SCENARIO 2: TCP Client Communication");
    println!("{}", "=".repeat(70));
    
    println!("\n🔌 Creating TCP client...");
    let tcp_addr = format!("{}:{}", tcp_config.host, tcp_config.port).parse()?;
    let tcp_client = MailboxClient::new(tcp_addr, Protocol::Tcp);
    println!("✅ TCP client created");
    
    // Send message via TCP
    println!("\n📨 Sending message from alice@dolda to bob@dolda via TCP...");
    tcp_client.send(
        "bob@dolda",
        "alice@dolda",
        b"Hello Bob from remote Alice via TCP!"
    ).await?;
    println!("✅ Message sent");
    
    // Receive message via TCP
    println!("\n📬 Receiving message at bob@dolda via TCP...");
    if let Some(msg) = tcp_client.receive("bob@dolda").await? {
        println!("   From: {}", msg.from);
        println!("   To: {}", msg.to);
        println!("   Body: {}", String::from_utf8_lossy(&msg.body));
        println!("   ID: {}", msg.id);
    }
    
    // 4. Send message with subject
    println!("\n{}", "=".repeat(70));
    println!("💬 SCENARIO 3: Message with Subject");
    println!("{}", "=".repeat(70));
    
    println!("\n📨 Sending message with subject...");
    tcp_client.send_with_subject(
        "charlie@dolda",
        "alice@dolda",
        "Meeting Request",
        b"Can we meet tomorrow at 2pm?"
    ).await?;
    println!("✅ Message with subject sent");
    
    println!("\n📬 Receiving message at charlie@dolda...");
    if let Some(msg) = tcp_client.receive("charlie@dolda").await? {
        println!("   From: {}", msg.from);
        println!("   Subject: {}", msg.subject.unwrap_or_else(|| "None".to_string()));
        println!("   Body: {}", String::from_utf8_lossy(&msg.body));
    }
    
    // 5. Start UDP server
    println!("\n{}", "=".repeat(70));
    println!("📡 SCENARIO 4: UDP Server (Low Latency)");
    println!("{}", "=".repeat(70));
    
    let udp_config = NetworkConfig {
        host: "127.0.0.1".to_string(),
        port: 19091,
        protocol: Protocol::Udp,
        ..Default::default()
    };
    
    println!("\n🚀 Starting UDP server on {}:{}...", udp_config.host, udp_config.port);
    let system2 = Arc::new(RwLock::new(
        MailboxSystem::new(temp_dir.path().to_str().unwrap().to_string()).await?
    ));
    let udp_server = MailboxServer::new(udp_config.clone(), system2).await?;
    
    tokio::spawn(async move {
        if let Err(e) = udp_server.start().await {
            eprintln!("❌ UDP server error: {}", e);
        }
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    println!("✅ UDP server started");
    
    // Create UDP client
    println!("\n🔌 Creating UDP client...");
    let udp_addr = format!("{}:{}", udp_config.host, udp_config.port).parse()?;
    let udp_client = MailboxClient::new(udp_addr, Protocol::Udp);
    println!("✅ UDP client created");
    
    // Send via UDP
    println!("\n📨 Sending message via UDP (low latency)...");
    udp_client.send(
        "bob@dolda",
        "alice@dolda",
        b"Fast message via UDP!"
    ).await?;
    println!("✅ Message sent via UDP");
    
    println!("\n📬 Receiving message via UDP...");
    if let Some(msg) = udp_client.receive("bob@dolda").await? {
        println!("   Protocol: UDP (low latency)");
        println!("   Body: {}", String::from_utf8_lossy(&msg.body));
    }
    
    // 6. Multiple messages
    println!("\n{}", "=".repeat(70));
    println!("📊 SCENARIO 5: Multiple Messages");
    println!("{}", "=".repeat(70));
    
    println!("\n📨 Sending 5 messages via TCP...");
    for i in 1..=5 {
        tcp_client.send(
            "inbox@dolda",
            "sender@dolda",
            format!("Message number {}", i).as_bytes()
        ).await?;
    }
    println!("✅ Sent 5 messages");
    
    println!("\n📬 Receiving all messages from inbox...");
    for i in 1..=5 {
        if let Some(msg) = tcp_client.receive("inbox@dolda").await? {
            println!("   Message {}: {}", i, String::from_utf8_lossy(&msg.body));
        }
    }
    
    // 7. Performance comparison
    println!("\n{}", "=".repeat(70));
    println!("⚡ SCENARIO 6: Performance Comparison");
    println!("{}", "=".repeat(70));
    
    println!("\n🏃 TCP latency test...");
    let start = std::time::Instant::now();
    tcp_client.send("perf@dolda", "test@dolda", b"test").await?;
    let _ = tcp_client.receive("perf@dolda").await?;
    let tcp_latency = start.elapsed();
    println!("   TCP round-trip: {:?}", tcp_latency);
    
    println!("\n🏃 UDP latency test...");
    let start = std::time::Instant::now();
    udp_client.send("perf@dolda", "test@dolda", b"test").await?;
    let _ = udp_client.receive("perf@dolda").await?;
    let udp_latency = start.elapsed();
    println!("   UDP round-trip: {:?}", udp_latency);
    
    println!("\n📊 Latency comparison:");
    println!("   TCP: {:?}", tcp_latency);
    println!("   UDP: {:?}", udp_latency);
    println!("   UDP is {:.2}x faster", tcp_latency.as_micros() as f64 / udp_latency.as_micros() as f64);
    
    println!("\n{}", "=".repeat(70));
    println!("✨ Example complete!");
    println!("{}", "=".repeat(70));
    
    println!("\n💡 Key Takeaways:");
    println!("   • Mailboxes accessible over TCP/UDP network");
    println!("   • TCP provides reliable delivery");
    println!("   • UDP provides low latency");
    println!("   • Same mailbox API, remote access");
    println!("   • Binary protocol with automatic serialization");
    println!("   • Perfect for distributed microservices");
    
    Ok(())
}

