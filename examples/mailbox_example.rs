//! Mailbox System Example - Actor-like messaging between queues
//!
//! This example demonstrates using mailboxes for inter-queue communication,
//! where each mailbox is like an email address.
//!
//! Run with:
//! ```bash
//! cargo run --example mailbox_example --release
//! ```

use dolda::mailbox::MailboxSystem;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("📬 DOLDA Mailbox System Example\n");
    println!("{}", "=".repeat(70));
    
    // 1. Create mailbox system
    println!("\n🏗️  Creating mailbox system...");
    let temp_dir = tempfile::tempdir()?;
    let mut system = MailboxSystem::new(temp_dir.path().to_str().unwrap().to_string()).await?;
    println!("✅ System created at: {}", temp_dir.path().display());
    
    // 2. Create mailboxes (like email addresses)
    println!("\n📫 Creating mailboxes...");
    let alice = system.create_mailbox("alice@dolda").await?;
    let bob = system.create_mailbox("bob@dolda").await?;
    let charlie = system.create_mailbox("charlie@dolda").await?;
    
    println!("   ✅ alice@dolda");
    println!("   ✅ bob@dolda");
    println!("   ✅ charlie@dolda");
    
    // 3. Simple message exchange
    println!("\n{}", "=".repeat(70));
    println!("📨 SCENARIO 1: Simple Message Exchange");
    println!("{}", "=".repeat(70));
    
    println!("\n👤 Alice sends to Bob...");
    alice.send_to("bob@dolda", b"Hello Bob! How are you?").await?;
    println!("   ✉️  Sent: 'Hello Bob! How are you?'");
    
    println!("\n👤 Bob receives message...");
    let msg = bob.receive().await?.unwrap();
    println!("   📬 Received from: {}", msg.from);
    println!("   📄 Body: {}", String::from_utf8_lossy(&msg.body));
    println!("   🕐 Timestamp: {}", msg.timestamp);
    println!("   🆔 Message ID: {}", msg.id);
    
    // 4. Reply functionality
    println!("\n{}", "=".repeat(70));
    println!("💬 SCENARIO 2: Reply to Message");
    println!("{}", "=".repeat(70));
    
    println!("\n👤 Charlie sends to Alice with subject...");
    charlie.send_with_subject("alice@dolda", "Meeting Request", b"Can we meet tomorrow at 2pm?").await?;
    println!("   ✉️  Sent: Subject='Meeting Request', Body='Can we meet tomorrow at 2pm?'");
    
    println!("\n👤 Alice receives and replies...");
    let msg = alice.receive().await?.unwrap();
    println!("   📬 Received from: {}", msg.from);
    println!("   📋 Subject: {}", msg.subject.as_ref().unwrap());
    println!("   📄 Body: {}", String::from_utf8_lossy(&msg.body));
    
    alice.reply(&msg, b"Yes, 2pm works great! See you then.").await?;
    println!("   📤 Replied: 'Yes, 2pm works great! See you then.'");
    
    println!("\n👤 Charlie receives reply...");
    let reply = charlie.receive().await?.unwrap();
    println!("   📬 Received from: {}", reply.from);
    println!("   📋 Subject: {}", reply.subject.as_ref().unwrap());
    println!("   📄 Body: {}", String::from_utf8_lossy(&reply.body));
    
    // 5. Multiple messages and pending count
    println!("\n{}", "=".repeat(70));
    println!("📊 SCENARIO 3: Multiple Messages & Pending Count");
    println!("{}", "=".repeat(70));
    
    println!("\n👤 Multiple people send to Bob...");
    alice.send_to("bob@dolda", b"Bob, can you review my code?").await?;
    charlie.send_to("bob@dolda", b"Bob, lunch today?").await?;
    alice.send_to("bob@dolda", b"Bob, meeting in 5 minutes!").await?;
    println!("   ✉️  Alice sent 2 messages");
    println!("   ✉️  Charlie sent 1 message");
    
    let pending = bob.pending_count().await;
    println!("\n📬 Bob's mailbox: {} pending messages", pending);
    
    println!("\n👤 Bob reads all messages...");
    let mut count = 1;
    while let Some(msg) = bob.receive().await? {
        println!("   Message {}: From={}, Body={}", 
            count, 
            msg.from, 
            String::from_utf8_lossy(&msg.body)
        );
        count += 1;
    }
    
    println!("\n📬 Bob's mailbox: {} pending messages (all read)", bob.pending_count().await);
    
    // 6. Peek without consuming
    println!("\n{}", "=".repeat(70));
    println!("👁️  SCENARIO 4: Peek Without Consuming");
    println!("{}", "=".repeat(70));
    
    println!("\n👤 Alice sends to Charlie...");
    alice.send_to("charlie@dolda", b"Important notification!").await?;
    
    println!("\n👤 Charlie peeks at message (doesn't consume)...");
    let msg = charlie.peek().await?.unwrap();
    println!("   👁️  Peeked: {}", String::from_utf8_lossy(&msg.body));
    println!("   📬 Still pending: {}", charlie.pending_count().await);
    
    println!("\n👤 Charlie peeks again (same message)...");
    let msg2 = charlie.peek().await?.unwrap();
    println!("   👁️  Peeked: {}", String::from_utf8_lossy(&msg2.body));
    println!("   🆔 Same message ID: {}", msg.id == msg2.id);
    
    println!("\n👤 Charlie now receives (consumes) the message...");
    let msg3 = charlie.receive().await?.unwrap();
    println!("   📬 Received: {}", String::from_utf8_lossy(&msg3.body));
    println!("   📭 Pending after receive: {}", charlie.pending_count().await);
    
    // 7. Broadcast pattern
    println!("\n{}", "=".repeat(70));
    println!("📢 SCENARIO 5: Broadcast Pattern");
    println!("{}", "=".repeat(70));
    
    println!("\n👤 Alice broadcasts announcement to everyone...");
    let announcement = b"Team meeting at 3pm in conference room!";
    alice.send_to("bob@dolda", announcement).await?;
    alice.send_to("charlie@dolda", announcement).await?;
    println!("   📢 Broadcasted to Bob and Charlie");
    
    println!("\n👥 Everyone receives the announcement:");
    if let Some(msg) = bob.receive().await? {
        println!("   Bob received: {}", String::from_utf8_lossy(&msg.body));
    }
    if let Some(msg) = charlie.receive().await? {
        println!("   Charlie received: {}", String::from_utf8_lossy(&msg.body));
    }
    
    // 8. System statistics
    println!("\n{}", "=".repeat(70));
    println!("📊 SYSTEM STATISTICS");
    println!("{}", "=".repeat(70));
    
    let mailboxes = system.list_mailboxes().await;
    println!("\n📬 Total mailboxes: {}", mailboxes.len());
    for addr in &mailboxes {
        println!("   - {}", addr);
    }
    
    let stats = system.stats().await;
    println!("\n📨 Message counts:");
    for (addr, stat) in stats {
        println!("   {}: {} total messages", addr, stat.total_messages);
    }
    
    println!("\n{}", "=".repeat(70));
    println!("✨ Example complete!");
    println!("{}", "=".repeat(70));
    
    println!("\n💡 Key Takeaways:");
    println!("   • Each mailbox is like an email address");
    println!("   • Mailboxes can send messages to each other");
    println!("   • Messages are persistent and reliable");
    println!("   • Supports reply, subject, and message IDs");
    println!("   • Can peek at messages without consuming them");
    println!("   • Perfect for actor-like systems and microservices");
    
    Ok(())
}

