//! Production-quality DOLDA demonstration
//!
//! This example demonstrates:
//! - Storage engine with Record format
//! - Health check HTTP server
//! - Observability (metrics and tracing)
//! - Concurrent operations
//!
//! Run with:
//! ```bash
//! cargo run --example production_demo
//! ```
//!
//! Then check health endpoints:
//! ```bash
//! curl http://localhost:8080/health
//! curl http://localhost:8080/health/detail
//! ```

use dolda::record::Record;
use dolda::storage::{RecordSegment, SegmentManager};
use dolda::health::{HealthServer, HealthChecker, HealthStatus};
use dolda::observability;
use std::sync::Arc;
use std::time::Duration;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize observability
    observability::init();
    tracing::info!("=== DOLDA Production Demo ===");
    
    // Create health server
    let health_server = Arc::new(HealthServer::new(env!("CARGO_PKG_VERSION")));
    
    // Register components
    health_server.register_component("storage");
    health_server.register_component("demo_task");
    
    // Start health check HTTP server in background
    let health_server_clone = Arc::clone(&health_server);
    tokio::spawn(async move {
        if let Err(e) = health_server_clone.serve("127.0.0.1:8080").await {
            tracing::error!("Health server error: {}", e);
        }
    });
    
    tracing::info!("Health check server started on http://127.0.0.1:8080");
    tracing::info!("Endpoints: /health, /health/ready, /health/detail");
    
    // Start background health checker
    let health_checker = HealthChecker::new(Arc::clone(&health_server), Duration::from_secs(5));
    tokio::spawn(async move {
        health_checker.run().await;
    });
    
    // Update initial health
    health_server.update_component("storage", HealthStatus::Healthy, "Initializing");
    health_server.update_component("demo_task", HealthStatus::Healthy, "Starting");
    
    // Create storage directory
    let storage_dir = "/tmp/dolda_production_demo";
    std::fs::create_dir_all(storage_dir)?;
    tracing::info!("Storage directory: {}", storage_dir);
    
    // Demo 1: Single segment operations
    tracing::info!("\n--- Demo 1: Single Segment Operations ---");
    demo_single_segment(&health_server).await?;
    
    // Demo 2: Segment manager with rollover
    tracing::info!("\n--- Demo 2: Segment Manager with Rollover ---");
    demo_segment_manager(&health_server).await?;
    
    // Demo 3: Concurrent operations
    tracing::info!("\n--- Demo 3: Concurrent Operations ---");
    demo_concurrent_operations(&health_server).await?;
    
    // Demo 4: Large dataset
    tracing::info!("\n--- Demo 4: Large Dataset Performance ---");
    demo_large_dataset(&health_server).await?;
    
    // Update final status
    health_server.update_component("demo_task", HealthStatus::Healthy, "Completed successfully");
    
    tracing::info!("\n=== Demo Complete ===");
    tracing::info!("Check health at: curl http://localhost:8080/health/detail");
    tracing::info!("Press Ctrl+C to exit...");
    
    // Keep server running
    tokio::signal::ctrl_c().await?;
    
    Ok(())
}

async fn demo_single_segment(health: &Arc<HealthServer>) -> Result<(), Box<dyn std::error::Error>> {
    health.update_component("storage", HealthStatus::Healthy, "Running single segment demo");
    
    let segment = RecordSegment::new(
        "/tmp/dolda_production_demo/demo1.dat".to_string(),
        10 * 1024 * 1024, // 10MB
        true, // Enable indexing
    ).await?;
    
    // Write some records
    tracing::info!("Writing 100 records...");
    for i in 0..100 {
        let data = format!("Record number {}: Hello from DOLDA!", i);
        let record = Record::new(data.as_bytes().to_vec())?;
        let offset = segment.append_record(&record)?;
        
        if i < 3 {
            tracing::info!("  Wrote record {} at offset {}", i, offset.offset);
        }
    }
    
    // Read them back
    tracing::info!("Reading records back...");
    let stats = segment.stats();
    tracing::info!("Segment stats: {} records, {} bytes used, {:.1}% full",
        stats.record_count,
        stats.used_bytes,
        segment.utilization()
    );
    
    // Iterate over all records
    let count = segment.iter_records()
        .filter_map(|r| r.ok())
        .count();
    tracing::info!("Verified {} records via iteration", count);
    
    segment.flush()?;
    tracing::info!("Flushed to disk");
    
    Ok(())
}

async fn demo_segment_manager(health: &Arc<HealthServer>) -> Result<(), Box<dyn std::error::Error>> {
    health.update_component("storage", HealthStatus::Healthy, "Running segment manager demo");
    
    let manager = SegmentManager::new(
        0, // Partition 0
        128 * 1024, // Small 128KB segments to force rollover
        "/tmp/dolda_production_demo/partition0".to_string(),
        true, // Enable indexing
    ).await?;
    
    // Write enough data to cause segment rollovers
    tracing::info!("Writing 1000 records to trigger segment rollovers...");
    let large_data = vec![0u8; 1024]; // 1KB per record
    
    for i in 0..1000 {
        let mut data = large_data.clone();
        data.extend_from_slice(format!("Record {}", i).as_bytes());
        
        let record = Record::new(data)?;
        manager.append_record(&record).await?;
        
        if (i + 1) % 100 == 0 {
            let stats = manager.stats();
            tracing::info!("  Progress: {} records, {} segments",
                stats.total_records,
                stats.segment_count
            );
        }
    }
    
    manager.flush().await?;
    
    let final_stats = manager.stats();
    tracing::info!("Final stats: {} records across {} segments, {:.1}% avg utilization",
        final_stats.total_records,
        final_stats.segment_count,
        final_stats.avg_utilization
    );
    
    Ok(())
}

async fn demo_concurrent_operations(health: &Arc<HealthServer>) -> Result<(), Box<dyn std::error::Error>> {
    health.update_component("storage", HealthStatus::Healthy, "Running concurrent demo");
    
    let manager = Arc::new(SegmentManager::new(
        1, // Partition 1
        1024 * 1024, // 1MB segments
        "/tmp/dolda_production_demo/partition1".to_string(),
        false, // Disable indexing for performance
    ).await?);
    
    tracing::info!("Spawning 10 concurrent writers...");
    let mut handles = Vec::new();
    
    for thread_id in 0..10 {
        let manager_clone = Arc::clone(&manager);
        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let data = format!("Thread {} - Record {}", thread_id, i);
                let record = Record::new(data.as_bytes().to_vec()).unwrap();
                manager_clone.append_record(&record).await.unwrap();
            }
        });
        handles.push(handle);
    }
    
    // Wait for all writers
    for handle in handles {
        handle.await?;
    }
    
    let stats = manager.stats();
    tracing::info!("Concurrent write complete: {} total records", stats.total_records);
    
    Ok(())
}

async fn demo_large_dataset(health: &Arc<HealthServer>) -> Result<(), Box<dyn std::error::Error>> {
    health.update_component("storage", HealthStatus::Healthy, "Running large dataset demo");
    
    let manager = SegmentManager::new(
        2, // Partition 2
        10 * 1024 * 1024, // 10MB segments
        "/tmp/dolda_production_demo/partition2".to_string(),
        false,
    ).await?;
    
    tracing::info!("Writing 10,000 records...");
    let start = std::time::Instant::now();
    
    let data = vec![0u8; 512]; // 512 bytes per record
    for _ in 0..10000 {
        let record = Record::new(data.clone())?;
        manager.append_record(&record).await?;
    }
    
    let elapsed = start.elapsed();
    let stats = manager.stats();
    
    let throughput_mbs = (stats.total_bytes as f64 / 1_000_000.0) / elapsed.as_secs_f64();
    let ops_per_sec = (stats.total_records as f64) / elapsed.as_secs_f64();
    
    tracing::info!("Performance metrics:");
    tracing::info!("  Total time: {:.2}s", elapsed.as_secs_f64());
    tracing::info!("  Throughput: {:.2} MB/s", throughput_mbs);
    tracing::info!("  Operations: {:.0} ops/sec", ops_per_sec);
    tracing::info!("  Total data: {:.2} MB", stats.total_bytes as f64 / 1_000_000.0);
    
    Ok(())
}

