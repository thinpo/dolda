//! persistQ - Distributed Persistent Queue using DOLDA Architecture
//!
//! This is the main entry point for persistQ, a high-performance distributed
//! persistent queue system built on DOLDA's architecture.

mod demo;
#[path = "persistq.rs"]
mod persistq;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let demo_mode = args.contains(&"--demo".to_string());
    let perf_mode = args.contains(&"--perf".to_string());
    let large_mode = args.contains(&"--large".to_string());

    println!("🚀 persistQ - Distributed Persistent Queue using DOLDA");
    println!("======================================================");

    if large_mode {
        println!("Running large-scale test with 10M random numbers\n");
        run_large_scale_test().await?;
    } else if perf_mode {
        println!("Running performance testing mode\n");
        run_performance_tests().await?;
    } else if demo_mode {
        println!("Running in DOLDA demo mode (architecture concepts)\n");
        demo::demonstrate_architecture();
    } else {
        println!("Running persistQ distributed queue system\n");
        run_persistq_demo().await?;
    }

    println!("\n✨ persistQ testing completed successfully!");
    Ok(())
}

/// Run comprehensive performance tests
async fn run_performance_tests() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Running persistQ Performance Tests");
    println!("====================================");

    // Run the performance test suite
    persistq::perf::run_performance_tests().await?;

    // Run individual analyses
    println!("\n🔍 Detailed Performance Analysis");
    println!("================================");

    persistq::perf::analyze_memory_usage()?;
    persistq::perf::analyze_cpu_utilization()?;
    persistq::perf::analyze_io_patterns()?;

    Ok(())
}

/// Run large-scale test with 10 million random numbers
async fn run_large_scale_test() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔢 Running Large-Scale persistQ Test");
    println!("===================================");

    // Run the 10 million random numbers test
    let result = persistq::large_scale_test::test_10m_random_numbers().await?;

    // Print comprehensive results
    persistq::large_scale_test::print_large_scale_results(&result);

    // Verify repeated reads work
    println!("\n🔄 Testing Repeated Read Capability");
    println!("==================================");

    if result.verification_correct {
        println!("✅ persistQ successfully persisted 10,000,000 random numbers");
        println!("✅ All numbers were correctly retrieved and verified");
        println!("✅ Repeated reads (10 iterations) completed successfully");
        println!("✅ Data integrity maintained across multiple read operations");

        println!("\n📊 Key Metrics:");
        println!("   • Append Throughput: {:.0} ops/sec", result.append_throughput);
        println!("   • Verification Time: {}ms", result.verification_duration_ms);
        println!("   • Memory Usage: {:.1} MB", result.memory_usage_mb);
        println!("   • Average Read Time: {:.1}ms per iteration",
                result.read_durations.iter().sum::<u64>() as f64 / result.read_durations.len() as f64);

        println!("\n🎉 LARGE-SCALE TEST PASSED!");
        println!("   persistQ can handle 10M+ records with perfect persistence and repeated reads");
    } else {
        println!("❌ LARGE-SCALE TEST FAILED!");
        println!("   Data verification failed - persistence not reliable");
        return Err("Large-scale test verification failed".into());
    }

    Ok(())
}

/// Demonstrate the full persistQ system
async fn run_persistq_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("1. 🏗️  Initializing persistQ Distributed Queue System...");

    // Create configuration
    let config = persistq::PersistQConfig {
        storage_dir: "/tmp/persistq_demo".to_string(),
        segment_size: 1024 * 1024, // 1MB segments for demo
        num_partitions: 4,
        replication_factor: 2,
        cache_config: persistq::CacheConfig {
            enabled: true,
            size_bytes: 10 * 1024 * 1024, // 10MB cache
            hot_data_threshold: 0.2,
        },
    };

    // Create a demo cluster (simplified)
    let cluster = std::sync::Arc::new(persistq::Cluster {});

    // Initialize persistQ
    let persistq = persistq::PersistQ::new(config, cluster).await?;
    println!("   ✅ persistQ initialized with {} partitions", persistq.config.num_partitions);

    println!("\n2. 📝 Appending Records to Distributed Queue...");

    // Test records to append
    let test_records = vec![
        b"Hello, persistQ!".to_vec(),
        b"Distributed queues are powerful".to_vec(),
        b"DOLDA architecture rocks!".to_vec(),
        b"Zero-copy operations".to_vec(),
        b"High-performance storage".to_vec(),
        b"Raft-based replication".to_vec(),
        b"Segment-based persistence".to_vec(),
        b"Multi-level caching".to_vec(),
    ];

    let mut append_results = Vec::new();

    for (i, record) in test_records.iter().enumerate() {
        let result = persistq.append(record).await?;
        
        println!("   ✅ Record {}: Global Offset {}, Partition {}, Node {}, {}μs latency",
                i + 1, result.global_offset, result.partition_id,
                result.node_id, result.latency_us);
        
        append_results.push(result);
    }

    println!("\n3. 📖 Reading Records from Distributed Queue...");

    // Read back some records
    for (i, append_result) in append_results.iter().enumerate().step_by(2) {
        let read_result = persistq.read(append_result.global_offset).await?;
        let data_str = String::from_utf8_lossy(&read_result.record.data);

        println!("   📖 Record {}: '{}' ({}μs latency, cache_hit: {})",
                i + 1, data_str, read_result.latency_us, read_result.cache_hit);
    }

    println!("\n4. 📊 persistQ System Statistics...");

    let stats = persistq.stats();
    println!("   📊 Total Records: {}", stats.total_records);
    println!("   📊 Partitions: {}", stats.num_partitions);
    println!("   📊 Total Segments: {}", stats.total_segments);
    println!("   📊 Replication Factor: {}", stats.replication_factor);

    // Show partition distribution
    println!("   📊 Partition Distribution:");
    for segment_manager in persistq.segment_managers.values() {
        let segments = segment_manager.segments.read();
        println!("      Partition {}: {} segments",
                segment_manager.partition_id, segments.len());
    }

    println!("\n5. 🧹 Shutting Down persistQ System...");
    persistq.shutdown().await?;
    println!("   ✅ persistQ shutdown completed successfully");

    // Cleanup demo files
    std::fs::remove_dir_all("/tmp/persistq_demo").ok();

    Ok(())
}
