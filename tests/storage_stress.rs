//! Storage Engine Stress Tests
//!
//! These tests verify the production storage engine under extreme load.
//! Run with: cargo test --release --test storage_stress -- --nocapture

use dolda::storage::{RecordSegment, SegmentManager};
use dolda::record::Record;
use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{AtomicU64, Ordering};

const TEST_DIR: &str = "/tmp/dolda_storage_stress";

fn setup_test_dir(test_name: &str) -> String {
    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = format!("{}/{}_{}", TEST_DIR, test_name, test_id);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &str) {
    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn stress_sequential_writes() {
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║   STRESS TEST: Sequential Writes                  ║");
    println!("╚════════════════════════════════════════════════════╝");
    
    let test_dir = setup_test_dir("sequential");
    let segment = RecordSegment::new(
        format!("{}/test.dat", test_dir),
        100 * 1024 * 1024,  // 100MB
        false,  // No index for pure write performance
    ).await.unwrap();
    
    let record_count = 50_000;
    let record_size = 1024;
    
    println!("\nWriting {} records of {} bytes each...", record_count, record_size);
    
    let start = Instant::now();
    let mut latencies = Vec::with_capacity(record_count);
    
    for i in 0..record_count {
        let data = vec![(i % 256) as u8; record_size];
        let record = Record::new(data).unwrap();
        
        let write_start = Instant::now();
        segment.append_record(&record).unwrap();
        latencies.push(write_start.elapsed().as_micros() as u64);
        
        if (i + 1) % 10000 == 0 {
            print!("\r  Progress: {} / {} records", i + 1, record_count);
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
    }
    
    let duration = start.elapsed();
    
    // Calculate statistics
    latencies.sort();
    let avg_latency = latencies.iter().sum::<u64>() / latencies.len() as u64;
    let p50_latency = latencies[latencies.len() / 2];
    let p95_latency = latencies[(latencies.len() * 95) / 100];
    let p99_latency = latencies[(latencies.len() * 99) / 100];
    let throughput = record_count as f64 / duration.as_secs_f64();
    let total_mb = (record_count * record_size) as f64 / (1024.0 * 1024.0);
    let mb_per_sec = total_mb / duration.as_secs_f64();
    
    println!("\n\n╔════════════════════════════════════════════════════╗");
    println!("║                    RESULTS                         ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ Records written:    {:>12}                   ║", record_count);
    println!("║ Duration:           {:>12.2} seconds           ║", duration.as_secs_f64());
    println!("║ Throughput:         {:>12.0} ops/sec          ║", throughput);
    println!("║ Data written:       {:>12.2} MB               ║", total_mb);
    println!("║ Write speed:        {:>12.2} MB/s             ║", mb_per_sec);
    println!("║                                                    ║");
    println!("║ LATENCY                                            ║");
    println!("║ Average:            {:>12} μs                 ║", avg_latency);
    println!("║ Median (p50):       {:>12} μs                 ║", p50_latency);
    println!("║ p95:                {:>12} μs                 ║", p95_latency);
    println!("║ p99:                {:>12} μs                 ║", p99_latency);
    println!("╚════════════════════════════════════════════════════╝\n");
    
    // Verify reads
    println!("Verifying random reads...");
    let read_start = Instant::now();
    let mut iter = segment.iter_records();
    let mut count = 0;
    for _ in 0..100 {
        if let Some(Ok(_)) = iter.next() {
            count += 1;
        }
    }
    println!("✓ Verified {} records in {:.2}ms\n", count, read_start.elapsed().as_millis());
    
    cleanup(&test_dir);
    
    // Performance assertions
    assert!(throughput > 50_000.0, "Throughput too low: {:.0} ops/sec (expected > 50K)", throughput);
    assert!(avg_latency < 100, "Average latency too high: {} μs (expected < 100)", avg_latency);
    assert!(p99_latency < 500, "P99 latency too high: {} μs (expected < 500)", p99_latency);
}

#[tokio::test]
async fn stress_concurrent_writes() {
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║   STRESS TEST: Concurrent Writes                  ║");
    println!("╚════════════════════════════════════════════════════╝");
    
    let test_dir = setup_test_dir("concurrent");
    let segment = Arc::new(RecordSegment::new(
        format!("{}/test.dat", test_dir),
        200 * 1024 * 1024,  // 200MB
        false,
    ).await.unwrap());
    
    let num_threads = 16;
    let records_per_thread = 5_000;
    let record_size = 512;
    
    println!("\nSpawning {} writer threads...", num_threads);
    println!("Each thread writes {} records of {} bytes", records_per_thread, record_size);
    
    let start = Instant::now();
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let seg = Arc::clone(&segment);
        let success = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);
        
        let handle = std::thread::spawn(move || {
            let data = vec![(thread_id % 256) as u8; record_size];
            
            for _ in 0..records_per_thread {
                let record = Record::new(data.clone()).unwrap();
                
                match seg.append_record(&record) {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_writes = success_count.load(Ordering::Relaxed);
    let total_errors = error_count.load(Ordering::Relaxed);
    let throughput = total_writes as f64 / duration.as_secs_f64();
    let total_mb = (total_writes as usize * record_size) as f64 / (1024.0 * 1024.0);
    let mb_per_sec = total_mb / duration.as_secs_f64();
    
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║                    RESULTS                         ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ Threads:            {:>12}                   ║", num_threads);
    println!("║ Successful writes:  {:>12}                   ║", total_writes);
    println!("║ Failed writes:      {:>12}                   ║", total_errors);
    println!("║ Duration:           {:>12.2} seconds           ║", duration.as_secs_f64());
    println!("║ Throughput:         {:>12.0} ops/sec          ║", throughput);
    println!("║ Data written:       {:>12.2} MB               ║", total_mb);
    println!("║ Write speed:        {:>12.2} MB/s             ║", mb_per_sec);
    println!("╚════════════════════════════════════════════════════╝\n");
    
    cleanup(&test_dir);
    
    assert_eq!(total_errors, 0, "Should have no write errors");
    assert!(throughput > 100_000.0, "Concurrent throughput too low: {:.0} ops/sec (expected > 100K)", throughput);
}

#[tokio::test]
async fn stress_segment_manager() {
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║   STRESS TEST: Segment Manager & Rollover         ║");
    println!("╚════════════════════════════════════════════════════╝");
    
    let test_dir = setup_test_dir("manager");
    let manager = SegmentManager::new(
        1,  // Partition 1
        512 * 1024,  // Small 512KB segments to force rollovers
        test_dir.clone(),
        false,
    ).await.unwrap();
    
    let record_count = 10_000;
    let record_size = 1024;
    
    println!("\nWriting {} records to trigger multiple segment rollovers...", record_count);
    
    let start = Instant::now();
    let data = vec![0x42u8; record_size];
    
    for i in 0..record_count {
        let record = Record::new(data.clone()).unwrap();
        manager.append_record(&record).await.unwrap();
        
        if (i + 1) % 2000 == 0 {
            let stats = manager.stats();
            print!("\r  Progress: {} records, {} segments", i + 1, stats.segment_count);
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
    }
    
    manager.flush().await.unwrap();
    let duration = start.elapsed();
    
    let stats = manager.stats();
    let throughput = record_count as f64 / duration.as_secs_f64();
    let total_mb = (record_count * record_size) as f64 / (1024.0 * 1024.0);
    let expected_segments = (record_count * record_size) / (512 * 1024);
    
    println!("\n\n╔════════════════════════════════════════════════════╗");
    println!("║                    RESULTS                         ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ Records written:    {:>12}                   ║", stats.total_records);
    println!("║ Segments created:   {:>12}                   ║", stats.segment_count);
    println!("║ Expected segments:  {:>12}                   ║", expected_segments);
    println!("║ Duration:           {:>12.2} seconds           ║", duration.as_secs_f64());
    println!("║ Throughput:         {:>12.0} ops/sec          ║", throughput);
    println!("║ Data written:       {:>12.2} MB               ║", total_mb);
    println!("║ Avg utilization:    {:>12.1} %                ║", stats.avg_utilization);
    println!("╚════════════════════════════════════════════════════╝\n");
    
    cleanup(&test_dir);
    
    assert_eq!(stats.total_records, record_count as u64);
    assert!(stats.segment_count > 10, "Expected multiple segment rollovers");
    assert!(throughput > 20_000.0, "Throughput too low: {:.0} ops/sec", throughput);
}

#[tokio::test]
async fn stress_large_records() {
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║   STRESS TEST: Variable Record Sizes              ║");
    println!("╚════════════════════════════════════════════════════╝");
    
    let test_dir = setup_test_dir("large");
    let segment = RecordSegment::new(
        format!("{}/test.dat", test_dir),
        100 * 1024 * 1024,  // 100MB
        false,
    ).await.unwrap();
    
    let test_sizes = vec![
        (1024, 1000),           // 1KB x 1000
        (10 * 1024, 500),       // 10KB x 500
        (100 * 1024, 100),      // 100KB x 100
        (1024 * 1024, 20),      // 1MB x 20
    ];
    
    println!();
    for (size, count) in test_sizes {
        print!("Testing {:>6} bytes x {:>4} records... ", size, count);
        std::io::Write::flush(&mut std::io::stdout()).unwrap();
        
        let data = vec![0xAAu8; size];
        let start = Instant::now();
        
        for _ in 0..count {
            let record = Record::new(data.clone()).unwrap();
            segment.append_record(&record).unwrap();
        }
        
        let duration = start.elapsed();
        let total_mb = (size * count) as f64 / (1024.0 * 1024.0);
        let throughput_mb = total_mb / duration.as_secs_f64();
        
        println!("{:>6.2} MB/s", throughput_mb);
    }
    
    let stats = segment.stats();
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║ Total records:      {:>12}                   ║", stats.record_count);
    println!("║ Total bytes:        {:>12.2} MB               ║", stats.used_bytes as f64 / (1024.0 * 1024.0));
    println!("║ Utilization:        {:>12.1} %                ║", segment.utilization());
    println!("╚════════════════════════════════════════════════════╝\n");
    
    cleanup(&test_dir);
}

#[tokio::test]
async fn stress_iteration_performance() {
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║   STRESS TEST: Iteration Performance              ║");
    println!("╚════════════════════════════════════════════════════╝");
    
    let test_dir = setup_test_dir("iteration");
    let segment = RecordSegment::new(
        format!("{}/test.dat", test_dir),
        50 * 1024 * 1024,
        false,
    ).await.unwrap();
    
    let record_count = 10_000;
    let record_size = 256;
    
    println!("\nPreparing {} records...", record_count);
    let data = vec![0x55u8; record_size];
    for _ in 0..record_count {
        let record = Record::new(data.clone()).unwrap();
        segment.append_record(&record).unwrap();
    }
    
    println!("Iterating over all records...");
    let start = Instant::now();
    
    let mut count = 0;
    for result in segment.iter_records() {
        if result.is_ok() {
            count += 1;
        }
    }
    
    let duration = start.elapsed();
    let throughput = count as f64 / duration.as_secs_f64();
    
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║                    RESULTS                         ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ Records iterated:   {:>12}                   ║", count);
    println!("║ Duration:           {:>12.2} ms                ║", duration.as_millis());
    println!("║ Throughput:         {:>12.0} records/sec      ║", throughput);
    println!("╚════════════════════════════════════════════════════╝\n");
    
    cleanup(&test_dir);
    
    assert_eq!(count, record_count);
    assert!(throughput > 500_000.0, "Iteration too slow: {:.0} records/sec", throughput);
}

#[tokio::test]
#[ignore]  // Long-running test, run with --ignored
async fn stress_sustained_load() {
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║   STRESS TEST: Sustained Load (60 seconds)        ║");
    println!("╚════════════════════════════════════════════════════╝");
    
    let test_dir = setup_test_dir("sustained");
    let segment = Arc::new(RecordSegment::new(
        format!("{}/test.dat", test_dir),
        500 * 1024 * 1024,  // 500MB
        false,
    ).await.unwrap());
    
    let duration = std::time::Duration::from_secs(60);
    let num_threads = 8;
    let record_size = 1024;
    
    println!("\nRunning {} writer threads for {} seconds...", num_threads, duration.as_secs());
    
    let start = Instant::now();
    let stop_flag = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let seg = Arc::clone(&segment);
        let stop = Arc::clone(&stop_flag);
        let writes = Arc::clone(&total_writes);
        
        let handle = std::thread::spawn(move || {
            let data = vec![(thread_id % 256) as u8; record_size];
            
            while stop.load(Ordering::Relaxed) == 0 {
                let record = Record::new(data.clone()).unwrap();
                if seg.append_record(&record).is_ok() {
                    writes.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Run for specified duration
    std::thread::sleep(duration);
    stop_flag.store(1, Ordering::Relaxed);
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    let writes = total_writes.load(Ordering::Relaxed);
    let throughput = writes as f64 / elapsed.as_secs_f64();
    let total_mb = (writes as usize * record_size) as f64 / (1024.0 * 1024.0);
    
    println!("\n╔════════════════════════════════════════════════════╗");
    println!("║                    RESULTS                         ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ Duration:           {:>12.2} seconds           ║", elapsed.as_secs_f64());
    println!("║ Total writes:       {:>12}                   ║", writes);
    println!("║ Avg throughput:     {:>12.0} ops/sec          ║", throughput);
    println!("║ Total data:         {:>12.2} MB               ║", total_mb);
    println!("╚════════════════════════════════════════════════════╝\n");
    
    cleanup(&test_dir);
    
    assert!(throughput > 100_000.0, "Sustained throughput too low: {:.0} ops/sec", throughput);
}

