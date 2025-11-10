// Comprehensive benchmarks for production storage system
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use dolda::record::Record;
use dolda::storage::{RecordSegment, SegmentManager};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_record_append(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let test_dir = "/tmp/dolda_bench_append";
    std::fs::create_dir_all(test_dir).ok();
    
    let segment = rt.block_on(async {
        RecordSegment::new(
            format!("{}/bench.dat", test_dir),
            1024 * 1024 * 1024,  // 1GB
            false,
        ).await.unwrap()
    });
    
    let mut group = c.benchmark_group("record_append");
    
    for size in [64, 256, 1024, 4096, 16384].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let data = vec![0u8; size];
            b.iter(|| {
                let record = Record::new(data.clone()).unwrap();
                segment.append_record(&record).unwrap();
            });
        });
    }
    
    group.finish();
    std::fs::remove_dir_all(test_dir).ok();
}

fn bench_record_read(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let test_dir = "/tmp/dolda_bench_read";
    std::fs::create_dir_all(test_dir).ok();
    
    let segment = rt.block_on(async {
        RecordSegment::new(
            format!("{}/bench.dat", test_dir),
            1024 * 1024 * 1024,
            false,
        ).await.unwrap()
    });
    
    // Pre-populate with records
    let mut offsets = Vec::new();
    for size in [64, 256, 1024, 4096, 16384].iter() {
        let data = vec![0u8; *size];
        for _ in 0..100 {
            let record = Record::new(data.clone()).unwrap();
            let offset = segment.append_record(&record).unwrap();
            offsets.push(offset.offset);
        }
    }
    
    let mut group = c.benchmark_group("record_read");
    
    let mut idx = 0;
    group.bench_function("random_read", |b| {
        b.iter(|| {
            let offset = offsets[idx % offsets.len()];
            idx += 1;
            black_box(segment.read_record(offset).unwrap());
        });
    });
    
    group.finish();
    std::fs::remove_dir_all(test_dir).ok();
}

fn bench_concurrent_appends(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let test_dir = "/tmp/dolda_bench_concurrent";
    std::fs::create_dir_all(test_dir).ok();
    
    let segment = Arc::new(rt.block_on(async {
        RecordSegment::new(
            format!("{}/bench.dat", test_dir),
            1024 * 1024 * 1024,
            false,
        ).await.unwrap()
    }));
    
    let mut group = c.benchmark_group("concurrent_appends");
    
    for thread_count in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_count),
            thread_count,
            |b, &&thread_count| {
                b.iter(|| {
                    let handles: Vec<_> = (0..thread_count)
                        .map(|_| {
                            let seg = Arc::clone(&segment);
                            std::thread::spawn(move || {
                                let data = vec![0u8; 1024];
                                let record = Record::new(data).unwrap();
                                seg.append_record(&record).unwrap();
                            })
                        })
                        .collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }
    
    group.finish();
    std::fs::remove_dir_all(test_dir).ok();
}

fn bench_record_iteration(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let test_dir = "/tmp/dolda_bench_iter";
    std::fs::create_dir_all(test_dir).ok();
    
    let segment = rt.block_on(async {
        RecordSegment::new(
            format!("{}/bench.dat", test_dir),
            1024 * 1024 * 1024,
            false,
        ).await.unwrap()
    });
    
    // Add 10000 records
    let data = vec![0u8; 256];
    for _ in 0..10000 {
        let record = Record::new(data.clone()).unwrap();
        segment.append_record(&record).unwrap();
    }
    
    c.bench_function("iterate_10k_records", |b| {
        b.iter(|| {
            let count = segment.iter_records()
                .filter_map(|r| r.ok())
                .count();
            black_box(count);
        });
    });
    
    std::fs::remove_dir_all(test_dir).ok();
}

criterion_group!(
    benches,
    bench_record_append,
    bench_record_read,
    bench_concurrent_appends,
    bench_record_iteration
);
criterion_main!(benches);

