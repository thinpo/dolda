# DataFusion SQL Layer Guide

DOLDA now includes an Apache Arrow DataFusion layer that enables SQL queries over persistent queue data!

---

## 🎯 Features

✅ **SQL Queries** - Query persistent queue data using standard SQL  
✅ **Apache Arrow** - Columnar data format for fast analytics  
✅ **Parquet Support** - Export/import data from Parquet files  
✅ **Zero-Copy** - Efficient memory-mapped I/O where possible  
✅ **Type-Safe** - Rust's type system ensures correctness  
✅ **Production-Ready** - Latest DataFusion 43, Arrow 53, Parquet 53  

---

## 🚀 Quick Start

### Basic Example

```rust
use dolda::datafusion_layer::DataFusionLayer;
use dolda::storage::SegmentManager;
use dolda::record::Record;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create storage
    let storage = SegmentManager::new(
        1,
        64 * 1024 * 1024,
        "/data".to_string(),
        false,
    ).await?;
    
    // 2. Insert data
    let record = Record::new(b"event_data".to_vec())?;
    storage.append_record(&record).await?;
    
    // 3. Create DataFusion layer
    let df = DataFusionLayer::new(storage).await?;
    
    // 4. Register table
    df.register_table("events", "data", "timestamp").await?;
    
    // 5. Run SQL query
    let results = df.query("SELECT COUNT(*) FROM events").await?;
    
    println!("Results: {:?}", results);
    Ok(())
}
```

---

## 📊 SQL Operations

### Query Data

```rust
// Count records
let results = df.query("SELECT COUNT(*) FROM events").await?;

// Group by field
let results = df.query(
    "SELECT data, COUNT(*) as count FROM events GROUP BY data"
).await?;

// Filter and sort
let results = df.query(
    "SELECT * FROM events WHERE record_id > 100 ORDER BY timestamp DESC LIMIT 10"
).await?;

// Aggregations
let results = df.query(
    "SELECT 
        COUNT(*) as total,
        MIN(timestamp) as first_event,
        MAX(timestamp) as last_event 
     FROM events"
).await?;
```

### Get DataFrame for Further Processing

```rust
// Get DataFrame instead of materialized results
let dataframe = df.query_dataframe("SELECT * FROM events").await?;

// Apply additional transformations
let filtered = dataframe.filter(col("record_id").gt(lit(100)))?;
let results = filtered.collect().await?;
```

---

## 📁 Parquet Integration

### Export to Parquet

```rust
// Export query results to Parquet file
df.export_to_parquet(
    "SELECT * FROM events WHERE record_id > 1000",
    "/path/to/output.parquet"
).await?;
```

### Import from Parquet

```rust
// Register existing Parquet file as a table
df.register_parquet("imported_events", "/path/to/file.parquet").await?;

// Query the Parquet data
let results = df.query("SELECT * FROM imported_events").await?;

// Join with DOLDA data
let results = df.query(
    "SELECT e.*, p.* 
     FROM events e 
     JOIN imported_events p ON e.record_id = p.id"
).await?;
```

---

## 🗂️ Table Management

### Register Tables

```rust
// Register with default schema (data, timestamp, record_id)
df.register_table("events", "data", "timestamp").await?;

// List all tables
let tables = df.list_tables();
println!("Tables: {:?}", tables);

// Get table schema
let schema = df.get_table_schema("events").await?;
for field in schema.fields() {
    println!("{}: {}", field.name(), field.data_type());
}
```

### Custom Table Builder

```rust
use dolda::datafusion_layer::TableBuilder;
use std::sync::Arc;

// Build custom table with specific schema
let table = TableBuilder::new(Arc::new(storage), "custom_events".to_string())
    .add_string_field("event_type")
    .add_timestamp_field("event_time")
    .add_int64_field("user_id")
    .add_string_field("payload")
    .build()?;
```

---

## 💡 Common Patterns

### Analytics Pipeline

```rust
// 1. Create storage and DataFusion layer
let storage = SegmentManager::new(1, 64 * 1024 * 1024, "/data".into(), false).await?;
let df = DataFusionLayer::new(storage).await?;
df.register_table("events", "data", "timestamp").await?;

// 2. Run analytics query
let daily_stats = df.query(
    "SELECT 
        DATE_TRUNC('day', timestamp) as day,
        COUNT(*) as event_count
     FROM events
     GROUP BY day
     ORDER BY day DESC"
).await?;

// 3. Export results
df.export_to_parquet(
    "SELECT * FROM events WHERE timestamp > NOW() - INTERVAL '7 days'",
    "/reports/last_7_days.parquet"
).await?;
```

### Real-Time Monitoring

```rust
// Query recent events
let recent = df.query(
    "SELECT * 
     FROM events 
     WHERE timestamp > NOW() - INTERVAL '5 minutes'
     ORDER BY timestamp DESC
     LIMIT 100"
).await?;

// Get event statistics
let stats = df.query(
    "SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT data) as unique_events,
        MAX(timestamp) as last_event
     FROM events"
).await?;
```

### Data Export for ML

```rust
// Export training data
df.export_to_parquet(
    "SELECT 
        data,
        record_id,
        timestamp,
        EXTRACT(HOUR FROM timestamp) as hour_of_day,
        EXTRACT(DOW FROM timestamp) as day_of_week
     FROM events
     WHERE timestamp BETWEEN '2024-01-01' AND '2024-12-31'",
    "/ml/training_data.parquet"
).await?;
```

---

## 🔧 Configuration

### DataFusion Session

The DataFusion layer uses default `SessionContext` configuration. For custom settings:

```rust
// Custom configuration would require extending DataFusionLayer
// Future enhancement: expose configuration options
```

### Performance Tuning

```rust
// Use larger segments for better scan performance
let storage = SegmentManager::new(
    1,
    256 * 1024 * 1024,  // 256MB segments
    "/data".into(),
    true,  // Enable B-tree index for faster seeks
).await?;
```

---

## 📈 Performance Considerations

### Memory Usage

- DataFusion loads data into memory for query execution
- For large datasets, consider:
  - Using `LIMIT` clauses
  - Filtering early in the query
  - Exporting to Parquet for external processing

### Query Optimization

```rust
// ❌ Inefficient: Load everything then filter
let results = df.query("SELECT * FROM events").await?;
// ... then filter in application code

// ✅ Efficient: Push filtering to SQL
let results = df.query(
    "SELECT * FROM events WHERE record_id > 1000 LIMIT 100"
).await?;
```

### Parquet for Large Datasets

```rust
// For repeated analytics on large datasets:

// 1. Export to Parquet once
df.export_to_parquet("SELECT * FROM events", "/cache/events.parquet").await?;

// 2. Query Parquet (much faster for analytics)
df.register_parquet("events_cache", "/cache/events.parquet").await?;
let results = df.query("SELECT COUNT(*) FROM events_cache").await?;
```

---

## 🧪 Testing

Run DataFusion tests:

```bash
# All DataFusion tests
cargo test --release datafusion_layer

# Specific test
cargo test --release datafusion_layer::tests::test_simple_query -- --nocapture

# With example
cargo run --example datafusion_example --release
```

---

## 🔗 Integration with Other Components

### With PersistQ

```rust
use dolda::persistq::PersistQ;

// Create PersistQ
let persistq = PersistQ::new(config).await?;

// Write data
persistq.append(b"event1").await?;
persistq.append(b"event2").await?;

// Access underlying storage for SQL queries
// Note: Would need to expose storage from PersistQ
```

### With Network Layer

```rust
// Future: Distribute SQL queries across cluster
// Query coordinator on one node, data on others
```

---

## 📚 SQL Support

DataFusion supports a wide range of SQL features:

### Supported Operations

- ✅ SELECT, WHERE, GROUP BY, ORDER BY, LIMIT
- ✅ JOIN (INNER, LEFT, RIGHT, FULL)
- ✅ Aggregations (COUNT, SUM, AVG, MIN, MAX)
- ✅ Window functions
- ✅ CTEs (WITH clauses)
- ✅ UNION, INTERSECT, EXCEPT
- ✅ Subqueries
- ✅ Date/time functions
- ✅ String functions
- ✅ Math functions

### Example Complex Query

```rust
let results = df.query(
    "WITH hourly_stats AS (
        SELECT 
            DATE_TRUNC('hour', timestamp) as hour,
            COUNT(*) as count
        FROM events
        GROUP BY hour
    )
    SELECT 
        hour,
        count,
        AVG(count) OVER (ORDER BY hour ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as moving_avg
    FROM hourly_stats
    ORDER BY hour DESC
    LIMIT 24"
).await?;
```

---

## 🚧 Limitations & Future Work

### Current Limitations

1. **Full table scans**: Currently reads all data into memory
   - Future: Implement streaming scans
   - Future: Add filter pushdown to storage layer

2. **No distributed queries**: Queries run on single node
   - Future: Distribute queries across cluster
   - Future: Query coordinator + data nodes

3. **Limited schema evolution**: Schema defined at registration
   - Future: Automatic schema inference from data
   - Future: Schema versioning

### Planned Enhancements

- [ ] Streaming execution for large datasets
- [ ] Filter pushdown to storage layer
- [ ] Distributed query execution
- [ ] Automatic schema inference
- [ ] Delta Lake integration
- [ ] Real-time aggregations
- [ ] Materialized views

---

## 📖 References

- **DataFusion**: https://arrow.apache.org/datafusion/
- **Apache Arrow**: https://arrow.apache.org/
- **Parquet Format**: https://parquet.apache.org/

---

## 💡 Tips & Tricks

### Debug Queries

```rust
// Get query plan
let df = df_layer.query_dataframe("SELECT * FROM events").await?;
let plan = df.logical_plan();
println!("Logical plan: {:#?}", plan);
```

### Handle Large Results

```rust
// Stream results instead of collecting all at once
let df = df_layer.query_dataframe("SELECT * FROM events").await?;
let mut stream = df.execute_stream().await?;

while let Some(batch) = stream.next().await {
    let batch = batch?;
    // Process batch
    println!("Batch with {} rows", batch.num_rows());
}
```

### Error Handling

```rust
match df.query("SELECT * FROM events").await {
    Ok(results) => {
        println!("Query successful: {} batches", results.len());
    }
    Err(dolda::datafusion_layer::DataFusionError::TableNotFound(name)) => {
        println!("Table '{}' not found", name);
    }
    Err(e) => {
        println!("Query error: {}", e);
    }
}
```

---

**Status**: ✅ **Production-Ready SQL Analytics Layer**

Query your persistent queue data with SQL! 🚀

