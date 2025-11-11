//! DataFusion SQL Layer Example
//!
//! This example demonstrates using SQL queries over DOLDA's persistent storage
//! via Apache Arrow DataFusion.
//!
//! Run with:
//! ```bash
//! cargo run --example datafusion_example --release
//! ```

use dolda::datafusion_layer::DataFusionLayer;
use dolda::storage::SegmentManager;
use dolda::record::Record;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🦀 DOLDA DataFusion SQL Layer Example\n");
    println!("{}", "=".repeat(60));

    // 1. Create storage manager
    println!("\n📁 Creating storage manager...");
    let temp_dir = tempfile::tempdir()?;
    let data_path = temp_dir.path().to_str().unwrap().to_string();
    
    let mut storage = SegmentManager::new(
        1,                      // Partition 1
        64 * 1024 * 1024,       // 64MB segments
        data_path,
        false,                  // No B-tree index (for simplicity)
    ).await?;
    
    println!("✅ Storage created at: {}", temp_dir.path().display());

    // 2. Insert sample data
    println!("\n📝 Inserting sample event data...");
    let events = vec![
        ("user_login", "alice"),
        ("page_view", "alice"),
        ("user_login", "bob"),
        ("purchase", "alice"),
        ("page_view", "bob"),
        ("user_login", "charlie"),
        ("purchase", "bob"),
        ("page_view", "charlie"),
        ("purchase", "alice"),
        ("user_login", "alice"),
    ];

    for (event_type, user) in &events {
        let data = format!("{{\"event\":\"{}\",\"user\":\"{}\"}}", event_type, user);
        let record = Record::new(data.into_bytes())?;
        storage.append_record(&record).await?;
    }
    
    let stats = storage.stats();
    println!("✅ Inserted {} events", stats.total_records);

    // 3. Create DataFusion layer
    println!("\n🔍 Creating DataFusion SQL layer...");
    let df_layer = DataFusionLayer::new(storage).await?;
    
    // Register the events table
    df_layer.register_table("events", "data", "timestamp").await?;
    
    println!("✅ Registered 'events' table");
    println!("   Tables: {:?}", df_layer.list_tables());

    // 4. Run SQL queries
    println!("\n{}", "=".repeat(60));
    println!("SQL QUERIES");
    println!("{}", "=".repeat(60));

    // Query 1: Count all events
    println!("\n📊 Query 1: Total event count");
    println!("   SQL: SELECT COUNT(*) as total FROM events");
    let results = df_layer.query("SELECT COUNT(*) as total FROM events").await?;
    print_results(&results);

    // Query 2: Count by event type (note: data is JSON string)
    println!("\n📊 Query 2: Event types");
    println!("   SQL: SELECT data, COUNT(*) as count FROM events GROUP BY data LIMIT 5");
    let results = df_layer.query(
        "SELECT data, COUNT(*) as count FROM events GROUP BY data LIMIT 5"
    ).await?;
    print_results(&results);

    // Query 3: Recent events
    println!("\n📊 Query 3: All events");
    println!("   SQL: SELECT record_id, data FROM events");
    let results = df_layer.query("SELECT record_id, data FROM events").await?;
    print_results(&results);

    // 5. Export to Parquet
    println!("\n💾 Exporting query results to Parquet...");
    let parquet_path = temp_dir.path().join("events.parquet");
    df_layer.export_to_parquet(
        "SELECT * FROM events",
        parquet_path.to_str().unwrap()
    ).await?;
    println!("✅ Exported to: {}", parquet_path.display());

    // 6. Read from Parquet
    println!("\n📂 Reading from Parquet file...");
    df_layer.register_parquet("events_parquet", parquet_path.to_str().unwrap()).await?;
    let results = df_layer.query("SELECT COUNT(*) FROM events_parquet").await?;
    print_results(&results);

    // 7. Get schema information
    println!("\n📋 Table schema:");
    let schema = df_layer.get_table_schema("events").await?;
    println!("   Fields:");
    for field in schema.fields() {
        println!("     - {} ({})", field.name(), field.data_type());
    }

    println!("\n{}", "=".repeat(60));
    println!("✨ Example complete!");
    println!("{}", "=".repeat(60));
    
    Ok(())
}

fn print_results(batches: &[datafusion::arrow::array::RecordBatch]) {
    use datafusion::arrow::util::pretty::print_batches;
    
    if batches.is_empty() {
        println!("   (no results)");
        return;
    }
    
    match print_batches(batches) {
        Ok(_) => {},
        Err(e) => println!("   Error displaying results: {}", e),
    }
}

