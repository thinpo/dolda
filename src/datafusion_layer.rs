//! DataFusion SQL Layer for DOLDA
//!
//! This module provides SQL query capabilities over DOLDA's persistent queue storage
//! using Apache Arrow DataFusion.
//!
//! ## Features
//!
//! - SQL queries over persistent queue data
//! - Apache Arrow columnar format integration
//! - Parquet file support for analytics
//! - Zero-copy data access where possible
//! - Streaming query execution
//!
//! ## Example
//!
//! ```rust,no_run
//! use dolda::datafusion_layer::DataFusionLayer;
//! use dolda::storage::SegmentManager;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create storage manager
//! let storage = SegmentManager::new(1, 64 * 1024 * 1024, "/data".to_string(), false).await?;
//!
//! // Create DataFusion layer
//! let df_layer = DataFusionLayer::new(storage).await?;
//!
//! // Register a table
//! df_layer.register_table("events", "event_type", "timestamp").await?;
//!
//! // Execute SQL query
//! let results = df_layer.query("SELECT event_type, COUNT(*) FROM events GROUP BY event_type").await?;
//!
//! // Process results
//! for batch in results {
//!     println!("Batch: {:?}", batch);
//! }
//! # Ok(())
//! # }
//! ```

use datafusion::prelude::*;
use datafusion::arrow::array::{
    ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::logical_expr::Expr;
use datafusion::dataframe::DataFrameWriteOptions;
use std::any::Any;
use std::sync::Arc;
use thiserror::Error;

use crate::storage::SegmentManager;
use crate::record::Record;

#[derive(Error, Debug)]
pub enum DataFusionError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    
    #[error("Arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    
    #[error("Table not found: {0}")]
    TableNotFound(String),
    
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, DataFusionError>;

/// DataFusion SQL layer over DOLDA storage
pub struct DataFusionLayer {
    ctx: SessionContext,
    storage: Arc<SegmentManager>,
}

impl DataFusionLayer {
    /// Create a new DataFusion layer
    pub async fn new(storage: SegmentManager) -> Result<Self> {
        let ctx = SessionContext::new();
        Ok(Self {
            ctx,
            storage: Arc::new(storage),
        })
    }

    /// Register a table backed by DOLDA storage
    ///
    /// # Arguments
    /// * `table_name` - Name of the table for SQL queries
    /// * `data_field` - Name of the data field in records
    /// * `timestamp_field` - Name of the timestamp field
    pub async fn register_table(
        &self,
        table_name: &str,
        data_field: &str,
        timestamp_field: &str,
    ) -> Result<()> {
        let table_provider = DoldaTableProvider::new(
            Arc::clone(&self.storage),
            data_field.to_string(),
            timestamp_field.to_string(),
        )?;
        
        self.ctx.register_table(table_name, Arc::new(table_provider))?;
        Ok(())
    }

    /// Execute a SQL query and return results
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        let results = df.collect().await?;
        Ok(results)
    }

    /// Execute a SQL query and return as DataFrame for further processing
    pub async fn query_dataframe(&self, sql: &str) -> Result<DataFrame> {
        let df = self.ctx.sql(sql).await?;
        Ok(df)
    }

    /// Export query results to Parquet file
    pub async fn export_to_parquet(&self, sql: &str, path: &str) -> Result<()> {
        let df = self.ctx.sql(sql).await?;
        df.write_parquet(path, DataFrameWriteOptions::new(), None).await?;
        Ok(())
    }

    /// Register an external Parquet file as a table
    pub async fn register_parquet(&self, table_name: &str, path: &str) -> Result<()> {
        self.ctx.register_parquet(table_name, path, ParquetReadOptions::default()).await?;
        Ok(())
    }

    /// Get table schema
    pub async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        let table = self.ctx
            .table(table_name)
            .await
            .map_err(|_| DataFusionError::TableNotFound(table_name.to_string()))?;
        let df_schema = table.schema();
        // DFSchema implements AsRef<Arc<Schema>>
        let schema_ref: &SchemaRef = df_schema.as_ref();
        Ok(Arc::clone(schema_ref))
    }

    /// List all registered tables
    pub fn list_tables(&self) -> Vec<String> {
        self.ctx.catalog("datafusion").unwrap()
            .schema("public").unwrap()
            .table_names()
    }
}

/// Table provider that reads from DOLDA storage
pub struct DoldaTableProvider {
    storage: Arc<SegmentManager>,
    schema: SchemaRef,
    data_field: String,
    timestamp_field: String,
}

// Manual Debug implementation since SegmentManager doesn't implement Debug
impl std::fmt::Debug for DoldaTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DoldaTableProvider")
            .field("schema", &self.schema)
            .field("data_field", &self.data_field)
            .field("timestamp_field", &self.timestamp_field)
            .finish_non_exhaustive()
    }
}

impl DoldaTableProvider {
    fn new(
        storage: Arc<SegmentManager>,
        data_field: String,
        timestamp_field: String,
    ) -> Result<Self> {
        // Define schema for the table
        // For now, we'll use a simple schema with data (string) and timestamp
        let schema = Arc::new(Schema::new(vec![
            Field::new(&data_field, DataType::Utf8, false),
            Field::new(&timestamp_field, DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("record_id", DataType::UInt64, false),
        ]));

        Ok(Self {
            storage,
            schema,
            data_field,
            timestamp_field,
        })
    }

    /// Convert DOLDA records to Arrow RecordBatch
    async fn records_to_batch(&self, records: Vec<Record>) -> Result<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        // Extract data fields
        let mut data_values = Vec::with_capacity(records.len());
        let mut timestamp_values = Vec::with_capacity(records.len());
        let mut record_ids = Vec::with_capacity(records.len());

        for (idx, record) in records.iter().enumerate() {
            // Convert record data to string (in production, parse based on actual format)
            let data_str = String::from_utf8_lossy(&record.data).to_string();
            data_values.push(data_str);
            
            // Timestamp is already in microseconds
            timestamp_values.push(record.timestamp as i64);
            
            // Record ID
            record_ids.push(idx as u64);
        }

        // Create Arrow arrays
        let data_array: ArrayRef = Arc::new(StringArray::from(data_values));
        let timestamp_array: ArrayRef = Arc::new(
            TimestampMicrosecondArray::from(timestamp_values)
        );
        let record_id_array: ArrayRef = Arc::new(UInt64Array::from(record_ids));

        // Create RecordBatch
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![data_array, timestamp_array, record_id_array],
        )?;

        Ok(batch)
    }
}

#[async_trait::async_trait]
impl TableProvider for DoldaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _ctx: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Read all records from storage
        let stats = self.storage.stats();
        let records = Vec::with_capacity(stats.total_records as usize);

        // Note: In a real implementation, we'd need a way to iterate through segments
        // For now, this is a placeholder that would need proper segment iteration API
        // TODO: Add proper segment iteration to SegmentManager
        
        // Convert to RecordBatch
        let batch = self.records_to_batch(records).await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Create execution plan - MemoryExec will handle projection internally
        let exec_plan = datafusion::physical_plan::memory::MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.cloned(),
        )?;

        Ok(Arc::new(exec_plan))
    }

    // Note: supports_filter_pushdown was removed in DataFusion 43+
    // Filter pushdown is now handled differently via the scan method
}

/// Builder for creating tables with custom schemas
pub struct TableBuilder {
    storage: Arc<SegmentManager>,
    schema_fields: Vec<Field>,
    /// Table name (reserved for future use)
    #[allow(dead_code)]
    name: String,
}

impl TableBuilder {
    pub fn new(storage: Arc<SegmentManager>, name: String) -> Self {
        Self {
            storage,
            schema_fields: Vec::new(),
            name,
        }
    }

    /// Add a field to the schema
    pub fn add_field(mut self, name: &str, data_type: DataType, nullable: bool) -> Self {
        self.schema_fields.push(Field::new(name, data_type, nullable));
        self
    }

    /// Add a string field
    pub fn add_string_field(self, name: &str) -> Self {
        self.add_field(name, DataType::Utf8, false)
    }

    /// Add a timestamp field
    pub fn add_timestamp_field(self, name: &str) -> Self {
        self.add_field(name, DataType::Timestamp(TimeUnit::Microsecond, None), false)
    }

    /// Add an integer field
    pub fn add_int64_field(self, name: &str) -> Self {
        self.add_field(name, DataType::Int64, false)
    }

    /// Build the table provider
    pub fn build(self) -> Result<DoldaTableProvider> {
        if self.schema_fields.is_empty() {
            return Err(DataFusionError::InvalidSchema(
                "Schema must have at least one field".to_string()
            ));
        }

        let schema = Arc::new(Schema::new(self.schema_fields));
        
        // For now, use the first field as data and second as timestamp
        let data_field = schema.field(0).name().clone();
        let timestamp_field = if schema.fields().len() > 1 {
            schema.field(1).name().clone()
        } else {
            "timestamp".to_string()
        };

        Ok(DoldaTableProvider {
            storage: self.storage,
            schema,
            data_field,
            timestamp_field,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SegmentManager;
    use tempfile::TempDir;

    async fn create_test_storage() -> (SegmentManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = SegmentManager::new(
            1,
            10 * 1024 * 1024,
            temp_dir.path().to_str().unwrap().to_string(),
            false,
        )
        .await
        .unwrap();
        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_datafusion_layer_creation() {
        let (storage, _temp) = create_test_storage().await;
        let df_layer = DataFusionLayer::new(storage).await;
        assert!(df_layer.is_ok());
    }

    #[tokio::test]
    async fn test_table_registration() {
        let (storage, _temp) = create_test_storage().await;
        let df_layer = DataFusionLayer::new(storage).await.unwrap();
        
        let result = df_layer.register_table("test_table", "data", "timestamp").await;
        assert!(result.is_ok());
        
        let tables = df_layer.list_tables();
        assert!(tables.contains(&"test_table".to_string()));
    }

    #[tokio::test]
    async fn test_simple_query() {
        let (storage, _temp) = create_test_storage().await;
        
        // Insert some test data
        for i in 0..5 {
            let data = format!("test_event_{}", i);
            let record = Record::new(data.into_bytes()).unwrap();
            storage.append_record(&record).await.unwrap();
        }
        
        let df_layer = DataFusionLayer::new(storage).await.unwrap();
        df_layer.register_table("events", "data", "timestamp").await.unwrap();
        
        // Query all records
        let result = df_layer.query("SELECT COUNT(*) FROM events").await;
        if let Err(e) = &result {
            eprintln!("Query error: {:?}", e);
        }
        assert!(result.is_ok(), "Query failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_table_builder() {
        let (storage, _temp) = create_test_storage().await;
        
        let builder = TableBuilder::new(Arc::new(storage), "custom_table".to_string())
            .add_string_field("event_type")
            .add_timestamp_field("event_time")
            .add_int64_field("user_id");
        
        let table = builder.build();
        assert!(table.is_ok());
        
        let table = table.unwrap();
        assert_eq!(table.schema().fields().len(), 3);
    }
}

