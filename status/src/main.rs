use anyhow::Result;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePool, sqlite::SqliteConnectOptions, Row};
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use std::fs::File;
use std::sync::Arc;
use clap::{Parser, Subcommand};
use askama::Template;

const API_URL: &str = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status";

#[derive(Debug, Deserialize, Serialize)]
struct StationStatusResponse {
    last_updated: i64,
    ttl: i32,
    data: StationData,
}

#[derive(Debug, Deserialize, Serialize)]
struct StationData {
    stations: Vec<Station>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Station {
    station_id: String,
    num_bikes_available: i32,
    num_bikes_available_types: BikeTypes,
    num_bikes_disabled: i32,
    num_docks_available: i32,
    num_docks_disabled: i32,
    last_reported: Option<i64>,
    is_charging_station: bool,
    status: String,
    is_installed: i32,
    is_renting: i32,
    is_returning: i32,
    traffic: Option<i32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BikeTypes {
    mechanical: i32,
    ebike: i32,
}

#[derive(Debug, Serialize)]
struct StationStatusRecord {
    timestamp: i64,
    station_id: String,
    num_bikes_available: i32,
    num_bikes_available_mechanical: i32,
    num_bikes_available_ebike: i32,
    num_bikes_disabled: i32,
    num_docks_available: i32,
    num_docks_disabled: i32,
    last_reported: Option<i64>,
    is_charging_station: bool,
    status: String,
    is_installed: bool,
    is_renting: bool,
    is_returning: bool,
    traffic: Option<i32>,
}

#[derive(Parser)]
#[command(name = "bixi-status")]
#[command(about = "Bixi status collector and converter")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Collect,
    ToParquet { output: String },
    Graph {
        station_id: String,
        parquet_file: String,
        output: String,
        #[arg(long, help = "Number of hours to show (default: 24)")]
        hours: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Collect => collect_data().await,
        Commands::ToParquet { output } => convert_to_parquet(&output).await,
        Commands::Graph { station_id, parquet_file, output, hours } => {
            generate_station_graph(&station_id, &parquet_file, &output, hours.unwrap_or(24)).await
        }
    }
}

async fn collect_data() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("Starting Bixi status collector");

    // Check if current directory is writable
    let current_dir = std::env::current_dir()
        .map_err(|e| anyhow::anyhow!("Failed to get current directory: {}", e))?;
    info!("Current directory: {}", current_dir.display());
    
    // Test if we can create a file in the current directory
    let test_file = current_dir.join("test_write.tmp");
    std::fs::write(&test_file, "test")
        .map_err(|e| anyhow::anyhow!("Cannot write to current directory: {}", e))?;
    std::fs::remove_file(&test_file)
        .map_err(|e| anyhow::anyhow!("Cannot remove test file: {}", e))?;
    info!("Directory is writable");

    // Create database connection with absolute path
    let db_path = current_dir.join("bixi_status.db");
    let db_url = format!("sqlite:{}", db_path.display());
    info!("Connecting to database: {}", db_url);
    
    // Ensure the database file can be created
    if !db_path.exists() {
        info!("Creating new database file: {}", db_path.display());
    }
    
    let options = SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true);
    
    let pool = SqlitePool::connect_with(options).await
        .map_err(|e| {
            error!("Failed to connect to database: {}", e);
            error!("Database path: {}", db_path.display());
            e
        })?;
    info!("Successfully connected to database");
    
    // Initialize database schema
    init_database(&pool).await?;

    // Create HTTP client
    let client = reqwest::Client::new();

    // Main collection loop
    let mut next_interval = Duration::from_secs(1); // Start with 1 second, will be updated from TTL
    
    loop {
        // Wait for the appropriate interval
        time::sleep(next_interval).await;
        
        match collect_and_store_data(&client, &pool).await {
            Ok((count, ttl)) => {
                info!("Successfully stored {} station records", count);
                // Update interval based on TTL, with a minimum of 1 second
                next_interval = Duration::from_secs(ttl.max(1));
                info!("Next collection in {} seconds", next_interval.as_secs());
            }
            Err(e) => {
                error!("Failed to collect data: {}", e);
                // On error, wait a bit before retrying
                next_interval = Duration::from_secs(5);
                info!("Retrying in {} seconds", next_interval.as_secs());
            }
        }
    }
}

async fn init_database(pool: &SqlitePool) -> Result<()> {
    info!("Initializing database schema");
    
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS station_status_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            last_updated INTEGER NOT NULL,
            station_id TEXT NOT NULL,
            num_bikes_available INTEGER NOT NULL,
            num_bikes_available_mechanical INTEGER NOT NULL,
            num_bikes_available_ebike INTEGER NOT NULL,
            num_bikes_disabled INTEGER NOT NULL,
            num_docks_available INTEGER NOT NULL,
            num_docks_disabled INTEGER NOT NULL,
            last_reported INTEGER,
            is_charging_station BOOLEAN NOT NULL,
            status TEXT NOT NULL,
            is_installed BOOLEAN NOT NULL,
            is_renting BOOLEAN NOT NULL,
            is_returning BOOLEAN NOT NULL,
            traffic INTEGER
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create index for efficient time-series queries
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_timestamp_station ON station_status_history (last_updated, station_id)"
    )
    .execute(pool)
    .await?;

    info!("Database schema initialized");
    Ok(())
}

async fn collect_and_store_data(client: &reqwest::Client, pool: &SqlitePool) -> Result<(usize, u64)> {
    // Fetch data from API
    let response = client.get(API_URL).send().await?;
    
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("API request failed with status: {}", response.status()));
    }

    let station_data: StationStatusResponse = response.json().await?;
    let last_updated = station_data.last_updated;

    info!("Fetched data for {} stations at {} (TTL: {} seconds)",
          station_data.data.stations.len(),
          last_updated,
          station_data.ttl);

    // Convert and store each station record
    let mut stored_count = 0;

    for station in station_data.data.stations {
        let record = StationStatusRecord {
            timestamp: last_updated,
            station_id: station.station_id,
            num_bikes_available: station.num_bikes_available,
            num_bikes_available_mechanical: station.num_bikes_available_types.mechanical,
            num_bikes_available_ebike: station.num_bikes_available_types.ebike,
            num_bikes_disabled: station.num_bikes_disabled,
            num_docks_available: station.num_docks_available,
            num_docks_disabled: station.num_docks_disabled,
            last_reported: station.last_reported,
            is_charging_station: station.is_charging_station,
            status: station.status,
            is_installed: station.is_installed != 0,
            is_renting: station.is_renting != 0,
            is_returning: station.is_returning != 0,
            traffic: station.traffic,
        };

        match store_station_record(pool, &record).await {
            Ok(_) => stored_count += 1,
            Err(e) => {
                warn!("Failed to store record for station {}: {}", record.station_id, e);
            }
        }
    }

    Ok((stored_count, station_data.ttl as u64))
}

async fn store_station_record(pool: &SqlitePool, record: &StationStatusRecord) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO station_status_history (
            last_updated, station_id, num_bikes_available, num_bikes_available_mechanical,
            num_bikes_available_ebike, num_bikes_disabled, num_docks_available,
            num_docks_disabled, last_reported, is_charging_station, status,
            is_installed, is_renting, is_returning, traffic
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(record.timestamp)
    .bind(&record.station_id)
    .bind(record.num_bikes_available)
    .bind(record.num_bikes_available_mechanical)
    .bind(record.num_bikes_available_ebike)
    .bind(record.num_bikes_disabled)
    .bind(record.num_docks_available)
    .bind(record.num_docks_disabled)
    .bind(record.last_reported)
    .bind(record.is_charging_station)
    .bind(&record.status)
    .bind(record.is_installed)
    .bind(record.is_renting)
    .bind(record.is_returning)
    .bind(record.traffic)
    .execute(pool)
    .await?;

    Ok(())
}

async fn convert_to_parquet(output_path: &str) -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("Converting SQLite database to Parquet format with station columns");

    let current_dir = std::env::current_dir()?;
    let db_path = current_dir.join("bixi_status.db");

    let options = SqliteConnectOptions::new().filename(&db_path);
    let pool = SqlitePool::connect_with(options).await?;

    // Get all unique station IDs
    let station_rows = sqlx::query("SELECT DISTINCT station_id FROM station_status_history ORDER BY station_id")
        .fetch_all(&pool)
        .await?;

    let station_ids: Vec<String> = station_rows
        .iter()
        .map(|row| row.get::<String, _>("station_id"))
        .collect();

    info!("Found {} unique stations", station_ids.len());

    let schema = create_station_columns_schema(&station_ids);
    let output_file = File::create(output_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(output_file, schema.clone(), Some(props))?;

    // Get all data ordered by timestamp
    let rows = sqlx::query(
        "SELECT last_updated, station_id, num_bikes_available, num_bikes_available_mechanical, num_bikes_available_ebike,
                num_bikes_disabled, num_docks_available, num_docks_disabled, last_reported,
                is_charging_station, status, is_installed, is_renting, is_returning, traffic
         FROM station_status_history ORDER BY last_updated, station_id"
    )
    .fetch_all(&pool)
    .await?;

    info!("Processing {} total records", rows.len());

    // Process in batches of timestamps
    const BATCH_SIZE: usize = 1000;
    let mut current_batch_timestamps = Vec::new();
    let mut current_batch_rows = Vec::new();
    let mut last_timestamp: Option<i64> = None;
    let mut processed_timestamps = 0;

    for row in rows.iter() {
        let timestamp: i64 = row.get("last_updated");

        // If timestamp changed and we have enough timestamps in batch, write it
        if let Some(last_ts) = last_timestamp {
            if timestamp != last_ts {
                current_batch_timestamps.push(last_ts);

                if current_batch_timestamps.len() >= BATCH_SIZE {
                    let batch = rows_to_record_batch(&current_batch_rows, &station_ids, &schema)?;
                    writer.write(&batch)?;
                    processed_timestamps += current_batch_timestamps.len();
                    info!("Processed {} timestamps", processed_timestamps);
                    current_batch_timestamps.clear();
                    current_batch_rows.clear();
                }
            }
        }

        current_batch_rows.push(row);
        last_timestamp = Some(timestamp);
    }

    // Write remaining batch
    if !current_batch_rows.is_empty() {
        let batch = rows_to_record_batch(&current_batch_rows, &station_ids, &schema)?;
        writer.write(&batch)?;
        processed_timestamps += current_batch_timestamps.len() + 1;
        info!("Processed {} timestamps (final batch)", processed_timestamps);
    }

    writer.close()?;
    info!("Successfully converted database to Parquet: {}", output_path);

    Ok(())
}

fn create_station_columns_schema(station_ids: &[String]) -> Arc<Schema> {
    let mut fields = vec![Field::new("last_updated", DataType::Int64, false)];

    // Add columns for each station's metrics
    for station_id in station_ids {
        let prefix = format!("station_{}", station_id);

        fields.push(Field::new(&format!("{}_bikes_available", prefix), DataType::Int32, true));
        fields.push(Field::new(&format!("{}_bikes_mechanical", prefix), DataType::Int32, true));
        fields.push(Field::new(&format!("{}_bikes_ebike", prefix), DataType::Int32, true));
        fields.push(Field::new(&format!("{}_bikes_disabled", prefix), DataType::Int32, true));
        fields.push(Field::new(&format!("{}_docks_available", prefix), DataType::Int32, true));
        fields.push(Field::new(&format!("{}_docks_disabled", prefix), DataType::Int32, true));
        fields.push(Field::new(&format!("{}_last_reported", prefix), DataType::Int64, true));
        fields.push(Field::new(&format!("{}_is_charging", prefix), DataType::Boolean, true));
        fields.push(Field::new(&format!("{}_status", prefix), DataType::Utf8, true));
        fields.push(Field::new(&format!("{}_is_installed", prefix), DataType::Boolean, true));
        fields.push(Field::new(&format!("{}_is_renting", prefix), DataType::Boolean, true));
        fields.push(Field::new(&format!("{}_is_returning", prefix), DataType::Boolean, true));
        fields.push(Field::new(&format!("{}_traffic", prefix), DataType::Int32, true));
    }

    Arc::new(Schema::new(fields))
}

#[derive(Debug, Default)]
struct StationMetrics {
    bikes_available: Option<i32>,
    bikes_mechanical: Option<i32>,
    bikes_ebike: Option<i32>,
    bikes_disabled: Option<i32>,
    docks_available: Option<i32>,
    docks_disabled: Option<i32>,
    last_reported: Option<i64>,
    is_charging: Option<bool>,
    status: Option<String>,
    is_installed: Option<bool>,
    is_renting: Option<bool>,
    is_returning: Option<bool>,
    traffic: Option<i32>,
}

fn rows_to_record_batch(
    rows: &[&sqlx::sqlite::SqliteRow],
    station_ids: &[String],
    schema: &Arc<Schema>,
) -> Result<RecordBatch> {
    // Group rows by timestamp: timestamp -> (station_id -> StationMetrics)
    let mut timestamp_data: std::collections::BTreeMap<i64, std::collections::HashMap<String, StationMetrics>> =
        std::collections::BTreeMap::new();

    for row in rows {
        let timestamp: i64 = row.get("last_updated");
        let station_id: String = row.get("station_id");

        let data = StationMetrics {
            bikes_available: Some(row.get("num_bikes_available")),
            bikes_mechanical: Some(row.get("num_bikes_available_mechanical")),
            bikes_ebike: Some(row.get("num_bikes_available_ebike")),
            bikes_disabled: Some(row.get("num_bikes_disabled")),
            docks_available: Some(row.get("num_docks_available")),
            docks_disabled: Some(row.get("num_docks_disabled")),
            last_reported: row.get("last_reported"),
            is_charging: Some(row.get("is_charging_station")),
            status: Some(row.get("status")),
            is_installed: Some(row.get("is_installed")),
            is_renting: Some(row.get("is_renting")),
            is_returning: Some(row.get("is_returning")),
            traffic: row.get("traffic"),
        };

        timestamp_data.entry(timestamp)
            .or_insert_with(std::collections::HashMap::new)
            .insert(station_id, data);
    }

    let num_rows = timestamp_data.len();

    // Create arrays for the record batch
    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Timestamp column
    let timestamps: Vec<i64> = timestamp_data.keys().copied().collect();
    arrays.push(Arc::new(Int64Array::from(timestamps.clone())));

    // Station columns - 13 columns per station
    let default_data = StationMetrics::default();

    for station_id in station_ids {
        let mut bikes_available = Vec::with_capacity(num_rows);
        let mut bikes_mechanical = Vec::with_capacity(num_rows);
        let mut bikes_ebike = Vec::with_capacity(num_rows);
        let mut bikes_disabled = Vec::with_capacity(num_rows);
        let mut docks_available = Vec::with_capacity(num_rows);
        let mut docks_disabled = Vec::with_capacity(num_rows);
        let mut last_reported = Vec::with_capacity(num_rows);
        let mut is_charging = Vec::with_capacity(num_rows);
        let mut status = Vec::with_capacity(num_rows);
        let mut is_installed = Vec::with_capacity(num_rows);
        let mut is_renting = Vec::with_capacity(num_rows);
        let mut is_returning = Vec::with_capacity(num_rows);
        let mut traffic = Vec::with_capacity(num_rows);

        for timestamp in &timestamps {
            let data = timestamp_data
                .get(timestamp)
                .and_then(|map| map.get(station_id))
                .unwrap_or(&default_data);

            bikes_available.push(data.bikes_available);
            bikes_mechanical.push(data.bikes_mechanical);
            bikes_ebike.push(data.bikes_ebike);
            bikes_disabled.push(data.bikes_disabled);
            docks_available.push(data.docks_available);
            docks_disabled.push(data.docks_disabled);
            last_reported.push(data.last_reported);
            is_charging.push(data.is_charging);
            status.push(data.status.as_deref());
            is_installed.push(data.is_installed);
            is_renting.push(data.is_renting);
            is_returning.push(data.is_returning);
            traffic.push(data.traffic);
        }

        arrays.push(Arc::new(Int32Array::from(bikes_available)));
        arrays.push(Arc::new(Int32Array::from(bikes_mechanical)));
        arrays.push(Arc::new(Int32Array::from(bikes_ebike)));
        arrays.push(Arc::new(Int32Array::from(bikes_disabled)));
        arrays.push(Arc::new(Int32Array::from(docks_available)));
        arrays.push(Arc::new(Int32Array::from(docks_disabled)));
        arrays.push(Arc::new(Int64Array::from(last_reported)));
        arrays.push(Arc::new(BooleanArray::from(is_charging)));
        arrays.push(Arc::new(StringArray::from(status)));
        arrays.push(Arc::new(BooleanArray::from(is_installed)));
        arrays.push(Arc::new(BooleanArray::from(is_renting)));
        arrays.push(Arc::new(BooleanArray::from(is_returning)));
        arrays.push(Arc::new(Int32Array::from(traffic)));
    }

    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

#[derive(Template)]
#[template(path = "station_graph.html")]
struct StationGraphTemplate {
    station_id: String,
    data_points: String,
    hours: u32,
}

#[derive(Debug)]
struct DataPoint {
    timestamp: String,
    bikes_available: i32,
}

async fn generate_station_graph(station_id: &str, parquet_file: &str, output_path: &str, hours: u32) -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("Generating graph for station {} from Parquet file {}", station_id, parquet_file);

    // Open the Parquet file
    let file = File::open(parquet_file)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let station_column = format!("station_{}_bikes_available", station_id);

    let mut data_points = Vec::new();
    let cutoff_time = chrono::Utc::now() - chrono::Duration::hours(hours as i64);

    // Read all batches and extract data for the specific station
    for batch_result in reader {
        let batch = batch_result?;

        // Find timestamp and station column indices
        let schema = batch.schema();
        let timestamp_idx = schema.index_of("last_updated")?;
        let station_idx = match schema.index_of(&station_column) {
            Ok(idx) => idx,
            Err(_) => return Err(anyhow::anyhow!("Station {} not found in Parquet file", station_id)),
        };

        let timestamp_array = batch.column(timestamp_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Timestamp column is not an int64 array"))?;

        let bikes_array = batch.column(station_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("Station column is not an int32 array"))?;

        // Process each row in the batch
        for row_idx in 0..batch.num_rows() {
            if !bikes_array.is_null(row_idx) {
                let timestamp_secs = timestamp_array.value(row_idx);
                if let Some(timestamp_dt) = DateTime::from_timestamp(timestamp_secs, 0) {
                    let bikes_count = bikes_array.value(row_idx);

                    // Filter by time window
                    if timestamp_dt >= cutoff_time {
                        data_points.push(DataPoint {
                            timestamp: timestamp_dt.to_rfc3339(),
                            bikes_available: bikes_count,
                        });
                    }
                }
            }
        }
    }

    if data_points.is_empty() {
        return Err(anyhow::anyhow!("No data found for station {} in the last {} hours", station_id, hours));
    }

    // Sort by timestamp
    data_points.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    info!("Found {} data points for station {}", data_points.len(), station_id);

    // Convert data points to JavaScript array format
    let js_data_points = data_points
        .iter()
        .map(|dp| format!("['{}', {}]", dp.timestamp, dp.bikes_available))
        .collect::<Vec<_>>()
        .join(",\n        ");

    let template = StationGraphTemplate {
        station_id: station_id.to_string(),
        data_points: js_data_points,
        hours,
    };

    let html_content = template.render()?;
    std::fs::write(output_path, html_content)?;

    info!("Graph generated successfully: {}", output_path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_station_status_deserialization() {
        let json = r#"{
            "last_updated": 1758848344,
            "ttl": 1,
            "data": {
                "stations": [{
                    "station_id": "7000",
                    "num_bikes_available": 42,
                    "num_bikes_available_types": {
                        "mechanical": 42,
                        "ebike": 0
                    },
                    "num_bikes_disabled": 2,
                    "num_docks_available": 1,
                    "num_docks_disabled": 2,
                    "last_reported": 1758848244,
                    "is_charging_station": false,
                    "status": "IN_SERVICE",
                    "is_installed": 1,
                    "is_renting": 1,
                    "is_returning": 1,
                    "traffic": null
                }]
            }
        }"#;

        let response: StationStatusResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.data.stations.len(), 1);
        assert_eq!(response.data.stations[0].station_id, "7000");
        assert_eq!(response.data.stations[0].num_bikes_available, 42);
    }
}
