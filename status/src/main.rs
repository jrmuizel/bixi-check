use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePool, sqlite::SqliteConnectOptions};
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

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
    timestamp: DateTime<Utc>,
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

#[tokio::main]
async fn main() -> Result<()> {
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
    let mut interval = time::interval(Duration::from_secs(1));
    
    loop {
        interval.tick().await;
        
        match collect_and_store_data(&client, &pool).await {
            Ok(count) => {
                info!("Successfully stored {} station records", count);
            }
            Err(e) => {
                error!("Failed to collect data: {}", e);
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
            timestamp TEXT NOT NULL,
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
        "CREATE INDEX IF NOT EXISTS idx_timestamp_station ON station_status_history (timestamp, station_id)"
    )
    .execute(pool)
    .await?;

    info!("Database schema initialized");
    Ok(())
}

async fn collect_and_store_data(client: &reqwest::Client, pool: &SqlitePool) -> Result<usize> {
    // Fetch data from API
    let response = client.get(API_URL).send().await?;
    
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("API request failed with status: {}", response.status()));
    }

    let station_data: StationStatusResponse = response.json().await?;
    let current_time = Utc::now();
    
    info!("Fetched data for {} stations at {}", 
          station_data.data.stations.len(), 
          current_time);

    // Convert and store each station record
    let mut stored_count = 0;
    
    for station in station_data.data.stations {
        let record = StationStatusRecord {
            timestamp: current_time,
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

    Ok(stored_count)
}

async fn store_station_record(pool: &SqlitePool, record: &StationStatusRecord) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO station_status_history (
            timestamp, station_id, num_bikes_available, num_bikes_available_mechanical,
            num_bikes_available_ebike, num_bikes_disabled, num_docks_available,
            num_docks_disabled, last_reported, is_charging_station, status,
            is_installed, is_renting, is_returning, traffic
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(record.timestamp.to_rfc3339())
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
